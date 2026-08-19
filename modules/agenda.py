from __future__ import annotations

from datetime import date, datetime, timedelta

from flask import Blueprint, flash, jsonify, redirect, render_template, request, session, url_for

from database import execute_db, insert_db, now_iso, query_db
from services.agenda_service import (
    HORAS_AGENDA,
    SERVICOS_AGENDA,
    STATUS_AGENDA,
    conflito_horario,
    intervalo_visualizacao,
    listar_agendamentos,
    listar_bloqueios,
    listar_espera,
    montar_calendario,
    navegar_referencia,
    ocupacao_profissionais,
    resumo_agenda,
    obter_configuracao_capacidade,
    ocupacao_horario,
    pode_fazer_encaixe,
    mapa_ocupacao,
)

from services.appointment_grooming_sync import sincronizar_agendamento_com_banho
from modules.comunicacao import preparar_whatsapp_agendamento

agenda_bp = Blueprint("agenda", __name__)


STATUS_RAPIDOS = {
    "Agendado": ("Agendado", None),
    "Confirmado": ("Confirmado", None),
    "Na loja": ("Na loja", "checkin_at"),
    "Em atendimento": ("Em atendimento", None),
    "Pronto": ("Pronto", "ready_at"),
    "Entregue": ("Entregue", "delivered_at"),
    "Cancelado": ("Cancelado", None),
}


def _valor_decimal(valor):
    texto = (valor or "0").strip().replace("R$", "").replace(" ", "")
    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    try:
        return float(texto or 0)
    except (TypeError, ValueError):
        return 0.0



def _duracao_valida(valor):
    try:
        duracao = int(valor or 60)
    except (TypeError, ValueError):
        return 60
    return max(15, min(duracao, 720))


def _validar_capacidade(data_agendamento, hora, ignorar_id=None):
    cfg = obter_configuracao_capacidade()
    capacidade = max(1, int(cfg.get("default_capacity") or 3))
    ocupados = ocupacao_horario(data_agendamento, hora, ignorar_id)
    lotado = ocupados >= capacidade
    solicitou_encaixe = bool(request.form.get("capacity_override"))
    autorizado = pode_fazer_encaixe(session.get("role"))
    if lotado and not (solicitou_encaixe and autorizado):
        return False, ocupados, capacidade, False
    return True, ocupados, capacidade, bool(lotado and solicitou_encaixe and autorizado)


def _pet_pertence_ao_cliente(pet_id, client_id):
    if not pet_id or not client_id:
        return False
    return bool(query_db(
        "SELECT id FROM pets WHERE id = ? AND client_id = ? AND COALESCE(ativo,1)=1",
        (pet_id, client_id),
        one=True,
    ))



def _marcar_conflitos_visuais(agendamentos):
    """Marca conflitos reais por profissional para destacar na agenda.

    ``query_db`` retorna ``sqlite3.Row`` no SQLite. Esse tipo permite leitura
    por chave, mas não possui ``get`` e também não aceita atribuição. Por isso,
    normalizamos cada registro para ``dict`` antes de acrescentar o campo
    calculado ``tem_conflito``. A mesma rotina continua compatível com os
    dicionários retornados pelo PostgreSQL.
    """
    registros = [dict(item) for item in (agendamentos or [])]
    grupos = {}

    for item in registros:
        item["tem_conflito"] = False
        try:
            data = str(item.get("data_agendamento") or "")
            profissional = str(item.get("employee_id") or "sem-profissional")
            horario = str(item.get("horario") or "00:00")[:5]
            duracao = int(item.get("duration_minutes") or 60)

            chave = (data, profissional)
            inicio = datetime.strptime(f"{data} {horario}", "%Y-%m-%d %H:%M")
            fim = inicio + timedelta(minutes=duracao)
        except (TypeError, ValueError):
            continue

        grupos.setdefault(chave, []).append((inicio, fim, item))

    for eventos in grupos.values():
        eventos.sort(key=lambda evento: evento[0])
        for indice, (inicio, fim, item) in enumerate(eventos):
            for outro_inicio, outro_fim, outro in eventos[indice + 1:]:
                if outro_inicio >= fim:
                    break
                if inicio < outro_fim and outro_inicio < fim:
                    item["tem_conflito"] = True
                    outro["tem_conflito"] = True

    return registros


def _dados_base():
    return {
        "clientes": query_db("SELECT id, nome FROM clients WHERE COALESCE(ativo,1)=1 ORDER BY nome"),
        "pets": query_db(
            """
            SELECT p.id, p.nome, p.client_id, c.nome AS cliente_nome
            FROM pets p
            LEFT JOIN clients c ON c.id = p.client_id
            WHERE COALESCE(p.ativo,1)=1
            ORDER BY p.nome
            """
        ),
        "funcionarios": query_db("SELECT id, name FROM employees WHERE active = 1 ORDER BY name"),
    }


@agenda_bp.route("/agenda", methods=["GET", "POST"])
def agenda():
    if request.method == "POST":
        client_id = request.form.get("client_id", "").strip()
        pet_id = request.form.get("pet_id", "").strip()
        data_agendamento = request.form.get("data", "").strip()
        hora = request.form.get("hora", "").strip()
        servico = request.form.get("servico", "").strip()
        employee_id = request.form.get("employee_id", "").strip() or None
        status = request.form.get("status", "Agendado").strip()
        duracao = _duracao_valida(request.form.get("duration_minutes", "60"))
        transporte = 1 if request.form.get("transport_required") else 0
        observacoes = request.form.get("observacoes", "").strip()
        valor = _valor_decimal(request.form.get("valor"))

        if not client_id or not pet_id or not data_agendamento or not hora or not servico:
            flash("Preencha tutor, pet, data, hora e serviço.", "danger")
            return redirect(url_for("agenda.agenda"))

        if not _pet_pertence_ao_cliente(pet_id, client_id):
            flash("O pet selecionado não pertence ao tutor informado.", "danger")
            return redirect(url_for("agenda.agenda"))

        if conflito_horario(data_agendamento, hora, duracao, employee_id):
            flash("O horário conflita com outro atendimento do profissional ou bloqueio.", "danger")
            return redirect(url_for("agenda.agenda", referencia=data_agendamento, modo="dia"))

        permitido, ocupados, capacidade, encaixe = _validar_capacidade(data_agendamento, hora)
        if not permitido:
            flash(f"Horário lotado: {ocupados}/{capacidade} pets. Marque 'Confirmar encaixe' para ultrapassar a capacidade.", "danger")
            return redirect(url_for("agenda.agenda", referencia=data_agendamento, modo="dia"))

        agendamento_id = insert_db(
            """
            INSERT INTO appointments
            (client_id, pet_id, employee_id, data_agendamento, horario, duration_minutes,
             servico, valor, status, transport_required, observacoes, capacity_override,
             capacity_override_by, capacity_override_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                client_id, pet_id, employee_id, data_agendamento, hora, duracao, servico, valor,
                status, transporte, observacoes, 1 if encaixe else 0,
                session.get("user_name") if encaixe else None, now_iso() if encaixe else None,
                now_iso(), now_iso(),
            ),
        )
        sincronizar_agendamento_com_banho(agendamento_id)
        flash("Agendamento criado como encaixe acima da capacidade." if encaixe else "Agendamento criado com sucesso.", "warning" if encaixe else "success")
        return redirect(url_for("agenda.agenda", referencia=data_agendamento, modo="dia"))

    modo, referencia, inicio, fim = intervalo_visualizacao(
        request.args.get("modo", "semana"), request.args.get("referencia")
    )
    status = request.args.get("status", "").strip()
    busca = request.args.get("busca", "").strip()
    employee_id = request.args.get("employee_id", "").strip()

    agendamentos = _marcar_conflitos_visuais(listar_agendamentos(inicio, fim, status, busca, employee_id))
    bloqueios = listar_bloqueios(inicio, fim)
    calendario = montar_calendario(modo, inicio, fim, agendamentos, bloqueios)
    base = _dados_base()

    return render_template(
        "agenda.html",
        **base,
        agendamentos=agendamentos,
        bloqueios=bloqueios,
        espera=listar_espera(),
        calendario=calendario,
        ocupacao=ocupacao_profissionais(inicio, fim),
        resumo=resumo_agenda(inicio, fim),
        status_lista=STATUS_AGENDA,
        servicos=SERVICOS_AGENDA,
        horas=HORAS_AGENDA,
        modo=modo,
        referencia=referencia.isoformat(),
        inicio=inicio,
        fim=fim,
        anterior=navegar_referencia(modo, referencia, -1).isoformat(),
        proximo=navegar_referencia(modo, referencia, 1).isoformat(),
        status_filtro=status,
        funcionario_filtro=employee_id,
        busca=busca,
        capacidade_config=obter_configuracao_capacidade(),
        ocupacao_slots=mapa_ocupacao(agendamentos),
        pode_encaixar=pode_fazer_encaixe(session.get("role")),
        prefill_client_id=request.args.get("client_id", ""),
        prefill_pet_id=request.args.get("pet_id", ""),
    )


@agenda_bp.route("/agenda/<int:agendamento_id>/editar", methods=["GET", "POST"])
def editar_agendamento(agendamento_id):
    agendamento = query_db("SELECT * FROM appointments WHERE id = ?", (agendamento_id,), one=True)
    if not agendamento:
        flash("Agendamento não encontrado.", "danger")
        return redirect(url_for("agenda.agenda"))

    if request.method == "POST":
        client_id = request.form.get("client_id", "").strip()
        pet_id = request.form.get("pet_id", "").strip()
        data_agendamento = request.form.get("data", "").strip()
        hora = request.form.get("hora", "").strip()
        duracao = _duracao_valida(request.form.get("duration_minutes"))
        employee_id = request.form.get("employee_id") or None
        status = request.form.get("status", "Agendado").strip()

        if not client_id or not pet_id or not data_agendamento or not hora:
            flash("Preencha tutor, pet, data e hora.", "danger")
            return redirect(url_for("agenda.editar_agendamento", agendamento_id=agendamento_id))
        if not _pet_pertence_ao_cliente(pet_id, client_id):
            flash("O pet selecionado não pertence ao tutor informado.", "danger")
            return redirect(url_for("agenda.editar_agendamento", agendamento_id=agendamento_id))
        if status not in STATUS_AGENDA:
            status = "Agendado"
        if conflito_horario(data_agendamento, hora, duracao, employee_id, agendamento_id):
            flash("O horário conflita com outro atendimento do profissional ou bloqueio.", "danger")
            return redirect(url_for("agenda.editar_agendamento", agendamento_id=agendamento_id))
        permitido, ocupados, capacidade, encaixe = _validar_capacidade(data_agendamento, hora, agendamento_id)
        if not permitido:
            flash(f"Horário lotado: {ocupados}/{capacidade} pets. Confirme o encaixe para salvar.", "danger")
            return redirect(url_for("agenda.editar_agendamento", agendamento_id=agendamento_id))

        execute_db(
            """
            UPDATE appointments SET client_id=?, pet_id=?, employee_id=?, data_agendamento=?,
                horario=?, duration_minutes=?, servico=?, valor=?, status=?,
                transport_required=?, observacoes=?, capacity_override=?, capacity_override_by=?,
                capacity_override_at=?, updated_at=?
            WHERE id=?
            """,
            (
                client_id,
                pet_id,
                employee_id,
                data_agendamento,
                hora,
                duracao,
                request.form.get("servico"),
                _valor_decimal(request.form.get("valor")),
                status,
                1 if request.form.get("transport_required") else 0,
                request.form.get("observacoes", "").strip(),
                1 if encaixe else 0,
                session.get("user_name") if encaixe else None,
                now_iso() if encaixe else None,
                now_iso(),
                agendamento_id,
            ),
        )
        sincronizar_agendamento_com_banho(agendamento_id)
        flash("Agendamento atualizado.", "success")
        return redirect(url_for("agenda.agenda", referencia=data_agendamento, modo="dia"))

    return render_template(
        "agenda_editar.html",
        agendamento=agendamento,
        **_dados_base(),
        status_lista=STATUS_AGENDA,
        servicos=SERVICOS_AGENDA,
    )


@agenda_bp.route("/agenda/api/<int:agendamento_id>/mover", methods=["POST"])
def mover_agendamento(agendamento_id):
    payload = request.get_json(silent=True) or {}
    nova_data = str(payload.get("date") or "").strip()
    nova_hora = str(payload.get("time") or "").strip()
    employee_id = payload.get("employee_id") or None

    agendamento = query_db("SELECT * FROM appointments WHERE id = ?", (agendamento_id,), one=True)
    if not agendamento or not nova_data or not nova_hora:
        return jsonify({"ok": False, "message": "Dados inválidos."}), 400

    employee_id = employee_id if employee_id is not None else agendamento["employee_id"]
    if conflito_horario(
        nova_data,
        nova_hora,
        agendamento["duration_minutes"] or 60,
        employee_id,
        agendamento_id,
    ):
        return jsonify({"ok": False, "message": "Conflito com profissional ou bloqueio."}), 409

    cfg = obter_configuracao_capacidade()
    capacidade = max(1, int(cfg.get("default_capacity") or 3))
    ocupados = ocupacao_horario(nova_data, nova_hora, agendamento_id)
    force = bool(payload.get("force_capacity_override"))
    autorizado = pode_fazer_encaixe(session.get("role"))
    if ocupados >= capacidade and not (force and autorizado):
        return jsonify({
            "ok": False,
            "requires_override": autorizado,
            "message": f"Horário lotado: {ocupados}/{capacidade} pets.",
        }), 409
    encaixe = ocupados >= capacidade and force and autorizado

    execute_db(
        """
        UPDATE appointments
        SET data_agendamento = ?, horario = ?, employee_id = ?, status = ?,
            capacity_override=?, capacity_override_by=?, capacity_override_at=?, updated_at = ?
        WHERE id = ?
        """,
        (nova_data, nova_hora, employee_id, "Reagendado", 1 if encaixe else 0,
         session.get("user_name") if encaixe else None, now_iso() if encaixe else None,
         now_iso(), agendamento_id),
    )
    sincronizar_agendamento_com_banho(agendamento_id)
    return jsonify({"ok": True, "message": "Agendamento reagendado."})



@agenda_bp.route("/agenda/api/disponibilidade")
def disponibilidade_agenda():
    """Sugere os próximos horários livres para o formulário de agendamento."""
    data_ref = (request.args.get("date") or date.today().isoformat()).strip()
    employee_id = (request.args.get("employee_id") or "").strip() or None
    try:
        duracao = _duracao_valida(request.args.get("duration", 60))
        dia = datetime.strptime(data_ref, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"ok": False, "message": "Data inválida."}), 400

    cfg = obter_configuracao_capacidade()
    capacidade = max(1, int(cfg.get("default_capacity") or 3))
    sugestoes = []
    for hora in HORAS_AGENDA:
        if len(sugestoes) >= 8:
            break
        if dia == date.today():
            try:
                instante = datetime.combine(dia, datetime.strptime(hora, "%H:%M").time())
                if instante < datetime.now() + timedelta(minutes=15):
                    continue
            except ValueError:
                pass
        if conflito_horario(data_ref, hora, duracao, employee_id):
            continue
        ocupados = ocupacao_horario(data_ref, hora)
        if ocupados >= capacidade:
            continue
        sugestoes.append({
            "time": hora,
            "label": f"{hora} · {capacidade - ocupados} vaga(s)",
            "occupied": ocupados,
            "capacity": capacidade,
        })
    return jsonify({"ok": True, "date": data_ref, "suggestions": sugestoes})


@agenda_bp.route("/agenda/<int:agendamento_id>/checkin-inteligente", methods=["POST"])
def checkin_inteligente(agendamento_id):
    """Registra a chegada e prepara o atendimento no Kanban sem recarregar a agenda."""
    agendamento = query_db("SELECT * FROM appointments WHERE id=?", (agendamento_id,), one=True)
    wants_json = request.headers.get("X-Requested-With") == "XMLHttpRequest" or request.accept_mimetypes.best == "application/json"

    if not agendamento:
        if wants_json:
            return jsonify({"ok": False, "message": "Agendamento não encontrado."}), 404
        flash("Agendamento não encontrado.", "danger")
        return redirect(url_for("agenda.agenda"))

    if (agendamento["status"] or "") in ("Cancelado", "Recusado", "Entregue", "Finalizado"):
        if wants_json:
            return jsonify({"ok": False, "message": "Esse agendamento não aceita check-in."}), 400
        flash("Esse agendamento não aceita check-in.", "danger")
        return redirect(request.referrer or url_for("agenda.agenda"))

    instante = now_iso()
    execute_db(
        "UPDATE appointments SET status='Na loja', checkin_at=COALESCE(checkin_at,?), updated_at=? WHERE id=?",
        (instante, instante, agendamento_id),
    )

    grooming_id = sincronizar_agendamento_com_banho(agendamento_id)
    if grooming_id:
        execute_db(
            """UPDATE grooming_services
               SET status='Agendado', checked_in_at=COALESCE(checked_in_at,?),
                   started_at=COALESCE(started_at,?), updated_at=?
             WHERE id=?""",
            (instante, instante, instante, grooming_id),
        )
        checklist = query_db("SELECT id FROM grooming_checklists WHERE grooming_id=?", (grooming_id,), one=True)
        if checklist:
            execute_db(
                "UPDATE grooming_checklists SET checked_in_at=COALESCE(checked_in_at,?), updated_at=? WHERE grooming_id=?",
                (instante, instante, grooming_id),
            )
        else:
            execute_db(
                "INSERT INTO grooming_checklists (grooming_id,checked_in_at,updated_at) VALUES (?,?,?)",
                (grooming_id, instante, instante),
            )
        from services.historico_service import registrar_historico
        registrar_historico(grooming_id, "Check-in inteligente realizado", session.get("user_name") or "Recepção")

    if wants_json:
        return jsonify({
            "ok": True,
            "status": "Na loja",
            "checkin_at": instante,
            "grooming_id": grooming_id,
            "message": f"Check-in de {agendamento['pet_id']} registrado com sucesso.",
        })

    flash("Check-in registrado e pet enviado para a Recepção.", "success")
    return redirect(request.referrer or url_for("agenda.agenda"))


@agenda_bp.route("/agenda/<int:agendamento_id>/status-rapido", methods=["POST"])
def atualizar_status_rapido(agendamento_id):
    status = (request.form.get("status") or "").strip()
    if status not in STATUS_RAPIDOS:
        flash("Status inválido.", "danger")
        return redirect(request.referrer or url_for("agenda.agenda"))

    agendamento = query_db("SELECT id, status FROM appointments WHERE id=?", (agendamento_id,), one=True)
    if not agendamento:
        flash("Agendamento não encontrado.", "danger")
        return redirect(url_for("agenda.agenda"))

    novo_status, coluna_data = STATUS_RAPIDOS[status]
    if coluna_data:
        execute_db(
            f"UPDATE appointments SET status=?, {coluna_data}=?, updated_at=? WHERE id=?",
            (novo_status, now_iso(), now_iso(), agendamento_id),
        )
    else:
        execute_db(
            "UPDATE appointments SET status=?, updated_at=? WHERE id=?",
            (novo_status, now_iso(), agendamento_id),
        )
    sincronizar_agendamento_com_banho(agendamento_id)
    flash(f"Status atualizado para {novo_status}.", "success")
    return redirect(request.referrer or url_for("agenda.agenda"))


@agenda_bp.route("/agenda/<int:agendamento_id>/status", methods=["POST"])
def atualizar_status(agendamento_id):
    status = request.form.get("status", "Agendado")
    execute_db(
        "UPDATE appointments SET status=?, updated_at=? WHERE id=?",
        (status, now_iso(), agendamento_id),
    )
    sincronizar_agendamento_com_banho(agendamento_id)
    flash(f"Status atualizado para {status}.", "success")
    return redirect(request.referrer or url_for("agenda.agenda"))


@agenda_bp.route("/agenda/<int:agendamento_id>/iniciar", methods=["POST"])
def iniciar_atendimento(agendamento_id):
    agendamento = query_db("SELECT * FROM appointments WHERE id=?", (agendamento_id,), one=True)
    if not agendamento:
        flash("Agendamento não encontrado.", "danger")
        return redirect(url_for("agenda.agenda"))

    existente = query_db(
        "SELECT id FROM grooming_services WHERE appointment_id=?",
        (agendamento_id,),
        one=True,
    )
    if existente:
        flash("Esse agendamento já possui atendimento vinculado.", "info")
        return redirect(url_for("banho_tosa.banho_tosa"))

    insert_db(
        """
        INSERT INTO grooming_services
        (appointment_id, client_id, pet_id, employee_id, data, hora_entrada, servico,
         valor, status, observacoes, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            agendamento_id,
            agendamento["client_id"],
            agendamento["pet_id"],
            agendamento["employee_id"],
            agendamento["data_agendamento"],
            agendamento["horario"],
            agendamento["servico"],
            agendamento["valor"] or 0,
            "Em atendimento",
            agendamento["observacoes"],
            now_iso(),
            now_iso(),
        ),
    )
    execute_db(
        "UPDATE appointments SET status='Em atendimento', updated_at=? WHERE id=?",
        (now_iso(), agendamento_id),
    )
    flash("Atendimento iniciado e enviado para Banho e Tosa.", "success")
    return redirect(url_for("banho_tosa.banho_tosa"))


@agenda_bp.route("/agenda/<int:agendamento_id>/excluir", methods=["POST"])
def excluir_agendamento(agendamento_id):
    vinculado = query_db(
        "SELECT id FROM grooming_services WHERE appointment_id=?",
        (agendamento_id,),
        one=True,
    )
    if vinculado:
        flash("Não é possível excluir: já existe atendimento vinculado.", "danger")
    else:
        execute_db("DELETE FROM appointments WHERE id=?", (agendamento_id,))
        flash("Agendamento excluído.", "info")
    return redirect(request.referrer or url_for("agenda.agenda"))


@agenda_bp.route("/agenda/bloqueios/adicionar", methods=["POST"])
def adicionar_bloqueio():
    employee_id = request.form.get("employee_id") or None
    block_date = request.form.get("block_date", "").strip()
    start_time = request.form.get("start_time", "").strip()
    end_time = request.form.get("end_time", "").strip()
    if not block_date or not start_time or not end_time or end_time <= start_time:
        flash("Informe um bloqueio válido.", "danger")
        return redirect(request.referrer or url_for("agenda.agenda"))

    insert_db(
        """
        INSERT INTO schedule_blocks
        (employee_id, block_date, start_time, end_time, block_type, title, notes, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            employee_id,
            block_date,
            start_time,
            end_time,
            request.form.get("block_type", "Bloqueio"),
            request.form.get("title", "").strip(),
            request.form.get("notes", "").strip(),
            now_iso(),
        ),
    )
    flash("Horário bloqueado.", "success")
    return redirect(url_for("agenda.agenda", referencia=block_date, modo="dia"))


@agenda_bp.route("/agenda/bloqueios/<int:block_id>/excluir", methods=["POST"])
def excluir_bloqueio(block_id):
    execute_db("DELETE FROM schedule_blocks WHERE id = ?", (block_id,))
    flash("Bloqueio removido.", "info")
    return redirect(request.referrer or url_for("agenda.agenda"))


@agenda_bp.route("/agenda/espera/adicionar", methods=["POST"])
def adicionar_espera():
    client_id = request.form.get("client_id", "").strip()
    pet_id = request.form.get("pet_id", "").strip()
    if not client_id or not pet_id:
        flash("Informe tutor e pet para a lista de espera.", "danger")
        return redirect(request.referrer or url_for("agenda.agenda"))

    insert_db(
        """
        INSERT INTO appointment_waitlist
        (client_id, pet_id, preferred_date, preferred_period, service, employee_id,
         priority, status, notes, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'Aguardando', ?, ?, ?)
        """,
        (
            client_id,
            pet_id,
            request.form.get("preferred_date") or None,
            request.form.get("preferred_period", "Qualquer"),
            request.form.get("service", "").strip(),
            request.form.get("employee_id") or None,
            int(request.form.get("priority") or 0),
            request.form.get("notes", "").strip(),
            now_iso(),
            now_iso(),
        ),
    )
    flash("Cliente incluído na lista de espera.", "success")
    return redirect(request.referrer or url_for("agenda.agenda"))


@agenda_bp.route("/agenda/espera/<int:wait_id>/remover", methods=["POST"])
def remover_espera(wait_id):
    execute_db(
        "UPDATE appointment_waitlist SET status='Removido', updated_at=? WHERE id=?",
        (now_iso(), wait_id),
    )
    flash("Item removido da lista de espera.", "info")
    return redirect(request.referrer or url_for("agenda.agenda"))


@agenda_bp.route("/agenda/solicitacoes-online")
def solicitacoes_online():
    solicitacoes = query_db(
        """
        SELECT a.*, c.nome AS cliente_nome, c.telefone, c.whatsapp, p.nome AS pet_nome
        FROM appointments a
        JOIN clients c ON c.id=a.client_id
        JOIN pets p ON p.id=a.pet_id
        WHERE COALESCE(a.requested_online,0)=1 AND a.status='Aguardando aprovação'
        ORDER BY a.created_at ASC
        """
    )
    cfg = obter_configuracao_capacidade()
    capacidade = max(1, int(cfg.get("default_capacity") or 3))
    itens = []
    for row in solicitacoes:
        item = dict(row)
        item["ocupados"] = ocupacao_horario(item["data_agendamento"], item["horario"], item["id"])
        item["capacidade"] = capacidade
        item["lotado"] = item["ocupados"] >= capacidade
        itens.append(item)
    return render_template("solicitacoes_online.html", solicitacoes=itens, pode_encaixar=pode_fazer_encaixe(session.get("role")))


@agenda_bp.route("/agenda/solicitacoes-online/<int:agendamento_id>/aprovar", methods=["POST"])
def aprovar_solicitacao_online(agendamento_id):
    item = query_db("SELECT * FROM appointments WHERE id=? AND status='Aguardando aprovação'", (agendamento_id,), one=True)
    if not item:
        flash("Solicitação não encontrada ou já analisada.", "warning")
        return redirect(url_for("agenda.solicitacoes_online"))
    duracao = item["duration_minutes"] or 60
    if conflito_horario(item["data_agendamento"], item["horario"], duracao, item["employee_id"], agendamento_id):
        flash("Este horário conflita com um profissional ou bloqueio. Reagende antes de aprovar.", "danger")
        return redirect(url_for("agenda.editar_agendamento", agendamento_id=agendamento_id))
    permitido, ocupados, capacidade, encaixe = _validar_capacidade(item["data_agendamento"], item["horario"], agendamento_id)
    if not permitido:
        flash(f"Horário lotado: {ocupados}/{capacidade}. Marque 'Aprovar como encaixe' para continuar.", "danger")
        return redirect(url_for("agenda.solicitacoes_online"))
    execute_db(
        "UPDATE appointments SET status='Agendado', approval_notes=?, approved_at=?, approved_by=?, capacity_override=?, capacity_override_by=?, capacity_override_at=?, updated_at=? WHERE id=?",
        (request.form.get("approval_notes", "").strip() or "Horário aprovado pela Pet & Gatô.", now_iso(), session.get("user_name", "Sistema"), 1 if encaixe else 0, session.get("user_name") if encaixe else None, now_iso() if encaixe else None, now_iso(), agendamento_id),
    )
    sincronizar_agendamento_com_banho(agendamento_id)
    flash("Solicitação aprovada e adicionada à agenda.", "success")
    return redirect(url_for("agenda.solicitacoes_online"))


@agenda_bp.route("/agenda/solicitacoes-online/<int:agendamento_id>/recusar", methods=["POST"])
def recusar_solicitacao_online(agendamento_id):
    motivo = request.form.get("approval_notes", "").strip()
    if not motivo:
        flash("Informe o motivo da recusa.", "danger")
        return redirect(url_for("agenda.solicitacoes_online"))
    execute_db(
        "UPDATE appointments SET status='Recusado', approval_notes=?, approved_at=?, approved_by=?, updated_at=? WHERE id=?",
        (motivo, now_iso(), session.get("user_name", "Sistema"), now_iso(), agendamento_id),
    )
    flash("Solicitação recusada. O cliente verá o motivo no portal.", "success")
    return redirect(url_for("agenda.solicitacoes_online"))


@agenda_bp.route("/agenda/<int:agendamento_id>/whatsapp/<action>")
def whatsapp_agendamento(agendamento_id, action):
    try:
        return redirect(preparar_whatsapp_agendamento(agendamento_id, action))
    except ValueError as exc:
        flash(str(exc), "danger")
    except Exception:
        flash("Não foi possível preparar a mensagem do WhatsApp.", "danger")
    return redirect(request.referrer or url_for("agenda.agenda"))
