from __future__ import annotations

from datetime import date

from flask import Blueprint, flash, jsonify, redirect, render_template, request, url_for

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
)

agenda_bp = Blueprint("agenda", __name__)


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


def _pet_pertence_ao_cliente(pet_id, client_id):
    if not pet_id or not client_id:
        return False
    return bool(query_db(
        "SELECT id FROM pets WHERE id = ? AND client_id = ? AND COALESCE(ativo,1)=1",
        (pet_id, client_id),
        one=True,
    ))


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
            flash("O horário conflita com outro atendimento ou bloqueio.", "danger")
            return redirect(url_for("agenda.agenda", referencia=data_agendamento, modo="dia"))

        insert_db(
            """
            INSERT INTO appointments
            (client_id, pet_id, employee_id, data_agendamento, horario, duration_minutes,
             servico, valor, status, transport_required, observacoes, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                client_id,
                pet_id,
                employee_id,
                data_agendamento,
                hora,
                duracao,
                servico,
                valor,
                status,
                transporte,
                observacoes,
                now_iso(),
                now_iso(),
            ),
        )
        flash("Agendamento criado com sucesso.", "success")
        return redirect(url_for("agenda.agenda", referencia=data_agendamento, modo="dia"))

    modo, referencia, inicio, fim = intervalo_visualizacao(
        request.args.get("modo", "semana"), request.args.get("referencia")
    )
    status = request.args.get("status", "").strip()
    busca = request.args.get("busca", "").strip()
    employee_id = request.args.get("employee_id", "").strip()

    agendamentos = listar_agendamentos(inicio, fim, status, busca, employee_id)
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
            flash("O horário conflita com outro atendimento ou bloqueio.", "danger")
            return redirect(url_for("agenda.editar_agendamento", agendamento_id=agendamento_id))

        execute_db(
            """
            UPDATE appointments SET client_id=?, pet_id=?, employee_id=?, data_agendamento=?,
                horario=?, duration_minutes=?, servico=?, valor=?, status=?,
                transport_required=?, observacoes=?, updated_at=?
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
                now_iso(),
                agendamento_id,
            ),
        )
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
        return jsonify({"ok": False, "message": "Conflito de horário."}), 409

    execute_db(
        """
        UPDATE appointments
        SET data_agendamento = ?, horario = ?, employee_id = ?, status = ?, updated_at = ?
        WHERE id = ?
        """,
        (nova_data, nova_hora, employee_id, "Reagendado", now_iso(), agendamento_id),
    )
    return jsonify({"ok": True, "message": "Agendamento reagendado."})


@agenda_bp.route("/agenda/<int:agendamento_id>/status", methods=["POST"])
def atualizar_status(agendamento_id):
    status = request.form.get("status", "Agendado")
    execute_db(
        "UPDATE appointments SET status=?, updated_at=? WHERE id=?",
        (status, now_iso(), agendamento_id),
    )
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
