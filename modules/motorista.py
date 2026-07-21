from __future__ import annotations

from datetime import date
from urllib.parse import quote_plus

from flask import Blueprint, flash, redirect, render_template, request, url_for

from database import execute_db, now_iso, query_db

motorista_bp = Blueprint("motorista", __name__)

FLUXOS = {
    "Coleta": ["Agendado", "Motorista saiu", "Pet coletado", "Chegou ao Pet Shop", "Finalizado"],
    "Entrega": ["Agendado", "Pronto para entrega", "Saiu para entrega", "Entregue"],
    "Coleta e Entrega": ["Agendado", "Motorista saiu", "Pet coletado", "Chegou ao Pet Shop", "Em atendimento", "Pronto para entrega", "Saiu para entrega", "Entregue"],
}


def _valor(texto):
    texto = (texto or "0").strip().replace("R$", "").replace(" ", "")
    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    try:
        return max(0.0, float(texto))
    except ValueError:
        return 0.0


def _proximo_status(tipo, atual):
    fluxo = FLUXOS.get(tipo) or FLUXOS["Coleta e Entrega"]
    if atual not in fluxo:
        return fluxo[0]
    indice = fluxo.index(atual)
    return fluxo[min(indice + 1, len(fluxo) - 1)]


def _criar_receita_transporte(corrida):
    if not corrida or float(corrida["fee"] or 0) <= 0:
        return
    existe = query_db("SELECT id FROM financial_transactions WHERE source_type='Transporte' AND source_id=?", (corrida["id"],), one=True)
    if existe:
        return
    execute_db("""
        INSERT INTO financial_transactions
        (type, category, description, amount, payment_method, transaction_date, due_date,
         status, reference, account, notes, source_type, source_id, created_by, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        "Entrada", "Táxi Dog", f"{corrida['service_type']} - {corrida['pet']} - {corrida['cliente']}",
        corrida["fee"], "A definir", corrida["pickup_date"], corrida["pickup_date"], "Pendente",
        f"CORRIDA-{corrida['id']}", "Caixa", corrida["observations"] or "", "Transporte",
        corrida["id"], "Sistema", now_iso(), now_iso(),
    ))


@motorista_bp.route("/motorista", methods=["GET", "POST"])
def motorista():
    if request.method == "POST":
        client_id = request.form.get("client_id", "").strip()
        pet_id = request.form.get("pet_id", "").strip()
        service_type = request.form.get("service_type", "").strip()
        pickup_date = request.form.get("pickup_date", "").strip()
        if not client_id or not pet_id or service_type not in FLUXOS or not pickup_date:
            flash("Informe tutor, pet, tipo e data.", "danger")
            return redirect(url_for("motorista.motorista"))
        execute_db("""
            INSERT INTO transport_services
            (appointment_id, grooming_id, client_id, pet_id, driver_name, driver_phone,
             service_type, pickup_address, delivery_address, pickup_date, pickup_time,
             expected_return_time, status, fee, distance_km, payment_status, observations,
             created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            request.form.get("appointment_id") or None, request.form.get("grooming_id") or None,
            client_id, pet_id, request.form.get("driver_name", "").strip(),
            request.form.get("driver_phone", "").strip(), service_type,
            request.form.get("pickup_address", "").strip(), request.form.get("delivery_address", "").strip(),
            pickup_date, request.form.get("pickup_time", "").strip(),
            request.form.get("expected_return_time", "").strip(), "Agendado",
            _valor(request.form.get("fee")), _valor(request.form.get("distance_km")),
            "Pendente", request.form.get("observations", "").strip(), now_iso(), now_iso(),
        ))
        flash("Transporte agendado com sucesso.", "success")
        return redirect(url_for("motorista.motorista"))

    filtro_data = request.args.get("data", date.today().isoformat())
    filtro_status = request.args.get("status", "")
    filtro_motorista = request.args.get("motorista", "")
    busca = request.args.get("busca", "").strip()
    where, params = ["1=1"], []
    if filtro_data:
        where.append("t.pickup_date = ?"); params.append(filtro_data)
    if filtro_status:
        where.append("t.status = ?"); params.append(filtro_status)
    if filtro_motorista:
        where.append("t.driver_name = ?"); params.append(filtro_motorista)
    if busca:
        termo = f"%{busca}%"
        where.append("(c.nome LIKE ? OR p.nome LIKE ? OR t.pickup_address LIKE ? OR t.delivery_address LIKE ?)")
        params.extend([termo, termo, termo, termo])

    corridas = query_db(f"""
        SELECT t.*, c.nome cliente, c.telefone, c.whatsapp, p.nome pet,
               a.servico agenda_servico, g.status grooming_status
        FROM transport_services t
        LEFT JOIN clients c ON c.id=t.client_id
        LEFT JOIN pets p ON p.id=t.pet_id
        LEFT JOIN appointments a ON a.id=t.appointment_id
        LEFT JOIN grooming_services g ON g.id=t.grooming_id
        WHERE {' AND '.join(where)}
        ORDER BY t.pickup_date, t.pickup_time, t.id
    """, tuple(params))

    resumo = query_db("""
        SELECT COUNT(*) total,
          COALESCE(SUM(CASE WHEN status='Agendado' THEN 1 ELSE 0 END),0) agendadas,
          COALESCE(SUM(CASE WHEN status IN ('Motorista saiu','Pet coletado','Saiu para entrega','Em atendimento','Pronto para entrega','Chegou ao Pet Shop') THEN 1 ELSE 0 END),0) andamento,
          COALESCE(SUM(CASE WHEN status IN ('Entregue','Finalizado') THEN 1 ELSE 0 END),0) finalizadas,
          COALESCE(SUM(CASE WHEN service_type IN ('Coleta','Coleta e Entrega') THEN 1 ELSE 0 END),0) coletas,
          COALESCE(SUM(CASE WHEN service_type IN ('Entrega','Coleta e Entrega') THEN 1 ELSE 0 END),0) entregas
        FROM transport_services WHERE pickup_date=?
    """, (filtro_data,), one=True)

    clientes = query_db("SELECT id,nome,endereco FROM clients WHERE COALESCE(ativo,1)=1 ORDER BY nome")
    pets = query_db("SELECT p.id,p.nome,p.client_id,c.nome cliente FROM pets p LEFT JOIN clients c ON c.id=p.client_id WHERE COALESCE(p.ativo,1)=1 ORDER BY p.nome")
    motoristas = query_db("SELECT DISTINCT driver_name FROM transport_services WHERE driver_name IS NOT NULL AND driver_name<>'' ORDER BY driver_name")
    agendamentos = query_db("""
        SELECT a.id, a.client_id, a.pet_id, a.data_agendamento, a.horario, a.servico,
               c.nome cliente, p.nome pet FROM appointments a
        LEFT JOIN clients c ON c.id=a.client_id LEFT JOIN pets p ON p.id=a.pet_id
        WHERE a.transport_required=1 AND a.status NOT IN ('Cancelado','Concluído')
        ORDER BY a.data_agendamento,a.horario
    """)
    return render_template("motorista.html", clientes=clientes, pets=pets, corridas=corridas,
                           resumo=resumo, motoristas=motoristas, agendamentos=agendamentos,
                           filtro_data=filtro_data, filtro_status=filtro_status,
                           filtro_motorista=filtro_motorista, busca=busca, fluxos=FLUXOS,
                           quote_plus=quote_plus)


@motorista_bp.route("/motorista/<int:corrida_id>/editar", methods=["GET", "POST"])
def editar_corrida(corrida_id):
    corrida = query_db("SELECT * FROM transport_services WHERE id=?", (corrida_id,), one=True)
    if not corrida:
        flash("Corrida não encontrada.", "danger")
        return redirect(url_for("motorista.motorista"))
    if request.method == "POST":
        execute_db("""
          UPDATE transport_services SET driver_name=?, driver_phone=?, service_type=?,
          pickup_address=?, delivery_address=?, pickup_date=?, pickup_time=?, expected_return_time=?,
          fee=?, distance_km=?, payment_status=?, observations=?, updated_at=? WHERE id=?
        """, (request.form.get("driver_name","").strip(), request.form.get("driver_phone","").strip(),
              request.form.get("service_type","Coleta"), request.form.get("pickup_address","").strip(),
              request.form.get("delivery_address","").strip(), request.form.get("pickup_date","").strip(),
              request.form.get("pickup_time","").strip(), request.form.get("expected_return_time","").strip(),
              _valor(request.form.get("fee")), _valor(request.form.get("distance_km")),
              request.form.get("payment_status","Pendente"), request.form.get("observations","").strip(),
              now_iso(), corrida_id))
        flash("Corrida atualizada.", "success")
        return redirect(url_for("motorista.motorista", data=request.form.get("pickup_date")))
    clientes = query_db("SELECT id,nome FROM clients ORDER BY nome")
    pets = query_db("SELECT p.id,p.nome,p.client_id,c.nome cliente FROM pets p LEFT JOIN clients c ON c.id=p.client_id ORDER BY p.nome")
    return render_template("motorista_editar.html", corrida=corrida, clientes=clientes, pets=pets, fluxos=FLUXOS)


@motorista_bp.route("/motorista/<int:corrida_id>/avancar", methods=["POST"])
def avancar_status(corrida_id):
    corrida = query_db("""SELECT t.*, c.nome cliente, p.nome pet FROM transport_services t
                           LEFT JOIN clients c ON c.id=t.client_id LEFT JOIN pets p ON p.id=t.pet_id
                           WHERE t.id=?""", (corrida_id,), one=True)
    if not corrida:
        flash("Corrida não encontrada.", "danger")
        return redirect(url_for("motorista.motorista"))
    novo = _proximo_status(corrida["service_type"], corrida["status"])
    campos = ["status=?", "updated_at=?"]
    valores = [novo, now_iso()]
    if novo in {"Motorista saiu", "Saiu para entrega"}: campos.append("started_at=?"); valores.append(now_iso())
    if novo in {"Entregue", "Finalizado"}: campos.append("finished_at=?"); valores.append(now_iso())
    valores.append(corrida_id)
    execute_db(f"UPDATE transport_services SET {', '.join(campos)} WHERE id=?", tuple(valores))
    if novo in {"Entregue", "Finalizado"}:
        corrida = dict(corrida); corrida["status"] = novo
        _criar_receita_transporte(corrida)
    flash(f"Status atualizado para {novo}.", "success")
    return redirect(request.referrer or url_for("motorista.motorista"))


@motorista_bp.route("/motorista/<int:corrida_id>/status", methods=["POST"])
def definir_status(corrida_id):
    status = request.form.get("status", "Agendado")
    execute_db("UPDATE transport_services SET status=?, updated_at=? WHERE id=?", (status, now_iso(), corrida_id))
    flash("Status atualizado.", "success")
    return redirect(request.referrer or url_for("motorista.motorista"))


@motorista_bp.route("/motorista/<int:corrida_id>/excluir", methods=["POST"])
def excluir_corrida(corrida_id):
    execute_db("DELETE FROM transport_services WHERE id=?", (corrida_id,))
    flash("Corrida excluída.", "info")
    return redirect(url_for("motorista.motorista"))
