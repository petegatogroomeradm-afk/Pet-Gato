from datetime import datetime

from flask import Blueprint, flash, redirect, render_template, request, url_for

from database import execute_db, now_iso, query_db

ponto_bp = Blueprint("ponto", __name__)

TIPOS = ["Entrada", "Saída intervalo", "Retorno intervalo", "Saída"]


@ponto_bp.route("/ponto", methods=["GET", "POST"])
def ponto():
    if request.method == "POST":
        employee_id = request.form.get("employee_id")
        tipo = request.form.get("record_type")
        momento = request.form.get("record_time", "").strip() or now_iso()
        if not employee_id or tipo not in TIPOS:
            flash("Selecione funcionário e tipo de marcação.", "danger")
            return redirect(url_for("ponto.ponto"))
        execute_db("""INSERT INTO time_records (employee_id, record_type, record_time, source, notes, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)""", (employee_id, tipo, momento, request.form.get("source", "Manual"), request.form.get("notes", "").strip(), now_iso(), now_iso()))
        flash("Ponto registrado.", "success")
        return redirect(url_for("ponto.ponto"))

    data = request.args.get("data", datetime.now().strftime("%Y-%m-%d"))
    funcionario = request.args.get("funcionario", "")
    where, params = ["tr.record_time LIKE ?"], [f"{data}%"]
    if funcionario:
        where.append("tr.employee_id=?")
        params.append(funcionario)
    funcionarios = query_db("SELECT id,name,registration FROM employees WHERE active=1 ORDER BY name")
    registros = query_db(f"""SELECT tr.*, e.name employee_name, e.registration FROM time_records tr LEFT JOIN employees e ON e.id=tr.employee_id WHERE {' AND '.join(where)} ORDER BY tr.record_time DESC""", tuple(params))
    return render_template("ponto.html", funcionarios=funcionarios, registros=registros, data=data, funcionario=funcionario, tipos=TIPOS)


@ponto_bp.route("/ponto/<int:registro_id>/editar", methods=["POST"])
def editar_registro(registro_id):
    tipo = request.form.get("record_type")
    momento = request.form.get("record_time", "").strip()
    if tipo not in TIPOS or not momento:
        flash("Dados inválidos.", "danger")
    else:
        execute_db("UPDATE time_records SET record_type=?, record_time=?, notes=?, updated_at=? WHERE id=?", (tipo, momento, request.form.get("notes", "").strip(), now_iso(), registro_id))
        flash("Marcação atualizada.", "success")
    return redirect(request.referrer or url_for("ponto.ponto"))


@ponto_bp.route("/ponto/<int:registro_id>/excluir", methods=["POST"])
def excluir_registro(registro_id):
    execute_db("DELETE FROM time_records WHERE id=?", (registro_id,))
    flash("Marcação excluída.", "info")
    return redirect(request.referrer or url_for("ponto.ponto"))


@ponto_bp.route("/ponto/ausencias", methods=["POST"])
def adicionar_ausencia():
    employee_id = request.form.get("employee_id")
    tipo = request.form.get("absence_type", "Falta")
    inicio = request.form.get("start_date", "")
    if not employee_id or not inicio:
        flash("Informe funcionário e data.", "danger")
    else:
        execute_db("""INSERT INTO employee_absences (employee_id, absence_type, start_date, end_date, justified, notes, attachment, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", (employee_id, tipo, inicio, request.form.get("end_date", "") or inicio, 1 if request.form.get("justified") else 0, request.form.get("notes", "").strip(), "", now_iso()))
        flash("Ocorrência registrada.", "success")
    return redirect(url_for("resumo_ponto.resumo_ponto", data=inicio))
