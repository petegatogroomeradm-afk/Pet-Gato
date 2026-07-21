from flask import Blueprint, flash, redirect, render_template, request, url_for

from database import execute_db, now_iso, query_db

jornadas_bp = Blueprint("jornadas", __name__)


@jornadas_bp.route("/jornadas", methods=["GET", "POST"])
def jornadas():
    if request.method == "POST":
        descricao = request.form.get("description", "").strip()
        if not descricao:
            flash("Informe a descrição da jornada.", "danger")
            return redirect(url_for("jornadas.jornadas"))
        execute_db(
            """
            INSERT INTO work_schedules
            (description, entrada, saida_intervalo, retorno_intervalo, saida_final, tolerancia, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (descricao, request.form.get("entrada", ""), request.form.get("saida_intervalo", ""), request.form.get("retorno_intervalo", ""), request.form.get("saida_final", ""), request.form.get("tolerancia", "10"), now_iso()),
        )
        flash("Jornada cadastrada.", "success")
        return redirect(url_for("jornadas.jornadas"))
    lista = query_db("SELECT w.*, COUNT(e.id) AS employees_count FROM work_schedules w LEFT JOIN employees e ON e.schedule_id=w.id AND e.active=1 GROUP BY w.id ORDER BY w.description")
    return render_template("jornadas.html", jornadas=lista)


@jornadas_bp.route("/jornadas/<int:jornada_id>/editar", methods=["GET", "POST"])
def editar_jornada(jornada_id):
    jornada = query_db("SELECT * FROM work_schedules WHERE id=?", (jornada_id,), one=True)
    if not jornada:
        flash("Jornada não encontrada.", "danger")
        return redirect(url_for("jornadas.jornadas"))
    if request.method == "POST":
        execute_db("""UPDATE work_schedules SET description=?, entrada=?, saida_intervalo=?, retorno_intervalo=?, saida_final=?, tolerancia=? WHERE id=?""", (request.form.get("description", "").strip(), request.form.get("entrada", ""), request.form.get("saida_intervalo", ""), request.form.get("retorno_intervalo", ""), request.form.get("saida_final", ""), request.form.get("tolerancia", "10"), jornada_id))
        flash("Jornada atualizada.", "success")
        return redirect(url_for("jornadas.jornadas"))
    return render_template("jornada_editar.html", jornada=jornada)


@jornadas_bp.route("/jornadas/<int:jornada_id>/excluir", methods=["POST"])
def excluir_jornada(jornada_id):
    em_uso = query_db("SELECT id FROM employees WHERE schedule_id=? LIMIT 1", (jornada_id,), one=True)
    if em_uso:
        flash("A jornada está vinculada a funcionários e não pode ser excluída.", "danger")
    else:
        execute_db("DELETE FROM work_schedules WHERE id=?", (jornada_id,))
        flash("Jornada excluída.", "info")
    return redirect(url_for("jornadas.jornadas"))
