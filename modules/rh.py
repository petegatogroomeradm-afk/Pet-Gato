from datetime import date

from flask import Blueprint, flash, redirect, render_template, request, session, url_for

from database import execute_db, now_iso
from services.rh_service import close_month, get_rh_dashboard

rh_bp = Blueprint("rh", __name__, url_prefix="/rh")


def _money(value):
    text = (value or "0").strip().replace("R$", "").replace(" ", "")
    if "," in text:
        text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except (TypeError, ValueError):
        return 0.0


@rh_bp.route("/gestao")
def gestao():
    reference_month = request.args.get("mes") or date.today().strftime("%Y-%m")
    return render_template("rh.html", reference_month=reference_month,
                           **get_rh_dashboard(reference_month))


@rh_bp.route("/ferias", methods=["POST"])
def add_vacation():
    employee_id = request.form.get("employee_id")
    start_date = request.form.get("start_date", "")
    end_date = request.form.get("end_date", "")
    if not employee_id or not start_date or not end_date:
        flash("Informe funcionário e período das férias.", "danger")
        return redirect(url_for("rh.gestao"))
    execute_db("""INSERT INTO employee_vacations
        (employee_id, acquisition_start, acquisition_end, start_date, end_date,
         days, status, vacation_pay, advance_amount, notes, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (employee_id, request.form.get("acquisition_start", ""),
         request.form.get("acquisition_end", ""), start_date, end_date,
         int(request.form.get("days") or 30), request.form.get("status", "Planejada"),
         _money(request.form.get("vacation_pay")), _money(request.form.get("advance_amount")),
         request.form.get("notes", "").strip(), now_iso(), now_iso()))
    flash("Férias registradas com sucesso.", "success")
    return redirect(url_for("rh.gestao", mes=start_date[:7]))


@rh_bp.route("/ferias/<int:vacation_id>/status", methods=["POST"])
def vacation_status(vacation_id):
    execute_db("UPDATE employee_vacations SET status=?, updated_at=? WHERE id=?",
               (request.form.get("status", "Planejada"), now_iso(), vacation_id))
    flash("Status das férias atualizado.", "success")
    return redirect(request.referrer or url_for("rh.gestao"))


@rh_bp.route("/ajustes", methods=["POST"])
def add_adjustment():
    employee_id = request.form.get("employee_id")
    reference_month = request.form.get("reference_month")
    if not employee_id or not reference_month:
        flash("Informe funcionário e mês de referência.", "danger")
        return redirect(url_for("rh.gestao"))
    execute_db("""INSERT INTO employee_adjustments
        (employee_id, adjustment_type, reference_month, description,
         amount, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (employee_id, request.form.get("adjustment_type", "Desconto"),
         reference_month, request.form.get("description", "").strip(),
         _money(request.form.get("amount")), request.form.get("status", "Pendente"),
         now_iso(), now_iso()))
    flash("Ajuste lançado com sucesso.", "success")
    return redirect(url_for("rh.gestao", mes=reference_month))


@rh_bp.route("/fechamento", methods=["POST"])
def close_employee_month():
    employee_id = request.form.get("employee_id")
    reference_month = request.form.get("reference_month")
    try:
        close_month(int(employee_id), reference_month, session.get("user_name", "Sistema"))
        flash("Mês fechado e banco de horas atualizado.", "success")
    except (TypeError, ValueError) as exc:
        flash(str(exc), "danger")
    return redirect(url_for("rh.gestao", mes=reference_month))
