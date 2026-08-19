from __future__ import annotations

import csv
import io
from datetime import date

from flask import Blueprint, Response, flash, redirect, render_template, request, session, url_for

from database import execute_db, now_iso, query_db
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


def _redirect_month(reference_month=None):
    return redirect(url_for("rh.gestao", mes=reference_month or request.form.get("reference_month") or date.today().strftime("%Y-%m")))


@rh_bp.route("/gestao")
def gestao():
    reference_month = request.args.get("mes") or date.today().strftime("%Y-%m")
    return render_template("rh.html", reference_month=reference_month, **get_rh_dashboard(reference_month))


@rh_bp.route("/ausencias", methods=["POST"])
def add_absence():
    employee_id = request.form.get("employee_id")
    start_date = request.form.get("start_date", "")
    if not employee_id or not start_date:
        flash("Informe funcionário e data inicial da ausência.", "danger")
        return _redirect_month(start_date[:7] if start_date else None)
    end_date = request.form.get("end_date", "") or start_date
    execute_db("""INSERT INTO employee_absences
        (employee_id, absence_type, start_date, end_date, justified, notes, attachment, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (employee_id, request.form.get("absence_type", "Falta"), start_date, end_date,
         1 if request.form.get("justified") else 0, request.form.get("notes", "").strip(), "", now_iso()))
    flash("Ausência registrada com sucesso.", "success")
    return _redirect_month(start_date[:7])


@rh_bp.route("/ausencias/<int:absence_id>/excluir", methods=["POST"])
def delete_absence(absence_id):
    execute_db("DELETE FROM employee_absences WHERE id=?", (absence_id,))
    flash("Ausência removida.", "info")
    return redirect(request.referrer or url_for("rh.gestao"))


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


@rh_bp.route("/ferias/<int:vacation_id>/excluir", methods=["POST"])
def delete_vacation(vacation_id):
    execute_db("DELETE FROM employee_vacations WHERE id=?", (vacation_id,))
    flash("Registro de férias removido.", "info")
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


@rh_bp.route("/ajustes/<int:adjustment_id>/status", methods=["POST"])
def adjustment_status(adjustment_id):
    execute_db("UPDATE employee_adjustments SET status=?, updated_at=? WHERE id=?",
               (request.form.get("status", "Pendente"), now_iso(), adjustment_id))
    flash("Status do ajuste atualizado.", "success")
    return redirect(request.referrer or url_for("rh.gestao"))


@rh_bp.route("/ajustes/<int:adjustment_id>/excluir", methods=["POST"])
def delete_adjustment(adjustment_id):
    execute_db("DELETE FROM employee_adjustments WHERE id=?", (adjustment_id,))
    flash("Ajuste removido.", "info")
    return redirect(request.referrer or url_for("rh.gestao"))


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


@rh_bp.route("/fechamento/<int:closing_id>/reabrir", methods=["POST"])
def reopen_month(closing_id):
    execute_db("UPDATE employee_monthly_closings SET status='Reaberto', updated_at=? WHERE id=?",
               (now_iso(), closing_id))
    flash("Fechamento reaberto para conferência.", "info")
    return redirect(request.referrer or url_for("rh.gestao"))


@rh_bp.route("/exportar.csv")
def export_csv():
    reference_month = request.args.get("mes") or date.today().strftime("%Y-%m")
    data = get_rh_dashboard(reference_month)
    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")
    writer.writerow(["Funcionário", "Cargo", "Trabalhado", "Previsto", "Saldo", "Faltas", "Justificadas", "Comissões", "Acréscimos", "Descontos", "Líquido estimado"])
    for item in data["summaries"]:
        writer.writerow([
            item["employee"]["name"], item["employee"]["role_name"] or "", item["worked_label"],
            item["expected_label"], item["balance_label"], item["absences"], item["justified_absences"],
            f'{item["commission_amount"]:.2f}'.replace(".", ","), f'{item["additions"]:.2f}'.replace(".", ","),
            f'{item["deductions"]:.2f}'.replace(".", ","), f'{item["estimated_net"]:.2f}'.replace(".", ","),
        ])
    content = "\ufeff" + output.getvalue()
    return Response(content, mimetype="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f"attachment; filename=rh_{reference_month}.csv"})
