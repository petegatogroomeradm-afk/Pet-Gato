from __future__ import annotations

from flask import Blueprint, flash, redirect, render_template, request, url_for

from database import execute_db, now_iso, query_db

funcionarios_bp = Blueprint("funcionarios", __name__)


def _numero(v):
    t = (v or "0").strip().replace("R$", "").replace(" ", "")
    if "," in t:
        t = t.replace(".", "").replace(",", ".")
    try:
        return float(t)
    except (TypeError, ValueError):
        return 0.0


@funcionarios_bp.route("/funcionarios", methods=["GET", "POST"])
def funcionarios():
    if request.method == "POST":
        nome = request.form.get("name", "").strip()
        matricula = request.form.get("registration", "").strip()
        if not nome or not matricula:
            flash("Informe nome e matrícula.", "danger")
            return redirect(url_for("funcionarios.funcionarios"))
        existe = query_db("SELECT id FROM employees WHERE registration = ?", (matricula,), one=True)
        if existe:
            flash("Já existe um funcionário com esta matrícula.", "danger")
            return redirect(url_for("funcionarios.funcionarios"))
        execute_db(
            """
            INSERT INTO employees
            (name, registration, cpf, phone, email, role_name, admission_date, salary,
             schedule_id, commission_rate, active, notes, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                nome, matricula, request.form.get("cpf", "").strip(), request.form.get("phone", "").strip(),
                request.form.get("email", "").strip(), request.form.get("role_name", "").strip(),
                request.form.get("admission_date", "").strip(), _numero(request.form.get("salary")),
                request.form.get("schedule_id") or None, _numero(request.form.get("commission_rate")),
                1, request.form.get("notes", "").strip(), now_iso(), now_iso(),
            ),
        )
        flash("Funcionário cadastrado com sucesso.", "success")
        return redirect(url_for("funcionarios.funcionarios"))

    busca = request.args.get("busca", "").strip()
    status = request.args.get("status", "ativos")
    where, params = ["1=1"], []
    if busca:
        termo = f"%{busca}%"
        where.append("(e.name LIKE ? OR e.registration LIKE ? OR e.cpf LIKE ? OR e.role_name LIKE ?)")
        params.extend([termo, termo, termo, termo])
    if status == "ativos":
        where.append("e.active = 1")
    elif status == "inativos":
        where.append("e.active = 0")
    funcionarios_lista = query_db(
        f"""
        SELECT e.*, w.description AS schedule_name
        FROM employees e LEFT JOIN work_schedules w ON w.id=e.schedule_id
        WHERE {' AND '.join(where)} ORDER BY e.name
        """, tuple(params)
    )
    jornadas = query_db("SELECT * FROM work_schedules ORDER BY description")
    resumo = query_db("SELECT COUNT(*) total, COALESCE(SUM(CASE WHEN active=1 THEN 1 ELSE 0 END),0) ativos, COALESCE(SUM(salary),0) folha FROM employees", one=True)
    return render_template("funcionarios.html", funcionarios=funcionarios_lista, jornadas=jornadas, resumo=resumo, busca=busca, status=status)


@funcionarios_bp.route("/funcionarios/<int:funcionario_id>/editar", methods=["GET", "POST"])
def editar_funcionario(funcionario_id):
    funcionario = query_db("SELECT * FROM employees WHERE id=?", (funcionario_id,), one=True)
    if not funcionario:
        flash("Funcionário não encontrado.", "danger")
        return redirect(url_for("funcionarios.funcionarios"))
    if request.method == "POST":
        execute_db(
            """
            UPDATE employees SET name=?, registration=?, cpf=?, phone=?, email=?, role_name=?,
            admission_date=?, salary=?, schedule_id=?, commission_rate=?, notes=?, updated_at=? WHERE id=?
            """,
            (
                request.form.get("name", "").strip(), request.form.get("registration", "").strip(),
                request.form.get("cpf", "").strip(), request.form.get("phone", "").strip(),
                request.form.get("email", "").strip(), request.form.get("role_name", "").strip(),
                request.form.get("admission_date", "").strip(), _numero(request.form.get("salary")),
                request.form.get("schedule_id") or None, _numero(request.form.get("commission_rate")),
                request.form.get("notes", "").strip(), now_iso(), funcionario_id,
            ),
        )
        flash("Funcionário atualizado.", "success")
        return redirect(url_for("funcionarios.funcionarios"))
    jornadas = query_db("SELECT * FROM work_schedules ORDER BY description")
    return render_template("funcionario_editar.html", funcionario=funcionario, jornadas=jornadas)


@funcionarios_bp.route("/funcionarios/<int:funcionario_id>/status", methods=["POST"])
def alterar_status(funcionario_id):
    funcionario = query_db("SELECT active FROM employees WHERE id=?", (funcionario_id,), one=True)
    if funcionario:
        execute_db("UPDATE employees SET active=?, updated_at=? WHERE id=?", (0 if funcionario["active"] else 1, now_iso(), funcionario_id))
        flash("Status atualizado.", "success")
    return redirect(url_for("funcionarios.funcionarios"))


@funcionarios_bp.route("/funcionarios/<int:funcionario_id>/excluir", methods=["POST"])
def excluir_funcionario(funcionario_id):
    execute_db("UPDATE employees SET active=0, updated_at=? WHERE id=?", (now_iso(), funcionario_id))
    flash("Funcionário inativado.", "info")
    return redirect(url_for("funcionarios.funcionarios"))
