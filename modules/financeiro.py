from __future__ import annotations

import csv
import io
import json
from calendar import monthrange
from datetime import date, datetime

from flask import Blueprint, Response, flash, redirect, render_template, request, url_for

from database import execute_db, insert_db, now_iso, query_db
from services.financeiro_service import (
    calculate_dre,
    daily_cash_flow,
    gerar_recorrencias_pendentes,
    get_goal,
)

financeiro_bp = Blueprint("financeiro", __name__)

PAYMENT_METHODS = [
    "PIX", "Dinheiro", "Cartão de Débito", "Cartão de Crédito",
    "Boleto", "Transferência", "Link de pagamento", "A definir",
]
STATUSES = ["Pago", "Pendente", "Vencido", "Cancelado"]


def _decimal(value: str | None) -> float:
    text = (value or "0").strip().replace("R$", "").replace(" ", "")
    if "," in text:
        text = text.replace(".", "").replace(",", ".")
    try:
        return max(0.0, float(text or 0))
    except (TypeError, ValueError):
        return 0.0


def _integer(value: str | None, default: int = 1) -> int:
    try:
        return max(1, int(value or default))
    except (TypeError, ValueError):
        return default


def _month_add(value: date, months: int) -> date:
    index = value.month - 1 + months
    year = value.year + index // 12
    month = index % 12 + 1
    day = min(value.day, monthrange(year, month)[1])
    return date(year, month, day)


def _filters():
    today = date.today()
    month = request.args.get("mes", f"{today.year:04d}-{today.month:02d}").strip()
    type_ = request.args.get("tipo", "").strip()
    status = request.args.get("status", "").strip()
    category = request.args.get("categoria", "").strip()
    account = request.args.get("conta", "").strip()
    search = request.args.get("busca", "").strip()
    return month, type_, status, category, account, search


def _where(month, type_, status, category, account, search):
    clauses = ["1=1"]
    params = []
    if month:
        clauses.append("SUBSTR(transaction_date,1,7)=?")
        params.append(month)
    if type_:
        clauses.append("type=?")
        params.append(type_)
    if status:
        clauses.append("status=?")
        params.append(status)
    if category:
        clauses.append("category=?")
        params.append(category)
    if account:
        clauses.append("account=?")
        params.append(account)
    if search:
        term = f"%{search}%"
        clauses.append("(description LIKE ? OR reference LIKE ? OR notes LIKE ? OR cost_center LIKE ?)")
        params.extend([term, term, term, term])
    return " AND ".join(clauses), params


@financeiro_bp.route("/financeiro", methods=["GET", "POST"])
def financeiro():
    gerar_recorrencias_pendentes()

    if request.method == "POST":
        type_ = request.form.get("type", "").strip()
        description = request.form.get("description", "").strip()
        amount = _decimal(request.form.get("amount"))
        total_installments = _integer(request.form.get("total_installments"), 1)

        if type_ not in {"Entrada", "Saída"} or not description or amount <= 0:
            flash("Informe tipo, descrição e um valor maior que zero.", "danger")
            return redirect(url_for("financeiro.financeiro"))

        transaction_date = request.form.get("transaction_date", "").strip() or date.today().isoformat()
        due_date = request.form.get("due_date", "").strip() or transaction_date
        base_due = datetime.strptime(due_date, "%Y-%m-%d").date()
        installment_amount = round(amount / total_installments, 2)
        batch_reference = request.form.get("reference", "").strip() or f"MAN-{now_iso().replace(':','').replace(' ','-')}"

        for number in range(1, total_installments + 1):
            current_due = _month_add(base_due, number - 1)
            current_amount = installment_amount
            if number == total_installments:
                current_amount = round(amount - installment_amount * (total_installments - 1), 2)

            status = request.form.get("status", "Pago").strip() or "Pago"
            if total_installments > 1 and number > 1 and status == "Pago":
                status = "Pendente"

            insert_db("""
                INSERT INTO financial_transactions
                (type, category, description, amount, payment_method,
                 transaction_date, due_date, status, reference, account,
                 cost_center, installment_number, total_installments,
                 paid_at, notes, source_type, source_id, created_by, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                type_, request.form.get("category", "").strip(), description,
                current_amount, request.form.get("payment_method", "").strip(),
                transaction_date if number == 1 else current_due.isoformat(),
                current_due.isoformat(), status,
                f"{batch_reference}-{number}/{total_installments}" if total_installments > 1 else batch_reference,
                request.form.get("account", "Caixa").strip() or "Caixa",
                request.form.get("cost_center", "").strip(),
                number, total_installments, now_iso() if status == "Pago" else None,
                request.form.get("notes", "").strip(), "Manual", None,
                "Administrador", now_iso(), now_iso(),
            ))

        flash(
            "Lançamento salvo com sucesso."
            if total_installments == 1
            else f"{total_installments} parcelas criadas com sucesso.",
            "success",
        )
        return redirect(url_for("financeiro.financeiro"))

    month, type_, status, category, account, search = _filters()
    where, params = _where(month, type_, status, category, account, search)

    transactions = query_db(f"""
        SELECT * FROM financial_transactions
        WHERE {where}
        ORDER BY transaction_date DESC, due_date DESC, id DESC
    """, tuple(params))

    totals = query_db(f"""
        SELECT
            COALESCE(SUM(CASE WHEN type='Entrada' AND status='Pago' THEN amount ELSE 0 END),0) entries_paid,
            COALESCE(SUM(CASE WHEN type='Saída' AND status='Pago' THEN amount ELSE 0 END),0) exits_paid,
            COALESCE(SUM(CASE WHEN type='Entrada' AND status NOT IN ('Pago','Cancelado') THEN amount ELSE 0 END),0) receivable,
            COALESCE(SUM(CASE WHEN type='Saída' AND status NOT IN ('Pago','Cancelado') THEN amount ELSE 0 END),0) payable,
            COALESCE(SUM(CASE WHEN status='Vencido' THEN amount ELSE 0 END),0) overdue,
            COUNT(*) total_transactions
        FROM financial_transactions WHERE {where}
    """, tuple(params), one=True)

    entries = float(totals["entries_paid"] or 0)
    exits = float(totals["exits_paid"] or 0)
    receivable = float(totals["receivable"] or 0)
    payable = float(totals["payable"] or 0)
    overdue = float(totals["overdue"] or 0)

    dre = calculate_dre(month)
    goal = get_goal(month)
    revenue_goal = float(goal["revenue_goal"] or 0)
    goal_percent = min(100, (dre["revenue"] / revenue_goal * 100)) if revenue_goal else 0

    by_payment = query_db(f"""
        SELECT COALESCE(NULLIF(payment_method,''),'Não informado') name,
               COALESCE(SUM(CASE WHEN type='Entrada' THEN amount ELSE -amount END),0) total
        FROM financial_transactions
        WHERE {where} AND status='Pago'
        GROUP BY payment_method ORDER BY total DESC
    """, tuple(params))

    by_category = query_db(f"""
        SELECT COALESCE(NULLIF(category,''),'Sem categoria') name,
               type,
               COALESCE(SUM(amount),0) total
        FROM financial_transactions
        WHERE {where} AND status <> 'Cancelado'
        GROUP BY category,type
        ORDER BY total DESC LIMIT 12
    """, tuple(params))

    flow = daily_cash_flow(month)
    flow_json = json.dumps({
        "labels": [item["day"][-2:] for item in flow],
        "entries": [float(item["entries"] or 0) for item in flow],
        "exits": [float(item["exits"] or 0) for item in flow],
    })

    categories = query_db("""
        SELECT DISTINCT category FROM financial_transactions
        WHERE category IS NOT NULL AND category <> ''
        ORDER BY category
    """)
    accounts = query_db("""
        SELECT DISTINCT account FROM financial_transactions
        WHERE account IS NOT NULL AND account <> ''
        ORDER BY account
    """)
    recurrences = query_db("""
        SELECT * FROM financial_recurrences
        ORDER BY active DESC, next_due_date, id DESC
    """)
    closings = query_db("""
        SELECT * FROM cash_closings
        ORDER BY closing_date DESC LIMIT 10
    """)

    return render_template(
        "financeiro.html",
        lancamentos=transactions, entradas=entries, saidas=exits,
        saldo=entries-exits, receber=receivable, pagar=payable,
        vencido=overdue, totais=totals, dre=dre, meta=goal,
        meta_percentual=goal_percent, por_forma=by_payment,
        por_categoria=by_category, categorias=categories, contas=accounts,
        recorrencias=recurrences, fechamentos=closings, fluxo_json=flow_json,
        mes=month, tipo=type_, status=status, categoria=category,
        conta=account, busca=search, formas_pagamento=PAYMENT_METHODS,
        status_opcoes=STATUSES,
    )


@financeiro_bp.route("/financeiro/meta", methods=["POST"])
def salvar_meta():
    reference_month = request.form.get("reference_month", "").strip()
    if not reference_month:
        flash("Informe o mês da meta.", "danger")
        return redirect(url_for("financeiro.financeiro"))

    revenue_goal = _decimal(request.form.get("revenue_goal"))
    expense_limit = _decimal(request.form.get("expense_limit"))
    existing = query_db(
        "SELECT id FROM financial_goals WHERE reference_month=?",
        (reference_month,), one=True,
    )
    if existing:
        execute_db("""
            UPDATE financial_goals
            SET revenue_goal=?, expense_limit=?, updated_at=?
            WHERE id=?
        """, (revenue_goal, expense_limit, now_iso(), existing["id"]))
    else:
        insert_db("""
            INSERT INTO financial_goals
            (reference_month,revenue_goal,expense_limit,created_at,updated_at)
            VALUES (?,?,?,?,?)
        """, (reference_month, revenue_goal, expense_limit, now_iso(), now_iso()))
    flash("Meta financeira atualizada.", "success")
    return redirect(url_for("financeiro.financeiro", mes=reference_month))


@financeiro_bp.route("/financeiro/recorrencias", methods=["POST"])
def adicionar_recorrencia():
    type_ = request.form.get("transaction_type", "").strip()
    description = request.form.get("description", "").strip()
    amount = _decimal(request.form.get("amount"))
    start_date = request.form.get("start_date", "").strip()

    if type_ not in {"Entrada", "Saída"} or not description or amount <= 0 or not start_date:
        flash("Preencha tipo, descrição, valor e início da recorrência.", "danger")
        return redirect(url_for("financeiro.financeiro"))

    insert_db("""
        INSERT INTO financial_recurrences
        (transaction_type,category,description,amount,payment_method,account,
         cost_center,frequency,start_date,next_due_date,end_date,active,notes,
         created_at,updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        type_, request.form.get("category", "").strip(), description, amount,
        request.form.get("payment_method", "").strip(),
        request.form.get("account", "Caixa").strip() or "Caixa",
        request.form.get("cost_center", "").strip(),
        request.form.get("frequency", "Mensal").strip(),
        start_date, start_date, request.form.get("end_date", "").strip() or None,
        1, request.form.get("notes", "").strip(), now_iso(), now_iso(),
    ))
    gerar_recorrencias_pendentes()
    flash("Conta recorrente cadastrada.", "success")
    return redirect(url_for("financeiro.financeiro"))


@financeiro_bp.route("/financeiro/recorrencias/<int:recurrence_id>/alternar", methods=["POST"])
def alternar_recorrencia(recurrence_id):
    item = query_db(
        "SELECT active FROM financial_recurrences WHERE id=?",
        (recurrence_id,), one=True,
    )
    if item:
        execute_db(
            "UPDATE financial_recurrences SET active=?,updated_at=? WHERE id=?",
            (0 if item["active"] else 1, now_iso(), recurrence_id),
        )
    flash("Recorrência atualizada.", "success")
    return redirect(url_for("financeiro.financeiro"))


@financeiro_bp.route("/financeiro/fechamento", methods=["POST"])
def fechar_caixa():
    closing_date = request.form.get("closing_date", "").strip() or date.today().isoformat()
    opening_balance = _decimal(request.form.get("opening_balance"))
    counted_balance = _decimal(request.form.get("counted_balance"))

    totals = query_db("""
        SELECT
            COALESCE(SUM(CASE WHEN type='Entrada' AND status='Pago' THEN amount ELSE 0 END),0) entries,
            COALESCE(SUM(CASE WHEN type='Saída' AND status='Pago' THEN amount ELSE 0 END),0) exits
        FROM financial_transactions
        WHERE SUBSTR(transaction_date,1,10)=?
    """, (closing_date,), one=True)
    entries = float(totals["entries"] or 0)
    exits = float(totals["exits"] or 0)
    expected = opening_balance + entries - exits
    difference = counted_balance - expected

    existing = query_db(
        "SELECT id FROM cash_closings WHERE closing_date=?",
        (closing_date,), one=True,
    )
    values = (
        opening_balance, entries, exits, expected, counted_balance,
        difference, request.form.get("notes", "").strip(),
        "Administrador", now_iso(),
    )
    if existing:
        execute_db("""
            UPDATE cash_closings
            SET opening_balance=?,total_entries=?,total_exits=?,expected_balance=?,
                counted_balance=?,difference=?,notes=?,closed_by=?,created_at=?
            WHERE id=?
        """, values + (existing["id"],))
    else:
        insert_db("""
            INSERT INTO cash_closings
            (closing_date,opening_balance,total_entries,total_exits,expected_balance,
             counted_balance,difference,notes,closed_by,created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (closing_date,) + values)
    flash("Fechamento de caixa registrado.", "success")
    return redirect(url_for("financeiro.financeiro", mes=closing_date[:7]))


@financeiro_bp.route("/financeiro/<int:lancamento_id>/editar", methods=["GET", "POST"])
def editar_lancamento(lancamento_id):
    lancamento = query_db(
        "SELECT * FROM financial_transactions WHERE id=?",
        (lancamento_id,), one=True,
    )
    if not lancamento:
        flash("Lançamento não encontrado.", "danger")
        return redirect(url_for("financeiro.financeiro"))
    if request.method == "POST":
        amount = _decimal(request.form.get("amount"))
        description = request.form.get("description", "").strip()
        if amount <= 0 or not description:
            flash("Informe descrição e valor válidos.", "danger")
            return redirect(url_for("financeiro.editar_lancamento", lancamento_id=lancamento_id))
        status = request.form.get("status", "Pago")
        execute_db("""
            UPDATE financial_transactions
            SET type=?,category=?,description=?,amount=?,payment_method=?,
                transaction_date=?,due_date=?,status=?,reference=?,account=?,
                cost_center=?,paid_at=?,notes=?,updated_at=?
            WHERE id=?
        """, (
            request.form.get("type", "Entrada"),
            request.form.get("category", "").strip(), description, amount,
            request.form.get("payment_method", "").strip(),
            request.form.get("transaction_date", "").strip(),
            request.form.get("due_date", "").strip(), status,
            request.form.get("reference", "").strip(),
            request.form.get("account", "Caixa").strip(),
            request.form.get("cost_center", "").strip(),
            now_iso() if status == "Pago" else None,
            request.form.get("notes", "").strip(), now_iso(), lancamento_id,
        ))
        flash("Lançamento atualizado.", "success")
        return redirect(url_for("financeiro.financeiro"))
    return render_template(
        "financeiro_editar.html", lancamento=lancamento,
        formas_pagamento=PAYMENT_METHODS, status_opcoes=STATUSES,
    )


@financeiro_bp.route("/financeiro/<int:lancamento_id>/status", methods=["POST"])
def alterar_status(lancamento_id):
    new_status = request.form.get("status", "Pendente")
    if new_status not in STATUSES:
        new_status = "Pendente"
    execute_db("""
        UPDATE financial_transactions
        SET status=?,paid_at=?,updated_at=?
        WHERE id=?
    """, (
        new_status, now_iso() if new_status == "Pago" else None,
        now_iso(), lancamento_id,
    ))
    flash(f"Status alterado para {new_status}.", "success")
    return redirect(request.referrer or url_for("financeiro.financeiro"))


@financeiro_bp.route("/financeiro/<int:lancamento_id>/excluir", methods=["POST"])
def excluir_lancamento(lancamento_id):
    execute_db("DELETE FROM financial_transactions WHERE id=?", (lancamento_id,))
    flash("Lançamento excluído.", "info")
    return redirect(url_for("financeiro.financeiro"))


@financeiro_bp.route("/financeiro/exportar.csv")
def exportar_csv():
    month, type_, status, category, account, search = _filters()
    where, params = _where(month, type_, status, category, account, search)
    data = query_db(
        f"SELECT * FROM financial_transactions WHERE {where} ORDER BY transaction_date,id",
        tuple(params),
    )
    stream = io.StringIO()
    writer = csv.writer(stream, delimiter=";")
    writer.writerow([
        "Data", "Vencimento", "Tipo", "Status", "Categoria", "Centro de custo",
        "Descrição", "Valor", "Pagamento", "Conta", "Parcela", "Referência", "Origem",
    ])
    for item in data:
        writer.writerow([
            item["transaction_date"], item["due_date"], item["type"], item["status"],
            item["category"], item["cost_center"], item["description"], item["amount"],
            item["payment_method"], item["account"],
            f"{item['installment_number'] or 1}/{item['total_installments'] or 1}",
            item["reference"], item["source_type"],
        ])
    content = "\ufeff" + stream.getvalue()
    return Response(
        content, mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename=financeiro_{month or 'completo'}.csv"},
    )
