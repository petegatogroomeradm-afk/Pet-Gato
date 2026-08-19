from __future__ import annotations

import csv
import io
import json
from calendar import monthrange
from datetime import date, datetime, timedelta

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
STATUSES = ["Pago", "Parcial", "Pendente", "Vencido", "Cancelado"]


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




def _sync_overdue_statuses(reference_date: date | None = None) -> None:
    reference_date = reference_date or date.today()
    execute_db("""
        UPDATE financial_transactions
        SET status='Vencido', updated_at=?
        WHERE status IN ('Pendente','Parcial')
          AND due_date IS NOT NULL
          AND due_date <> ''
          AND due_date < ?
    """, (now_iso(), reference_date.isoformat()))


def _accounts_filters():
    today = date.today()
    start = request.args.get('inicio', today.isoformat()).strip()
    end = request.args.get('fim', (today + timedelta(days=30)).isoformat()).strip()
    type_ = request.args.get('tipo', '').strip()
    status = request.args.get('status', '').strip()
    search = request.args.get('busca', '').strip()
    return start, end, type_, status, search

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
    _sync_overdue_statuses()

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
            COALESCE(SUM(CASE WHEN type='Entrada' AND status NOT IN ('Pago','Cancelado') THEN (amount-COALESCE(paid_amount,0)) ELSE 0 END),0) receivable,
            COALESCE(SUM(CASE WHEN type='Saída' AND status NOT IN ('Pago','Cancelado') THEN (amount-COALESCE(paid_amount,0)) ELSE 0 END),0) payable,
            COALESCE(SUM(CASE WHEN status='Vencido' THEN (amount-COALESCE(paid_amount,0)) ELSE 0 END),0) overdue,
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

    # Indicadores executivos independentes dos filtros da tabela.
    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    week_end = week_start + timedelta(days=6)
    month_start = today.replace(day=1)
    month_end = today.replace(day=monthrange(today.year, today.month)[1])

    executive = query_db("""
        SELECT
          COALESCE(SUM(CASE WHEN type='Entrada' AND status='Pago' AND transaction_date=? THEN amount ELSE 0 END),0) revenue_today,
          COALESCE(SUM(CASE WHEN type='Saída' AND status='Pago' AND transaction_date=? THEN amount ELSE 0 END),0) expense_today,
          COALESCE(SUM(CASE WHEN type='Entrada' AND status='Pago' AND transaction_date BETWEEN ? AND ? THEN amount ELSE 0 END),0) revenue_week,
          COALESCE(SUM(CASE WHEN type='Saída' AND status='Pago' AND transaction_date BETWEEN ? AND ? THEN amount ELSE 0 END),0) expense_week,
          COALESCE(SUM(CASE WHEN type='Entrada' AND status='Pago' AND transaction_date BETWEEN ? AND ? THEN amount ELSE 0 END),0) revenue_month,
          COALESCE(SUM(CASE WHEN type='Saída' AND status='Pago' AND transaction_date BETWEEN ? AND ? THEN amount ELSE 0 END),0) expense_month,
          COALESCE(SUM(CASE WHEN type='Entrada' AND status='Pago' THEN amount WHEN type='Saída' AND status='Pago' THEN -amount ELSE 0 END),0) cash_balance
        FROM financial_transactions
        WHERE status <> 'Cancelado'
    """, (
        today.isoformat(), today.isoformat(),
        week_start.isoformat(), week_end.isoformat(),
        week_start.isoformat(), week_end.isoformat(),
        month_start.isoformat(), month_end.isoformat(),
        month_start.isoformat(), month_end.isoformat(),
    ), one=True)
    executive = dict(executive) if executive else {}

    priorities = query_db("""
        SELECT
          COALESCE(SUM(CASE WHEN status='Vencido' THEN amount ELSE 0 END),0) overdue_amount,
          COALESCE(SUM(CASE WHEN status='Vencido' THEN 1 ELSE 0 END),0) overdue_count,
          COALESCE(SUM(CASE WHEN status='Pendente' AND due_date BETWEEN ? AND ? THEN amount ELSE 0 END),0) next_7_amount,
          COALESCE(SUM(CASE WHEN status='Pendente' AND due_date BETWEEN ? AND ? THEN 1 ELSE 0 END),0) next_7_count
        FROM financial_transactions
        WHERE status <> 'Cancelado'
    """, (today.isoformat(), (today + timedelta(days=7)).isoformat(),
          today.isoformat(), (today + timedelta(days=7)).isoformat()), one=True) or {}

    latest_transactions = query_db("""
        SELECT id, transaction_date, due_date, type, status, description, amount,
               payment_method, source_type, account
        FROM financial_transactions
        WHERE status <> 'Cancelado'
        ORDER BY COALESCE(updated_at, created_at, transaction_date) DESC, id DESC
        LIMIT 8
    """)

    source_summary = query_db("""
        SELECT COALESCE(NULLIF(source_type,''),'Manual') source_name,
               COALESCE(SUM(CASE WHEN type='Entrada' AND status='Pago' THEN amount ELSE 0 END),0) entries,
               COALESCE(SUM(CASE WHEN type='Saída' AND status='Pago' THEN amount ELSE 0 END),0) exits
        FROM financial_transactions
        WHERE transaction_date BETWEEN ? AND ? AND status <> 'Cancelado'
        GROUP BY COALESCE(NULLIF(source_type,''),'Manual')
        ORDER BY entries DESC, exits DESC
        LIMIT 8
    """, (month_start.isoformat(), month_end.isoformat()))

    executive_data = {
        'revenue_today': float(executive.get('revenue_today', 0) or 0),
        'expense_today': float(executive.get('expense_today', 0) or 0),
        'result_today': float(executive.get('revenue_today', 0) or 0) - float(executive.get('expense_today', 0) or 0),
        'revenue_week': float(executive.get('revenue_week', 0) or 0),
        'expense_week': float(executive.get('expense_week', 0) or 0),
        'result_week': float(executive.get('revenue_week', 0) or 0) - float(executive.get('expense_week', 0) or 0),
        'revenue_month': float(executive.get('revenue_month', 0) or 0),
        'expense_month': float(executive.get('expense_month', 0) or 0),
        'result_month': float(executive.get('revenue_month', 0) or 0) - float(executive.get('expense_month', 0) or 0),
        'cash_balance': float(executive.get('cash_balance', 0) or 0),
    }

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
        status_opcoes=STATUSES, executivo=executive_data, prioridades=priorities,
        ultimos_lancamentos=latest_transactions, resumo_origens=source_summary,
        hoje=today.isoformat(), semana_inicio=week_start.isoformat(), semana_fim=week_end.isoformat(),
    )




@financeiro_bp.route("/financeiro/contas")
def contas_pagar_receber():
    _sync_overdue_statuses()
    start, end, type_, status, search = _accounts_filters()

    clauses = ["status <> 'Cancelado'", "due_date IS NOT NULL", "due_date <> ''"]
    params = []
    if start:
        clauses.append('due_date >= ?')
        params.append(start)
    if end:
        clauses.append('due_date <= ?')
        params.append(end)
    if type_ in {'Entrada', 'Saída'}:
        clauses.append('type = ?')
        params.append(type_)
    if status in STATUSES:
        clauses.append('status = ?')
        params.append(status)
    if search:
        term = f"%{search}%"
        clauses.append('(description LIKE ? OR category LIKE ? OR reference LIKE ? OR account LIKE ?)')
        params.extend([term, term, term, term])

    where = ' AND '.join(clauses)
    raw_items = query_db(f"""
        SELECT * FROM financial_transactions
        WHERE {where}
        ORDER BY due_date, CASE WHEN type='Saída' THEN 0 ELSE 1 END, id
    """, tuple(params))
    items = [dict(item) for item in raw_items]
    item_ids = [item["id"] for item in items]
    payments_by_transaction = {}
    if item_ids:
        marks = ",".join("?" for _ in item_ids)
        payments = query_db(f"""
            SELECT * FROM financial_payments
            WHERE transaction_id IN ({marks})
            ORDER BY payment_date DESC, id DESC
        """, tuple(item_ids))
        for payment in payments:
            payments_by_transaction.setdefault(payment["transaction_id"], []).append(dict(payment))
    for item in items:
        paid = float(item.get("paid_amount") or 0)
        if item.get("status") == "Pago" and paid <= 0:
            paid = float(item.get("amount") or 0)
        item["paid_amount"] = paid
        item["balance_amount"] = max(0.0, float(item.get("amount") or 0) - paid)
        item["payments"] = payments_by_transaction.get(item["id"], [])

    today = date.today().isoformat()
    next_7 = (date.today() + timedelta(days=7)).isoformat()
    summary = query_db("""
        SELECT
            COALESCE(SUM(CASE WHEN type='Entrada' AND status IN ('Pendente','Parcial','Vencido') THEN (amount-COALESCE(paid_amount,0)) ELSE 0 END),0) receivable,
            COALESCE(SUM(CASE WHEN type='Saída' AND status IN ('Pendente','Parcial','Vencido') THEN (amount-COALESCE(paid_amount,0)) ELSE 0 END),0) payable,
            COALESCE(SUM(CASE WHEN status='Vencido' THEN (amount-COALESCE(paid_amount,0)) ELSE 0 END),0) overdue,
            COALESCE(SUM(CASE WHEN due_date=? AND status IN ('Pendente','Parcial','Vencido') THEN (amount-COALESCE(paid_amount,0)) ELSE 0 END),0) due_today,
            COALESCE(SUM(CASE WHEN due_date>? AND due_date<=? AND status IN ('Pendente','Parcial','Vencido') THEN (amount-COALESCE(paid_amount,0)) ELSE 0 END),0) next_7_days,
            COUNT(CASE WHEN status IN ('Pendente','Parcial','Vencido') THEN 1 END) open_count
        FROM financial_transactions
        WHERE status <> 'Cancelado'
    """, (today, today, next_7), one=True)

    receivable = float(summary['receivable'] or 0)
    payable = float(summary['payable'] or 0)
    projected = receivable - payable

    groups = {'overdue': [], 'today': [], 'upcoming': [], 'paid': []}
    for item in items:
        if item['status'] == 'Pago':
            groups['paid'].append(item)
        elif item['due_date'] < today:
            groups['overdue'].append(item)
        elif item['due_date'] == today:
            groups['today'].append(item)
        else:
            groups['upcoming'].append(item)

    return render_template(
        'financeiro_contas.html',
        grupos=groups, resumo=summary, receber=receivable, pagar=payable,
        saldo_projetado=projected, inicio=start, fim=end, tipo=type_,
        status=status, busca=search, status_opcoes=STATUSES, hoje=today,
        recorrencias=query_db("SELECT * FROM financial_recurrences ORDER BY active DESC,next_due_date,id DESC"),
        formas_pagamento=PAYMENT_METHODS,
    )


@financeiro_bp.route('/financeiro/<int:lancamento_id>/pagamento', methods=['POST'])
def registrar_pagamento(lancamento_id):
    item = query_db('SELECT * FROM financial_transactions WHERE id=?', (lancamento_id,), one=True)
    if not item:
        flash('Lançamento não encontrado.', 'danger')
        return redirect(url_for('financeiro.contas_pagar_receber'))
    if item['status'] in {'Cancelado', 'Pago'}:
        flash('Este lançamento não aceita novos pagamentos.', 'warning')
        return redirect(request.referrer or url_for('financeiro.contas_pagar_receber'))

    total = float(item['amount'] or 0)
    current_paid = float(item['paid_amount'] or 0)
    amount = _decimal(request.form.get('amount'))
    balance = max(0.0, total - current_paid)
    if amount <= 0 or amount > balance + 0.005:
        flash(f'Informe um valor entre R$ 0,01 e R$ {balance:.2f}.', 'danger')
        return redirect(request.referrer or url_for('financeiro.contas_pagar_receber'))

    payment_date = request.form.get('payment_date', '').strip() or date.today().isoformat()
    method = request.form.get('payment_method', '').strip() or item['payment_method'] or 'A definir'
    account = request.form.get('account', '').strip() or item['account'] or 'Caixa'
    insert_db("""
        INSERT INTO financial_payments
        (transaction_id,amount,payment_date,payment_method,account,notes,created_by,created_at)
        VALUES (?,?,?,?,?,?,?,?)
    """, (lancamento_id, amount, payment_date, method, account,
          request.form.get('notes', '').strip(), 'Administrador', now_iso()))

    new_paid = round(current_paid + amount, 2)
    new_status = 'Pago' if new_paid >= total - 0.005 else 'Parcial'
    execute_db("""
        UPDATE financial_transactions
        SET paid_amount=?, status=?, payment_method=?, account=?,
            paid_at=?, last_payment_at=?, updated_at=?
        WHERE id=?
    """, (new_paid, new_status, method, account,
          now_iso() if new_status == 'Pago' else None, now_iso(), now_iso(), lancamento_id))
    flash('Pagamento registrado com sucesso.' if new_status == 'Pago' else 'Pagamento parcial registrado.', 'success')
    return redirect(request.referrer or url_for('financeiro.contas_pagar_receber'))


@financeiro_bp.route('/financeiro/pagamentos/<int:payment_id>/estornar', methods=['POST'])
def estornar_pagamento(payment_id):
    payment = query_db('SELECT * FROM financial_payments WHERE id=?', (payment_id,), one=True)
    if not payment:
        flash('Pagamento não encontrado.', 'danger')
        return redirect(url_for('financeiro.contas_pagar_receber'))
    transaction_id = payment['transaction_id']
    execute_db('DELETE FROM financial_payments WHERE id=?', (payment_id,))
    sums = query_db('SELECT COALESCE(SUM(amount),0) total FROM financial_payments WHERE transaction_id=?', (transaction_id,), one=True)
    item = query_db('SELECT amount,due_date FROM financial_transactions WHERE id=?', (transaction_id,), one=True)
    paid = float(sums['total'] or 0)
    total = float(item['amount'] or 0) if item else 0
    if paid >= total - 0.005 and total > 0:
        status = 'Pago'
    elif paid > 0:
        status = 'Parcial'
    elif item and item['due_date'] and item['due_date'] < date.today().isoformat():
        status = 'Vencido'
    else:
        status = 'Pendente'
    execute_db('UPDATE financial_transactions SET paid_amount=?,status=?,paid_at=?,last_payment_at=?,updated_at=? WHERE id=?',
               (paid, status, now_iso() if status == 'Pago' else None, now_iso() if paid else None, now_iso(), transaction_id))
    flash('Pagamento estornado e saldo recalculado.', 'success')
    return redirect(request.referrer or url_for('financeiro.contas_pagar_receber'))


@financeiro_bp.route('/financeiro/recorrencias/<int:recurrence_id>/gerar', methods=['POST'])
def gerar_recorrencia_agora(recurrence_id):
    recurrence = query_db('SELECT * FROM financial_recurrences WHERE id=?', (recurrence_id,), one=True)
    if not recurrence:
        flash('Recorrência não encontrada.', 'danger')
        return redirect(url_for('financeiro.contas_pagar_receber'))
    due = recurrence['next_due_date'] or recurrence['start_date']
    exists = query_db('SELECT id FROM financial_transactions WHERE recurrence_id=? AND due_date=? LIMIT 1',
                      (recurrence_id, due), one=True)
    if not exists:
        insert_db("""
            INSERT INTO financial_transactions
            (type,category,description,amount,payment_method,transaction_date,due_date,status,
             reference,account,cost_center,installment_number,total_installments,recurrence_id,
             paid_amount,notes,source_type,created_by,created_at,updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (recurrence['transaction_type'], recurrence['category'], recurrence['description'],
              recurrence['amount'], recurrence['payment_method'], due, due, 'Pendente',
              f"REC-{recurrence_id}-{due}", recurrence['account'] or 'Caixa', recurrence['cost_center'],
              1, 1, recurrence_id, 0, recurrence['notes'] or '', 'Recorrência', 'Sistema', now_iso(), now_iso()))
    next_due = _next_date_for_route(due, recurrence['frequency'])
    execute_db('UPDATE financial_recurrences SET next_due_date=?,updated_at=? WHERE id=?',
               (next_due, now_iso(), recurrence_id))
    flash('Próxima conta recorrente gerada.', 'success')
    return redirect(request.referrer or url_for('financeiro.contas_pagar_receber'))


@financeiro_bp.route('/financeiro/recorrencias/<int:recurrence_id>/excluir', methods=['POST'])
def excluir_recorrencia(recurrence_id):
    execute_db('DELETE FROM financial_recurrences WHERE id=?', (recurrence_id,))
    flash('Recorrência excluída. Lançamentos já gerados foram preservados.', 'success')
    return redirect(request.referrer or url_for('financeiro.contas_pagar_receber'))


@financeiro_bp.route('/financeiro/<int:lancamento_id>/adiar', methods=['POST'])
def adiar_vencimento(lancamento_id):
    item = query_db('SELECT due_date,status FROM financial_transactions WHERE id=?', (lancamento_id,), one=True)
    if not item:
        flash('Lançamento não encontrado.', 'danger')
        return redirect(url_for('financeiro.contas_pagar_receber'))

    try:
        days = max(1, min(365, int(request.form.get('dias', '7'))))
    except (TypeError, ValueError):
        days = 7
    base = date.today()
    if item['due_date']:
        try:
            base = datetime.strptime(item['due_date'], '%Y-%m-%d').date()
        except ValueError:
            pass
    new_due = base + timedelta(days=days)
    new_status = 'Pendente' if item['status'] == 'Vencido' else item['status']
    execute_db("""
        UPDATE financial_transactions
        SET due_date=?, status=?, updated_at=?
        WHERE id=?
    """, (new_due.isoformat(), new_status, now_iso(), lancamento_id))
    flash(f'Vencimento adiado para {new_due.strftime("%d/%m/%Y")}.', 'success')
    return redirect(request.referrer or url_for('financeiro.contas_pagar_receber'))



@financeiro_bp.route("/financeiro/previsao")
def previsao_fluxo_caixa():
    """Fluxo de caixa projetado calculado em Python.

    A consulta foi mantida deliberadamente simples para funcionar de forma
    idêntica em SQLite e PostgreSQL, sem aliases ou agregações específicas de
    um banco.
    """
    _sync_overdue_statuses()
    today = date.today()
    try:
        days = int(request.args.get("dias", "30"))
    except (TypeError, ValueError):
        days = 30
    days = days if days in {30, 60, 90} else 30
    account = request.args.get("conta", "").strip()
    end_date = today + timedelta(days=days - 1)

    clauses = ["status <> ?"]
    params = ["Cancelado"]
    if account:
        clauses.append("account = ?")
        params.append(account)

    transactions = query_db(f"""
        SELECT id, type, description, amount, transaction_date, due_date,
               status, category, account
        FROM financial_transactions
        WHERE {' AND '.join(clauses)}
        ORDER BY id
    """, tuple(params))

    opening_balance = 0.0
    daily = {}
    overdue_entries = 0.0
    overdue_exits = 0.0

    for item in transactions:
        amount = float(item["amount"] or 0)
        tx_type = item["type"]
        status = item["status"]

        if status == "Pago":
            raw_date = item["transaction_date"] or item["due_date"]
            try:
                paid_date = datetime.strptime(str(raw_date)[:10], "%Y-%m-%d").date()
            except (TypeError, ValueError):
                continue
            if paid_date <= today:
                opening_balance += amount if tx_type == "Entrada" else -amount
            continue

        if status not in {"Pendente", "Vencido"} or not item["due_date"]:
            continue
        try:
            due = datetime.strptime(str(item["due_date"])[:10], "%Y-%m-%d").date()
        except (TypeError, ValueError):
            continue
        if due > end_date:
            continue

        forecast_day = today if due < today else due
        bucket = daily.setdefault(
            forecast_day.isoformat(),
            {"entries": 0.0, "exits": 0.0, "items": []},
        )
        if tx_type == "Entrada":
            bucket["entries"] += amount
            if due < today:
                overdue_entries += amount
        else:
            bucket["exits"] += amount
            if due < today:
                overdue_exits += amount
        bucket["items"].append(item)

    rows = []
    balance = opening_balance
    lowest_balance = opening_balance
    lowest_date = today
    negative_days = 0
    total_entries = 0.0
    total_exits = 0.0

    for offset in range(days):
        day = today + timedelta(days=offset)
        bucket = daily.get(
            day.isoformat(), {"entries": 0.0, "exits": 0.0, "items": []}
        )
        entries = float(bucket["entries"])
        exits = float(bucket["exits"])
        balance += entries - exits
        total_entries += entries
        total_exits += exits
        if balance < lowest_balance:
            lowest_balance = balance
            lowest_date = day
        if balance < 0:
            negative_days += 1
        rows.append({
            "date": day.isoformat(),
            "date_br": day.strftime("%d/%m/%Y"),
            "weekday": ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"][day.weekday()],
            "entries": entries,
            "exits": exits,
            "net": entries - exits,
            "balance": balance,
            "items": bucket["items"],
        })

    accounts = query_db("""
        SELECT DISTINCT account FROM financial_transactions
        WHERE account IS NOT NULL AND account <> '' ORDER BY account
    """)
    chart_json = json.dumps({
        "labels": [r["date_br"][:5] for r in rows],
        "entries": [r["entries"] for r in rows],
        "exits": [r["exits"] for r in rows],
        "balance": [r["balance"] for r in rows],
    }, ensure_ascii=False)

    return render_template(
        "financeiro_previsao.html", rows=rows, dias=days, conta=account,
        contas=accounts, saldo_atual=opening_balance,
        entradas_previstas=total_entries, saidas_previstas=total_exits,
        saldo_final=balance, menor_saldo=lowest_balance,
        menor_saldo_data=lowest_date.strftime("%d/%m/%Y"),
        dias_negativos=negative_days, vencido_receber=overdue_entries,
        vencido_pagar=overdue_exits, grafico_json=chart_json,
        inicio=today.isoformat(), fim=end_date.isoformat(),
    )


@financeiro_bp.route("/financeiro/previsao.csv")
def exportar_previsao_csv():
    today = date.today()
    try:
        days = int(request.args.get("dias", "30"))
    except (TypeError, ValueError):
        days = 30
    days = days if days in {30, 60, 90} else 30
    account = request.args.get("conta", "").strip()
    end_date = today + timedelta(days=days - 1)

    clauses = ["status <> ?"]
    params = ["Cancelado"]
    if account:
        clauses.append("account = ?")
        params.append(account)
    items = query_db(f"""
        SELECT type, amount, transaction_date, due_date, status
        FROM financial_transactions
        WHERE {' AND '.join(clauses)}
        ORDER BY id
    """, tuple(params))

    balance = 0.0
    grouped = {}
    for item in items:
        amount = float(item["amount"] or 0)
        tx_type = item["type"]
        if item["status"] == "Pago":
            raw_date = item["transaction_date"] or item["due_date"]
            try:
                paid_date = datetime.strptime(str(raw_date)[:10], "%Y-%m-%d").date()
            except (TypeError, ValueError):
                continue
            if paid_date <= today:
                balance += amount if tx_type == "Entrada" else -amount
            continue
        if item["status"] not in {"Pendente", "Vencido"} or not item["due_date"]:
            continue
        try:
            due = datetime.strptime(str(item["due_date"])[:10], "%Y-%m-%d").date()
        except (TypeError, ValueError):
            continue
        day = today if due < today else due
        if day > end_date:
            continue
        grouped.setdefault(day.isoformat(), [0.0, 0.0])
        grouped[day.isoformat()][0 if tx_type == "Entrada" else 1] += amount

    stream = io.StringIO()
    writer = csv.writer(stream, delimiter=";")
    writer.writerow(["Data", "Dia", "Entradas previstas", "Saídas previstas", "Resultado do dia", "Saldo projetado"])
    weekdays = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
    for offset in range(days):
        day = today + timedelta(days=offset)
        entries, exits = grouped.get(day.isoformat(), [0.0, 0.0])
        balance += entries - exits
        writer.writerow([
            day.strftime("%d/%m/%Y"), weekdays[day.weekday()],
            f"{entries:.2f}".replace(".", ","),
            f"{exits:.2f}".replace(".", ","),
            f"{entries-exits:.2f}".replace(".", ","),
            f"{balance:.2f}".replace(".", ","),
        ])
    content = "\ufeff" + stream.getvalue()
    return Response(
        content, mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename=previsao_caixa_{days}_dias.csv"},
    )


@financeiro_bp.route("/financeiro/despesas", methods=["GET", "POST"])
def despesas_loja():
    """Cadastro simplificado de dívidas e despesas da loja."""
    if request.method == "POST":
        description = request.form.get("description", "").strip()
        amount = _decimal(request.form.get("amount"))
        due_date = request.form.get("due_date", "").strip()
        category = request.form.get("category", "Outros").strip() or "Outros"
        recurring = request.form.get("recurring") == "1"

        if not description or amount <= 0 or not due_date:
            flash("Informe descrição, valor e vencimento da despesa.", "danger")
            return redirect(url_for("financeiro.despesas_loja"))

        if recurring:
            frequency = request.form.get("frequency", "Mensal").strip() or "Mensal"
            recurrence_id = insert_db("""
                INSERT INTO financial_recurrences
                (transaction_type,category,description,amount,payment_method,account,
                 cost_center,frequency,start_date,next_due_date,end_date,active,notes,
                 created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                "Saída", category, description, amount,
                request.form.get("payment_method", "Boleto").strip() or "Boleto",
                request.form.get("account", "Caixa").strip() or "Caixa",
                "Despesas da loja", frequency, due_date, due_date,
                request.form.get("end_date", "").strip() or None, 1,
                request.form.get("notes", "").strip(), now_iso(), now_iso(),
            ))
            insert_db("""
                INSERT INTO financial_transactions
                (type,category,description,amount,payment_method,transaction_date,
                 due_date,status,reference,account,cost_center,installment_number,
                 total_installments,recurrence_id,notes,source_type,created_by,
                 created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                "Saída", category, description, amount,
                request.form.get("payment_method", "Boleto").strip() or "Boleto",
                due_date, due_date, "Pendente", f"DIV-REC-{recurrence_id}-{due_date}",
                request.form.get("account", "Caixa").strip() or "Caixa",
                "Despesas da loja", 1, 1, recurrence_id,
                request.form.get("notes", "").strip(), "Despesa recorrente",
                "Administrador", now_iso(), now_iso(),
            ))
            execute_db("""
                UPDATE financial_recurrences SET next_due_date=?, updated_at=? WHERE id=?
            """, (_next_date_for_route(due_date, frequency), now_iso(), recurrence_id))
        else:
            insert_db("""
                INSERT INTO financial_transactions
                (type,category,description,amount,payment_method,transaction_date,
                 due_date,status,reference,account,cost_center,installment_number,
                 total_installments,notes,source_type,created_by,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                "Saída", category, description, amount,
                request.form.get("payment_method", "Boleto").strip() or "Boleto",
                due_date, due_date, "Pendente", f"DIV-{now_iso().replace(':','').replace(' ','-')}",
                request.form.get("account", "Caixa").strip() or "Caixa",
                "Despesas da loja", 1, 1, request.form.get("notes", "").strip(),
                "Despesa da loja", "Administrador", now_iso(), now_iso(),
            ))
        flash("Despesa cadastrada com sucesso.", "success")
        return redirect(url_for("financeiro.despesas_loja"))

    _sync_overdue_statuses()
    items = query_db("""
        SELECT * FROM financial_transactions
        WHERE type='Saída' AND cost_center='Despesas da loja'
          AND status <> 'Cancelado'
        ORDER BY CASE WHEN status='Pago' THEN 1 ELSE 0 END, due_date, id DESC
    """)
    summary = query_db("""
        SELECT
          COALESCE(SUM(CASE WHEN status IN ('Pendente','Vencido') THEN amount ELSE 0 END),0) open_total,
          COALESCE(SUM(CASE WHEN status='Vencido' THEN amount ELSE 0 END),0) overdue_total,
          COALESCE(SUM(CASE WHEN status='Pago' THEN amount ELSE 0 END),0) paid_total,
          COUNT(CASE WHEN status IN ('Pendente','Parcial','Vencido') THEN 1 END) open_count
        FROM financial_transactions
        WHERE type='Saída' AND cost_center='Despesas da loja'
          AND status <> 'Cancelado'
    """, one=True)
    return render_template(
        "financeiro_despesas.html", despesas=items, resumo=summary,
        formas_pagamento=PAYMENT_METHODS, hoje=date.today().isoformat(),
    )


def _next_date_for_route(raw_date: str, frequency: str) -> str:
    base = datetime.strptime(raw_date, "%Y-%m-%d").date()
    normalized = (frequency or "Mensal").strip().lower()
    if normalized == "semanal":
        result = base + timedelta(days=7)
    elif normalized == "quinzenal":
        result = base + timedelta(days=15)
    elif normalized == "bimestral":
        result = _month_add(base, 2)
    elif normalized == "trimestral":
        result = _month_add(base, 3)
    elif normalized == "semestral":
        result = _month_add(base, 6)
    elif normalized == "anual":
        result = _month_add(base, 12)
    else:
        result = _month_add(base, 1)
    return result.isoformat()


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


@financeiro_bp.route("/financeiro/conciliacao")
def conciliacao_financeira():
    account = request.args.get("conta", "").strip()
    status = request.args.get("situacao", "").strip()
    month = request.args.get("mes", date.today().strftime("%Y-%m")).strip()

    accounts = query_db("SELECT * FROM financial_accounts WHERE active=1 ORDER BY name")
    if not accounts:
        insert_db(
            "INSERT INTO financial_accounts (name,account_type,opening_balance,active,created_at) VALUES (?,?,?,?,?)",
            ("Caixa", "Caixa", 0, 1, now_iso()),
        )
        accounts = query_db("SELECT * FROM financial_accounts WHERE active=1 ORDER BY name")

    clauses = ["status='Pago'"]
    params = []
    if month:
        clauses.append("SUBSTR(transaction_date,1,7)=?")
        params.append(month)
    if account:
        clauses.append("account=?")
        params.append(account)
    if status:
        clauses.append("COALESCE(reconciliation_status,'Pendente')=?")
        params.append(status)
    where = " AND ".join(clauses)
    transactions = query_db(
        f"SELECT * FROM financial_transactions WHERE {where} ORDER BY transaction_date DESC,id DESC",
        tuple(params),
    )

    totals = query_db(
        f"""SELECT
            COALESCE(SUM(CASE WHEN type='Entrada' THEN amount ELSE 0 END),0) AS entries,
            COALESCE(SUM(CASE WHEN type='Saída' THEN amount ELSE 0 END),0) AS exits,
            COALESCE(SUM(CASE WHEN COALESCE(reconciliation_status,'Pendente')='Conciliado' THEN amount ELSE 0 END),0) AS reconciled,
            COALESCE(SUM(CASE WHEN COALESCE(reconciliation_status,'Pendente')='Pendente' THEN amount ELSE 0 END),0) AS pending,
            COALESCE(SUM(CASE WHEN COALESCE(reconciliation_status,'Pendente')='Divergente' THEN amount ELSE 0 END),0) AS divergent
        FROM financial_transactions WHERE {where}""",
        tuple(params), one=True,
    )

    account_summaries = []
    for item in accounts:
        movement = query_db(
            """SELECT COALESCE(SUM(CASE WHEN type='Entrada' THEN amount ELSE -amount END),0) AS balance,
               COALESCE(SUM(CASE WHEN COALESCE(reconciliation_status,'Pendente')='Conciliado' THEN CASE WHEN type='Entrada' THEN amount ELSE -amount END ELSE 0 END),0) AS reconciled_balance
               FROM financial_transactions WHERE status='Pago' AND account=?""",
            (item["name"],), one=True,
        )
        opening = float(item["opening_balance"] or 0)
        account_summaries.append({
            "id": item["id"], "name": item["name"], "account_type": item["account_type"],
            "opening_balance": opening,
            "current_balance": opening + float(movement["balance"] or 0),
            "reconciled_balance": opening + float(movement["reconciled_balance"] or 0),
        })

    return render_template(
        "financeiro_conciliacao.html", accounts=accounts,
        account_summaries=account_summaries, transactions=transactions,
        totals=totals, conta=account, situacao=status, mes=month,
    )


@financeiro_bp.route("/financeiro/contas", methods=["POST"])
def adicionar_conta_financeira():
    name = request.form.get("name", "").strip()
    if not name:
        flash("Informe o nome da conta.", "danger")
        return redirect(url_for("financeiro.conciliacao_financeira"))
    existing = query_db("SELECT id FROM financial_accounts WHERE LOWER(name)=LOWER(?)", (name,), one=True)
    if existing:
        flash("Já existe uma conta com esse nome.", "warning")
        return redirect(url_for("financeiro.conciliacao_financeira"))
    insert_db(
        "INSERT INTO financial_accounts (name,account_type,opening_balance,active,created_at) VALUES (?,?,?,?,?)",
        (name, request.form.get("account_type", "Conta bancária"),
         _decimal(request.form.get("opening_balance")), 1, now_iso()),
    )
    flash("Conta financeira cadastrada.", "success")
    return redirect(url_for("financeiro.conciliacao_financeira"))


@financeiro_bp.route("/financeiro/<int:lancamento_id>/conciliar", methods=["POST"])
def conciliar_lancamento(lancamento_id):
    status = request.form.get("reconciliation_status", "Pendente")
    if status not in ("Conciliado", "Pendente", "Divergente"):
        status = "Pendente"
    execute_db(
        """UPDATE financial_transactions
           SET reconciliation_status=?,reconciled_at=?,bank_reference=?,updated_at=? WHERE id=?""",
        (status, now_iso() if status == "Conciliado" else None,
         request.form.get("bank_reference", "").strip(), now_iso(), lancamento_id),
    )
    flash(f"Lançamento marcado como {status.lower()}.", "success")
    return redirect(request.referrer or url_for("financeiro.conciliacao_financeira"))

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
    item = query_db("SELECT amount,paid_amount FROM financial_transactions WHERE id=?", (lancamento_id,), one=True)
    paid_amount = float(item["amount"] or 0) if new_status == "Pago" and item else float(item["paid_amount"] or 0) if item else 0
    if new_status in {"Pendente", "Vencido"}:
        paid_amount = 0
    execute_db("""
        UPDATE financial_transactions
        SET status=?,paid_amount=?,paid_at=?,last_payment_at=?,updated_at=?
        WHERE id=?
    """, (
        new_status, paid_amount, now_iso() if new_status == "Pago" else None,
        now_iso() if paid_amount else None, now_iso(), lancamento_id,
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

@financeiro_bp.route('/financeiro/relatorio-mensal')
def relatorio_mensal():
    gerar_recorrencias_pendentes()
    _sync_overdue_statuses()

    today = date.today()
    month = request.args.get('mes', f'{today.year:04d}-{today.month:02d}').strip()
    try:
        year, month_number = [int(part) for part in month.split('-', 1)]
        first_day = date(year, month_number, 1)
    except (TypeError, ValueError):
        first_day = date(today.year, today.month, 1)
        month = first_day.strftime('%Y-%m')
    last_day = date(first_day.year, first_day.month, monthrange(first_day.year, first_day.month)[1])

    summary = query_db("""
        SELECT
            COALESCE(SUM(CASE WHEN type='Entrada' AND status='Pago' THEN amount ELSE 0 END),0) AS paid_revenue,
            COALESCE(SUM(CASE WHEN type='Saída' AND status='Pago' THEN amount ELSE 0 END),0) AS paid_expenses,
            COALESCE(SUM(CASE WHEN type='Entrada' AND status IN ('Pendente','Parcial','Vencido') THEN (amount-COALESCE(paid_amount,0)) ELSE 0 END),0) AS receivable,
            COALESCE(SUM(CASE WHEN type='Saída' AND status IN ('Pendente','Parcial','Vencido') THEN (amount-COALESCE(paid_amount,0)) ELSE 0 END),0) AS payable,
            COALESCE(SUM(CASE WHEN status='Vencido' THEN amount ELSE 0 END),0) AS overdue,
            COUNT(*) AS transaction_count
        FROM financial_transactions
        WHERE SUBSTR(COALESCE(transaction_date,due_date),1,7)=?
          AND status <> 'Cancelado'
    """, (month,), one=True)

    paid_revenue = float(summary['paid_revenue'] or 0)
    paid_expenses = float(summary['paid_expenses'] or 0)
    profit = paid_revenue - paid_expenses
    margin = (profit / paid_revenue * 100) if paid_revenue else 0

    categories = query_db("""
        SELECT COALESCE(NULLIF(category,''),'Sem categoria') AS category_name,
               type AS transaction_type,
               COALESCE(SUM(amount),0) AS total_amount,
               COUNT(*) AS total_count
        FROM financial_transactions
        WHERE SUBSTR(COALESCE(transaction_date,due_date),1,7)=?
          AND status='Pago'
        GROUP BY COALESCE(NULLIF(category,''),'Sem categoria'), type
        ORDER BY transaction_type, total_amount DESC
    """, (month,))

    payments = query_db("""
        SELECT COALESCE(NULLIF(payment_method,''),'Não informado') AS payment_name,
               COALESCE(SUM(CASE WHEN type='Entrada' THEN amount ELSE 0 END),0) AS entries,
               COALESCE(SUM(CASE WHEN type='Saída' THEN amount ELSE 0 END),0) AS exits
        FROM financial_transactions
        WHERE SUBSTR(COALESCE(transaction_date,due_date),1,7)=?
          AND status='Pago'
        GROUP BY COALESCE(NULLIF(payment_method,''),'Não informado')
        ORDER BY entries DESC, exits DESC
    """, (month,))

    daily = query_db("""
        SELECT SUBSTR(COALESCE(transaction_date,due_date),1,10) AS reference_date,
               COALESCE(SUM(CASE WHEN type='Entrada' AND status='Pago' THEN amount ELSE 0 END),0) AS entries,
               COALESCE(SUM(CASE WHEN type='Saída' AND status='Pago' THEN amount ELSE 0 END),0) AS exits
        FROM financial_transactions
        WHERE SUBSTR(COALESCE(transaction_date,due_date),1,7)=?
          AND status <> 'Cancelado'
        GROUP BY SUBSTR(COALESCE(transaction_date,due_date),1,10)
        ORDER BY reference_date
    """, (month,))

    transactions = query_db("""
        SELECT * FROM financial_transactions
        WHERE SUBSTR(COALESCE(transaction_date,due_date),1,7)=?
          AND status <> 'Cancelado'
        ORDER BY COALESCE(transaction_date,due_date) DESC, id DESC
    """, (month,))

    previous_month_date = _month_add(first_day, -1)
    next_month_date = _month_add(first_day, 1)
    return render_template(
        'financeiro_relatorio_mensal.html',
        mes=month, inicio=first_day.isoformat(), fim=last_day.isoformat(),
        mes_anterior=previous_month_date.strftime('%Y-%m'),
        proximo_mes=next_month_date.strftime('%Y-%m'),
        resumo=summary, receitas=paid_revenue, despesas=paid_expenses,
        resultado=profit, margem=margin, categorias=categories,
        formas_pagamento=payments, diario=daily, lancamentos=transactions,
    )


@financeiro_bp.route('/financeiro/relatorio-mensal.csv')
def exportar_relatorio_mensal_csv():
    today = date.today()
    month = request.args.get('mes', f'{today.year:04d}-{today.month:02d}').strip()
    rows = query_db("""
        SELECT transaction_date, due_date, type, category, description, amount,
               payment_method, status, account, cost_center, reference, notes
        FROM financial_transactions
        WHERE SUBSTR(COALESCE(transaction_date,due_date),1,7)=?
          AND status <> 'Cancelado'
        ORDER BY COALESCE(transaction_date,due_date), id
    """, (month,))

    output = io.StringIO()
    output.write('\ufeff')
    writer = csv.writer(output, delimiter=';')
    writer.writerow(['Data', 'Vencimento', 'Tipo', 'Categoria', 'Descrição', 'Valor',
                     'Forma de pagamento', 'Status', 'Conta', 'Centro de custo',
                     'Referência', 'Observações'])
    for row in rows:
        writer.writerow([
            row['transaction_date'] or '', row['due_date'] or '', row['type'] or '',
            row['category'] or '', row['description'] or '',
            f"{float(row['amount'] or 0):.2f}".replace('.', ','),
            row['payment_method'] or '', row['status'] or '', row['account'] or '',
            row['cost_center'] or '', row['reference'] or '', row['notes'] or '',
        ])
    return Response(
        output.getvalue(), mimetype='text/csv; charset=utf-8',
        headers={'Content-Disposition': f'attachment; filename=relatorio_financeiro_{month}.csv'},
    )


@financeiro_bp.route('/financeiro/orcamento-mensal', methods=['GET', 'POST'])
def orcamento_mensal():
    gerar_recorrencias_pendentes()
    _sync_overdue_statuses()
    today = date.today()
    month = request.values.get('mes', f'{today.year:04d}-{today.month:02d}').strip()
    try:
        year, month_number = [int(part) for part in month.split('-', 1)]
        first_day = date(year, month_number, 1)
    except (TypeError, ValueError):
        first_day = date(today.year, today.month, 1)
        month = first_day.strftime('%Y-%m')

    if request.method == 'POST':
        category = request.form.get('category', '').strip()
        planned_amount = _decimal(request.form.get('planned_amount'))
        notes = request.form.get('notes', '').strip()
        if not category or planned_amount <= 0:
            flash('Informe a categoria e um valor planejado maior que zero.', 'danger')
            return redirect(url_for('financeiro.orcamento_mensal', mes=month))
        existing = query_db("""
            SELECT id FROM financial_category_budgets
            WHERE reference_month=? AND category=?
        """, (month, category), one=True)
        if existing:
            execute_db("""
                UPDATE financial_category_budgets
                SET planned_amount=?, notes=?, updated_at=? WHERE id=?
            """, (planned_amount, notes, now_iso(), existing['id']))
            flash('Orçamento da categoria atualizado.', 'success')
        else:
            insert_db("""
                INSERT INTO financial_category_budgets
                (reference_month, category, planned_amount, notes, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (month, category, planned_amount, notes, now_iso(), now_iso()))
            flash('Orçamento da categoria criado.', 'success')
        return redirect(url_for('financeiro.orcamento_mensal', mes=month))

    rows = query_db("""
        SELECT b.id, b.category, b.planned_amount, b.notes,
               COALESCE(SUM(CASE WHEN t.type='Saída' AND t.status='Pago' THEN t.amount ELSE 0 END),0) AS realized_amount,
               COALESCE(SUM(CASE WHEN t.type='Saída' AND t.status IN ('Pendente','Vencido') THEN t.amount ELSE 0 END),0) AS committed_amount
        FROM financial_category_budgets b
        LEFT JOIN financial_transactions t
          ON COALESCE(NULLIF(t.category,''),'Sem categoria')=b.category
         AND SUBSTR(COALESCE(t.transaction_date,t.due_date),1,7)=b.reference_month
         AND t.status <> 'Cancelado'
        WHERE b.reference_month=?
        GROUP BY b.id, b.category, b.planned_amount, b.notes
        ORDER BY b.category
    """, (month,))
    result=[]
    total_planned=total_realized=total_committed=0.0
    for row in rows:
        item=dict(row)
        planned=float(item.get('planned_amount') or 0)
        realized=float(item.get('realized_amount') or 0)
        committed=float(item.get('committed_amount') or 0)
        projected=realized+committed
        item['projected_amount']=projected
        item['remaining_amount']=planned-projected
        item['percent_used']=min(999.0, (projected/planned*100) if planned else 0)
        item['status_label']='Estourado' if projected>planned else ('Atenção' if projected>=planned*0.8 else 'Dentro do limite')
        result.append(item)
        total_planned += planned
        total_realized += realized
        total_committed += committed

    used_categories={r['category'] for r in result}
    categories=query_db("""
        SELECT DISTINCT COALESCE(NULLIF(category,''),'Sem categoria') AS category
        FROM financial_transactions
        WHERE type='Saída' ORDER BY category
    """)
    category_options=[r['category'] for r in categories if r['category'] not in used_categories]
    previous_month=_month_add(first_day,-1).strftime('%Y-%m')
    next_month=_month_add(first_day,1).strftime('%Y-%m')
    return render_template(
        'financeiro_orcamento_mensal.html', mes=month, orcamentos=result,
        categorias=category_options, total_planejado=total_planned,
        total_realizado=total_realized, total_comprometido=total_committed,
        total_projetado=total_realized+total_committed,
        total_disponivel=total_planned-total_realized-total_committed,
        mes_anterior=previous_month, proximo_mes=next_month,
    )


@financeiro_bp.route('/financeiro/orcamento-mensal/<int:budget_id>/excluir', methods=['POST'])
def excluir_orcamento_mensal(budget_id):
    month = request.form.get('mes', '').strip()
    execute_db('DELETE FROM financial_category_budgets WHERE id=?', (budget_id,))
    flash('Orçamento removido.', 'info')
    return redirect(url_for('financeiro.orcamento_mensal', mes=month))


@financeiro_bp.route('/financeiro/calendario')
def calendario_financeiro():
    """Agenda mensal de contas a pagar e receber."""
    gerar_recorrencias_pendentes()
    _sync_overdue_statuses()
    today = date.today()
    month = request.args.get('mes', f'{today.year:04d}-{today.month:02d}').strip()
    account = request.args.get('conta', '').strip()
    type_ = request.args.get('tipo', '').strip()

    try:
        year, month_number = [int(part) for part in month.split('-', 1)]
        first_day = date(year, month_number, 1)
    except (TypeError, ValueError):
        first_day = date(today.year, today.month, 1)
        year, month_number = first_day.year, first_day.month
        month = first_day.strftime('%Y-%m')

    last_day = date(year, month_number, monthrange(year, month_number)[1])
    clauses = ["status <> 'Cancelado'", "due_date >= ?", "due_date <= ?"]
    params = [first_day.isoformat(), last_day.isoformat()]
    if account:
        clauses.append('account = ?')
        params.append(account)
    if type_ in {'Entrada', 'Saída'}:
        clauses.append('type = ?')
        params.append(type_)

    transactions = query_db(f"""
        SELECT * FROM financial_transactions
        WHERE {' AND '.join(clauses)}
        ORDER BY due_date, CASE WHEN type='Saída' THEN 0 ELSE 1 END, id
    """, tuple(params))

    by_day = {}
    for row in transactions:
        key = str(row['due_date'])[:10]
        by_day.setdefault(key, []).append(row)

    # Grade sempre inicia na segunda-feira e termina no domingo.
    grid_start = first_day - timedelta(days=first_day.weekday())
    grid_end = last_day + timedelta(days=6 - last_day.weekday())
    weeks = []
    cursor = grid_start
    while cursor <= grid_end:
        week = []
        for _ in range(7):
            items = by_day.get(cursor.isoformat(), [])
            entries = sum(float(item['amount'] or 0) for item in items if item['type'] == 'Entrada')
            exits = sum(float(item['amount'] or 0) for item in items if item['type'] == 'Saída')
            open_items = [item for item in items if item['status'] in {'Pendente', 'Vencido'}]
            week.append({
                'date': cursor.isoformat(),
                'day': cursor.day,
                'in_month': cursor.month == month_number,
                'is_today': cursor == today,
                'is_past': cursor < today,
                'items': items,
                'open_count': len(open_items),
                'entries': entries,
                'exits': exits,
                'net': entries - exits,
            })
            cursor += timedelta(days=1)
        weeks.append(week)

    summary = query_db(f"""
        SELECT
          COALESCE(SUM(CASE WHEN type='Entrada' AND status IN ('Pendente','Parcial','Vencido') THEN (amount-COALESCE(paid_amount,0)) ELSE 0 END),0) AS receivable,
          COALESCE(SUM(CASE WHEN type='Saída' AND status IN ('Pendente','Parcial','Vencido') THEN (amount-COALESCE(paid_amount,0)) ELSE 0 END),0) AS payable,
          COALESCE(SUM(CASE WHEN status='Vencido' THEN amount ELSE 0 END),0) AS overdue,
          COALESCE(SUM(CASE WHEN status='Pago' THEN amount ELSE 0 END),0) AS paid,
          COUNT(CASE WHEN status IN ('Pendente','Parcial','Vencido') THEN 1 END) AS open_count
        FROM financial_transactions
        WHERE {' AND '.join(clauses)}
    """, tuple(params), one=True)

    alerts = query_db("""
        SELECT * FROM financial_transactions
        WHERE status IN ('Pendente','Vencido')
          AND due_date IS NOT NULL AND due_date <> ''
          AND due_date <= ?
        ORDER BY due_date, CASE WHEN type='Saída' THEN 0 ELSE 1 END, id
        LIMIT 20
    """, ((today + timedelta(days=7)).isoformat(),))

    accounts = query_db("""
        SELECT DISTINCT account FROM financial_transactions
        WHERE account IS NOT NULL AND account <> '' ORDER BY account
    """)
    previous_month = _month_add(first_day, -1).strftime('%Y-%m')
    next_month = _month_add(first_day, 1).strftime('%Y-%m')
    month_names = [
        '', 'Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
        'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'
    ]
    return render_template(
        'financeiro_calendario.html', semanas=weeks, resumo=summary,
        alertas=alerts, mes=month, mes_titulo=f'{month_names[month_number]} de {year}',
        mes_anterior=previous_month, proximo_mes=next_month, contas=accounts,
        conta=account, tipo=type_, hoje=today.isoformat(),
    )

@financeiro_bp.route('/financeiro/painel-gerencial')
def painel_gerencial_financeiro():
    """Visão executiva do financeiro com alertas e prioridades."""
    gerar_recorrencias_pendentes()
    _sync_overdue_statuses()
    today = date.today()
    month = request.args.get('mes', today.strftime('%Y-%m')).strip()
    try:
        year, month_number = [int(part) for part in month.split('-', 1)]
        first_day = date(year, month_number, 1)
    except (TypeError, ValueError):
        first_day = date(today.year, today.month, 1)
        year, month_number = first_day.year, first_day.month
        month = first_day.strftime('%Y-%m')
    last_day = date(year, month_number, monthrange(year, month_number)[1])

    summary = query_db("""
        SELECT
          COALESCE(SUM(CASE WHEN type='Entrada' AND status='Pago' THEN amount ELSE 0 END),0) AS revenue,
          COALESCE(SUM(CASE WHEN type='Saída' AND status='Pago' THEN amount ELSE 0 END),0) AS expenses,
          COALESCE(SUM(CASE WHEN type='Entrada' AND status IN ('Pendente','Parcial','Vencido') THEN (amount-COALESCE(paid_amount,0)) ELSE 0 END),0) AS receivable,
          COALESCE(SUM(CASE WHEN type='Saída' AND status IN ('Pendente','Parcial','Vencido') THEN (amount-COALESCE(paid_amount,0)) ELSE 0 END),0) AS payable,
          COALESCE(SUM(CASE WHEN status='Vencido' THEN amount ELSE 0 END),0) AS overdue,
          COUNT(CASE WHEN status IN ('Pendente','Parcial','Vencido') THEN 1 END) AS open_count
        FROM financial_transactions
        WHERE SUBSTR(COALESCE(due_date, transaction_date),1,7)=?
          AND status <> 'Cancelado'
    """, (month,), one=True)

    revenue = float(summary['revenue'] or 0)
    expenses = float(summary['expenses'] or 0)
    profit = revenue - expenses
    margin = (profit / revenue * 100) if revenue else 0

    next_days = query_db("""
        SELECT * FROM financial_transactions
        WHERE status IN ('Pendente','Vencido')
          AND due_date IS NOT NULL AND due_date <> ''
          AND due_date <= ?
        ORDER BY due_date, CASE WHEN type='Saída' THEN 0 ELSE 1 END, amount DESC
        LIMIT 12
    """, ((today + timedelta(days=7)).isoformat(),))

    categories = query_db("""
        SELECT COALESCE(NULLIF(category,''),'Sem categoria') AS category,
               COALESCE(SUM(CASE WHEN status='Pago' THEN amount ELSE 0 END),0) AS paid,
               COALESCE(SUM(CASE WHEN status IN ('Pendente','Vencido') THEN amount ELSE 0 END),0) AS committed
        FROM financial_transactions
        WHERE type='Saída' AND status <> 'Cancelado'
          AND SUBSTR(COALESCE(due_date, transaction_date),1,7)=?
        GROUP BY COALESCE(NULLIF(category,''),'Sem categoria')
        ORDER BY (COALESCE(SUM(CASE WHEN status='Pago' THEN amount ELSE 0 END),0) +
                  COALESCE(SUM(CASE WHEN status IN ('Pendente','Vencido') THEN amount ELSE 0 END),0)) DESC
        LIMIT 8
    """, (month,))

    budgets = query_db("""
        SELECT category, planned_amount FROM financial_category_budgets
        WHERE reference_month=? ORDER BY category
    """, (month,))
    budget_map = {str(row['category']): float(row['planned_amount'] or 0) for row in budgets}
    budget_alerts = []
    for row in categories:
        total = float(row['paid'] or 0) + float(row['committed'] or 0)
        planned = budget_map.get(str(row['category']), 0.0)
        if planned > 0:
            percent = total / planned * 100
            if percent >= 80:
                budget_alerts.append({
                    'category': row['category'], 'planned': planned, 'total': total,
                    'percent': percent, 'status': 'Estourado' if percent > 100 else 'Atenção'
                })

    daily = query_db("""
        SELECT SUBSTR(COALESCE(paid_at, transaction_date),1,10) AS movement_date,
               COALESCE(SUM(CASE WHEN type='Entrada' AND status='Pago' THEN amount ELSE 0 END),0) AS entries,
               COALESCE(SUM(CASE WHEN type='Saída' AND status='Pago' THEN amount ELSE 0 END),0) AS exits
        FROM financial_transactions
        WHERE status='Pago' AND SUBSTR(COALESCE(paid_at, transaction_date),1,7)=?
        GROUP BY SUBSTR(COALESCE(paid_at, transaction_date),1,10)
        ORDER BY movement_date
    """, (month,))

    previous_month = _month_add(first_day, -1).strftime('%Y-%m')
    next_month = _month_add(first_day, 1).strftime('%Y-%m')
    month_names = ['', 'Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
                   'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
    return render_template(
        'financeiro_painel_gerencial.html', resumo=summary, receita=revenue,
        despesas=expenses, resultado=profit, margem=margin, proximos=next_days,
        categorias=categories, alertas_orcamento=budget_alerts, diario=daily,
        mes=month, mes_titulo=f'{month_names[month_number]} de {year}',
        mes_anterior=previous_month, proximo_mes=next_month, hoje=today.isoformat(),
    )

@financeiro_bp.route('/financeiro/comparativo')
def comparativo_financeiro():
    """Comparativo financeiro dos últimos meses, calculado em Python para SQLite/PostgreSQL."""
    gerar_recorrencias_pendentes()
    _sync_overdue_statuses()

    today = date.today()
    months_count = _integer(request.args.get('meses'), 6)
    months_count = min(max(months_count, 3), 24)
    end_month = request.args.get('ate', today.strftime('%Y-%m')).strip()
    try:
        end_year, end_number = [int(part) for part in end_month.split('-', 1)]
        end_date = date(end_year, end_number, 1)
    except (TypeError, ValueError):
        end_date = date(today.year, today.month, 1)
        end_month = end_date.strftime('%Y-%m')

    month_keys = [_month_add(end_date, offset).strftime('%Y-%m') for offset in range(-(months_count - 1), 1)]
    start_key = month_keys[0]

    transactions = query_db("""
        SELECT type, amount, status, transaction_date, due_date, paid_at, category
        FROM financial_transactions
        WHERE status <> 'Cancelado'
          AND SUBSTR(COALESCE(due_date, transaction_date),1,7) >= ?
          AND SUBSTR(COALESCE(due_date, transaction_date),1,7) <= ?
        ORDER BY COALESCE(due_date, transaction_date), id
    """, (start_key, end_month))

    data = {
        key: {
            'month': key, 'revenue': 0.0, 'expenses': 0.0, 'profit': 0.0,
            'margin': 0.0, 'receivable': 0.0, 'payable': 0.0, 'overdue': 0.0,
        }
        for key in month_keys
    }
    category_totals = {}

    for row in transactions:
        reference = str(row['due_date'] or row['transaction_date'] or '')[:7]
        if reference not in data:
            continue
        amount = float(row['amount'] or 0)
        type_ = str(row['type'] or '')
        status = str(row['status'] or '')
        if status == 'Pago':
            if type_ == 'Entrada':
                data[reference]['revenue'] += amount
            elif type_ == 'Saída':
                data[reference]['expenses'] += amount
                category = str(row['category'] or 'Sem categoria')
                category_totals[category] = category_totals.get(category, 0.0) + amount
        elif status in {'Pendente', 'Vencido'}:
            if type_ == 'Entrada':
                data[reference]['receivable'] += amount
            elif type_ == 'Saída':
                data[reference]['payable'] += amount
            if status == 'Vencido':
                data[reference]['overdue'] += amount

    rows = []
    previous_revenue = None
    previous_profit = None
    for key in month_keys:
        item = data[key]
        item['profit'] = item['revenue'] - item['expenses']
        item['margin'] = (item['profit'] / item['revenue'] * 100) if item['revenue'] else 0.0
        item['revenue_change'] = (
            ((item['revenue'] - previous_revenue) / previous_revenue * 100)
            if previous_revenue not in (None, 0) else None
        )
        item['profit_change'] = (
            item['profit'] - previous_profit if previous_profit is not None else None
        )
        previous_revenue = item['revenue']
        previous_profit = item['profit']
        rows.append(item)

    total_revenue = sum(item['revenue'] for item in rows)
    total_expenses = sum(item['expenses'] for item in rows)
    total_profit = total_revenue - total_expenses
    average_revenue = total_revenue / len(rows) if rows else 0
    average_profit = total_profit / len(rows) if rows else 0
    best_month = max(rows, key=lambda item: item['profit']) if rows else None
    worst_month = min(rows, key=lambda item: item['profit']) if rows else None
    top_categories = sorted(
        ({'category': key, 'amount': value} for key, value in category_totals.items()),
        key=lambda item: item['amount'], reverse=True,
    )[:8]

    labels = []
    month_names_short = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']
    for key in month_keys:
        year, number = [int(part) for part in key.split('-')]
        labels.append(f'{month_names_short[number - 1]}/{str(year)[2:]}')

    chart_json = json.dumps({
        'labels': labels,
        'revenue': [round(item['revenue'], 2) for item in rows],
        'expenses': [round(item['expenses'], 2) for item in rows],
        'profit': [round(item['profit'], 2) for item in rows],
        'margin': [round(item['margin'], 2) for item in rows],
    }, ensure_ascii=False)

    previous_end = _month_add(end_date, -1).strftime('%Y-%m')
    next_end = _month_add(end_date, 1).strftime('%Y-%m')
    return render_template(
        'financeiro_comparativo.html', rows=rows, meses=months_count, ate=end_month,
        total_receita=total_revenue, total_despesas=total_expenses,
        total_resultado=total_profit, media_receita=average_revenue,
        media_resultado=average_profit, melhor_mes=best_month, pior_mes=worst_month,
        categorias=top_categories, chart_json=chart_json,
        periodo_inicio=start_key, periodo_fim=end_month,
        ate_anterior=previous_end, ate_proximo=next_end,
    )
