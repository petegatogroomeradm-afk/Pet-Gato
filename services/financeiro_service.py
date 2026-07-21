from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime, timedelta

from database import execute_db, insert_db, now_iso, query_db


def _add_months(value: date, months: int) -> date:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, monthrange(year, month)[1])
    return date(year, month, day)


def _next_date(current: date, frequency: str) -> date:
    normalized = (frequency or "Mensal").strip().lower()
    if normalized == "semanal":
        return current + timedelta(days=7)
    if normalized == "quinzenal":
        return current + timedelta(days=15)
    if normalized == "bimestral":
        return _add_months(current, 2)
    if normalized == "trimestral":
        return _add_months(current, 3)
    if normalized == "semestral":
        return _add_months(current, 6)
    if normalized == "anual":
        return _add_months(current, 12)
    return _add_months(current, 1)


def lancar_receita_banho_tosa(atendimento_id):
    servico = query_db("""
        SELECT g.*, c.nome AS cliente_nome, p.nome AS pet_nome
        FROM grooming_services g
        LEFT JOIN clients c ON c.id = g.client_id
        LEFT JOIN pets p ON p.id = g.pet_id
        WHERE g.id = ?
    """, (atendimento_id,), one=True)
    if not servico:
        return False

    existe = query_db(
        "SELECT id FROM financial_transactions WHERE source_type = ? AND source_id = ? LIMIT 1",
        ("Banho e Tosa", atendimento_id), one=True,
    )
    if existe:
        return False

    descricao = f"Banho e Tosa - {servico['pet_nome']} - {servico['cliente_nome']}"
    metodo = servico["payment_method"] or "A definir"
    status = "Pago" if metodo not in {"", "A definir", None} else "Pendente"
    execute_db("""
        INSERT INTO financial_transactions
        (type, category, description, amount, payment_method, transaction_date, due_date,
         status, reference, account, cost_center, paid_at, notes, source_type, source_id,
         created_by, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        "Entrada", "Banho e Tosa", descricao, servico["valor"] or 0, metodo,
        servico["data"], servico["data"], status, f"ATEND-{atendimento_id}", "Caixa",
        "Operação", now_iso() if status == "Pago" else None,
        servico["observacoes"] or "", "Banho e Tosa", atendimento_id,
        "Sistema", now_iso(), now_iso(),
    ))
    return True


def gerar_recorrencias_pendentes(reference_date: date | None = None) -> int:
    reference_date = reference_date or date.today()
    generated = 0

    recurrences = query_db("""
        SELECT *
        FROM financial_recurrences
        WHERE active = 1
          AND next_due_date IS NOT NULL
          AND next_due_date <> ''
          AND next_due_date <= ?
        ORDER BY next_due_date, id
    """, (reference_date.isoformat(),))

    for recurrence in recurrences:
        due = datetime.strptime(recurrence["next_due_date"], "%Y-%m-%d").date()
        end_date = None
        if recurrence["end_date"]:
            end_date = datetime.strptime(recurrence["end_date"], "%Y-%m-%d").date()

        while due <= reference_date and (end_date is None or due <= end_date):
            exists = query_db("""
                SELECT id FROM financial_transactions
                WHERE recurrence_id = ? AND due_date = ?
                LIMIT 1
            """, (recurrence["id"], due.isoformat()), one=True)

            if not exists:
                insert_db("""
                    INSERT INTO financial_transactions
                    (type, category, description, amount, payment_method,
                     transaction_date, due_date, status, reference, account,
                     cost_center, installment_number, total_installments,
                     recurrence_id, notes, source_type, created_by, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    recurrence["transaction_type"], recurrence["category"],
                    recurrence["description"], recurrence["amount"],
                    recurrence["payment_method"], due.isoformat(), due.isoformat(),
                    "Pendente", f"REC-{recurrence['id']}-{due.isoformat()}",
                    recurrence["account"] or "Caixa", recurrence["cost_center"],
                    1, 1, recurrence["id"], recurrence["notes"] or "",
                    "Recorrência", "Sistema", now_iso(), now_iso(),
                ))
                generated += 1

            due = _next_date(due, recurrence["frequency"])

        active = 0 if end_date is not None and due > end_date else 1
        execute_db("""
            UPDATE financial_recurrences
            SET next_due_date = ?, active = ?, updated_at = ?
            WHERE id = ?
        """, (due.isoformat(), active, now_iso(), recurrence["id"]))

    return generated


def get_goal(reference_month: str):
    goal = query_db(
        "SELECT * FROM financial_goals WHERE reference_month = ?",
        (reference_month,), one=True,
    )
    if goal:
        return goal
    return {
        "reference_month": reference_month,
        "revenue_goal": 0,
        "expense_limit": 0,
    }


def calculate_dre(month: str):
    data = query_db("""
        SELECT
            COALESCE(SUM(CASE WHEN type='Entrada' AND status='Pago' THEN amount ELSE 0 END),0) revenue,
            COALESCE(SUM(CASE WHEN type='Saída' AND status='Pago' THEN amount ELSE 0 END),0) expenses
        FROM financial_transactions
        WHERE SUBSTR(transaction_date,1,7)=?
          AND status <> 'Cancelado'
    """, (month,), one=True)
    revenue = float(data["revenue"] or 0)
    expenses = float(data["expenses"] or 0)
    profit = revenue - expenses
    margin = (profit / revenue * 100) if revenue else 0
    return {
        "revenue": revenue,
        "expenses": expenses,
        "profit": profit,
        "margin": margin,
    }


def daily_cash_flow(month: str):
    return query_db("""
        SELECT SUBSTR(transaction_date,1,10) day,
               COALESCE(SUM(CASE WHEN type='Entrada' AND status='Pago' THEN amount ELSE 0 END),0) entries,
               COALESCE(SUM(CASE WHEN type='Saída' AND status='Pago' THEN amount ELSE 0 END),0) exits
        FROM financial_transactions
        WHERE SUBSTR(transaction_date,1,7)=?
          AND status <> 'Cancelado'
        GROUP BY SUBSTR(transaction_date,1,10)
        ORDER BY day
    """, (month,))
