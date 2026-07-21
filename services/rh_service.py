from calendar import monthrange
from datetime import date, timedelta

from database import execute_db, now_iso, query_db


def _minutes(value):
    if not value:
        return None
    try:
        value = str(value).split(" ")[-1]
        hour, minute = value[:5].split(":")
        return int(hour) * 60 + int(minute)
    except (TypeError, ValueError):
        return None


def format_minutes(value):
    value = int(value or 0)
    sign = "-" if value < 0 else ""
    value = abs(value)
    return f"{sign}{value // 60:02d}:{value % 60:02d}"


def month_range(reference_month):
    year, month = map(int, reference_month.split("-"))
    first = date(year, month, 1)
    last = date(year, month, monthrange(year, month)[1])
    return first, last


def calculate_employee_month(employee_id, reference_month):
    first, last = month_range(reference_month)
    employee = query_db("""
        SELECT e.*, w.entrada, w.saida_intervalo, w.retorno_intervalo, w.saida_final
        FROM employees e LEFT JOIN work_schedules w ON w.id=e.schedule_id
        WHERE e.id=?
    """, (employee_id,), one=True)
    if not employee:
        return None

    records = query_db("""
        SELECT record_type, record_time FROM time_records
        WHERE employee_id=? AND record_time>=? AND record_time<?
        ORDER BY record_time
    """, (employee_id, first.isoformat(), (last + timedelta(days=1)).isoformat()))

    by_day = {}
    for record in records:
        day = str(record["record_time"])[:10]
        by_day.setdefault(day, {})[record["record_type"]] = record["record_time"]

    worked = 0
    for day_records in by_day.values():
        entry = _minutes(day_records.get("Entrada"))
        pause = _minutes(day_records.get("Saída intervalo"))
        back = _minutes(day_records.get("Retorno intervalo"))
        exit_time = _minutes(day_records.get("Saída"))
        daily = 0
        if entry is not None and pause is not None:
            daily += max(0, pause - entry)
        if back is not None and exit_time is not None:
            daily += max(0, exit_time - back)
        elif entry is not None and exit_time is not None:
            daily = max(0, exit_time - entry)
        worked += daily

    start = _minutes(employee["entrada"])
    pause = _minutes(employee["saida_intervalo"])
    back = _minutes(employee["retorno_intervalo"])
    end = _minutes(employee["saida_final"])
    expected_daily = max(0, end - start) if start is not None and end is not None else 480
    if pause is not None and back is not None:
        expected_daily -= max(0, back - pause)

    useful_days = 0
    current = first
    while current <= last:
        if current.weekday() < 6:
            useful_days += 1
        current += timedelta(days=1)

    absences_rows = query_db("""
        SELECT justified, start_date, COALESCE(NULLIF(end_date,''), start_date) end_date
        FROM employee_absences
        WHERE employee_id=? AND start_date<=?
          AND COALESCE(NULLIF(end_date,''), start_date)>=?
    """, (employee_id, last.isoformat(), first.isoformat()))

    absences = 0
    justified = 0
    for row in absences_rows:
        start_date = max(first, date.fromisoformat(row["start_date"]))
        end_date = min(last, date.fromisoformat(row["end_date"]))
        days = (end_date - start_date).days + 1
        absences += days
        if row["justified"]:
            justified += days

    expected = max(0, useful_days * expected_daily - justified * expected_daily)

    commission = query_db("""
        SELECT COALESCE(SUM(amount),0) total FROM employee_commissions
        WHERE employee_id=? AND created_at>=? AND created_at<?
    """, (employee_id, first.isoformat(), (last + timedelta(days=1)).isoformat()), one=True)

    adjustments = query_db("""
        SELECT adjustment_type, COALESCE(SUM(amount),0) total
        FROM employee_adjustments
        WHERE employee_id=? AND reference_month=?
        GROUP BY adjustment_type
    """, (employee_id, reference_month))

    additions = sum(float(x["total"] or 0) for x in adjustments
                    if x["adjustment_type"] in ("Adiantamento", "Bônus", "Acréscimo"))
    deductions = sum(float(x["total"] or 0) for x in adjustments
                     if x["adjustment_type"] in ("Desconto", "Vale", "Falta"))
    balance = worked - expected

    return {
        "employee": employee,
        "worked_minutes": worked,
        "expected_minutes": expected,
        "balance_minutes": balance,
        "worked_label": format_minutes(worked),
        "expected_label": format_minutes(expected),
        "balance_label": format_minutes(balance),
        "absences": absences,
        "justified_absences": justified,
        "commission_amount": float(commission["total"] or 0),
        "additions": additions,
        "deductions": deductions,
    }


def get_rh_dashboard(reference_month):
    employees = query_db("""
        SELECT e.*, w.description schedule_name
        FROM employees e LEFT JOIN work_schedules w ON w.id=e.schedule_id
        WHERE e.active=1 ORDER BY e.name
    """)
    summaries = [calculate_employee_month(e["id"], reference_month) for e in employees]
    summaries = [x for x in summaries if x]

    today = date.today().isoformat()
    present = query_db("SELECT COUNT(DISTINCT employee_id) total FROM time_records WHERE record_time LIKE ?",
                       (f"{today}%",), one=True)
    away = query_db("""SELECT COUNT(DISTINCT employee_id) total FROM employee_absences
        WHERE start_date<=? AND COALESCE(NULLIF(end_date,''),start_date)>=?""",
        (today, today), one=True)
    vacations_now = query_db("""SELECT COUNT(DISTINCT employee_id) total FROM employee_vacations
        WHERE start_date<=? AND end_date>=? AND status IN ('Aprovada','Em andamento')""",
        (today, today), one=True)

    vacations = query_db("""SELECT v.*, e.name employee_name FROM employee_vacations v
        LEFT JOIN employees e ON e.id=v.employee_id ORDER BY v.start_date DESC LIMIT 30""")
    adjustments = query_db("""SELECT a.*, e.name employee_name FROM employee_adjustments a
        LEFT JOIN employees e ON e.id=a.employee_id
        WHERE a.reference_month=? ORDER BY a.created_at DESC LIMIT 30""", (reference_month,))

    return {
        "employees": employees,
        "summaries": summaries,
        "vacations": vacations,
        "adjustments": adjustments,
        "metrics": {
            "active": len(employees),
            "present": int(present["total"] or 0),
            "away": int(away["total"] or 0),
            "vacations": int(vacations_now["total"] or 0),
            "hour_balance": format_minutes(sum(x["balance_minutes"] for x in summaries)),
            "commissions": sum(x["commission_amount"] for x in summaries),
        },
    }


def close_month(employee_id, reference_month, user_name):
    item = calculate_employee_month(employee_id, reference_month)
    if not item:
        raise ValueError("Funcionário não encontrado.")

    execute_db("""
        INSERT INTO employee_monthly_closings
        (employee_id, reference_month, worked_minutes, expected_minutes, balance_minutes,
         absences, justified_absences, commission_amount, additions, deductions,
         status, closed_by, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Fechado', ?, ?, ?)
        ON CONFLICT(employee_id, reference_month) DO UPDATE SET
        worked_minutes=excluded.worked_minutes,
        expected_minutes=excluded.expected_minutes,
        balance_minutes=excluded.balance_minutes,
        absences=excluded.absences,
        justified_absences=excluded.justified_absences,
        commission_amount=excluded.commission_amount,
        additions=excluded.additions,
        deductions=excluded.deductions,
        status='Fechado', closed_by=excluded.closed_by, updated_at=excluded.updated_at
    """, (employee_id, reference_month, item["worked_minutes"], item["expected_minutes"],
          item["balance_minutes"], item["absences"], item["justified_absences"],
          item["commission_amount"], item["additions"], item["deductions"],
          user_name, now_iso(), now_iso()))

    execute_db("""INSERT INTO employee_hour_bank
        (employee_id, reference_date, minutes, movement_type, description,
         approved, approved_by, created_at)
        VALUES (?, ?, ?, 'Fechamento mensal', ?, 1, ?, ?)""",
        (employee_id, f"{reference_month}-01", item["balance_minutes"],
         f"Fechamento de {reference_month}", user_name, now_iso()))
    return item
