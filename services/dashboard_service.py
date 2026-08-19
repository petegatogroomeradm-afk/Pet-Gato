from __future__ import annotations

import os
from collections import defaultdict
from datetime import date, datetime, timedelta

from core.cache import cache
from database import query_db


def _scalar(sql: str, params=(), key: str = "total"):
    row = query_db(sql, params, one=True)
    if not row:
        return 0
    value = row[key]
    return value or 0


def _iso(value: date) -> str:
    return value.isoformat()


def _month_start(value: date) -> date:
    return value.replace(day=1)


def _shift_month(value: date, months: int) -> date:
    total = value.year * 12 + value.month - 1 + months
    year, month_index = divmod(total, 12)
    return date(year, month_index + 1, 1)


def _percent_change(current: float, previous: float) -> float:
    current = float(current or 0)
    previous = float(previous or 0)
    if previous == 0:
        return 100.0 if current > 0 else 0.0
    return ((current - previous) / abs(previous)) * 100


def _sum_transactions(start: date, end: date, transaction_type: str = "Entrada") -> float:
    return float(
        _scalar(
            """
            SELECT COALESCE(SUM(amount), 0) AS total
            FROM financial_transactions
            WHERE type = ?
              AND status = 'Pago'
              AND transaction_date >= ?
              AND transaction_date < ?
            """,
            (transaction_type, _iso(start), _iso(end)),
        )
    )


def _build_financial_series(start: date, days: int):
    end = start + timedelta(days=days)
    rows = query_db(
        """
        SELECT transaction_date, type, amount
        FROM financial_transactions
        WHERE status = 'Pago'
          AND transaction_date >= ?
          AND transaction_date < ?
        ORDER BY transaction_date
        """,
        (_iso(start), _iso(end)),
    )
    data = {
        _iso(start + timedelta(days=i)): {"entradas": 0.0, "saidas": 0.0}
        for i in range(days)
    }
    for row in rows:
        key = (row["transaction_date"] or "")[:10]
        if key not in data:
            continue
        amount = float(row["amount"] or 0)
        if row["type"] == "Entrada":
            data[key]["entradas"] += amount
        elif row["type"] == "Saída":
            data[key]["saidas"] += amount

    result = []
    for key, values in data.items():
        parsed = datetime.strptime(key, "%Y-%m-%d")
        result.append(
            {
                "date": key,
                "label": parsed.strftime("%d/%m"),
                "entradas": round(values["entradas"], 2),
                "saidas": round(values["saidas"], 2),
                "lucro": round(values["entradas"] - values["saidas"], 2),
            }
        )
    return result




def _build_monthly_revenue(today: date, months: int = 12):
    start = _shift_month(_month_start(today), -(months - 1))
    end = _shift_month(_month_start(today), 1)
    rows = query_db(
        """
        SELECT transaction_date, amount
        FROM financial_transactions
        WHERE type = 'Entrada'
          AND status = 'Pago'
          AND transaction_date >= ?
          AND transaction_date < ?
        ORDER BY transaction_date
        """,
        (_iso(start), _iso(end)),
    )
    totals = {}
    for index in range(months):
        month = _shift_month(start, index)
        totals[month.strftime('%Y-%m')] = 0.0
    for row in rows:
        key = str(row['transaction_date'] or '')[:7]
        if key in totals:
            totals[key] += float(row['amount'] or 0)
    labels = ('Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez')
    result = []
    for key, value in totals.items():
        year, month = [int(part) for part in key.split('-')]
        result.append({
            'month': key,
            'label': f'{labels[month - 1]}/{str(year)[2:]}',
            'value': round(value, 2),
        })
    return result


def _build_customer_growth(today: date, months: int = 12):
    start = _shift_month(_month_start(today), -(months - 1))
    end = _shift_month(_month_start(today), 1)
    rows = query_db(
        """
        SELECT created_at
        FROM clients
        WHERE created_at >= ?
          AND created_at < ?
        ORDER BY created_at
        """,
        (f'{_iso(start)} 00:00:00', f'{_iso(end)} 00:00:00'),
    )
    totals = {}
    for index in range(months):
        month = _shift_month(start, index)
        totals[month.strftime('%Y-%m')] = 0
    for row in rows:
        key = str(row['created_at'] or '')[:7]
        if key in totals:
            totals[key] += 1
    labels = ('Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez')
    result = []
    for key, value in totals.items():
        year, month = [int(part) for part in key.split('-')]
        result.append({
            'month': key,
            'label': f'{labels[month - 1]}/{str(year)[2:]}',
            'value': int(value),
        })
    return result


def _build_alerts(today: date):
    alerts = []

    stock = query_db(
        """
        SELECT id, name, quantity, min_quantity, unit, expiration_date
        FROM stock_products
        WHERE COALESCE(active, 1) = 1
          AND COALESCE(quantity, 0) <= COALESCE(min_quantity, 0)
        ORDER BY quantity, name
        LIMIT 6
        """
    )
    for item in stock:
        alerts.append(
            {
                "kind": "danger",
                "icon": "📦",
                "title": item["name"],
                "message": (
                    f"Estoque atual: {item['quantity'] or 0} {item['unit'] or ''}. "
                    f"Mínimo: {item['min_quantity'] or 0}."
                ),
                "url": "/estoque",
            }
        )

    limit_date = today + timedelta(days=30)
    vaccines = query_db(
        """
        SELECT v.vacina, v.proxima_dose, p.nome AS pet_nome, p.id AS pet_id
        FROM pet_vaccines v
        LEFT JOIN pets p ON p.id = v.pet_id
        WHERE v.proxima_dose IS NOT NULL
          AND v.proxima_dose <> ''
          AND v.proxima_dose <= ?
        ORDER BY v.proxima_dose
        LIMIT 6
        """,
        (_iso(limit_date),),
    )
    for item in vaccines:
        expired = item["proxima_dose"] < _iso(today)
        alerts.append(
            {
                "kind": "danger" if expired else "warning",
                "icon": "💉",
                "title": f"{item['vacina']} · {item['pet_nome'] or 'Pet'}",
                "message": (
                    f"Vencida em {item['proxima_dose']}."
                    if expired
                    else f"Próxima dose em {item['proxima_dose']}."
                ),
                "url": f"/pets/{item['pet_id']}",
            }
        )

    treatments = query_db(
        """
        SELECT t.treatment_type, t.next_date, p.nome AS pet_nome, p.id AS pet_id
        FROM pet_health_treatments t
        LEFT JOIN pets p ON p.id = t.pet_id
        WHERE t.next_date IS NOT NULL
          AND t.next_date <> ''
          AND t.next_date <= ?
        ORDER BY t.next_date
        LIMIT 6
        """,
        (_iso(limit_date),),
    )
    for item in treatments:
        expired = item["next_date"] < _iso(today)
        alerts.append(
            {
                "kind": "danger" if expired else "warning",
                "icon": "🩺",
                "title": f"{item['treatment_type']} · {item['pet_nome'] or 'Pet'}",
                "message": (
                    f"Aplicação vencida em {item['next_date']}."
                    if expired
                    else f"Próxima aplicação em {item['next_date']}."
                ),
                "url": f"/pets/{item['pet_id']}",
            }
        )

    inactive_limit = today - timedelta(days=60)
    inactive = query_db(
        """
        SELECT
            c.id,
            c.nome,
            MAX(g.data) AS ultima_visita
        FROM clients c
        LEFT JOIN grooming_services g ON g.client_id = c.id
        WHERE COALESCE(c.ativo, 1) = 1
        GROUP BY c.id, c.nome
        HAVING MAX(g.data) IS NOT NULL
           AND MAX(g.data) < ?
        ORDER BY ultima_visita
        LIMIT 5
        """,
        (_iso(inactive_limit),),
    )
    for item in inactive:
        alerts.append(
            {
                "kind": "info",
                "icon": "👥",
                "title": item["nome"],
                "message": f"Sem retornar desde {item['ultima_visita']}.",
                "url": f"/clientes/{item['id']}",
            }
        )

    return alerts[:15]


def _build_insights(data):
    insights = []
    current = data["financeiro"]["receita_hoje"]
    previous = data["comparativos"]["receita_ontem"]
    change = data["comparativos"]["receita_hoje_pct"]

    if current > previous:
        insights.append(
            f"O faturamento de hoje está {abs(change):.1f}% acima de ontem."
        )
    elif current < previous:
        insights.append(
            f"O faturamento de hoje está {abs(change):.1f}% abaixo de ontem."
        )
    else:
        insights.append("O faturamento de hoje está no mesmo nível de ontem.")

    goal_pct = data["financeiro"]["meta_percentual"]
    if goal_pct >= 100:
        insights.append("A meta mensal já foi atingida. Excelente desempenho.")
    elif goal_pct >= 70:
        insights.append("A meta mensal está próxima: faltam menos de 30%.")
    else:
        insights.append(
            f"A meta mensal está em {goal_pct:.1f}%. Reforce agendamentos e retornos."
        )

    if data["cards"]["estoque_critico"]:
        insights.append(
            f"Há {data['cards']['estoque_critico']} produto(s) em nível crítico."
        )

    if data["cards"]["vacinas_alerta"]:
        insights.append(
            f"Existem {data['cards']['vacinas_alerta']} vacina(s) vencida(s) ou próximas."
        )

    if data["cards"]["clientes_inativos"]:
        insights.append(
            f"{data['cards']['clientes_inativos']} cliente(s) estão há mais de 60 dias sem retornar."
        )

    return insights[:5]


def _build_dashboard():
    today = date.today()
    tomorrow = today + timedelta(days=1)
    yesterday = today - timedelta(days=1)
    week_start = today - timedelta(days=today.weekday())
    next_week = week_start + timedelta(days=7)
    month_start = _month_start(today)
    next_month = _shift_month(month_start, 1)
    previous_month = _shift_month(month_start, -1)
    year_start = date(today.year, 1, 1)
    next_year = date(today.year + 1, 1, 1)

    receita_hoje = _sum_transactions(today, tomorrow)
    receita_ontem = _sum_transactions(yesterday, today)
    receita_semana = _sum_transactions(week_start, next_week)
    receita_mes = _sum_transactions(month_start, next_month)
    receita_mes_anterior = _sum_transactions(previous_month, month_start)
    receita_ano = _sum_transactions(year_start, next_year)
    saidas_mes = _sum_transactions(month_start, next_month, "Saída")
    saldo_mes = receita_mes - saidas_mes
    margem_mes = (saldo_mes / receita_mes * 100) if receita_mes else 0

    atendimentos_mes = int(
        _scalar(
            """
            SELECT COUNT(*) AS total
            FROM grooming_services
            WHERE data >= ?
              AND data < ?
              AND status <> 'Cancelado'
            """,
            (_iso(month_start), _iso(next_month)),
        )
    )
    ticket_medio = receita_mes / atendimentos_mes if atendimentos_mes else 0

    monthly_goal = float(os.environ.get("DASHBOARD_MONTHLY_GOAL", "30000"))
    goal_pct = (receita_mes / monthly_goal * 100) if monthly_goal else 0

    clientes = int(
        _scalar("SELECT COUNT(*) AS total FROM clients WHERE COALESCE(ativo, 1) = 1")
    )
    pets = int(
        _scalar("SELECT COUNT(*) AS total FROM pets WHERE COALESCE(ativo, 1) = 1")
    )
    funcionarios = int(
        _scalar("SELECT COUNT(*) AS total FROM employees WHERE COALESCE(active, 1) = 1")
    )
    presentes = int(
        _scalar(
            """
            SELECT COUNT(DISTINCT employee_id) AS total
            FROM time_records
            WHERE record_time >= ?
              AND record_time < ?
            """,
            (f"{_iso(today)} 00:00:00", f"{_iso(tomorrow)} 00:00:00"),
        )
    )
    clientes_novos_mes = int(
        _scalar(
            """
            SELECT COUNT(*) AS total
            FROM clients
            WHERE created_at >= ?
              AND created_at < ?
            """,
            (f"{_iso(month_start)} 00:00:00", f"{_iso(next_month)} 00:00:00"),
        )
    )
    pets_novos_mes = int(
        _scalar(
            """
            SELECT COUNT(*) AS total
            FROM pets
            WHERE created_at >= ?
              AND created_at < ?
            """,
            (f"{_iso(month_start)} 00:00:00", f"{_iso(next_month)} 00:00:00"),
        )
    )
    agendamentos_hoje = int(
        _scalar(
            """
            SELECT COUNT(*) AS total
            FROM appointments
            WHERE data_agendamento = ?
              AND status NOT IN ('Cancelado', 'Concluído')
            """,
            (_iso(today),),
        )
    )
    pets_em_atendimento = int(
        _scalar(
            """
            SELECT COUNT(*) AS total
            FROM grooming_services
            WHERE status NOT IN ('Entregue', 'Finalizado', 'Cancelado')
            """
        )
    )
    transporte_hoje = int(
        _scalar(
            """
            SELECT COUNT(*) AS total
            FROM transport_services
            WHERE pickup_date = ?
              AND status NOT IN ('Entregue', 'Finalizado', 'Cancelado')
            """,
            (_iso(today),),
        )
    )
    estoque_critico = int(
        _scalar(
            """
            SELECT COUNT(*) AS total
            FROM stock_products
            WHERE COALESCE(active, 1) = 1
              AND COALESCE(quantity, 0) <= COALESCE(min_quantity, 0)
            """
        )
    )
    vacinas_alerta = int(
        _scalar(
            """
            SELECT COUNT(*) AS total
            FROM pet_vaccines
            WHERE proxima_dose IS NOT NULL
              AND proxima_dose <> ''
              AND proxima_dose <= ?
            """,
            (_iso(today + timedelta(days=30)),),
        )
    )
    clientes_inativos = int(
        _scalar(
            """
            SELECT COUNT(*) AS total
            FROM (
                SELECT c.id, MAX(g.data) AS ultima_visita
                FROM clients c
                LEFT JOIN grooming_services g ON g.client_id = c.id
                WHERE COALESCE(c.ativo, 1) = 1
                GROUP BY c.id
                HAVING MAX(g.data) IS NOT NULL
                   AND MAX(g.data) < ?
            ) x
            """,
            (_iso(today - timedelta(days=60)),),
        )
    )

    agenda_hoje = query_db(
        """
        SELECT
            a.id,
            a.horario,
            a.servico,
            a.status,
            a.duration_minutes,
            c.nome AS cliente_nome,
            p.nome AS pet_nome,
            e.name AS profissional
        FROM appointments a
        LEFT JOIN clients c ON c.id = a.client_id
        LEFT JOIN pets p ON p.id = a.pet_id
        LEFT JOIN employees e ON e.id = a.employee_id
        WHERE a.data_agendamento = ?
          AND a.status <> 'Cancelado'
        ORDER BY a.horario, a.id
        LIMIT 10
        """,
        (_iso(today),),
    )

    ranking_profissionais = query_db(
        """
        SELECT
            COALESCE(e.name, 'Não informado') AS profissional,
            COUNT(g.id) AS atendimentos,
            COALESCE(SUM(g.valor), 0) AS faturamento
        FROM grooming_services g
        LEFT JOIN employees e ON e.id = g.employee_id
        WHERE g.data >= ?
          AND g.data < ?
          AND g.status <> 'Cancelado'
        GROUP BY e.id, e.name
        ORDER BY faturamento DESC, atendimentos DESC
        LIMIT 6
        """,
        (_iso(month_start), _iso(next_month)),
    )

    ranking_servicos = query_db(
        """
        SELECT
            COALESCE(servico, 'Não informado') AS servico,
            COUNT(*) AS total,
            COALESCE(SUM(valor), 0) AS faturamento
        FROM grooming_services
        WHERE data >= ?
          AND data < ?
          AND status <> 'Cancelado'
        GROUP BY servico
        ORDER BY total DESC, faturamento DESC
        LIMIT 6
        """,
        (_iso(month_start), _iso(next_month)),
    )

    timeline = query_db(
        """
        SELECT user_name, action, entity_type, entity_id, details, created_at
        FROM audit_logs
        ORDER BY id DESC
        LIMIT 10
        """
    )

    financial_series = _build_financial_series(today - timedelta(days=13), 14)
    monthly_revenue = _build_monthly_revenue(today, 12)
    customer_growth = _build_customer_growth(today, 12)
    alerts = _build_alerts(today)

    operation_status = {
        "aguardando": int(_scalar("SELECT COUNT(*) AS total FROM grooming_services WHERE status IN ('Recepção','Aguardando')")),
        "banho": int(_scalar("SELECT COUNT(*) AS total FROM grooming_services WHERE status IN ('Banho iniciado','Banho')")),
        "secagem": int(_scalar("SELECT COUNT(*) AS total FROM grooming_services WHERE status='Secagem'")),
        "tosa": int(_scalar("SELECT COUNT(*) AS total FROM grooming_services WHERE status IN ('Tosa iniciada','Tosa')")),
        "prontos": int(_scalar("SELECT COUNT(*) AS total FROM grooming_services WHERE status IN ('Pagamento','Em entrega','Pronto')")),
    }
    contas_vencidas = int(_scalar(
        """SELECT COUNT(*) AS total FROM financial_transactions
        WHERE status IN ('Pendente','Vencido') AND due_date IS NOT NULL AND due_date <> '' AND due_date < ?""",
        (_iso(today),),
    ))
    valor_vencido = float(_scalar(
        """SELECT COALESCE(SUM(amount),0) AS total FROM financial_transactions
        WHERE status IN ('Pendente','Vencido') AND due_date IS NOT NULL AND due_date <> '' AND due_date < ?""",
        (_iso(today),),
    ))
    contas_hoje = int(_scalar(
        """SELECT COUNT(*) AS total FROM financial_transactions
        WHERE status IN ('Pendente','Vencido') AND due_date = ?""",
        (_iso(today),),
    ))
    prioridades = {
        "contas_vencidas": contas_vencidas,
        "valor_vencido": valor_vencido,
        "contas_hoje": contas_hoje,
        "crm_atrasadas": int(_scalar("SELECT COUNT(*) AS total FROM crm_tasks WHERE status='Pendente' AND due_date IS NOT NULL AND due_date <> '' AND due_date < ?", (_iso(today),))),
        "estoque_critico": estoque_critico,
        "vacinas_alerta": vacinas_alerta,
    }

    loyalty = {
        "members": int(_scalar("SELECT COUNT(*) AS total FROM loyalty_accounts")),
        "points": int(_scalar("SELECT COALESCE(SUM(points_balance),0) AS total FROM loyalty_accounts")),
        "cashback": float(_scalar("SELECT COALESCE(SUM(cashback_balance),0) AS total FROM loyalty_accounts")),
        "diamond": int(_scalar("SELECT COUNT(*) AS total FROM loyalty_accounts WHERE level='Diamante'")),
        "active_coupons": int(_scalar("SELECT COUNT(*) AS total FROM loyalty_coupons WHERE status='Ativo'")),
    }
    crm = {
        "pending_tasks": int(_scalar("SELECT COUNT(*) AS total FROM crm_tasks WHERE status='Pendente'")),
        "overdue_tasks": int(_scalar("SELECT COUNT(*) AS total FROM crm_tasks WHERE status='Pendente' AND due_date IS NOT NULL AND due_date<>'' AND due_date < ?", (_iso(today),))),
        "contacts_month": int(_scalar("SELECT COUNT(*) AS total FROM crm_contact_history WHERE created_at >= ? AND created_at < ?", (f"{_iso(month_start)} 00:00:00", f"{_iso(next_month)} 00:00:00"))),
    }
    crm_tasks = query_db(
        """
        SELECT t.id, t.title, t.due_date, t.priority, t.status,
               c.nome AS client_name, p.nome AS pet_name
        FROM crm_tasks t
        LEFT JOIN clients c ON c.id=t.client_id
        LEFT JOIN pets p ON p.id=t.pet_id
        WHERE t.status='Pendente'
        ORDER BY CASE WHEN t.due_date IS NULL OR t.due_date='' THEN 1 ELSE 0 END,
                 t.due_date,
                 CASE t.priority WHEN 'Alta' THEN 0 WHEN 'Média' THEN 1 ELSE 2 END,
                 t.id DESC
        LIMIT 6
        """
    )

    data = {
        "cards": {
            "clientes": clientes,
            "pets": pets,
            "funcionarios": funcionarios,
            "presentes_hoje": presentes,
            "clientes_novos_mes": clientes_novos_mes,
            "pets_novos_mes": pets_novos_mes,
            "agendamentos_hoje": agendamentos_hoje,
            "pets_em_atendimento": pets_em_atendimento,
            "transporte_hoje": transporte_hoje,
            "estoque_critico": estoque_critico,
            "vacinas_alerta": vacinas_alerta,
            "clientes_inativos": clientes_inativos,
            "atendimentos_mes": atendimentos_mes,
        },
        "financeiro": {
            "receita_hoje": receita_hoje,
            "receita_semana": receita_semana,
            "receita_mes": receita_mes,
            "receita_ano": receita_ano,
            "saidas_mes": saidas_mes,
            "saldo_mes": saldo_mes,
            "margem_mes": margem_mes,
            "ticket_medio": ticket_medio,
            "meta_mensal": monthly_goal,
            "meta_percentual": min(goal_pct, 999.0),
            "falta_meta": max(monthly_goal - receita_mes, 0),
        },
        "comparativos": {
            "receita_ontem": receita_ontem,
            "receita_hoje_pct": _percent_change(receita_hoje, receita_ontem),
            "receita_mes_anterior": receita_mes_anterior,
            "receita_mes_pct": _percent_change(receita_mes, receita_mes_anterior),
        },
        "agenda_hoje": agenda_hoje,
        "ranking_profissionais": ranking_profissionais,
        "ranking_servicos": ranking_servicos,
        "timeline": timeline,
        "grafico_financeiro": financial_series,
        "grafico_receita_mensal": monthly_revenue,
        "grafico_clientes": customer_growth,
        "fidelidade": loyalty,
        "crm": crm,
        "crm_tasks": crm_tasks,
        "alertas": alerts,
        "operation_status": operation_status,
        "prioridades": prioridades,
        "operacao_resumo": {
            "pendencias_criticas": prioridades["contas_vencidas"] + prioridades["crm_atrasadas"] + prioridades["estoque_critico"] + prioridades["vacinas_alerta"],
            "equipe_percentual": (presentes / funcionarios * 100) if funcionarios else 0,
            "resultado_mes": saldo_mes,
            "ticket_medio": ticket_medio,
        },
        "gerado_em": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "saudacao": (
            "Bom dia"
            if datetime.now().hour < 12
            else "Boa tarde"
            if datetime.now().hour < 18
            else "Boa noite"
        ),
    }
    data["insights"] = _build_insights(data)
    return data


def obter_dashboard(force_refresh: bool = False):
    if force_refresh:
        cache.delete("dashboard:enterprise")
    return cache.get_or_set("dashboard:enterprise", _build_dashboard, ttl_seconds=30)
