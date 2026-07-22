from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime, timedelta

from database import query_db

STATUS_AGENDA = [
    "Aguardando aprovação",
    "Agendado",
    "Confirmado",
    "Em atendimento",
    "Finalizado",
    "Cancelado",
    "Faltou",
    "Reagendado",
    "Recusado",
]
SERVICOS_AGENDA = [
    "Banho",
    "Tosa",
    "Banho + Tosa",
    "Higiênica",
    "Hidratação",
    "Corte de unha",
    "Táxi Dog",
]
HORAS_AGENDA = [f"{hora:02d}:{minuto:02d}" for hora in range(7, 21) for minuto in (0, 30)]


def intervalo_visualizacao(modo: str, referencia: str | None):
    try:
        ref = datetime.strptime(referencia, "%Y-%m-%d").date() if referencia else date.today()
    except (TypeError, ValueError):
        ref = date.today()

    if modo == "semana":
        inicio = ref - timedelta(days=ref.weekday())
        fim = inicio + timedelta(days=6)
    elif modo == "mes":
        inicio_mes = ref.replace(day=1)
        inicio = inicio_mes - timedelta(days=inicio_mes.weekday())
        ultimo = ref.replace(day=monthrange(ref.year, ref.month)[1])
        fim = ultimo + timedelta(days=6 - ultimo.weekday())
    else:
        modo = "dia"
        inicio = fim = ref

    return modo, ref, inicio, fim


def navegar_referencia(modo: str, referencia: date, direcao: int):
    if modo == "mes":
        base = referencia.replace(day=1)
        if direcao < 0:
            anterior = base - timedelta(days=1)
            return anterior.replace(day=1)
        proximo = (base.replace(day=28) + timedelta(days=4)).replace(day=1)
        return proximo
    if modo == "semana":
        return referencia + timedelta(days=7 * direcao)
    return referencia + timedelta(days=direcao)


def listar_agendamentos(inicio, fim, status="", busca="", employee_id=""):
    sql = """
        SELECT a.*, c.nome AS cliente_nome, c.whatsapp,
               p.nome AS pet_nome, e.name AS funcionario_nome
        FROM appointments a
        LEFT JOIN clients c ON c.id = a.client_id
        LEFT JOIN pets p ON p.id = a.pet_id
        LEFT JOIN employees e ON e.id = a.employee_id
        WHERE a.data_agendamento BETWEEN ? AND ?
    """
    params = [inicio.isoformat(), fim.isoformat()]
    if status:
        sql += " AND a.status = ?"
        params.append(status)
    if employee_id:
        sql += " AND a.employee_id = ?"
        params.append(employee_id)
    if busca:
        termo = f"%{busca}%"
        sql += " AND (c.nome LIKE ? OR p.nome LIKE ? OR a.servico LIKE ?)"
        params.extend([termo, termo, termo])
    sql += " ORDER BY a.data_agendamento ASC, a.horario ASC"
    return query_db(sql, tuple(params))


def listar_bloqueios(inicio, fim):
    return query_db(
        """
        SELECT b.*, e.name AS funcionario_nome
        FROM schedule_blocks b
        LEFT JOIN employees e ON e.id = b.employee_id
        WHERE b.block_date BETWEEN ? AND ?
        ORDER BY b.block_date, b.start_time
        """,
        (inicio.isoformat(), fim.isoformat()),
    )


def listar_espera():
    return query_db(
        """
        SELECT w.*, c.nome AS cliente_nome, p.nome AS pet_nome
        FROM appointment_waitlist w
        LEFT JOIN clients c ON c.id = w.client_id
        LEFT JOIN pets p ON p.id = w.pet_id
        WHERE w.status = 'Aguardando'
        ORDER BY w.priority DESC, w.preferred_date, w.created_at
        """
    )


def resumo_agenda(inicio, fim):
    return query_db(
        """
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN status = 'Confirmado' THEN 1 ELSE 0 END) AS confirmados,
               SUM(CASE WHEN status = 'Em atendimento' THEN 1 ELSE 0 END) AS em_atendimento,
               SUM(CASE WHEN status = 'Finalizado' THEN 1 ELSE 0 END) AS finalizados,
               SUM(CASE WHEN status = 'Cancelado' THEN 1 ELSE 0 END) AS cancelados,
               COALESCE(SUM(CASE WHEN status != 'Cancelado' THEN valor ELSE 0 END), 0) AS valor_previsto,
               COALESCE(SUM(CASE WHEN status NOT IN ('Cancelado','Faltou') THEN duration_minutes ELSE 0 END), 0) AS minutos_ocupados
        FROM appointments
        WHERE data_agendamento BETWEEN ? AND ?
        """,
        (inicio.isoformat(), fim.isoformat()),
        one=True,
    )


def ocupacao_profissionais(inicio, fim):
    return query_db(
        """
        SELECT e.id, e.name,
               COUNT(a.id) AS atendimentos,
               COALESCE(SUM(CASE WHEN a.status NOT IN ('Cancelado','Faltou') THEN a.duration_minutes ELSE 0 END),0) AS minutos
        FROM employees e
        LEFT JOIN appointments a ON a.employee_id = e.id
            AND a.data_agendamento BETWEEN ? AND ?
        WHERE e.active = 1
        GROUP BY e.id, e.name
        ORDER BY minutos DESC, e.name
        """,
        (inicio.isoformat(), fim.isoformat()),
    )


def conflito_horario(data_agendamento, horario, duracao, employee_id=None, ignorar_id=None):
    inicio_novo = datetime.strptime(f"{data_agendamento} {horario}", "%Y-%m-%d %H:%M")
    fim_novo = inicio_novo + timedelta(minutes=int(duracao or 60))

    # O conflito entre atendimentos só é rígido quando há um profissional definido.
    # Sem profissional, a capacidade da loja controla quantos pets podem ocupar o horário.
    if employee_id:
        sql = """
            SELECT id, horario, duration_minutes
            FROM appointments
            WHERE data_agendamento = ?
              AND employee_id = ?
              AND status NOT IN ('Recusado','Cancelado','Cancelado pelo cliente','Faltou')
        """
        params = [data_agendamento, employee_id]
        if ignorar_id:
            sql += " AND id <> ?"
            params.append(ignorar_id)

        for item in query_db(sql, tuple(params)):
            inicio_existente = datetime.strptime(
                f"{data_agendamento} {item['horario']}", "%Y-%m-%d %H:%M"
            )
            fim_existente = inicio_existente + timedelta(minutes=int(item["duration_minutes"] or 60))
            if inicio_novo < fim_existente and fim_novo > inicio_existente:
                return item

    bloqueios = query_db(
        """
        SELECT id, start_time, end_time
        FROM schedule_blocks
        WHERE block_date = ?
          AND (employee_id IS NULL OR employee_id = ?)
        """,
        (data_agendamento, employee_id),
    )
    for item in bloqueios:
        inicio_bloqueio = datetime.strptime(
            f"{data_agendamento} {item['start_time']}", "%Y-%m-%d %H:%M"
        )
        fim_bloqueio = datetime.strptime(
            f"{data_agendamento} {item['end_time']}", "%Y-%m-%d %H:%M"
        )
        if inicio_novo < fim_bloqueio and fim_novo > inicio_bloqueio:
            return item
    return None


def montar_calendario(modo, inicio, fim, agendamentos, bloqueios):
    eventos_por_data = {}
    bloqueios_por_data = {}
    for item in agendamentos:
        eventos_por_data.setdefault(item["data_agendamento"], []).append(item)
    for item in bloqueios:
        bloqueios_por_data.setdefault(item["block_date"], []).append(item)

    dias = []
    cursor = inicio
    while cursor <= fim:
        chave = cursor.isoformat()
        dias.append(
            {
                "date": cursor,
                "iso": chave,
                "is_today": cursor == date.today(),
                "is_current_month": True,
                "events": eventos_por_data.get(chave, []),
                "blocks": bloqueios_por_data.get(chave, []),
            }
        )
        cursor += timedelta(days=1)

    if modo == "mes":
        mes_ref = (inicio + timedelta(days=7)).month
        for dia in dias:
            dia["is_current_month"] = dia["date"].month == mes_ref
    return dias


def obter_configuracao_capacidade():
    item = query_db("SELECT * FROM agenda_capacity_settings ORDER BY id LIMIT 1", one=True)
    if item:
        return dict(item)
    return {
        "id": None,
        "default_capacity": 3,
        "allow_admin_override": 1,
        "allow_recepcao_override": 1,
    }


def ocupacao_horario(data_agendamento, horario, ignorar_id=None):
    sql = """
        SELECT COUNT(*) AS total
        FROM appointments
        WHERE data_agendamento = ?
          AND SUBSTR(CAST(horario AS TEXT),1,5) = ?
          AND status NOT IN ('Recusado','Cancelado','Cancelado pelo cliente','Faltou')
    """
    params = [data_agendamento, str(horario)[:5]]
    if ignorar_id:
        sql += " AND id <> ?"
        params.append(ignorar_id)
    row = query_db(sql, tuple(params), one=True)
    return int(row["total"] or 0) if row else 0


def pode_fazer_encaixe(role):
    cfg = obter_configuracao_capacidade()
    if role == "admin":
        return bool(cfg.get("allow_admin_override", 1))
    if role == "recepcao":
        return bool(cfg.get("allow_recepcao_override", 1))
    return False


def mapa_ocupacao(agendamentos):
    cfg = obter_configuracao_capacidade()
    capacidade = max(1, int(cfg.get("default_capacity") or 3))
    mapa = {}
    for item in agendamentos:
        if item["status"] in ("Recusado", "Cancelado", "Cancelado pelo cliente", "Faltou"):
            continue
        chave = f"{item['data_agendamento']}|{str(item['horario'])[:5]}"
        mapa[chave] = mapa.get(chave, 0) + 1
    return {
        chave: {
            "ocupados": total,
            "capacidade": capacidade,
            "lotado": total >= capacidade,
            "excedido": total > capacidade,
        }
        for chave, total in mapa.items()
    }
