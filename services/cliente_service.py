from datetime import date, datetime

from database import query_db


def _parse_date(value):
    if not value:
        return None
    text = str(value)[:10]
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    return None


def _days_since(value):
    parsed = _parse_date(value)
    return (date.today() - parsed).days if parsed else None


def _month_key(value):
    parsed = _parse_date(value)
    return parsed.strftime("%Y-%m") if parsed else None


def _average_interval(values):
    dates = sorted({d for d in (_parse_date(v) for v in values) if d})
    if len(dates) < 2:
        return None
    intervals = [(dates[idx] - dates[idx - 1]).days for idx in range(1, len(dates))]
    valid = [days for days in intervals if days > 0]
    return round(sum(valid) / len(valid)) if valid else None



def _birthday_info(value):
    nascimento = _parse_date(value)
    if not nascimento:
        return None, None
    hoje = date.today()
    idade = hoje.year - nascimento.year - ((hoje.month, hoje.day) < (nascimento.month, nascimento.day))
    try:
        proximo = nascimento.replace(year=hoje.year)
    except ValueError:
        proximo = date(hoje.year, 2, 28)
    if proximo < hoje:
        try:
            proximo = nascimento.replace(year=hoje.year + 1)
        except ValueError:
            proximo = date(hoje.year + 1, 2, 28)
    return idade, (proximo - hoje).days

def obter_ficha_cliente(cliente_id):
    cliente = query_db("SELECT * FROM clients WHERE id = ?", (cliente_id,), one=True)
    if not cliente:
        return None

    pets = query_db(
        "SELECT * FROM pets WHERE client_id = ? AND COALESCE(ativo, 1) = 1 ORDER BY nome",
        (cliente_id,),
    )
    historico = query_db(
        """
        SELECT g.*, p.nome AS pet_nome, e.name AS profissional_nome
        FROM grooming_services g
        LEFT JOIN pets p ON p.id = g.pet_id
        LEFT JOIN employees e ON e.id = g.employee_id
        WHERE g.client_id = ?
        ORDER BY g.data DESC, g.id DESC
        """,
        (cliente_id,),
    )
    agendamentos = query_db(
        """
        SELECT a.*, p.nome AS pet_nome, e.name AS profissional_nome
        FROM appointments a
        LEFT JOIN pets p ON p.id = a.pet_id
        LEFT JOIN employees e ON e.id = a.employee_id
        WHERE a.client_id = ?
        ORDER BY a.data_agendamento DESC, a.horario DESC
        """,
        (cliente_id,),
    )
    transacoes = query_db(
        """
        SELECT * FROM financial_transactions
        WHERE (source_type = 'Cliente' AND source_id = ?)
           OR description LIKE ?
        ORDER BY transaction_date DESC, id DESC
        LIMIT 50
        """,
        (cliente_id, f"%{cliente['nome']}%"),
    )
    transportes = query_db(
        """
        SELECT t.*, p.nome AS pet_nome
        FROM transport_services t
        LEFT JOIN pets p ON p.id = t.pet_id
        WHERE t.client_id = ?
        ORDER BY t.pickup_date DESC, t.id DESC
        """,
        (cliente_id,),
    )
    notas = query_db(
        "SELECT * FROM client_notes WHERE client_id = ? ORDER BY created_at DESC, id DESC",
        (cliente_id,),
    )
    documentos = query_db(
        "SELECT * FROM client_documents WHERE client_id = ? ORDER BY id DESC",
        (cliente_id,),
    )
    fotos = query_db(
        "SELECT * FROM client_photos WHERE client_id = ? ORDER BY created_at DESC, id DESC",
        (cliente_id,),
    )
    financeiro = query_db(
        """
        SELECT COUNT(*) AS total_atendimentos,
               COALESCE(SUM(valor), 0) AS total_gasto,
               COALESCE(AVG(valor), 0) AS ticket_medio,
               MAX(data) AS ultimo_atendimento,
               SUM(CASE WHEN servico LIKE '%%Banho%%' THEN 1 ELSE 0 END) AS total_banhos,
               SUM(CASE WHEN servico LIKE '%%Tosa%%' THEN 1 ELSE 0 END) AS total_tosas
        FROM grooming_services WHERE client_id = ?
        """,
        (cliente_id,),
        one=True,
    )
    pendencias = query_db(
        """
        SELECT COUNT(*) AS quantidade, COALESCE(SUM(amount), 0) AS valor
        FROM financial_transactions
        WHERE status IN ('Pendente', 'Vencido')
          AND type = 'Entrada'
          AND description LIKE ?
        """,
        (f"%{cliente['nome']}%",),
        one=True,
    )
    crm_tarefas = query_db(
        """
        SELECT t.*, p.nome AS pet_nome
        FROM crm_tasks t
        LEFT JOIN pets p ON p.id = t.pet_id
        WHERE t.client_id = ?
        ORDER BY CASE WHEN t.status IN ('Pendente', 'Aberta no WhatsApp') THEN 0 ELSE 1 END,
                 COALESCE(t.due_date, t.created_at) DESC, t.id DESC
        LIMIT 30
        """,
        (cliente_id,),
    )
    crm_contatos = query_db(
        """
        SELECT h.*, p.nome AS pet_nome
        FROM crm_contact_history h
        LEFT JOIN pets p ON p.id = h.pet_id
        WHERE h.client_id = ?
        ORDER BY h.created_at DESC, h.id DESC
        LIMIT 30
        """,
        (cliente_id,),
    )
    fidelidade = query_db(
        "SELECT * FROM loyalty_accounts WHERE client_id = ?",
        (cliente_id,),
        one=True,
    ) or {
        "points_balance": 0, "cashback_balance": 0,
        "lifetime_points": 0, "level": "Não participante"
    }
    fidelidade_movimentos = query_db(
        """
        SELECT * FROM loyalty_transactions
        WHERE client_id = ?
        ORDER BY created_at DESC, id DESC
        LIMIT 20
        """,
        (cliente_id,),
    )
    proximos_agendamentos = query_db(
        """
        SELECT a.*, p.nome AS pet_nome, e.name AS profissional_nome
        FROM appointments a
        LEFT JOIN pets p ON p.id = a.pet_id
        LEFT JOIN employees e ON e.id = a.employee_id
        WHERE a.client_id = ?
          AND a.data_agendamento >= ?
          AND a.status NOT IN ('Cancelado', 'Finalizado', 'Concluído')
        ORDER BY a.data_agendamento, a.horario
        LIMIT 10
        """,
        (cliente_id, date.today().isoformat()),
    )

    proximo = query_db(
        """
        SELECT a.*, p.nome AS pet_nome
        FROM appointments a
        LEFT JOIN pets p ON p.id = a.pet_id
        WHERE a.client_id = ?
          AND a.data_agendamento >= ?
          AND a.status NOT IN ('Cancelado', 'Finalizado', 'Concluído')
        ORDER BY a.data_agendamento, a.horario
        LIMIT 1
        """,
        (cliente_id, date.today().isoformat()),
        one=True,
    )

    # Inteligência do CRM calculada em Python para funcionar igual no SQLite e PostgreSQL.
    servicos = {}
    monthly = {}
    datas_atendimento = []
    for item in historico:
        nome_servico = item["servico"] or "Outros"
        servicos[nome_servico] = servicos.get(nome_servico, 0) + 1
        chave_mes = _month_key(item["data"] or item["created_at"])
        if chave_mes:
            monthly[chave_mes] = monthly.get(chave_mes, 0.0) + float(item["valor"] or 0)
        datas_atendimento.append(item["data"] or item["created_at"])

    ranking_servicos = [
        {"nome": nome, "quantidade": quantidade}
        for nome, quantidade in sorted(servicos.items(), key=lambda pair: (-pair[1], pair[0]))[:5]
    ]
    gastos_mensais = [
        {"mes": mes, "valor": round(valor, 2)}
        for mes, valor in sorted(monthly.items())[-6:]
    ]
    max_gasto_mensal = max((item["valor"] for item in gastos_mensais), default=0) or 1
    for item in gastos_mensais:
        item["percentual"] = round((item["valor"] / max_gasto_mensal) * 100, 1)

    frequencia_media = _average_interval(datas_atendimento)

    timeline = []
    for item in historico:
        timeline.append({
            "data": item["data"] or item["created_at"],
            "tipo": "Atendimento",
            "titulo": f"{item['servico'] or 'Serviço'} — {item['pet_nome'] or 'Pet'}",
            "detalhe": item["status"] or "Sem status",
            "icone": "🛁",
        })
    for item in agendamentos:
        timeline.append({
            "data": item["data_agendamento"],
            "tipo": "Agenda",
            "titulo": f"{item['servico'] or 'Agendamento'} — {item['pet_nome'] or 'Pet'}",
            "detalhe": f"{item['horario'] or ''} · {item['status'] or ''}",
            "icone": "📅",
        })
    for item in transportes:
        timeline.append({
            "data": item["pickup_date"] or item["created_at"],
            "tipo": "Transporte",
            "titulo": f"{item['service_type'] or 'Transporte'} — {item['pet_nome'] or 'Pet'}",
            "detalhe": item["status"] or "Sem status",
            "icone": "🚚",
        })
    for item in notas:
        timeline.append({
            "data": item["created_at"],
            "tipo": "Anotação",
            "titulo": item["observacao"],
            "detalhe": item["usuario"] or "Sistema",
            "icone": "📝",
        })
    for item in documentos:
        timeline.append({
            "data": item["created_at"],
            "tipo": "Documento",
            "titulo": item["nome"] or item["arquivo"],
            "detalhe": item["tipo"] or "Documento do cliente",
            "icone": "📄",
        })
    for item in fotos:
        timeline.append({
            "data": item["created_at"],
            "tipo": "Foto",
            "titulo": item["descricao"] or "Foto adicionada",
            "detalhe": item["categoria"] or "Galeria do cliente",
            "icone": "📷",
        })
    for item in transacoes:
        timeline.append({
            "data": item["transaction_date"] or item["created_at"],
            "tipo": "Financeiro",
            "titulo": item["description"] or "Lançamento financeiro",
            "detalhe": f"{item['status'] or '-'} · R$ {float(item['amount'] or 0):.2f}",
            "icone": "💰",
        })
    timeline.sort(key=lambda item: str(item["data"] or ""), reverse=True)

    ultimo = financeiro["ultimo_atendimento"] if financeiro else None
    dias_sem_retornar = _days_since(ultimo)
    vip = float(financeiro["total_gasto"] or 0) >= 1000 or int(financeiro["total_atendimentos"] or 0) >= 12

    previsao_retorno = None
    status_retorno = "Sem histórico"
    if ultimo:
        ultima_data = _parse_date(ultimo)
        intervalo = frequencia_media or 30
        if ultima_data:
            previsao_retorno = ultima_data.fromordinal(ultima_data.toordinal() + intervalo)
            atraso = (date.today() - previsao_retorno).days
            if atraso > 0:
                status_retorno = "Retorno atrasado"
            elif atraso >= -7:
                status_retorno = "Retorno próximo"
            else:
                status_retorno = "Em dia"

    if vip:
        nivel_relacionamento = "Ouro"
    elif float(financeiro["total_gasto"] or 0) >= 500 or int(financeiro["total_atendimentos"] or 0) >= 6:
        nivel_relacionamento = "Prata"
    else:
        nivel_relacionamento = "Bronze"

    idade_cliente, dias_aniversario = _birthday_info(cliente["data_nascimento"])
    cancelados = sum(1 for item in agendamentos if item["status"] == "Cancelado")
    concluidos = int(financeiro["total_atendimentos"] or 0)
    taxa_comparecimento = round((concluidos / max(concluidos + cancelados, 1)) * 100)
    score_relacionamento = min(100,
        min(concluidos * 4, 40) +
        min(float(financeiro["total_gasto"] or 0) / 25, 30) +
        (15 if proximo else 0) +
        (15 if not pendencias or float(pendencias["valor"] or 0) == 0 else 0)
    )

    alertas_crm = []
    if pendencias and float(pendencias["valor"] or 0) > 0:
        alertas_crm.append({"tipo": "financeiro", "titulo": "Pendência financeira", "detalhe": f"R$ {float(pendencias['valor'] or 0):.2f} em aberto"})
    if status_retorno == "Retorno atrasado":
        alertas_crm.append({"tipo": "retorno", "titulo": "Cliente sem retornar", "detalhe": f"Último atendimento há {dias_sem_retornar} dias"})
    if not proximo:
        alertas_crm.append({"tipo": "agenda", "titulo": "Sem próximo agendamento", "detalhe": "Contato recomendado para fidelização"})
    if dias_aniversario is not None and dias_aniversario <= 15:
        alertas_crm.append({"tipo": "aniversario", "titulo": "Aniversário próximo", "detalhe": f"Faltam {dias_aniversario} dia(s)"})

    return {
        "cliente": cliente,
        "pets": pets,
        "historico": historico,
        "agendamentos": agendamentos,
        "notas": notas,
        "documentos": documentos,
        "fotos": fotos,
        "financeiro": financeiro,
        "pendencias": pendencias,
        "proximo_agendamento": proximo,
        "transportes": transportes,
        "transacoes": transacoes,
        "timeline": timeline[:80],
        "dias_sem_retornar": dias_sem_retornar,
        "cliente_vip": vip,
        "nivel_relacionamento": nivel_relacionamento,
        "frequencia_media": frequencia_media,
        "previsao_retorno": previsao_retorno.isoformat() if previsao_retorno else None,
        "status_retorno": status_retorno,
        "alertas_crm": alertas_crm,
        "ranking_servicos": ranking_servicos,
        "gastos_mensais": gastos_mensais,
        "idade_cliente": idade_cliente,
        "dias_aniversario": dias_aniversario,
        "taxa_comparecimento": taxa_comparecimento,
        "score_relacionamento": round(score_relacionamento),
        "cancelamentos": cancelados,
        "crm_tarefas": crm_tarefas,
        "crm_contatos": crm_contatos,
        "fidelidade": fidelidade,
        "fidelidade_movimentos": fidelidade_movimentos,
        "proximos_agendamentos": proximos_agendamentos,
    }
