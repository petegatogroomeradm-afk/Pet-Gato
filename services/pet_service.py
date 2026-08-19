from datetime import date, datetime, timedelta

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


def _idade_detalhada(value):
    nascimento = _parse_date(value)
    if not nascimento:
        return None
    hoje = date.today()
    anos = hoje.year - nascimento.year - ((hoje.month, hoje.day) < (nascimento.month, nascimento.day))
    return anos


def obter_ficha_pet(pet_id):
    pet = query_db(
        """
        SELECT p.*, c.nome AS cliente_nome, c.telefone, c.whatsapp, c.email, c.endereco
        FROM pets p
        LEFT JOIN clients c ON c.id = p.client_id
        WHERE p.id = ?
        """,
        (pet_id,),
        one=True,
    )
    if not pet:
        return None

    financeiro = query_db(
        """
        SELECT COUNT(*) AS total_atendimentos,
               COALESCE(SUM(valor), 0) AS total_gasto,
               COALESCE(AVG(valor), 0) AS ticket_medio,
               MAX(data) AS ultimo_atendimento,
               SUM(CASE WHEN servico LIKE '%%Banho%%' THEN 1 ELSE 0 END) AS total_banhos,
               SUM(CASE WHEN servico LIKE '%%Tosa%%' THEN 1 ELSE 0 END) AS total_tosas
        FROM grooming_services WHERE pet_id = ?
        """,
        (pet_id,),
        one=True,
    )
    proximos_agendamentos = query_db(
        """
        SELECT id, data_agendamento, horario, servico, status, valor
        FROM appointments
        WHERE pet_id = ? AND data_agendamento >= ?
          AND status NOT IN ('Cancelado', 'Finalizado', 'Concluído')
        ORDER BY data_agendamento ASC, horario ASC LIMIT 8
        """,
        (pet_id, date.today().isoformat()),
    )
    proximo_agendamento = proximos_agendamentos[0] if proximos_agendamentos else None

    servico_favorito = query_db(
        """
        SELECT servico, COUNT(*) AS quantidade, COALESCE(SUM(valor), 0) AS total
        FROM grooming_services
        WHERE pet_id = ? AND COALESCE(servico, '') <> ''
        GROUP BY servico
        ORDER BY quantidade DESC, total DESC
        LIMIT 1
        """,
        (pet_id,),
        one=True,
    )
    historico = query_db(
        """
        SELECT g.*, e.name AS profissional_nome
        FROM grooming_services g
        LEFT JOIN employees e ON e.id = g.employee_id
        WHERE g.pet_id = ? ORDER BY g.data DESC, g.id DESC
        """,
        (pet_id,),
    )
    vacinas = query_db(
        "SELECT * FROM pet_vaccines WHERE pet_id = ? ORDER BY proxima_dose ASC, id DESC",
        (pet_id,),
    )
    tratamentos = query_db(
        """
        SELECT * FROM pet_health_treatments
        WHERE pet_id = ?
        ORDER BY next_date ASC, application_date DESC, id DESC
        """,
        (pet_id,),
    )
    pesos = query_db(
        "SELECT * FROM pet_weight_history WHERE pet_id = ? ORDER BY data ASC, id ASC",
        (pet_id,),
    )
    perfil_saude = query_db(
        "SELECT * FROM pet_health_profiles WHERE pet_id = ?",
        (pet_id,),
        one=True,
    )
    documentos = query_db("SELECT * FROM pet_documents WHERE pet_id = ? ORDER BY id DESC", (pet_id,))
    fotos = query_db("SELECT * FROM pet_photos WHERE pet_id = ? ORDER BY id DESC", (pet_id,))
    notas = query_db("SELECT * FROM pet_notes WHERE pet_id = ? ORDER BY created_at DESC, id DESC", (pet_id,))
    historico_medico = query_db(
        "SELECT * FROM pet_medical_history WHERE pet_id = ? ORDER BY data DESC, id DESC",
        (pet_id,),
    )
    transportes = query_db(
        "SELECT * FROM transport_services WHERE pet_id = ? ORDER BY pickup_date DESC, id DESC",
        (pet_id,),
    )
    documentos_recentes = documentos[:5]
    fotos_recentes = fotos[:8]

    hoje = date.today()
    limite = hoje + timedelta(days=30)
    alertas_vacina = []
    for vacina in vacinas:
        proxima = _parse_date(vacina["proxima_dose"])
        if not proxima:
            continue
        if proxima < hoje:
            status = "Vencida"
        elif proxima <= limite:
            status = "Próxima"
        else:
            continue
        alertas_vacina.append({"vacina": vacina, "status": status, "dias": (proxima - hoje).days})

    alertas_tratamento = []
    for tratamento in tratamentos:
        proxima = _parse_date(tratamento["next_date"])
        if not proxima:
            continue
        if proxima < hoje:
            status = "Vencido"
        elif proxima <= limite:
            status = "Próximo"
        else:
            continue
        alertas_tratamento.append({
            "tratamento": tratamento,
            "status": status,
            "dias": (proxima - hoje).days,
        })

    timeline = []
    for item in historico:
        timeline.append({
            "data": item["data"] or item["created_at"], "icone": "🛁", "tipo": "Atendimento",
            "titulo": item["servico"] or "Atendimento", "detalhe": item["status"] or "Sem status",
        })
    for item in vacinas:
        timeline.append({
            "data": item["data_aplicacao"] or item["created_at"], "icone": "💉", "tipo": "Vacina",
            "titulo": item["vacina"], "detalhe": f"Próxima dose: {item['proxima_dose'] or '-'}",
        })
    for item in tratamentos:
        timeline.append({
            "data": item["application_date"] or item["created_at"],
            "icone": "🧴" if item["treatment_type"] == "Antipulgas" else "💊",
            "tipo": item["treatment_type"],
            "titulo": item["product_name"] or item["treatment_type"],
            "detalhe": f"Próxima aplicação: {item['next_date'] or '-'}",
        })
    for item in historico_medico:
        timeline.append({
            "data": item["data"] or item["created_at"], "icone": "🩺", "tipo": "Saúde",
            "titulo": item["tipo"] or "Registro médico", "detalhe": item["descricao"],
        })
    for item in pesos:
        timeline.append({
            "data": item["data"] or item["created_at"], "icone": "⚖️", "tipo": "Peso",
            "titulo": f"Pesagem: {item['peso']} kg", "detalhe": "Evolução de peso",
        })
    for item in transportes:
        timeline.append({
            "data": item["pickup_date"] or item["created_at"], "icone": "🚚", "tipo": "Transporte",
            "titulo": item["service_type"] or "Transporte", "detalhe": item["status"] or "Sem status",
        })
    for item in notas:
        timeline.append({
            "data": item["created_at"], "icone": "📝", "tipo": "Anotação",
            "titulo": item["observacao"], "detalhe": item["usuario"] or "Sistema",
        })
    for item in documentos:
        timeline.append({
            "data": item["created_at"], "icone": "📄", "tipo": "Documento",
            "titulo": item["nome"] or item["arquivo"], "detalhe": item["tipo"] or "Documento",
        })
    for item in fotos:
        timeline.append({
            "data": item["created_at"], "icone": "📷", "tipo": "Foto",
            "titulo": item["categoria"] or "Foto", "detalhe": item["descricao"] or item["arquivo"],
        })
    timeline.sort(key=lambda item: str(item["data"] or ""), reverse=True)

    peso_chart = []
    if pesos:
        valores = [float(p["peso"] or 0) for p in pesos]
        minimo, maximo = min(valores), max(valores)
        amplitude = max(maximo - minimo, 1)
        total = max(len(pesos) - 1, 1)
        for idx, item in enumerate(pesos):
            x = round((idx / total) * 100, 2)
            y = round(90 - ((float(item["peso"] or 0) - minimo) / amplitude) * 75, 2)
            peso_chart.append({"x": x, "y": y, "peso": item["peso"], "data": item["data"] or item["created_at"]})

    peso_atual = float(pesos[-1]["peso"] or 0) if pesos else float(pet["peso"] or 0)
    peso_anterior = float(pesos[-2]["peso"] or 0) if len(pesos) >= 2 else None
    variacao_peso = round(peso_atual - peso_anterior, 2) if peso_anterior is not None else None

    datas_atendimento = [_parse_date(item["data"] or item["created_at"]) for item in historico]
    datas_atendimento = sorted({item for item in datas_atendimento if item})
    intervalo_medio = None
    if len(datas_atendimento) >= 2:
        intervalos = [(datas_atendimento[idx] - datas_atendimento[idx - 1]).days for idx in range(1, len(datas_atendimento))]
        intervalos = [valor for valor in intervalos if valor > 0]
        intervalo_medio = round(sum(intervalos) / len(intervalos)) if intervalos else None

    previsao_retorno = None
    status_retorno = "Sem histórico"
    ultimo_atendimento = _parse_date(financeiro["ultimo_atendimento"] if financeiro else None)
    if ultimo_atendimento:
        previsao_retorno = ultimo_atendimento + timedelta(days=intervalo_medio or 30)
        diferenca = (previsao_retorno - hoje).days
        if diferenca < 0:
            status_retorno = "Atrasado"
        elif diferenca <= 7:
            status_retorno = "Próximo"
        else:
            status_retorno = "Em dia"

    alertas_saude = []
    if pet["alergias"]:
        alertas_saude.append({"nivel": "alto", "titulo": "Alergia registrada", "detalhe": pet["alergias"]})
    if pet["medicamentos"]:
        alertas_saude.append({"nivel": "medio", "titulo": "Uso de medicamento", "detalhe": pet["medicamentos"]})
    if perfil_saude and perfil_saude["restricoes"]:
        alertas_saude.append({"nivel": "alto", "titulo": "Restrição registrada", "detalhe": perfil_saude["restricoes"]})
    if perfil_saude and perfil_saude["doencas_cronicas"]:
        alertas_saude.append({"nivel": "medio", "titulo": "Condição crônica", "detalhe": perfil_saude["doencas_cronicas"]})
    if perfil_saude and perfil_saude["cuidados_especiais"]:
        alertas_saude.append({"nivel": "baixo", "titulo": "Cuidado especial", "detalhe": perfil_saude["cuidados_especiais"]})
    if variacao_peso is not None and abs(variacao_peso) >= 1:
        alertas_saude.append({"nivel": "medio", "titulo": "Variação de peso", "detalhe": f"{variacao_peso:+.2f} kg desde a última pesagem"})
    if status_retorno == "Atrasado" and not proximo_agendamento:
        alertas_saude.append({"nivel": "baixo", "titulo": "Retorno recomendado", "detalhe": f"Previsão era {previsao_retorno.isoformat()}"})

    vacinas_vencidas = sum(1 for item in alertas_vacina if item["status"] == "Vencida")
    vacinas_proximas = sum(1 for item in alertas_vacina if item["status"] == "Próxima")
    tratamentos_vencidos = sum(1 for item in alertas_tratamento if item["status"] == "Vencido")
    tratamentos_proximos = sum(1 for item in alertas_tratamento if item["status"] == "Próximo")
    for item in alertas_tratamento:
        tratamento = item["tratamento"]
        alertas_saude.append({
            "nivel": "alto" if item["status"] == "Vencido" else "medio",
            "titulo": f"{tratamento['treatment_type']} {item['status'].lower()}",
            "detalhe": f"{tratamento['product_name'] or 'Produto não informado'} · {tratamento['next_date']}",
        })

    return {
        "pet": pet,
        "idade_calculada": _idade_detalhada(pet["data_nascimento"]),
        "financeiro": financeiro,
        "proximo_agendamento": proximo_agendamento,
        "proximos_agendamentos": proximos_agendamentos,
        "servico_favorito": servico_favorito,
        "historico": historico,
        "vacinas": vacinas,
        "alertas_vacina": alertas_vacina,
        "tratamentos": tratamentos,
        "alertas_tratamento": alertas_tratamento,
        "pesos": list(reversed(pesos)),
        "peso_chart": peso_chart,
        "documentos": documentos,
        "fotos": fotos,
        "notas": notas,
        "historico_medico": historico_medico,
        "transportes": transportes,
        "timeline": timeline[:100],
        "peso_atual": peso_atual,
        "variacao_peso": variacao_peso,
        "intervalo_medio": intervalo_medio,
        "previsao_retorno": previsao_retorno.isoformat() if previsao_retorno else None,
        "status_retorno": status_retorno,
        "alertas_saude": alertas_saude,
        "vacinas_vencidas": vacinas_vencidas,
        "vacinas_proximas": vacinas_proximas,
        "tratamentos_vencidos": tratamentos_vencidos,
        "tratamentos_proximos": tratamentos_proximos,
        "documentos_recentes": documentos_recentes,
        "fotos_recentes": fotos_recentes,
    }
