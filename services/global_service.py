from __future__ import annotations

from datetime import date, timedelta

from database import query_db


def _safe_query(sql: str, params=()):
    """Executa consultas auxiliares sem derrubar toda a interface.

    Alguns módulos são opcionais e bancos antigos podem ainda não possuir todas
    as tabelas. Busca global e notificações devem continuar disponíveis mesmo
    durante uma migração parcial.
    """
    try:
        return query_db(sql, params)
    except Exception:
        return []


def buscar_global(termo: str, limite: int = 8):
    termo = (termo or "").strip()
    if len(termo) < 2:
        return []

    padrao = f"%{termo}%"
    resultados = []

    clientes = _safe_query(
        """
        SELECT id, nome, telefone, whatsapp
        FROM clients
        WHERE COALESCE(ativo, 1) = 1
          AND (
              nome LIKE ?
              OR COALESCE(telefone, '') LIKE ?
              OR COALESCE(whatsapp, '') LIKE ?
              OR COALESCE(email, '') LIKE ?
          )
        ORDER BY nome
        LIMIT ?
        """,
        (padrao, padrao, padrao, padrao, limite),
    )
    for item in clientes:
        resultados.append(
            {
                "tipo": "Cliente",
                "icone": "👤",
                "titulo": item["nome"],
                "subtitulo": item["whatsapp"] or item["telefone"] or "Abrir CRM",
                "url": f"/clientes/{item['id']}",
            }
        )

    pets = _safe_query(
        """
        SELECT p.id, p.nome, p.raca, c.nome AS tutor
        FROM pets p
        LEFT JOIN clients c ON c.id = p.client_id
        WHERE COALESCE(p.ativo, 1) = 1
          AND (
              p.nome LIKE ?
              OR COALESCE(p.raca, '') LIKE ?
              OR COALESCE(c.nome, '') LIKE ?
          )
        ORDER BY p.nome
        LIMIT ?
        """,
        (padrao, padrao, padrao, limite),
    )
    for item in pets:
        detalhes = " · ".join(
            valor for valor in (item["raca"], item["tutor"]) if valor
        )
        resultados.append(
            {
                "tipo": "Pet",
                "icone": "🐶",
                "titulo": item["nome"],
                "subtitulo": detalhes or "Abrir prontuário",
                "url": f"/pets/{item['id']}",
            }
        )

    funcionarios = _safe_query(
        """
        SELECT id, name, role
        FROM employees
        WHERE COALESCE(active, 1) = 1
          AND (
              name LIKE ?
              OR COALESCE(role, '') LIKE ?
          )
        ORDER BY name
        LIMIT ?
        """,
        (padrao, padrao, limite),
    )
    for item in funcionarios:
        resultados.append(
            {
                "tipo": "Funcionário",
                "icone": "👔",
                "titulo": item["name"],
                "subtitulo": item["role"] or "Equipe",
                "url": "/funcionarios",
            }
        )

    produtos = _safe_query(
        """
        SELECT id, name, category, quantity, unit
        FROM stock_products
        WHERE COALESCE(active, 1) = 1
          AND (
              name LIKE ?
              OR COALESCE(category, '') LIKE ?
              OR COALESCE(sku, '') LIKE ?
              OR COALESCE(barcode, '') LIKE ?
          )
        ORDER BY name
        LIMIT ?
        """,
        (padrao, padrao, padrao, padrao, limite),
    )
    for item in produtos:
        resultados.append(
            {
                "tipo": "Produto",
                "icone": "📦",
                "titulo": item["name"],
                "subtitulo": (
                    f"{item['quantity'] or 0} {item['unit'] or ''}"
                    + (f" · {item['category']}" if item["category"] else "")
                ),
                "url": "/estoque",
            }
        )

    agendamentos = _safe_query(
        """
        SELECT a.id, a.data_agendamento, a.horario, a.servico,
               c.nome AS cliente, p.nome AS pet
        FROM appointments a
        LEFT JOIN clients c ON c.id = a.client_id
        LEFT JOIN pets p ON p.id = a.pet_id
        WHERE (
            COALESCE(c.nome, '') LIKE ?
            OR COALESCE(p.nome, '') LIKE ?
            OR COALESCE(a.servico, '') LIKE ?
        )
        ORDER BY a.data_agendamento DESC, a.horario DESC
        LIMIT ?
        """,
        (padrao, padrao, padrao, limite),
    )
    for item in agendamentos:
        resultados.append(
            {
                "tipo": "Agendamento",
                "icone": "📅",
                "titulo": f"{item['pet'] or 'Pet'} · {item['servico'] or 'Serviço'}",
                "subtitulo": (
                    f"{item['data_agendamento'] or '-'} {item['horario'] or ''}"
                    f" · {item['cliente'] or 'Tutor'}"
                ),
                "url": "/agenda",
            }
        )

    ordem = {"Cliente": 0, "Pet": 1, "Agendamento": 2, "Funcionário": 3, "Produto": 4}
    resultados.sort(key=lambda item: (ordem.get(item["tipo"], 99), item["titulo"]))
    return resultados[:30]


def obter_notificacoes():
    hoje = date.today()
    limite_preventivos = hoje + timedelta(days=30)
    notificacoes = []

    estoque = _safe_query(
        """
        SELECT name, quantity, min_quantity, unit
        FROM stock_products
        WHERE COALESCE(active, 1) = 1
          AND COALESCE(quantity, 0) <= COALESCE(min_quantity, 0)
        ORDER BY quantity, name
        LIMIT 8
        """
    )
    for item in estoque:
        notificacoes.append(
            {
                "nivel": "danger",
                "icone": "📦",
                "titulo": item["name"],
                "mensagem": (
                    f"Estoque em {item['quantity'] or 0} {item['unit'] or ''}; "
                    f"mínimo {item['min_quantity'] or 0}."
                ),
                "url": "/estoque",
            }
        )

    vacinas = _safe_query(
        """
        SELECT v.vacina, v.proxima_dose, p.nome AS pet, p.id AS pet_id
        FROM pet_vaccines v
        LEFT JOIN pets p ON p.id = v.pet_id
        WHERE v.proxima_dose IS NOT NULL
          AND v.proxima_dose <> ''
          AND v.proxima_dose <= ?
        ORDER BY v.proxima_dose
        LIMIT 8
        """,
        (limite_preventivos.isoformat(),),
    )
    for item in vacinas:
        vencida = item["proxima_dose"] < hoje.isoformat()
        notificacoes.append(
            {
                "nivel": "danger" if vencida else "warning",
                "icone": "💉",
                "titulo": f"{item['vacina']} · {item['pet'] or 'Pet'}",
                "mensagem": (
                    f"Vencida em {item['proxima_dose']}."
                    if vencida
                    else f"Próxima dose em {item['proxima_dose']}."
                ),
                "url": f"/pets/{item['pet_id']}",
            }
        )

    preventivos = _safe_query(
        """
        SELECT t.treatment_type, t.next_date, p.nome AS pet, p.id AS pet_id
        FROM pet_health_treatments t
        LEFT JOIN pets p ON p.id = t.pet_id
        WHERE t.next_date IS NOT NULL
          AND t.next_date <> ''
          AND t.next_date <= ?
        ORDER BY t.next_date
        LIMIT 8
        """,
        (limite_preventivos.isoformat(),),
    )
    for item in preventivos:
        vencido = item["next_date"] < hoje.isoformat()
        notificacoes.append(
            {
                "nivel": "danger" if vencido else "warning",
                "icone": "🩺",
                "titulo": f"{item['treatment_type']} · {item['pet'] or 'Pet'}",
                "mensagem": (
                    f"Aplicação vencida em {item['next_date']}."
                    if vencido
                    else f"Próxima aplicação em {item['next_date']}."
                ),
                "url": f"/pets/{item['pet_id']}",
            }
        )

    financeiro = _safe_query(
        """
        SELECT description, amount, due_date
        FROM financial_transactions
        WHERE status = 'Pendente'
          AND due_date IS NOT NULL
          AND due_date <> ''
          AND due_date <= ?
        ORDER BY due_date
        LIMIT 8
        """,
        (hoje.isoformat(),),
    )
    for item in financeiro:
        notificacoes.append(
            {
                "nivel": "danger",
                "icone": "💰",
                "titulo": item["description"] or "Lançamento pendente",
                "mensagem": (
                    f"Vencimento {item['due_date']} · "
                    f"R$ {float(item['amount'] or 0):,.2f}"
                ),
                "url": "/financeiro",
            }
        )

    agenda = _safe_query(
        """
        SELECT a.horario, a.servico, p.nome AS pet
        FROM appointments a
        LEFT JOIN pets p ON p.id = a.pet_id
        WHERE a.data_agendamento = ?
          AND a.status NOT IN ('Cancelado', 'Concluído')
        ORDER BY a.horario
        LIMIT 8
        """,
        (hoje.isoformat(),),
    )
    for item in agenda:
        notificacoes.append(
            {
                "nivel": "info",
                "icone": "📅",
                "titulo": f"{item['horario'] or '--:--'} · {item['pet'] or 'Pet'}",
                "mensagem": item["servico"] or "Agendamento de hoje",
                "url": "/agenda",
            }
        )

    prioridade = {"danger": 0, "warning": 1, "info": 2}
    notificacoes.sort(key=lambda item: prioridade.get(item["nivel"], 9))
    return notificacoes[:30]
