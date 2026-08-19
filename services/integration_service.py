from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import shutil
from time import perf_counter


from database import execute_db, now_iso, query_db, is_postgres
from services.banho_service import lancar_comissao
from services.estoque_service import baixar_estoque_banho_tosa
from services.financeiro_service import lancar_receita_banho_tosa
from services.event_bus import publish


def _count(sql: str, params=()) -> int:
    row = query_db(sql, params, one=True)
    return int(row["total"] or 0) if row else 0


def _status_infraestrutura() -> dict:
    inicio = perf_counter()
    banco_ok = True
    banco_erro = ""
    try:
        query_db("SELECT 1 AS ok", one=True)
    except Exception as exc:
        banco_ok = False
        banco_erro = str(exc)[:180]
    latencia_ms = round((perf_counter() - inicio) * 1000, 1)

    base_dir = Path(__file__).resolve().parents[1]
    candidatos = []
    for pasta_nome in ("backups", "backup", "instance/backups"):
        pasta = base_dir / pasta_nome
        if pasta.exists():
            candidatos.extend(arquivo for arquivo in pasta.rglob("*") if arquivo.is_file())
    ultimo = max(candidatos, key=lambda arquivo: arquivo.stat().st_mtime) if candidatos else None
    backups_db = sorted(
        [arquivo for arquivo in candidatos if arquivo.suffix.lower() == ".db"],
        key=lambda arquivo: arquivo.stat().st_mtime,
        reverse=True,
    )[:10]
    uso = shutil.disk_usage(base_dir)
    logs_dir = base_dir / "logs"
    error_log = logs_dir / "errors.log"
    error_log_kb = round(error_log.stat().st_size / 1024, 1) if error_log.exists() else 0
    return {
        "banco_tipo": "PostgreSQL" if is_postgres() else "SQLite local",
        "banco_ok": banco_ok,
        "banco_erro": banco_erro,
        "banco_latencia_ms": latencia_ms,
        "ultimo_backup": datetime.fromtimestamp(ultimo.stat().st_mtime).strftime("%d/%m/%Y %H:%M") if ultimo else "Não localizado",
        "backup_ok": bool(ultimo),
        "backups_recentes": [
            {
                "nome": arquivo.name,
                "data": datetime.fromtimestamp(arquivo.stat().st_mtime).strftime("%d/%m/%Y %H:%M"),
                "tamanho_mb": round(arquivo.stat().st_size / (1024 * 1024), 2),
            }
            for arquivo in backups_db
        ],
        "disco_livre_gb": round(uso.free / (1024 ** 3), 1),
        "disco_total_gb": round(uso.total / (1024 ** 3), 1),
        "error_log_kb": error_log_kb,
    }


def obter_integridade_integracoes() -> dict:
    limite = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    total_finalizados = _count("SELECT COUNT(*) total FROM grooming_services WHERE status='Finalizado'")
    financeiro_pendente = _count("""
        SELECT COUNT(*) total
        FROM grooming_services g
        WHERE g.status='Finalizado'
          AND NOT EXISTS (
              SELECT 1 FROM financial_transactions f
              WHERE f.source_type='Banho e Tosa' AND f.source_id=g.id
          )
    """)
    estoque_pendente = _count("""
        SELECT COUNT(DISTINCT g.id) total
        FROM grooming_services g
        JOIN grooming_product_usage u ON u.grooming_id=g.id
        WHERE g.status='Finalizado' AND COALESCE(u.stock_applied,0)=0
    """)
    agenda_pendente = _count("""
        SELECT COUNT(*) total
        FROM grooming_services g
        JOIN appointments a ON a.id=g.appointment_id
        WHERE COALESCE(a.status,'') <> COALESCE(g.status,'')
    """)
    comissao_pendente = _count("""
        SELECT COUNT(*) total
        FROM grooming_services g
        JOIN employees e ON e.id=g.employee_id
        WHERE g.status='Finalizado'
          AND COALESCE(e.commission_rate,0)>0
          AND NOT EXISTS (
              SELECT 1 FROM employee_commissions c WHERE c.grooming_id=g.id
          )
    """)
    eventos_24h = _count("SELECT COUNT(*) total FROM system_events WHERE created_at>=?", (limite,))
    eventos_erro_24h = _count("SELECT COUNT(*) total FROM system_events WHERE created_at>=? AND status='Erro'", (limite,))

    pendencias = query_db("""
        SELECT g.id, g.data, g.servico, g.valor, g.financeiro_lancado,
               g.estoque_baixado, g.commission_lancada,
               c.nome cliente_nome, p.nome pet_nome, e.name funcionario_nome,
               CASE WHEN EXISTS(
                   SELECT 1 FROM financial_transactions f
                   WHERE f.source_type='Banho e Tosa' AND f.source_id=g.id
               ) THEN 1 ELSE 0 END financeiro_ok,
               CASE WHEN EXISTS(
                   SELECT 1 FROM grooming_product_usage u
                   WHERE u.grooming_id=g.id AND COALESCE(u.stock_applied,0)=0
               ) THEN 0 ELSE 1 END estoque_ok,
               CASE WHEN COALESCE(e.commission_rate,0)<=0 OR EXISTS(
                   SELECT 1 FROM employee_commissions ec WHERE ec.grooming_id=g.id
               ) THEN 1 ELSE 0 END comissao_ok
        FROM grooming_services g
        LEFT JOIN clients c ON c.id=g.client_id
        LEFT JOIN pets p ON p.id=g.pet_id
        LEFT JOIN employees e ON e.id=g.employee_id
        WHERE g.status='Finalizado'
          AND (
              NOT EXISTS(SELECT 1 FROM financial_transactions f WHERE f.source_type='Banho e Tosa' AND f.source_id=g.id)
              OR EXISTS(SELECT 1 FROM grooming_product_usage u WHERE u.grooming_id=g.id AND COALESCE(u.stock_applied,0)=0)
              OR (COALESCE(e.commission_rate,0)>0 AND NOT EXISTS(SELECT 1 FROM employee_commissions ec WHERE ec.grooming_id=g.id))
          )
        ORDER BY g.data DESC, g.id DESC
        LIMIT 100
    """)
    eventos = query_db("SELECT * FROM system_events ORDER BY id DESC LIMIT 30")

    total_pendencias = financeiro_pendente + estoque_pendente + comissao_pendente + agenda_pendente
    infraestrutura = _status_infraestrutura()
    return {
        "total_finalizados": total_finalizados,
        "financeiro_pendente": financeiro_pendente,
        "estoque_pendente": estoque_pendente,
        "comissao_pendente": comissao_pendente,
        "agenda_pendente": agenda_pendente,
        "total_pendencias": total_pendencias,
        "eventos_24h": eventos_24h,
        "eventos_erro_24h": eventos_erro_24h,
        "pendencias": pendencias,
        "eventos": eventos,
        "status_geral": "Completo" if total_pendencias == 0 and eventos_erro_24h == 0 and infraestrutura["banco_ok"] else "Atenção",
        **infraestrutura,
    }


def reprocessar_atendimento(atendimento_id: int, user_name: str = "Sistema") -> dict:
    atendimento = query_db("SELECT * FROM grooming_services WHERE id=?", (atendimento_id,), one=True)
    if not atendimento:
        return {"ok": False, "mensagem": "Atendimento não encontrado.", "acoes": []}
    if atendimento["status"] != "Finalizado":
        return {"ok": False, "mensagem": "Somente atendimentos finalizados podem ser reprocessados.", "acoes": []}

    acoes = []
    if atendimento["appointment_id"]:
        agenda = query_db("SELECT status FROM appointments WHERE id=?", (atendimento["appointment_id"],), one=True)
        if agenda and (agenda["status"] or "") != (atendimento["status"] or ""):
            execute_db("UPDATE appointments SET status=?, updated_at=? WHERE id=?", (atendimento["status"], now_iso(), atendimento["appointment_id"]))
            acoes.append("agenda sincronizada")
    if lancar_receita_banho_tosa(atendimento_id):
        execute_db("UPDATE grooming_services SET financeiro_lancado=1, updated_at=? WHERE id=?", (now_iso(), atendimento_id))
        acoes.append("receita financeira criada")
    elif query_db("SELECT id FROM financial_transactions WHERE source_type='Banho e Tosa' AND source_id=?", (atendimento_id,), one=True):
        execute_db("UPDATE grooming_services SET financeiro_lancado=1, updated_at=? WHERE id=?", (now_iso(), atendimento_id))

    uso_pendente = query_db("SELECT id FROM grooming_product_usage WHERE grooming_id=? AND COALESCE(stock_applied,0)=0 LIMIT 1", (atendimento_id,), one=True)
    if uso_pendente and baixar_estoque_banho_tosa(atendimento_id):
        restante = query_db("SELECT id FROM grooming_product_usage WHERE grooming_id=? AND COALESCE(stock_applied,0)=0 LIMIT 1", (atendimento_id,), one=True)
        if not restante:
            execute_db("UPDATE grooming_services SET estoque_baixado=1, updated_at=? WHERE id=?", (now_iso(), atendimento_id))
        acoes.append("consumo de estoque aplicado")

    if lancar_comissao(atendimento_id):
        acoes.append("comissão calculada")

    publish("INTEGRACAO_REPROCESSADA", {
        "entity_type": "grooming_service",
        "entity_id": atendimento_id,
        "user_name": user_name,
        "acoes": acoes,
    })
    return {
        "ok": True,
        "mensagem": "Integrações verificadas." if not acoes else "Integrações corrigidas: " + ", ".join(acoes) + ".",
        "acoes": acoes,
    }


def reprocessar_pendencias(user_name: str = "Sistema", limite: int = 100) -> dict:
    itens = query_db("""
        SELECT DISTINCT g.id
        FROM grooming_services g
        LEFT JOIN employees e ON e.id=g.employee_id
        WHERE g.status='Finalizado' AND (
            NOT EXISTS(SELECT 1 FROM financial_transactions f WHERE f.source_type='Banho e Tosa' AND f.source_id=g.id)
            OR EXISTS(SELECT 1 FROM grooming_product_usage u WHERE u.grooming_id=g.id AND COALESCE(u.stock_applied,0)=0)
            OR (COALESCE(e.commission_rate,0)>0 AND NOT EXISTS(SELECT 1 FROM employee_commissions ec WHERE ec.grooming_id=g.id))
            OR (g.appointment_id IS NOT NULL AND EXISTS(SELECT 1 FROM appointments a WHERE a.id=g.appointment_id AND COALESCE(a.status,'')<>COALESCE(g.status,'')))
        )
        ORDER BY g.id
        LIMIT ?
    """, (limite,))
    corrigidos = 0
    processados = 0
    for item in itens:
        resultado = reprocessar_atendimento(item["id"], user_name=user_name)
        processados += 1
        if resultado["acoes"]:
            corrigidos += 1
    return {"processados": processados, "corrigidos": corrigidos}
