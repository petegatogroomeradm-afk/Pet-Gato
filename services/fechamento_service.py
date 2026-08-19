from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any

from database import execute_db, now_iso, query_db
from services.auditoria_service import registrar_auditoria
from services.financeiro_service import lancar_receita_banho_tosa
from services.estoque_service import baixar_estoque_banho_tosa
from services.banho_service import lancar_comissao, obter_atendimento, atualizar_status_atendimento
from services.historico_service import registrar_historico
from services.event_bus import publish


@dataclass
class IntegrationResult:
    name: str
    ok: bool
    status: str
    message: str


def _flag(atendimento: Any, key: str) -> bool:
    try:
        return bool(atendimento[key])
    except (KeyError, TypeError):
        return False


def resumo_fechamento(atendimento_id: int) -> dict:
    atendimento = obter_atendimento(atendimento_id)
    if not atendimento:
        raise ValueError("Atendimento não encontrado.")

    produtos = query_db(
        """SELECT u.*, p.name AS product_name, p.quantity AS stock_quantity, p.unit AS product_unit
           FROM grooming_product_usage u
           LEFT JOIN stock_products p ON p.id=u.product_id
           WHERE u.grooming_id=? ORDER BY u.id""",
        (atendimento_id,),
    )
    checklist = query_db(
        "SELECT * FROM grooming_checklists WHERE grooming_id=?",
        (atendimento_id,), one=True,
    )
    comissao = query_db(
        "SELECT * FROM employee_commissions WHERE grooming_id=? ORDER BY id DESC LIMIT 1",
        (atendimento_id,), one=True,
    )
    return {
        "atendimento": atendimento,
        "produtos": produtos,
        "checklist": checklist,
        "comissao": comissao,
        "integracoes": {
            "financeiro": _flag(atendimento, "financeiro_lancado"),
            "estoque": _flag(atendimento, "estoque_baixado"),
            "comissao": _flag(atendimento, "commission_lancada"),
        },
    }


def _run_financeiro(atendimento_id: int, atual: Any) -> IntegrationResult:
    if _flag(atual, "financeiro_lancado"):
        return IntegrationResult("Financeiro", True, "existente", "Lançamento financeiro já existente.")
    try:
        created = lancar_receita_banho_tosa(atendimento_id)
        # False também pode significar que a proteção contra duplicidade encontrou o lançamento.
        exists = query_db(
            "SELECT id FROM financial_transactions WHERE source_type=? AND source_id=? LIMIT 1",
            ("Banho e Tosa", atendimento_id), one=True,
        )
        if created or exists:
            execute_db("UPDATE grooming_services SET financeiro_lancado=1, updated_at=? WHERE id=?", (now_iso(), atendimento_id))
            registrar_historico(atendimento_id, "Financeiro confirmado no fechamento", "Sistema")
            return IntegrationResult("Financeiro", True, "concluido", "Receita registrada no Financeiro.")
        return IntegrationResult("Financeiro", False, "pendente", "Não foi possível confirmar o lançamento financeiro.")
    except Exception as exc:
        return IntegrationResult("Financeiro", False, "erro", str(exc))


def _run_estoque(atendimento_id: int, atual: Any) -> IntegrationResult:
    if _flag(atual, "estoque_baixado"):
        return IntegrationResult("Estoque", True, "existente", "Baixa de estoque já realizada.")
    try:
        usos = query_db("SELECT id, stock_applied FROM grooming_product_usage WHERE grooming_id=?", (atendimento_id,))
        if not usos:
            return IntegrationResult("Estoque", True, "nao_aplicavel", "Nenhum produto configurado para consumo.")
        baixar_estoque_banho_tosa(atendimento_id)
        pendentes = query_db(
            "SELECT id FROM grooming_product_usage WHERE grooming_id=? AND COALESCE(stock_applied,0)=0",
            (atendimento_id,),
        )
        if not pendentes:
            execute_db("UPDATE grooming_services SET estoque_baixado=1, updated_at=? WHERE id=?", (now_iso(), atendimento_id))
            registrar_historico(atendimento_id, "Estoque confirmado no fechamento", "Sistema")
            return IntegrationResult("Estoque", True, "concluido", "Consumo baixado do estoque.")
        return IntegrationResult("Estoque", False, "pendente", f"{len(pendentes)} consumo(s) ainda pendente(s).")
    except Exception as exc:
        return IntegrationResult("Estoque", False, "erro", str(exc))


def _run_comissao(atendimento_id: int, atual: Any) -> IntegrationResult:
    if _flag(atual, "commission_lancada"):
        return IntegrationResult("Comissão", True, "existente", "Comissão já calculada.")
    if not atual["employee_id"]:
        return IntegrationResult("Comissão", True, "nao_aplicavel", "Atendimento sem profissional responsável.")
    try:
        created = lancar_comissao(atendimento_id)
        exists = query_db("SELECT id FROM employee_commissions WHERE grooming_id=? LIMIT 1", (atendimento_id,), one=True)
        if created or exists:
            execute_db("UPDATE grooming_services SET commission_lancada=1, updated_at=? WHERE id=?", (now_iso(), atendimento_id))
            registrar_historico(atendimento_id, "Comissão confirmada no fechamento", "Sistema")
            return IntegrationResult("Comissão", True, "concluido", "Comissão calculada para o profissional.")
        return IntegrationResult("Comissão", False, "pendente", "Não foi possível confirmar a comissão.")
    except Exception as exc:
        return IntegrationResult("Comissão", False, "erro", str(exc))


def executar_fechamento(
    atendimento_id: int,
    *,
    payment_method: str,
    valor: float,
    checkout_notes: str,
    user_name: str = "Administrador",
) -> dict:
    atual = obter_atendimento(atendimento_id)
    if not atual:
        raise ValueError("Atendimento não encontrado.")

    execute_db(
        """UPDATE grooming_services
           SET payment_method=?, valor=?, checked_out_at=COALESCE(checked_out_at,?),
               finished_at=COALESCE(finished_at,?), updated_at=?
           WHERE id=?""",
        (payment_method or "A definir", valor, now_iso(), now_iso(), now_iso(), atendimento_id),
    )

    checklist = query_db("SELECT id FROM grooming_checklists WHERE grooming_id=?", (atendimento_id,), one=True)
    if checklist:
        execute_db(
            "UPDATE grooming_checklists SET checkout_notes=?, checked_out_at=COALESCE(checked_out_at,?), updated_at=? WHERE grooming_id=?",
            (checkout_notes, now_iso(), now_iso(), atendimento_id),
        )
    else:
        execute_db(
            "INSERT INTO grooming_checklists (grooming_id,checkout_notes,checked_out_at,updated_at) VALUES (?,?,?,?)",
            (atendimento_id, checkout_notes, now_iso(), now_iso()),
        )

    if atual["status"] != "Finalizado":
        atualizar_status_atendimento(atendimento_id, "Finalizado")
        registrar_historico(atendimento_id, f"Atendimento finalizado por {user_name}", user_name)

    atualizado = obter_atendimento(atendimento_id)
    results = [
        _run_financeiro(atendimento_id, atualizado),
        _run_estoque(atendimento_id, atualizado),
        _run_comissao(atendimento_id, atualizado),
    ]

    payload = {
        "entity_type": "grooming_service",
        "entity_id": atendimento_id,
        "user_name": user_name,
        "integrations": [asdict(item) for item in results],
    }
    event = publish("ATENDIMENTO_FINALIZADO", payload)
    registrar_auditoria(
        user_name,
        "finalizar_atendimento",
        "grooming_service",
        atendimento_id,
        "; ".join(f"{r.name}: {r.status}" for r in results),
    )
    return {
        "ok": all(item.ok for item in results),
        "results": results,
        "event": event,
        "atendimento": obter_atendimento(atendimento_id),
    }


def reprocessar_integracoes(atendimento_id: int, user_name: str = "Administrador") -> dict:
    atual = obter_atendimento(atendimento_id)
    if not atual:
        raise ValueError("Atendimento não encontrado.")
    results = [
        _run_financeiro(atendimento_id, atual),
        _run_estoque(atendimento_id, obter_atendimento(atendimento_id)),
        _run_comissao(atendimento_id, obter_atendimento(atendimento_id)),
    ]
    registrar_auditoria(
        user_name,
        "reprocessar_integracoes_atendimento",
        "grooming_service",
        atendimento_id,
        "; ".join(f"{r.name}: {r.status}" for r in results),
    )
    return {"ok": all(item.ok for item in results), "results": results}
