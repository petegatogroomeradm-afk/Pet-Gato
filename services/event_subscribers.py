from __future__ import annotations

import json

from core.cache import cache
from services.auditoria_service import registrar_auditoria
from services.event_bus import subscribe
from database import execute_db, now_iso, query_db

_registered = False


def _invalidate_cache(event_name: str, payload: dict):
    cache.clear("dashboard:")
    cache.clear("reports:")


def _audit_event(event_name: str, payload: dict):
    registrar_auditoria(
        payload.get("user_name", "Sistema"),
        event_name,
        payload.get("entity_type", "system_event"),
        payload.get("entity_id"),
        json.dumps(payload, ensure_ascii=False, default=str),
    )



def _sync_grooming_to_appointment(payload: dict):
    """Mantém Agenda e Banho & Tosa com o mesmo status operacional."""
    grooming_id = payload.get("entity_id")
    if not grooming_id:
        return
    grooming = query_db(
        "SELECT appointment_id, status FROM grooming_services WHERE id=?",
        (grooming_id,),
        one=True,
    )
    if not grooming or not grooming["appointment_id"]:
        return
    status = payload.get("novo_status") or grooming["status"] or "Agendado"
    execute_db(
        "UPDATE appointments SET status=?, updated_at=? WHERE id=?",
        (status, now_iso(), grooming["appointment_id"]),
    )


def _finish_grooming_timestamps(payload: dict):
    grooming_id = payload.get("entity_id")
    if not grooming_id:
        return
    execute_db(
        """UPDATE grooming_services
              SET finished_at=COALESCE(finished_at, ?),
                  hora_saida=COALESCE(NULLIF(hora_saida,''), ?),
                  updated_at=?
            WHERE id=?""",
        (now_iso(), now_iso()[11:16], now_iso(), grooming_id),
    )

def register_default_subscribers():
    global _registered
    if _registered:
        return
    subscribe("*", _invalidate_cache)
    subscribe("*", _audit_event)
    subscribe("ATENDIMENTO_STATUS_ALTERADO", _sync_grooming_to_appointment)
    subscribe("ATENDIMENTO_FINALIZADO", _sync_grooming_to_appointment)
    subscribe("ATENDIMENTO_FINALIZADO", _finish_grooming_timestamps)
    _registered = True
