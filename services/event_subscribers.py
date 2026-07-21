from __future__ import annotations

import json

from core.cache import cache
from services.auditoria_service import registrar_auditoria
from services.event_bus import subscribe

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


def register_default_subscribers():
    global _registered
    if _registered:
        return
    subscribe("*", _invalidate_cache)
    subscribe("*", _audit_event)
    _registered = True
