from __future__ import annotations

import json
import logging
from collections import defaultdict
from typing import Callable

from database import execute_db, insert_db, now_iso

logger = logging.getLogger(__name__)
_handlers: dict[str, list[Callable]] = defaultdict(list)


def subscribe(event_name: str, handler: Callable):
    if handler not in _handlers[event_name]:
        _handlers[event_name].append(handler)


def unsubscribe(event_name: str, handler: Callable):
    if handler in _handlers.get(event_name, []):
        _handlers[event_name].remove(handler)


def publish(event_name: str, payload: dict | None = None, raise_on_error: bool = False):
    payload = payload or {}
    event_id = insert_db(
        """INSERT INTO system_events
        (event_name, payload, status, created_at) VALUES (?, ?, ?, ?)""",
        (event_name, json.dumps(payload, ensure_ascii=False, default=str), "Processando", now_iso()),
    )

    errors = []
    handlers = list(_handlers.get(event_name, [])) + list(_handlers.get("*", []))
    for handler in handlers:
        try:
            handler(event_name, payload) if handler in _handlers.get("*", []) else handler(payload)
        except Exception as exc:
            logger.exception("Falha no evento %s usando %s", event_name, handler)
            errors.append(str(exc))

    execute_db(
        "UPDATE system_events SET status = ? WHERE id = ?",
        ("Erro" if errors else "Concluído", event_id),
    )

    if errors and raise_on_error:
        raise RuntimeError("; ".join(errors))
    return {"event_id": event_id, "errors": errors}
