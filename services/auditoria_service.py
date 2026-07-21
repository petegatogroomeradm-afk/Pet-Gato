from __future__ import annotations

from database import execute_db, now_iso


def registrar_auditoria(
    user_name: str,
    action: str,
    entity_type: str,
    entity_id=None,
    details: str = "",
):
    execute_db(
        """
        INSERT INTO audit_logs
        (user_name, action, entity_type, entity_id, details, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            user_name or "Sistema",
            action,
            entity_type,
            entity_id,
            details,
            now_iso(),
        ),
    )
