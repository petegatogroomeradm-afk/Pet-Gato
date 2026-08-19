from __future__ import annotations

from flask import has_request_context, request, session, g

from database import execute_db, now_iso


def _request_metadata():
    if not has_request_context():
        return None, None, None, None, None
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",")[0].strip()
    return (
        session.get("user_id"),
        ip,
        request.method,
        request.path,
        request.headers.get("User-Agent", "")[:500],
    )


def registrar_auditoria(
    user_name: str | None,
    action: str,
    entity_type: str,
    entity_id=None,
    details: str = "",
):
    user_id, ip_address, request_method, request_path, user_agent = _request_metadata()
    execute_db(
        """
        INSERT INTO audit_logs
        (user_id, user_name, action, entity_type, entity_id, details,
         ip_address, request_method, request_path, user_agent, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_id,
            user_name or (session.get("user_name") if has_request_context() else None) or "Sistema",
            action,
            entity_type,
            entity_id,
            details,
            ip_address,
            request_method,
            request_path,
            user_agent,
            now_iso(),
        ),
    )
    if has_request_context():
        g.audit_recorded = True
