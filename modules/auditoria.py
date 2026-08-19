from __future__ import annotations

import csv
import io
from datetime import date

from flask import Blueprint, Response, render_template, request

from core.permissions import require_permission
from database import query_db


auditoria_bp = Blueprint("auditoria", __name__, url_prefix="/auditoria")


def _filters():
    return {
        "q": request.args.get("q", "").strip(),
        "action": request.args.get("action", "").strip(),
        "entity_type": request.args.get("entity_type", "").strip(),
        "date_from": request.args.get("date_from", "").strip(),
        "date_to": request.args.get("date_to", "").strip(),
    }


def _query_logs(limit: int | None = 300):
    filters = _filters()
    sql = "SELECT * FROM audit_logs WHERE 1=1"
    params: list[object] = []

    if filters["q"]:
        like = f"%{filters['q'].lower()}%"
        sql += " AND (LOWER(COALESCE(user_name,'')) LIKE ? OR LOWER(COALESCE(details,'')) LIKE ? OR LOWER(COALESCE(request_path,'')) LIKE ?)"
        params.extend([like, like, like])
    if filters["action"]:
        sql += " AND action = ?"
        params.append(filters["action"])
    if filters["entity_type"]:
        sql += " AND entity_type = ?"
        params.append(filters["entity_type"])
    if filters["date_from"]:
        sql += " AND created_at >= ?"
        params.append(filters["date_from"] + " 00:00:00")
    if filters["date_to"]:
        sql += " AND created_at <= ?"
        params.append(filters["date_to"] + " 23:59:59")

    sql += " ORDER BY id DESC"
    if limit:
        sql += f" LIMIT {int(limit)}"
    return query_db(sql, tuple(params)), filters


@auditoria_bp.route("/")
@require_permission("auditoria")
def painel():
    logs, filters = _query_logs()
    actions = query_db("SELECT DISTINCT action FROM audit_logs WHERE action IS NOT NULL ORDER BY action")
    entities = query_db("SELECT DISTINCT entity_type FROM audit_logs WHERE entity_type IS NOT NULL ORDER BY entity_type")
    today_start = date.today().isoformat() + " 00:00:00"
    stats = {
        "total": (query_db("SELECT COUNT(*) AS total FROM audit_logs", one=True) or {"total": 0})["total"],
        "today": (query_db("SELECT COUNT(*) AS total FROM audit_logs WHERE created_at >= ?", (today_start,), one=True) or {"total": 0})["total"],
        "users": (query_db("SELECT COUNT(DISTINCT user_name) AS total FROM audit_logs", one=True) or {"total": 0})["total"],
        "failed_logins": (query_db("SELECT COUNT(*) AS total FROM user_access_logs WHERE COALESCE(success,0)=0 AND created_at >= ?", (today_start,), one=True) or {"total": 0})["total"],
    }
    top_actions = query_db("""
        SELECT action, COUNT(*) AS total
        FROM audit_logs
        WHERE created_at >= ? AND action IS NOT NULL
        GROUP BY action ORDER BY total DESC LIMIT 5
    """, (today_start,))
    return render_template("auditoria.html", logs=logs, filters=filters, actions=actions, entities=entities, stats=stats, top_actions=top_actions)


@auditoria_bp.route("/exportar.csv")
@require_permission("auditoria")
def exportar_csv():
    logs, _ = _query_logs(limit=None)
    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")
    writer.writerow(["Data", "Usuário", "Ação", "Entidade", "ID", "Detalhes", "IP", "Método", "Rota"])
    for row in logs:
        writer.writerow([
            row["created_at"], row["user_name"], row["action"], row["entity_type"], row["entity_id"],
            row["details"], row["ip_address"], row["request_method"], row["request_path"],
        ])
    content = "\ufeff" + output.getvalue()
    return Response(content, mimetype="text/csv; charset=utf-8", headers={"Content-Disposition": "attachment; filename=auditoria_petegato.csv"})
