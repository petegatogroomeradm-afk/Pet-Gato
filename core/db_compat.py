"""Camada pequena de compatibilidade entre SQLite e PostgreSQL.

Centraliza diferenças de DDL para que módulos não precisem escrever
AUTOINCREMENT/SERIAL diretamente.
"""
from __future__ import annotations

from typing import Iterable, Mapping

from database import execute_db, get_conn, is_postgres


def backend_name() -> str:
    return "postgresql" if is_postgres() else "sqlite"


def auto_pk_sql() -> str:
    return "SERIAL PRIMARY KEY" if is_postgres() else "INTEGER PRIMARY KEY AUTOINCREMENT"


def bool_sql(default: bool = False) -> str:
    # O projeto mantém 0/1 para compatibilidade com consultas existentes.
    return f"INTEGER NOT NULL DEFAULT {1 if default else 0}"


def table_exists(table: str) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    try:
        if is_postgres():
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema='public' AND table_name=%s",
                (table,),
            )
        else:
            cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,))
        return cur.fetchone() is not None
    finally:
        cur.close()
        conn.close()


def column_exists(table: str, column: str) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    try:
        if is_postgres():
            cur.execute(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name=%s AND column_name=%s",
                (table, column),
            )
            return cur.fetchone() is not None
        cur.execute(f'PRAGMA table_info("{table}")')
        return any((row[1] if not isinstance(row, dict) else row.get("name")) == column for row in cur.fetchall())
    finally:
        cur.close()
        conn.close()


def ensure_table(table: str, columns_sql: str) -> None:
    execute_db(f"CREATE TABLE IF NOT EXISTS {table} ({columns_sql})")


def ensure_columns(table: str, columns: Mapping[str, str]) -> None:
    for name, definition in columns.items():
        if not column_exists(table, name):
            execute_db(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")


def schema_report(required: Mapping[str, Iterable[str]]) -> dict:
    report = {"backend": backend_name(), "tables": {}, "ok": True}
    for table, columns in required.items():
        exists = table_exists(table)
        missing = [] if not exists else [c for c in columns if not column_exists(table, c)]
        report["tables"][table] = {"exists": exists, "missing_columns": missing}
        if not exists or missing:
            report["ok"] = False
    return report
