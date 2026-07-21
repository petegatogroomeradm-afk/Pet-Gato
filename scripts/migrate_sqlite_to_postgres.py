#!/usr/bin/env python3
"""Migra os dados do SQLite atual para o PostgreSQL definido em DATABASE_URL.

Uso:
  set DATABASE_URL=postgresql://...
  set ADMIN_PASSWORD=...
  python scripts/migrate_sqlite_to_postgres.py instance/petegato_business_v3.db
"""
from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main() -> int:
    source = Path(sys.argv[1] if len(sys.argv) > 1 else ROOT / "instance" / "petegato_business_v3.db")
    database_url = os.environ.get("DATABASE_URL")
    if not source.exists():
        raise SystemExit(f"SQLite não encontrado: {source}")
    if not database_url:
        raise SystemExit("Defina DATABASE_URL antes de executar.")
    if not os.environ.get("ADMIN_PASSWORD"):
        raise SystemExit("Defina ADMIN_PASSWORD antes de executar.")

    from database import init_db
    init_db()

    sqlite_conn = sqlite3.connect(source)
    sqlite_conn.row_factory = sqlite3.Row
    pg_conn = psycopg2.connect(database_url)
    try:
        table_rows = sqlite_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        with pg_conn.cursor() as pg_cur:
            for table_row in table_rows:
                table = table_row["name"]
                pg_cur.execute("SELECT to_regclass(%s)", (f"public.{table}",))
                if pg_cur.fetchone()[0] is None:
                    print(f"[ignorado] tabela ausente no destino: {table}")
                    continue
                columns = [row["name"] for row in sqlite_conn.execute(f'PRAGMA table_info("{table}")')]
                rows = sqlite_conn.execute(f'SELECT * FROM "{table}"').fetchall()
                if not rows:
                    print(f"[vazio] {table}")
                    continue
                values = [tuple(row[col] for col in columns) for row in rows]
                cols_sql = ", ".join(f'"{col}"' for col in columns)
                sql = f'INSERT INTO "{table}" ({cols_sql}) VALUES %s ON CONFLICT DO NOTHING'
                execute_values(pg_cur, sql, values, page_size=500)
                print(f"[ok] {table}: {len(values)} registro(s)")

            pg_cur.execute("""
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_schema='public' AND column_default LIKE 'nextval(%'
            """)
            for table, column in pg_cur.fetchall():
                pg_cur.execute("SELECT pg_get_serial_sequence(%s, %s)", (table, column))
                seq = pg_cur.fetchone()[0]
                if seq:
                    pg_cur.execute(
                        f'SELECT setval(%s, COALESCE((SELECT MAX("{column}") FROM "{table}"), 1), true)',
                        (seq,),
                    )
        pg_conn.commit()
        print("Migração concluída com sucesso.")
        return 0
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        sqlite_conn.close()
        pg_conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
