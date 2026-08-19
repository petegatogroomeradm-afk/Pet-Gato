"""Infraestrutura central de migrações do Pet & Gatô Business.

As migrações atuais reutilizam o init_db consolidado, que é idempotente:
cria tabelas ausentes e adiciona colunas sem apagar dados.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import shutil

from database import SQLITE_DB, execute_db, init_db, insert_db, is_postgres, now_iso, query_db

MIGRATION_VERSION = "12.0.0-RC1"


@dataclass
class MigrationResult:
    backend: str
    version: str
    backup_path: str | None
    applied: bool


def _ensure_history_table() -> None:
    pk = "SERIAL PRIMARY KEY" if is_postgres() else "INTEGER PRIMARY KEY AUTOINCREMENT"
    execute_db(
        f"""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id {pk},
            version TEXT NOT NULL UNIQUE,
            description TEXT,
            applied_at TEXT NOT NULL
        )
        """
    )


def _already_applied(version: str) -> bool:
    row = query_db("SELECT id FROM schema_migrations WHERE version = ?", (version,), one=True)
    return row is not None


def backup_sqlite() -> Path | None:
    if is_postgres() or not SQLITE_DB.exists():
        return None
    backup_dir = SQLITE_DB.parent.parent / "backups" / "database"
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    target = backup_dir / f"petegato_business_v3_antes_migracao_{stamp}.db"
    shutil.copy2(SQLITE_DB, target)
    return target


def run_migrations(*, create_backup: bool = True) -> MigrationResult:
    backup = backup_sqlite() if create_backup else None

    # init_db pode ser executado várias vezes com segurança.
    init_db()
    _ensure_history_table()

    applied = False
    if not _already_applied(MIGRATION_VERSION):
        insert_db(
            """
            INSERT INTO schema_migrations (version, description, applied_at)
            VALUES (?, ?, ?)
            """,
            (
                MIGRATION_VERSION,
                "V12 RC1: consolidação do schema financeiro e migrações automáticas",
                now_iso(),
            ),
        )
        applied = True

    return MigrationResult(
        backend="postgresql" if is_postgres() else "sqlite",
        version=MIGRATION_VERSION,
        backup_path=str(backup) if backup else None,
        applied=applied,
    )
