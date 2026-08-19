"""Atualiza com segurança o banco usado pelo projeto."""
from pathlib import Path
import argparse
import os
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.env_loader import load_project_env


def configure_backend(force_sqlite: bool) -> None:
    load_project_env(PROJECT_ROOT)
    if force_sqlite:
        os.environ.pop("DATABASE_URL", None)


def main() -> int:
    parser = argparse.ArgumentParser(description="Migra o banco Pet & Gatô sem apagar dados.")
    parser.add_argument("--sqlite", action="store_true", help="força a migração do SQLite local")
    parser.add_argument("--no-backup", action="store_true", help="não cria backup automático do SQLite")
    args = parser.parse_args()

    configure_backend(args.sqlite)

    from core.migrations import run_migrations

    print("=" * 60)
    print("MIGRAÇÃO SEGURA DO BANCO")
    print("=" * 60)
    result = run_migrations(create_backup=not args.no_backup)
    print(f"Banco atualizado: {result.backend}")
    print(f"Versão da migração: {result.version}")
    print("Status: aplicada agora" if result.applied else "Status: já estava aplicada")
    if result.backup_path:
        print(f"Backup automático: {result.backup_path}")
    elif result.backend == "postgresql":
        print("Backup local: não aplicável ao PostgreSQL")
    print("Migração concluída sem exclusão de dados.")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
