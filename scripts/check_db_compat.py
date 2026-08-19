"""Diagnóstico de compatibilidade do banco selecionado."""
from pathlib import Path
import os
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.env_loader import load_project_env

load_project_env(PROJECT_ROOT)

from core.db_compat import schema_report
from database import SQLITE_DB, is_postgres

REQUIRED = {
    "clients": ["id", "nome", "telefone", "whatsapp"],
    "message_templates": ["id", "name", "content", "active"],
    "communication_logs": ["id", "channel", "recipient", "message", "status"],
    "pos_sales": ["id", "sale_number", "total_amount", "status"],
    "financial_transactions": ["id", "type", "amount", "status"],
    "schema_migrations": ["id", "version", "applied_at"],
}


def main() -> int:
    print("=" * 60)
    print("DIAGNÓSTICO DE COMPATIBILIDADE DO BANCO")
    print("=" * 60)
    report = schema_report(REQUIRED)
    print(f"Banco selecionado: {report['backend']}")
    if is_postgres():
        url = os.environ.get("DATABASE_URL", "")
        host = url.split("@", 1)[-1].split("/", 1)[0] if "@" in url else "configurado"
        print(f"Origem: DATABASE_URL ({host})")
    else:
        print(f"Arquivo: {SQLITE_DB}")
    for table, info in report["tables"].items():
        status = "OK" if info["exists"] and not info["missing_columns"] else "ATENÇÃO"
        print(f"- {table}: {status}")
        if not info["exists"]:
            print("  tabela ausente")
        elif info["missing_columns"]:
            print("  colunas ausentes: " + ", ".join(info["missing_columns"]))
    print("=" * 60)
    print("Estrutura compatível." if report["ok"] else "Existem pendências. Execute: py scripts\\migrate_database.py")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
