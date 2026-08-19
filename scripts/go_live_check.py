from __future__ import annotations

import compileall
import os
import re
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERSION_FILE = ROOT / "VERSION"
SQLITE_DB = ROOT / "instance" / "petegato_business_v3.db"

CRITICAL_FILES = [
    "main.py", "database.py", "core/permissions.py", "core/version.py",
    "modules/agenda.py", "modules/banho_tosa.py", "modules/clientes.py",
    "modules/pets.py", "modules/financeiro.py", "modules/pdv.py",
    "modules/estoque.py", "modules/comissoes.py", "modules/auditoria.py",
    "modules/configuracoes.py", "scripts/check_release.py",
    "scripts/check_templates_routes.py",
]

CRITICAL_TABLES = [
    "users", "clients", "pets", "appointments", "grooming_services",
    "financial_transactions", "financial_payments", "financial_category_budgets",
    "financial_recurrences", "cash_register_sessions", "pos_sales",
    "stock_products", "employee_commissions", "audit_logs", "settings",
]

CRITICAL_COLUMNS = {
    "financial_transactions": {
        "type", "amount", "status", "paid_amount", "reconciliation_status",
        "reconciled_at", "bank_reference",
    },
    "users": {"username", "password_hash", "role", "active"},
    "clients": {"nome", "telefone", "whatsapp", "ativo"},
    "pets": {"client_id", "nome", "ativo"},
}


def emit(kind: str, message: str) -> None:
    print(f"[{kind}] {message}")


def check_files() -> tuple[int, int]:
    failures = warnings = 0
    for rel in CRITICAL_FILES:
        if not (ROOT / rel).exists():
            emit("FALHA", f"Arquivo obrigatório ausente: {rel}")
            failures += 1
    if failures == 0:
        emit("OK", "Arquivos críticos presentes.")
    return failures, warnings


def check_python() -> tuple[int, int]:
    ok = compileall.compile_dir(str(ROOT), quiet=1, rx=re.compile(r"[\\/]\.venv[\\/]"))
    if not ok:
        emit("FALHA", "Há arquivos Python com erro de compilação.")
        return 1, 0
    emit("OK", "Compilação Python concluída.")
    return 0, 0


def check_templates_routes() -> tuple[int, int]:
    checker = ROOT / "scripts" / "check_templates_routes.py"
    result = subprocess.run([sys.executable, str(checker)], cwd=ROOT)
    if result.returncode != 0:
        emit("FALHA", "A validação de templates/rotas encontrou problemas.")
        return 1, 0
    emit("OK", "Templates e rotas validados.")
    return 0, 0


def check_sqlite() -> tuple[int, int]:
    failures = warnings = 0
    if os.environ.get("DATABASE_URL"):
        emit("AVISO", "DATABASE_URL definida: validação profunda do PostgreSQL deve ser feita com check_db_compat.py.")
        return 0, 1
    if not SQLITE_DB.exists():
        emit("AVISO", f"Banco SQLite local não encontrado: {SQLITE_DB}")
        return 0, 1

    uri = SQLITE_DB.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        for table in CRITICAL_TABLES:
            if table not in tables:
                emit("FALHA", f"Tabela crítica ausente: {table}")
                failures += 1
        if failures == 0:
            emit("OK", "Tabelas críticas do SQLite presentes.")

        for table, required in CRITICAL_COLUMNS.items():
            if table not in tables:
                continue
            cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
            missing = sorted(required - cols)
            if missing:
                emit("FALHA", f"Colunas ausentes em {table}: {', '.join(missing)}")
                failures += 1
        if failures == 0:
            emit("OK", "Colunas críticas do SQLite presentes.")
    finally:
        conn.close()
    return failures, warnings


def check_directories() -> tuple[int, int]:
    warnings = 0
    for rel in ("instance", "uploads", "backups", "logs"):
        path = ROOT / rel
        if not path.exists():
            emit("AVISO", f"Pasta ainda não existe: {rel}")
            warnings += 1
        elif not os.access(path, os.W_OK):
            emit("AVISO", f"Pasta sem permissão de escrita: {rel}")
            warnings += 1
    if warnings == 0:
        emit("OK", "Pastas operacionais disponíveis para escrita.")
    return 0, warnings


def check_backup() -> tuple[int, int]:
    backup_dir = ROOT / "backups" / "database"
    backups = sorted(backup_dir.glob("*.db"), key=lambda p: p.stat().st_mtime, reverse=True) if backup_dir.exists() else []
    if not backups:
        emit("AVISO", "Nenhum backup SQLite encontrado em backups/database.")
        return 0, 1
    latest = backups[0]
    age_hours = (datetime.now().timestamp() - latest.stat().st_mtime) / 3600
    emit("OK", f"Último backup local: {latest.name} ({age_hours:.1f}h atrás).")
    if age_hours > 168:
        emit("AVISO", "O último backup local tem mais de 7 dias.")
        return 0, 1
    return 0, 0


def check_security() -> tuple[int, int]:
    warnings = 0
    app_env = os.environ.get("APP_ENV", "development").lower()
    if app_env == "production":
        if not os.environ.get("SECRET_KEY"):
            emit("FALHA", "SECRET_KEY não definida em produção.")
            return 1, warnings
        emit("OK", "SECRET_KEY definida em produção.")
        if not os.environ.get("DATABASE_URL"):
            emit("AVISO", "Produção sem DATABASE_URL: confirme se SQLite é realmente intencional.")
            warnings += 1
    else:
        emit("AVISO", "APP_ENV não está em production; adequado para teste local, não para go-live.")
        warnings += 1
    return 0, warnings


def main() -> int:
    version = VERSION_FILE.read_text(encoding="utf-8").strip() if VERSION_FILE.exists() else "desconhecida"
    print("=" * 68)
    print(f"PET & GATÔ BUSINESS — PRÉ-FLIGHT GO-LIVE — {version}")
    print("=" * 68)

    failures = warnings = 0
    for checker in (
        check_files,
        check_python,
        check_templates_routes,
        check_sqlite,
        check_directories,
        check_backup,
        check_security,
    ):
        f, w = checker()
        failures += f
        warnings += w

    print("-" * 68)
    print(f"Falhas: {failures} | Avisos: {warnings}")
    if failures:
        print("RESULTADO: NÃO LIBERAR PARA PRODUÇÃO até corrigir as falhas acima.")
        return 1
    if warnings:
        print("RESULTADO: ESTRUTURA APROVADA, com avisos a revisar antes do go-live.")
        return 0
    print("RESULTADO: PRÉ-FLIGHT APROVADO.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
