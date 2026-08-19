import sqlite3
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "instance" / "petegato_business_v3.db"

if not DB_PATH.exists():
    raise FileNotFoundError(f"Banco não encontrado: {DB_PATH}")

print("=" * 65)
print("CORREÇÃO DA ESTRUTURA FINANCEIRA")
print("=" * 65)
print(f"Banco: {DB_PATH}")

conn = sqlite3.connect(DB_PATH)

try:
    cursor = conn.cursor()

    # ---------------------------------------------------------
    # 1. Orçamento financeiro por categoria
    # ---------------------------------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS financial_category_budgets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reference_month TEXT NOT NULL,
            category TEXT NOT NULL,
            planned_amount REAL NOT NULL DEFAULT 0,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(reference_month, category)
        )
    """)

    print("[OK] Tabela financial_category_budgets verificada.")

    # ---------------------------------------------------------
    # 2. Colunas necessárias para conciliação financeira
    # ---------------------------------------------------------
    cursor.execute("PRAGMA table_info(financial_transactions)")
    existing_columns = {row[1] for row in cursor.fetchall()}

    required_columns = {
        "reconciliation_status": "TEXT DEFAULT 'Pendente'",
        "reconciled_at": "TEXT",
        "bank_reference": "TEXT",
    }

    for column_name, definition in required_columns.items():
        if column_name not in existing_columns:
            cursor.execute(
                f"ALTER TABLE financial_transactions "
                f"ADD COLUMN {column_name} {definition}"
            )
            print(f"[CRIADA] Coluna financial_transactions.{column_name}")
        else:
            print(f"[OK] Coluna financial_transactions.{column_name}")

    # Corrige registros antigos sem status
    cursor.execute("""
        UPDATE financial_transactions
           SET reconciliation_status = 'Pendente'
         WHERE reconciliation_status IS NULL
            OR TRIM(reconciliation_status) = ''
    """)

    # Índices para melhorar as consultas
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS
        idx_financial_budgets_reference_month
        ON financial_category_budgets(reference_month)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS
        idx_financial_transactions_reconciliation
        ON financial_transactions(reconciliation_status)
    """)

    conn.commit()

    # ---------------------------------------------------------
    # 3. Verificação final
    # ---------------------------------------------------------
    cursor.execute("""
        SELECT name
          FROM sqlite_master
         WHERE type = 'table'
           AND name = 'financial_category_budgets'
    """)

    budget_table_ok = cursor.fetchone() is not None

    cursor.execute("PRAGMA table_info(financial_transactions)")
    final_columns = {row[1] for row in cursor.fetchall()}

    missing = [
        column for column in required_columns
        if column not in final_columns
    ]

    print("-" * 65)

    if budget_table_ok and not missing:
        print("CORREÇÃO CONCLUÍDA COM SUCESSO.")
        print("Orçamento mensal: estrutura pronta.")
        print("Painel gerencial: estrutura pronta.")
        print("Conciliação financeira: estrutura pronta.")
    else:
        print("A correção terminou com pendências.")
        print(f"Tabela de orçamento criada: {budget_table_ok}")
        print(f"Colunas ainda ausentes: {missing}")

finally:
    conn.close()

print("=" * 65)
