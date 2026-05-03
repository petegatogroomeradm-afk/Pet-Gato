import sqlite3
import json
from pathlib import Path

DB_PATH = Path(r"D:\Relogio de ponto WEB\Backup\backup_20260503_152319.db")
OUT_PATH = Path("backup_exportado.json")

TABELAS = [
    "users",
    "schedules",
    "employees",
    "employee_schedule_days",
    "time_records",
    "time_adjustments",
    "system_logs",
    "settings",
]

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

dados = {}
for tabela in TABELAS:
    rows = conn.execute(f"SELECT * FROM {tabela}").fetchall()
    dados[tabela] = [dict(row) for row in rows]

OUT_PATH.write_text(json.dumps(dados, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"Exportado com sucesso: {OUT_PATH.resolve()}")
