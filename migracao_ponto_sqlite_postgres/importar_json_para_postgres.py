import json
import os
import sys
import psycopg2
from pathlib import Path

JSON_PATH = Path("backup_exportado.json")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("ERRO: variável DATABASE_URL não encontrada.")
    print("No PowerShell, defina assim:")
    print('$env:DATABASE_URL="postgresql://usuario:senha@host:5432/banco"')
    sys.exit(1)

if not JSON_PATH.exists():
    print(f"ERRO: arquivo não encontrado: {JSON_PATH}")
    sys.exit(1)

dados = json.loads(JSON_PATH.read_text(encoding="utf-8"))

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# Garante tabelas principais no PostgreSQL
cur.execute("""
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'admin',
    active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS schedules (
    id SERIAL PRIMARY KEY,
    description TEXT NOT NULL,
    entry_time TEXT NOT NULL,
    lunch_out_time TEXT NOT NULL,
    lunch_in_time TEXT NOT NULL,
    exit_time TEXT NOT NULL,
    tolerance_minutes INTEGER NOT NULL DEFAULT 10,
    active INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS employees (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    cpf TEXT,
    registration TEXT NOT NULL UNIQUE,
    role_name TEXT,
    phone TEXT,
    admission_date TEXT,
    pin_hash TEXT NOT NULL,
    schedule_id INTEGER,
    active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS employee_schedule_days (
    id SERIAL PRIMARY KEY,
    employee_id INTEGER NOT NULL,
    weekday INTEGER NOT NULL,
    schedule_id INTEGER NOT NULL,
    UNIQUE(employee_id, weekday)
);
CREATE TABLE IF NOT EXISTS time_records (
    id SERIAL PRIMARY KEY,
    employee_id INTEGER NOT NULL,
    record_type TEXT NOT NULL,
    record_time TEXT NOT NULL,
    notes TEXT,
    ip_origin TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS time_adjustments (
    id SERIAL PRIMARY KEY,
    record_id INTEGER,
    employee_id INTEGER NOT NULL,
    admin_user_id INTEGER NOT NULL,
    old_value TEXT,
    new_value TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS system_logs (
    id SERIAL PRIMARY KEY,
    user_name TEXT,
    action TEXT NOT NULL,
    details TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
""")
conn.commit()

def insert(table, columns, rows, conflict="id"):
    if not rows:
        return
    placeholders = ", ".join(["%s"] * len(columns))
    col_sql = ", ".join(columns)
    update_sql = ", ".join([f"{c}=EXCLUDED.{c}" for c in columns if c != conflict])
    sql = f"""
        INSERT INTO {table} ({col_sql})
        VALUES ({placeholders})
        ON CONFLICT ({conflict}) DO UPDATE SET {update_sql}
    """
    for row in rows:
        cur.execute(sql, tuple(row.get(c) for c in columns))
    conn.commit()

insert("users",
       ["id","name","username","password_hash","role","active","created_at"],
       dados.get("users", []), "id")

insert("schedules",
       ["id","description","entry_time","lunch_out_time","lunch_in_time","exit_time","tolerance_minutes","active"],
       dados.get("schedules", []), "id")

insert("employees",
       ["id","name","cpf","registration","role_name","phone","admission_date","pin_hash","schedule_id","active","created_at"],
       dados.get("employees", []), "id")

insert("employee_schedule_days",
       ["id","employee_id","weekday","schedule_id"],
       dados.get("employee_schedule_days", []), "id")

insert("time_records",
       ["id","employee_id","record_type","record_time","notes","ip_origin","created_at"],
       dados.get("time_records", []), "id")

insert("time_adjustments",
       ["id","record_id","employee_id","admin_user_id","old_value","new_value","reason","created_at"],
       dados.get("time_adjustments", []), "id")

insert("system_logs",
       ["id","user_name","action","details","created_at"],
       dados.get("system_logs", []), "id")

# Settings usa key como chave
for row in dados.get("settings", []):
    cur.execute("""
        INSERT INTO settings (key, value, updated_at)
        VALUES (%s, %s, %s)
        ON CONFLICT (key) DO UPDATE SET
            value = EXCLUDED.value,
            updated_at = EXCLUDED.updated_at
    """, (row.get("key"), row.get("value"), row.get("updated_at")))
conn.commit()

# Ajusta sequences para novos cadastros não darem conflito de ID
for table in ["users", "schedules", "employees", "employee_schedule_days", "time_records", "time_adjustments", "system_logs"]:
    cur.execute(f"""
        SELECT setval(
            pg_get_serial_sequence('{table}', 'id'),
            COALESCE((SELECT MAX(id) FROM {table}), 1),
            true
        )
    """)
conn.commit()

print("Importação concluída com sucesso.")
print(f"Usuários: {len(dados.get('users', []))}")
print(f"Funcionários: {len(dados.get('employees', []))}")
print(f"Registros de ponto: {len(dados.get('time_records', []))}")
