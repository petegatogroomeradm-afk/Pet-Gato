import os
import sqlite3
from datetime import datetime
from pathlib import Path

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    psycopg2 = None
    RealDictCursor = None
from werkzeug.security import generate_password_hash

BASE_DIR = Path(__file__).resolve().parent
INSTANCE_DIR = BASE_DIR / "instance"
SQLITE_DB = INSTANCE_DIR / "petegato_business_v3.db"


def now_iso():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def is_postgres():
    return bool(os.environ.get("DATABASE_URL"))


def adapt_query(sql):
    return sql.replace("?", "%s") if is_postgres() else sql


def get_conn():
    INSTANCE_DIR.mkdir(exist_ok=True)
    if is_postgres():
        if psycopg2 is None:
            raise RuntimeError("psycopg2-binary não está instalado. Execute: pip install -r requirements.txt")
        return psycopg2.connect(os.environ["DATABASE_URL"], cursor_factory=RealDictCursor)
    conn = sqlite3.connect(SQLITE_DB)
    conn.row_factory = sqlite3.Row
    return conn


def query_db(sql, params=(), one=False):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(adapt_query(sql), params)
        rows = cur.fetchall()
        return (rows[0] if rows else None) if one else rows
    finally:
        cur.close()
        conn.close()


def execute_db(sql, params=()):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(adapt_query(sql), params)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()



def insert_db(sql, params=()):
    """Executa um INSERT e devolve o ID criado em SQLite ou PostgreSQL."""
    conn = get_conn()
    cur = conn.cursor()
    try:
        statement = adapt_query(sql)
        if is_postgres() and "RETURNING" not in statement.upper():
            statement = statement.rstrip().rstrip(";") + " RETURNING id"
        cur.execute(statement, params)
        if is_postgres():
            row = cur.fetchone()
            new_id = row["id"] if isinstance(row, dict) else row[0]
        else:
            new_id = cur.lastrowid
        conn.commit()
        return new_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def execute_many(sql, rows):
    """Executa a mesma instrução para vários conjuntos de parâmetros."""
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.executemany(adapt_query(sql), rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def add_column_if_not_exists(cur, conn, table, column, definition):
    try:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        conn.commit()
    except Exception:
        conn.rollback()


def init_db():
    conn = get_conn()
    cur = conn.cursor()
    serial = "SERIAL PRIMARY KEY" if is_postgres() else "INTEGER PRIMARY KEY AUTOINCREMENT"

    statements = [
        f"""CREATE TABLE IF NOT EXISTS users (
            id {serial}, name TEXT NOT NULL, username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'admin',
            active INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL
        )""",
        f"""CREATE TABLE IF NOT EXISTS clients (
            id {serial}, nome TEXT NOT NULL, cpf TEXT, telefone TEXT, whatsapp TEXT,
            email TEXT, endereco TEXT, observacoes TEXT, foto TEXT, tags TEXT,
            contato_emergencia TEXT, data_nascimento TEXT, origem_cadastro TEXT,
            canal_preferido TEXT, consentimento_marketing INTEGER DEFAULT 0,
            ativo INTEGER DEFAULT 1, created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS client_portal_accounts (
            id {serial}, client_id INTEGER NOT NULL UNIQUE, email TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL, active INTEGER DEFAULT 1,
            last_login_at TEXT, created_at TEXT, updated_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS pets (
            id {serial}, client_id INTEGER, nome TEXT NOT NULL, especie TEXT, raca TEXT,
            porte TEXT, idade TEXT, sexo TEXT, cor TEXT, peso REAL,
            castrado INTEGER DEFAULT 0, data_nascimento TEXT, microchip TEXT,
            alergias TEXT, medicamentos TEXT, alimentacao TEXT, temperamento TEXT,
            preferencia_tosa TEXT, foto TEXT, observacoes TEXT, ativo INTEGER DEFAULT 1,
            created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS pet_vaccines (
            id {serial}, pet_id INTEGER NOT NULL, vacina TEXT NOT NULL,
            data_aplicacao TEXT, proxima_dose TEXT, veterinario TEXT,
            observacoes TEXT, created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS pet_health_treatments (
            id {serial}, pet_id INTEGER NOT NULL, treatment_type TEXT NOT NULL,
            product_name TEXT, application_date TEXT, next_date TEXT,
            veterinarian TEXT, observations TEXT, created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS pet_medical_history (
            id {serial}, pet_id INTEGER NOT NULL, data TEXT, tipo TEXT,
            descricao TEXT, veterinario TEXT, created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS pet_weight_history (
            id {serial}, pet_id INTEGER NOT NULL, peso REAL, data TEXT, created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS pet_photos (
            id {serial}, pet_id INTEGER NOT NULL, arquivo TEXT, categoria TEXT,
            descricao TEXT, created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS pet_documents (
            id {serial}, pet_id INTEGER NOT NULL, nome TEXT, arquivo TEXT,
            tipo TEXT, created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS pet_notes (
            id {serial}, pet_id INTEGER NOT NULL, observacao TEXT, usuario TEXT, created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS pet_health_profiles (
            id {serial}, pet_id INTEGER NOT NULL UNIQUE, altura_cm REAL, tipo_sanguineo TEXT,
            plano_saude TEXT, veterinario_nome TEXT, clinica_nome TEXT,
            doencas_cronicas TEXT, cirurgias TEXT, restricoes TEXT, medos TEXT,
            cuidados_especiais TEXT, updated_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS client_notes (
            id {serial}, client_id INTEGER NOT NULL, observacao TEXT, usuario TEXT, created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS client_documents (
            id {serial}, client_id INTEGER NOT NULL, nome TEXT, arquivo TEXT, tipo TEXT, created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS client_photos (
            id {serial}, client_id INTEGER NOT NULL, arquivo TEXT, categoria TEXT,
            descricao TEXT, created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS schedule_blocks (
            id {serial}, employee_id INTEGER, block_date TEXT NOT NULL,
            start_time TEXT NOT NULL, end_time TEXT NOT NULL, block_type TEXT DEFAULT 'Bloqueio',
            title TEXT, notes TEXT, created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS appointment_waitlist (
            id {serial}, client_id INTEGER NOT NULL, pet_id INTEGER NOT NULL,
            preferred_date TEXT, preferred_period TEXT, service TEXT, employee_id INTEGER,
            priority INTEGER DEFAULT 0, status TEXT DEFAULT 'Aguardando', notes TEXT,
            created_at TEXT, updated_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS employees (
            id {serial}, name TEXT NOT NULL, registration TEXT UNIQUE NOT NULL,
            cpf TEXT, phone TEXT, email TEXT, role_name TEXT, admission_date TEXT,
            salary REAL DEFAULT 0, schedule_id INTEGER, commission_rate REAL DEFAULT 0,
            active INTEGER NOT NULL DEFAULT 1, notes TEXT, created_at TEXT, updated_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS work_schedules (
            id {serial}, description TEXT NOT NULL, entrada TEXT, saida_intervalo TEXT,
            retorno_intervalo TEXT, saida_final TEXT, tolerancia INTEGER DEFAULT 10, created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS appointments (
            id {serial}, client_id INTEGER NOT NULL, pet_id INTEGER NOT NULL, employee_id INTEGER,
            data_agendamento TEXT NOT NULL, horario TEXT NOT NULL, duration_minutes INTEGER DEFAULT 60,
            servico TEXT, valor REAL DEFAULT 0, status TEXT DEFAULT 'Agendado',
            transport_required INTEGER DEFAULT 0, reminder_sent INTEGER DEFAULT 0,
            observacoes TEXT, created_at TEXT, updated_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS grooming_services (
            id {serial}, appointment_id INTEGER, client_id INTEGER NOT NULL, pet_id INTEGER NOT NULL,
            employee_id INTEGER, data TEXT NOT NULL, hora_entrada TEXT, hora_saida TEXT,
            servico TEXT, valor REAL DEFAULT 0, payment_method TEXT DEFAULT 'A definir',
            status TEXT DEFAULT 'Em atendimento', started_at TEXT, finished_at TEXT,
            pulgas INTEGER DEFAULT 0, carrapatos INTEGER DEFAULT 0, machucado INTEGER DEFAULT 0,
            no_pelo INTEGER DEFAULT 0, agressivo INTEGER DEFAULT 0,
            financeiro_lancado INTEGER DEFAULT 0, estoque_baixado INTEGER DEFAULT 0,
            observacoes TEXT, created_at TEXT, updated_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS grooming_checklists (
            id {serial}, grooming_id INTEGER NOT NULL UNIQUE, checkin_weight REAL,
            coat_condition TEXT, ears_condition TEXT, nails_condition TEXT,
            behavior TEXT, tutor_requests TEXT, checkout_notes TEXT,
            checked_in_at TEXT, checked_out_at TEXT, updated_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS grooming_product_usage (
            id {serial}, grooming_id INTEGER NOT NULL, product_id INTEGER NOT NULL,
            quantity REAL NOT NULL DEFAULT 0, unit TEXT, stock_applied INTEGER DEFAULT 0,
            created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS grooming_photos (
            id {serial}, grooming_id INTEGER NOT NULL, category TEXT NOT NULL,
            filename TEXT NOT NULL, description TEXT, created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS employee_commissions (
            id {serial}, grooming_id INTEGER NOT NULL, employee_id INTEGER NOT NULL,
            rate REAL DEFAULT 0, amount REAL DEFAULT 0, status TEXT DEFAULT 'Pendente',
            created_at TEXT, updated_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS financial_transactions (
            id {serial}, type TEXT NOT NULL, category TEXT, description TEXT,
            amount REAL DEFAULT 0, payment_method TEXT, transaction_date TEXT,
            due_date TEXT, status TEXT DEFAULT 'Pago', reference TEXT, account TEXT DEFAULT 'Caixa',
            notes TEXT, source_type TEXT DEFAULT 'Manual', source_id INTEGER, created_by TEXT,
            created_at TEXT, updated_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS financial_categories (
            id {serial}, name TEXT NOT NULL, transaction_type TEXT NOT NULL,
            cost_center TEXT, active INTEGER DEFAULT 1, created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS financial_accounts (
            id {serial}, name TEXT NOT NULL, account_type TEXT DEFAULT 'Caixa',
            opening_balance REAL DEFAULT 0, active INTEGER DEFAULT 1, created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS financial_goals (
            id {serial}, reference_month TEXT NOT NULL UNIQUE,
            revenue_goal REAL DEFAULT 0, expense_limit REAL DEFAULT 0,
            created_at TEXT, updated_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS financial_recurrences (
            id {serial}, transaction_type TEXT NOT NULL, category TEXT,
            description TEXT NOT NULL, amount REAL DEFAULT 0, payment_method TEXT,
            account TEXT DEFAULT 'Caixa', cost_center TEXT, frequency TEXT DEFAULT 'Mensal',
            start_date TEXT NOT NULL, next_due_date TEXT NOT NULL, end_date TEXT,
            active INTEGER DEFAULT 1, notes TEXT, created_at TEXT, updated_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS cash_closings (
            id {serial}, closing_date TEXT NOT NULL UNIQUE, opening_balance REAL DEFAULT 0,
            total_entries REAL DEFAULT 0, total_exits REAL DEFAULT 0,
            expected_balance REAL DEFAULT 0, counted_balance REAL DEFAULT 0,
            difference REAL DEFAULT 0, notes TEXT, closed_by TEXT, created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS stock_inventory_sessions (
            id {serial}, reference TEXT NOT NULL, status TEXT DEFAULT 'Aberto',
            notes TEXT, started_at TEXT, finished_at TEXT, created_by TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS stock_inventory_items (
            id {serial}, inventory_id INTEGER NOT NULL, product_id INTEGER NOT NULL,
            system_quantity REAL DEFAULT 0, counted_quantity REAL DEFAULT 0,
            difference REAL DEFAULT 0, adjusted INTEGER DEFAULT 0, created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS stock_products (
            id {serial}, name TEXT NOT NULL, category TEXT, sku TEXT, barcode TEXT,
            quantity REAL DEFAULT 0, unit TEXT, min_quantity REAL DEFAULT 0,
            cost_price REAL DEFAULT 0, sale_price REAL DEFAULT 0, supplier TEXT,
            expiration_date TEXT, location TEXT, active INTEGER DEFAULT 1,
            created_at TEXT, updated_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS stock_movements (
            id {serial}, product_id INTEGER NOT NULL, movement_type TEXT NOT NULL,
            quantity REAL NOT NULL, unit_cost REAL DEFAULT 0, reason TEXT,
            reference TEXT, user_name TEXT, movement_date TEXT, created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS stock_suppliers (
            id {serial}, name TEXT NOT NULL, document TEXT, phone TEXT, email TEXT,
            contact_name TEXT, notes TEXT, active INTEGER DEFAULT 1, created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS transport_services (
            id {serial}, appointment_id INTEGER, grooming_id INTEGER, client_id INTEGER, pet_id INTEGER,
            driver_name TEXT, driver_phone TEXT, service_type TEXT, pickup_address TEXT,
            delivery_address TEXT, pickup_date TEXT, pickup_time TEXT, expected_return_time TEXT,
            status TEXT DEFAULT 'Agendado', fee REAL DEFAULT 0, distance_km REAL DEFAULT 0,
            payment_status TEXT DEFAULT 'Pendente', started_at TEXT, finished_at TEXT,
            observations TEXT, created_at TEXT, updated_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS settings (
            id {serial}, company_name TEXT, cnpj TEXT, phone TEXT, whatsapp TEXT,
            email TEXT, address TEXT, pix_key TEXT, notes TEXT, created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS grooming_history (
            id {serial}, grooming_id INTEGER, event TEXT, user_name TEXT, created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS time_records (
            id {serial}, employee_id INTEGER, record_type TEXT, record_time TEXT,
            source TEXT DEFAULT 'Manual', notes TEXT, created_at TEXT, updated_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS employee_absences (
            id {serial}, employee_id INTEGER NOT NULL, absence_type TEXT NOT NULL,
            start_date TEXT NOT NULL, end_date TEXT, justified INTEGER DEFAULT 0,
            notes TEXT, attachment TEXT, created_at TEXT
        )""",

        f"""CREATE TABLE IF NOT EXISTS employee_vacations (
            id {serial}, employee_id INTEGER NOT NULL, acquisition_start TEXT,
            acquisition_end TEXT, start_date TEXT NOT NULL, end_date TEXT NOT NULL,
            days INTEGER DEFAULT 30, status TEXT DEFAULT 'Planejada',
            vacation_pay REAL DEFAULT 0, advance_amount REAL DEFAULT 0,
            notes TEXT, created_at TEXT, updated_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS employee_adjustments (
            id {serial}, employee_id INTEGER NOT NULL, adjustment_type TEXT NOT NULL,
            reference_month TEXT NOT NULL, description TEXT, amount REAL DEFAULT 0,
            status TEXT DEFAULT 'Pendente', created_at TEXT, updated_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS employee_hour_bank (
            id {serial}, employee_id INTEGER NOT NULL, reference_date TEXT NOT NULL,
            minutes INTEGER DEFAULT 0, movement_type TEXT DEFAULT 'Apuração',
            description TEXT, approved INTEGER DEFAULT 0,
            approved_by TEXT, created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS employee_monthly_closings (
            id {serial}, employee_id INTEGER NOT NULL, reference_month TEXT NOT NULL,
            worked_minutes INTEGER DEFAULT 0, expected_minutes INTEGER DEFAULT 0,
            balance_minutes INTEGER DEFAULT 0, absences INTEGER DEFAULT 0,
            justified_absences INTEGER DEFAULT 0, commission_amount REAL DEFAULT 0,
            additions REAL DEFAULT 0, deductions REAL DEFAULT 0,
            status TEXT DEFAULT 'Aberto', closed_by TEXT,
            created_at TEXT, updated_at TEXT,
            UNIQUE(employee_id, reference_month)
        )""",


        f"""CREATE TABLE IF NOT EXISTS crm_tasks (
            id {serial}, client_id INTEGER, pet_id INTEGER,
            task_type TEXT NOT NULL, title TEXT NOT NULL, message TEXT,
            due_date TEXT, priority TEXT DEFAULT 'Média',
            status TEXT DEFAULT 'Pendente', contact_channel TEXT DEFAULT 'WhatsApp',
            completed_at TEXT, completed_by TEXT, created_at TEXT NOT NULL
        )""",
        f"""CREATE TABLE IF NOT EXISTS crm_contact_history (
            id {serial}, client_id INTEGER, pet_id INTEGER, task_id INTEGER,
            channel TEXT, subject TEXT, message TEXT, result TEXT,
            user_name TEXT, created_at TEXT NOT NULL
        )""",


        f"""CREATE TABLE IF NOT EXISTS loyalty_settings (
            id {serial}, points_per_real REAL DEFAULT 1,
            cashback_bronze REAL DEFAULT 0, cashback_prata REAL DEFAULT 1,
            cashback_ouro REAL DEFAULT 2, cashback_diamante REAL DEFAULT 3,
            prata_min INTEGER DEFAULT 300, ouro_min INTEGER DEFAULT 1000,
            diamante_min INTEGER DEFAULT 2500, points_validity_days INTEGER DEFAULT 365,
            updated_by TEXT, updated_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS loyalty_accounts (
            id {serial}, client_id INTEGER NOT NULL UNIQUE,
            points_balance INTEGER DEFAULT 0, cashback_balance REAL DEFAULT 0,
            lifetime_points INTEGER DEFAULT 0, level TEXT DEFAULT 'Bronze',
            updated_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS loyalty_transactions (
            id {serial}, client_id INTEGER NOT NULL, movement_type TEXT NOT NULL,
            points INTEGER DEFAULT 0, cashback REAL DEFAULT 0,
            purchase_amount REAL DEFAULT 0, origin TEXT, reference TEXT,
            description TEXT, user_name TEXT, expires_at TEXT, created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS loyalty_coupons (
            id {serial}, code TEXT NOT NULL UNIQUE, title TEXT NOT NULL,
            discount_type TEXT DEFAULT 'Percentual', discount_value REAL DEFAULT 0,
            minimum_purchase REAL DEFAULT 0, valid_from TEXT, valid_until TEXT,
            usage_limit INTEGER DEFAULT 1, used_count INTEGER DEFAULT 0,
            client_id INTEGER, status TEXT DEFAULT 'Ativo', notes TEXT,
            created_by TEXT, created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS loyalty_coupon_redemptions (
            id {serial}, coupon_id INTEGER NOT NULL, client_id INTEGER NOT NULL,
            order_reference TEXT, discount_amount REAL DEFAULT 0,
            user_name TEXT, created_at TEXT
        )""",

        f"""CREATE TABLE IF NOT EXISTS system_events (
            id {serial}, event_name TEXT NOT NULL, payload TEXT,
            status TEXT DEFAULT 'Pendente', created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS audit_logs (
            id {serial}, user_name TEXT, action TEXT NOT NULL,
            entity_type TEXT, entity_id INTEGER, details TEXT, created_at TEXT
        )""",
        f"""CREATE TABLE IF NOT EXISTS user_access_logs (
            id {serial}, user_id INTEGER, username TEXT, success INTEGER DEFAULT 0,
            ip_address TEXT, user_agent TEXT, details TEXT, created_at TEXT
        )""",
    ]

    for statement in statements:
        cur.execute(statement)
    conn.commit()

    migrations = {
        "users": [("cpf", "TEXT"), ("phone", "TEXT"), ("email", "TEXT"),
            ("job_title", "TEXT"), ("admission_date", "TEXT"), ("avatar_path", "TEXT"),
            ("must_change_password", "INTEGER DEFAULT 0"), ("failed_attempts", "INTEGER DEFAULT 0"),
            ("locked_until", "TEXT"), ("last_login_at", "TEXT"), ("last_login_ip", "TEXT"),
            ("last_login_user_agent", "TEXT"), ("login_count", "INTEGER DEFAULT 0"),
            ("updated_at", "TEXT"), ("created_by", "TEXT")],
        "client_portal_accounts": [("terms_accepted_at", "TEXT")],
        "clients": [("foto", "TEXT"), ("tags", "TEXT"), ("contato_emergencia", "TEXT"), ("data_nascimento", "TEXT"), ("origem_cadastro", "TEXT"), ("canal_preferido", "TEXT"), ("consentimento_marketing", "INTEGER DEFAULT 0"), ("ativo", "INTEGER DEFAULT 1")],
        "pets": [
            ("sexo", "TEXT"), ("cor", "TEXT"), ("peso", "REAL"),
            ("castrado", "INTEGER DEFAULT 0"), ("data_nascimento", "TEXT"),
            ("microchip", "TEXT"), ("alergias", "TEXT"), ("medicamentos", "TEXT"),
            ("alimentacao", "TEXT"), ("temperamento", "TEXT"),
            ("preferencia_tosa", "TEXT"), ("foto", "TEXT"), ("ativo", "INTEGER DEFAULT 1")
        ],
        "appointments": [("employee_id", "INTEGER"), ("duration_minutes", "INTEGER DEFAULT 60"), ("transport_required", "INTEGER DEFAULT 0"), ("reminder_sent", "INTEGER DEFAULT 0"), ("updated_at", "TEXT"), ("requested_online", "INTEGER DEFAULT 0"), ("approval_notes", "TEXT"), ("approved_at", "TEXT"), ("approved_by", "TEXT")],
        "grooming_services": [("financeiro_lancado", "INTEGER DEFAULT 0"), ("estoque_baixado", "INTEGER DEFAULT 0"), ("payment_method", "TEXT DEFAULT 'A definir'"), ("started_at", "TEXT"), ("finished_at", "TEXT"), ("checked_in_at", "TEXT"), ("checked_out_at", "TEXT"), ("commission_lancada", "INTEGER DEFAULT 0"), ("updated_at", "TEXT")],
        "financial_transactions": [
            ("due_date", "TEXT"), ("status", "TEXT DEFAULT 'Pago'"),
            ("reference", "TEXT"), ("account", "TEXT DEFAULT 'Caixa'"),
            ("cost_center", "TEXT"), ("installment_number", "INTEGER DEFAULT 1"),
            ("total_installments", "INTEGER DEFAULT 1"), ("recurrence_id", "INTEGER"),
            ("paid_at", "TEXT"), ("notes", "TEXT"),
            ("source_type", "TEXT DEFAULT 'Manual'"), ("source_id", "INTEGER"),
            ("created_by", "TEXT"), ("updated_at", "TEXT")
        ],
        "transport_services": [("appointment_id", "INTEGER"), ("grooming_id", "INTEGER"), ("driver_phone", "TEXT"), ("delivery_address", "TEXT"), ("expected_return_time", "TEXT"), ("fee", "REAL DEFAULT 0"), ("distance_km", "REAL DEFAULT 0"), ("payment_status", "TEXT DEFAULT 'Pendente'"), ("started_at", "TEXT"), ("finished_at", "TEXT"), ("updated_at", "TEXT")],
        "pet_photos": [("categoria", "TEXT")],
        "stock_products": [("sku", "TEXT"), ("barcode", "TEXT"), ("subcategory", "TEXT"),
            ("brand", "TEXT"), ("max_quantity", "REAL DEFAULT 0"),
            ("cost_price", "REAL DEFAULT 0"), ("sale_price", "REAL DEFAULT 0"),
            ("location", "TEXT"), ("active", "INTEGER DEFAULT 1"), ("updated_at", "TEXT")],
        "employees": [("cpf", "TEXT"), ("phone", "TEXT"), ("email", "TEXT"), ("role_name", "TEXT"), ("admission_date", "TEXT"), ("salary", "REAL DEFAULT 0"), ("schedule_id", "INTEGER"), ("commission_rate", "REAL DEFAULT 0"), ("notes", "TEXT"), ("updated_at", "TEXT")],
        "time_records": [("source", "TEXT DEFAULT 'Manual'"), ("notes", "TEXT"), ("updated_at", "TEXT")],
    }
    for table, columns in migrations.items():
        for column, definition in columns:
            add_column_if_not_exists(cur, conn, table, column, definition)

    cur.execute(adapt_query("SELECT id FROM users WHERE username = ?"), ("admin",))
    if not cur.fetchone():
        admin_password = os.environ.get("ADMIN_PASSWORD")
        if is_postgres() and not admin_password:
            raise RuntimeError("ADMIN_PASSWORD é obrigatória ao criar o primeiro administrador no PostgreSQL.")
        admin_password = admin_password or "Pet&gato3264"
        cur.execute(adapt_query("""
            INSERT INTO users (name, username, password_hash, role, active, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """), ("Administrador", "admin", generate_password_hash(admin_password), "admin", 1, now_iso()))
        conn.commit()

    cur.execute("SELECT id FROM loyalty_settings LIMIT 1")
    if not cur.fetchone():
        cur.execute(adapt_query("""
            INSERT INTO loyalty_settings
            (points_per_real, cashback_bronze, cashback_prata, cashback_ouro,
             cashback_diamante, prata_min, ouro_min, diamante_min,
             points_validity_days, updated_by, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """), (1, 0, 1, 2, 3, 300, 1000, 2500, 365, "Sistema", now_iso()))
        conn.commit()

    cur.execute("SELECT id FROM clients")
    client_rows = cur.fetchall()
    for client_row in client_rows:
        client_id = client_row["id"] if hasattr(client_row, "keys") else client_row[0]
        cur.execute(adapt_query("SELECT id FROM loyalty_accounts WHERE client_id = ?"), (client_id,))
        if not cur.fetchone():
            cur.execute(adapt_query("""
                INSERT INTO loyalty_accounts
                (client_id, points_balance, cashback_balance, lifetime_points, level, updated_at)
                VALUES (?, 0, 0, 0, 'Bronze', ?)
            """), (client_id, now_iso()))
    conn.commit()

    cur.close()
    conn.close()


def registrar_historico(grooming_id, evento, usuario="Sistema"):
    execute_db("""
        INSERT INTO grooming_history (grooming_id, event, user_name, created_at)
        VALUES (?, ?, ?, ?)
    """, (grooming_id, evento, usuario, now_iso()))
