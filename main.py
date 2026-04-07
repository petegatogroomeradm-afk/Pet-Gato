import csv
import hashlib
import io
import json
import shutil
import os
import sqlite3
import urllib.parse
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

import boto3
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from fastapi import FastAPI, Form, Request, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

ROOT_DIR = Path(__file__).resolve().parent
load_dotenv(ROOT_DIR / ".env")

APP_DB_MODE = os.getenv("APP_DB_MODE", "postgres").lower()
DB_PATH = ROOT_DIR / os.getenv("SQLITE_PATH", "petgato_web.db")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://postgres:postgres@127.0.0.1:5432/petgato_web")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

templates = Jinja2Templates(directory=str(ROOT_DIR))


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def is_postgres() -> bool:
    return APP_DB_MODE == "postgres"


def _pg_conn_string() -> str:
    return DATABASE_URL.replace("postgresql+psycopg://", "postgresql://")


def qmark_to_percent(sql: str) -> str:
    if "?" not in sql:
        return sql
    return "%s".join(sql.split("?"))


class PGConnWrapper:
    def __init__(self):
        self.conn = psycopg.connect(_pg_conn_string(), row_factory=dict_row)

    def cursor(self):
        return self.conn.cursor()

    def execute(self, sql, params=None):
        return self.conn.execute(qmark_to_percent(sql), params or ())

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()


def get_conn():
    if is_postgres():
        return PGConnWrapper()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


TABLES_BACKUP_ORDER = ["profiles", "users", "clients", "pets", "appointments", "company_settings"]

def has_s3_config() -> bool:
    return all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_BUCKET_NAME, AWS_REGION])

def s3_client():
    if not has_s3_config():
        return None
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

def _table_rows_dicts(conn, table_name: str):
    rows = conn.execute(f"SELECT * FROM {table_name}").fetchall()
    return [dict(r) for r in rows]

def generate_app_backup_bytes():
    conn = get_conn()
    payload = {
        "meta": {"generated_at": datetime.now().isoformat(), "app_db_mode": APP_DB_MODE, "format_version": 1},
        "tables": {},
    }
    for table in TABLES_BACKUP_ORDER:
        payload["tables"][table] = _table_rows_dicts(conn, table)
    conn.close()
    return json.dumps(payload, ensure_ascii=False, indent=2, default=str).encode("utf-8")

def backup_filename():
    return f"petgato_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

def save_backup_local_and_s3(content: bytes, filename: str):
    backups_dir = ROOT_DIR / "backups"
    backups_dir.mkdir(exist_ok=True)
    file_path = backups_dir / filename
    file_path.write_bytes(content)
    if has_s3_config():
        s3_client().put_object(Bucket=AWS_BUCKET_NAME, Key=f"backups/{filename}", Body=content, ContentType="application/json")
    return file_path

def list_backup_items():
    items = []
    backups_dir = ROOT_DIR / "backups"
    backups_dir.mkdir(exist_ok=True)
    for p in backups_dir.glob("*.json"):
        items.append({"name": p.name, "source": "local", "updated_at": datetime.fromtimestamp(p.stat().st_mtime).strftime("%d/%m/%Y %H:%M")})
    if has_s3_config():
        try:
            resp = s3_client().list_objects_v2(Bucket=AWS_BUCKET_NAME, Prefix="backups/")
            for obj in resp.get("Contents", []):
                name = obj["Key"].split("/")[-1]
                if name and not any(i["name"] == name for i in items):
                    items.append({"name": name, "source": "s3", "updated_at": obj["LastModified"].strftime("%d/%m/%Y %H:%M")})
        except Exception:
            pass
    items.sort(key=lambda x: (x["updated_at"], x["name"]), reverse=True)
    return items

def download_backup_content(filename: str):
    backups_dir = ROOT_DIR / "backups"
    local = backups_dir / filename
    if local.exists():
        return local.read_bytes()
    if has_s3_config():
        obj = s3_client().get_object(Bucket=AWS_BUCKET_NAME, Key=f"backups/{filename}")
        return obj["Body"].read()
    raise FileNotFoundError(filename)

def _delete_all_data(conn):
    if is_postgres():
        for table in ["appointments", "pets", "clients", "users", "profiles", "company_settings"]:
            conn.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
    else:
        for table in ["appointments", "pets", "clients", "users", "profiles", "company_settings"]:
            conn.execute(f"DELETE FROM {table}")
        conn.execute("DELETE FROM sqlite_sequence WHERE name IN ('appointments','pets','clients','users','profiles')")
    conn.commit()

def _insert_rows(conn, table_name: str, rows: list[dict]):
    if not rows:
        return
    cols = list(rows[0].keys())
    col_sql = ", ".join(cols)
    placeholders = ", ".join(["?"] * len(cols))
    for row in rows:
        values = [row[c] for c in cols]
        conn.execute(f"INSERT INTO {table_name} ({col_sql}) VALUES ({placeholders})", values)
    if is_postgres() and "id" in cols and table_name != "company_settings":
        conn.execute("SELECT setval(pg_get_serial_sequence(%s, 'id'), COALESCE((SELECT MAX(id) FROM " + table_name + "), 1), true)", (table_name,))

def restore_backup_content(content: bytes):
    data = json.loads(content.decode("utf-8"))
    tables = data.get("tables", {})
    conn = get_conn()
    try:
        _delete_all_data(conn)
        for table in TABLES_BACKUP_ORDER:
            _insert_rows(conn, table, tables.get(table, []))
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def seed_defaults(conn):
    default_profile_permissions = {
        "Administrador": "dashboard,clients,pets,appointments,agenda,financeiro,motorista,relatorios,users,profiles,company",
        "Recepção": "dashboard,clients,pets,appointments,agenda,financeiro,relatorios",
        "Motorista": "dashboard,appointments,agenda,motorista",
    }

    for profile_name, permissions in default_profile_permissions.items():
        if is_postgres():
            conn.execute(
                "INSERT INTO profiles(name, permissions) VALUES (?, ?) ON CONFLICT (name) DO NOTHING",
                (profile_name, permissions),
            )
        else:
            conn.execute(
                "INSERT OR IGNORE INTO profiles(name, permissions) VALUES (?, ?)",
                (profile_name, permissions),
            )
        conn.execute(
            "UPDATE profiles SET permissions=? WHERE name=? AND (permissions IS NULL OR permissions='')",
            (permissions, profile_name),
        )

    if is_postgres():
        conn.execute(
            "INSERT INTO company_settings(id, nome_empresa, nome_fantasia) VALUES (1, 'Pet & Gato', 'Pet & Gato') ON CONFLICT (id) DO NOTHING"
        )
    else:
        conn.execute(
            "INSERT OR IGNORE INTO company_settings(id, nome_empresa, nome_fantasia) VALUES (1, 'Pet & Gato', 'Pet & Gato')"
        )

    default_users = [
        ("Pet&gato", "Administrador", "Pet&gato3264", "Administrador"),
        ("recepcao", "Usuário Recepção", "1234", "Recepção"),
        ("motorista", "Usuário Motorista", "1234", "Motorista"),
    ]
    for username, full_name, password, profile_name in default_users:
        if is_postgres():
            conn.execute(
                "INSERT INTO users(username, full_name, password_hash, profile_name, active) VALUES (?, ?, ?, ?, 1) ON CONFLICT (username) DO NOTHING",
                (username, full_name, hash_password(password), profile_name),
            )
        else:
            conn.execute(
                "INSERT OR IGNORE INTO users(username, full_name, password_hash, profile_name, active) VALUES (?, ?, ?, ?, 1)",
                (username, full_name, hash_password(password), profile_name),
            )

    conn.commit()


def init_db_sqlite():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            permissions TEXT DEFAULT ''
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            full_name TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            profile_name TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL,
            telefone TEXT,
            email TEXT,
            endereco TEXT,
            observacoes TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS pets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            nome TEXT NOT NULL,
            especie TEXT,
            raca TEXT,
            porte TEXT,
            idade TEXT,
            observacoes TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS appointments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            pet_id INTEGER NOT NULL,
            data_agendamento TEXT NOT NULL,
            horario TEXT NOT NULL,
            servico TEXT,
            valor REAL,
            forma_pagamento TEXT,
            status TEXT DEFAULT 'Agendado',
            observacoes TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS company_settings (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            nome_empresa TEXT,
            nome_fantasia TEXT,
            cnpj TEXT,
            telefone TEXT,
            whatsapp TEXT,
            email TEXT,
            endereco TEXT,
            cidade TEXT,
            estado TEXT,
            cep TEXT,
            pix_chave TEXT,
            pix_tipo TEXT,
            logo_url TEXT,
            observacoes TEXT
        )
    """)

    conn.commit()
    seed_defaults(conn)
    conn.close()


def init_db_postgres():
    conn = get_conn()

    ddls = [
        """
        CREATE TABLE IF NOT EXISTS profiles (
            id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            name VARCHAR(255) UNIQUE NOT NULL,
            permissions TEXT DEFAULT ''
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            username VARCHAR(255) UNIQUE NOT NULL,
            full_name VARCHAR(255) NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            profile_name VARCHAR(255) NOT NULL,
            active INTEGER NOT NULL DEFAULT 1
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS clients (
            id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            nome VARCHAR(255) NOT NULL,
            telefone TEXT,
            email TEXT,
            endereco TEXT,
            observacoes TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS pets (
            id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            client_id INTEGER NOT NULL,
            nome VARCHAR(255) NOT NULL,
            especie TEXT,
            raca TEXT,
            porte TEXT,
            idade TEXT,
            observacoes TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS appointments (
            id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            client_id INTEGER NOT NULL,
            pet_id INTEGER NOT NULL,
            data_agendamento VARCHAR(30) NOT NULL,
            horario VARCHAR(30) NOT NULL,
            servico TEXT,
            valor NUMERIC(12,2),
            forma_pagamento TEXT,
            status VARCHAR(100) DEFAULT 'Agendado',
            observacoes TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS company_settings (
            id INTEGER PRIMARY KEY,
            nome_empresa TEXT,
            nome_fantasia TEXT,
            cnpj TEXT,
            telefone TEXT,
            whatsapp TEXT,
            email TEXT,
            endereco TEXT,
            cidade TEXT,
            estado TEXT,
            cep TEXT,
            pix_chave TEXT,
            pix_tipo TEXT,
            logo_url TEXT,
            observacoes TEXT
        )
        """,
    ]

    for ddl in ddls:
        conn.execute(ddl)

    conn.commit()
    seed_defaults(conn)
    conn.close()


def init_db():
    if is_postgres():
        init_db_postgres()
    else:
        init_db_sqlite()


def current_user(request: Request):
    username = request.cookies.get("username")
    if not username:
        return None
    conn = get_conn()
    user = conn.execute(
        "SELECT * FROM users WHERE username=? AND active=1",
        (username,),
    ).fetchone()
    conn.close()
    return user


def to_float_br(value: str):
    raw = str(value or "").strip()
    if not raw:
        return None
    raw = raw.replace("R$", "").replace(" ", "")
    if "," in raw:
        raw = raw.replace(".", "").replace(",", ".")
    try:
        return float(raw)
    except Exception:
        return 0.0


def normalize_phone_br(phone):
    digits = "".join(ch for ch in str(phone or "") if ch.isdigit())
    if not digits:
        return ""
    return digits if digits.startswith("55") else "55" + digits


def wa_link(phone, message: str) -> str:
    number = normalize_phone_br(phone)
    if not number:
        return ""
    return f"https://wa.me/{number}?text={urllib.parse.quote(message)}"


def can_access(user, section: str) -> bool:
    if not user:
        return False
    if is_postgres() and section == 'backup':
        return False
    conn = get_conn()
    row = conn.execute("SELECT permissions FROM profiles WHERE name=?", (user["profile_name"],)).fetchone()
    conn.close()
    permissions = set()
    if row and row["permissions"]:
        permissions = {p.strip() for p in str(row["permissions"]).split(",") if p.strip()}
    return section in permissions


def guard_section(user, section: str):
    if not can_access(user, section):
        return RedirectResponse(url="/dashboard", status_code=303)
    return None


templates.env.globals["can_access"] = can_access


def month_matrix(year: int, month: int):
    import calendar
    cal = calendar.Calendar(firstweekday=6)
    return cal.monthdatescalendar(year, month)


def build_wa_messages(item: dict) -> dict:
    confirm_msg = (
        f"Olá, {item['cliente_nome']}! 🐾\n\n"
        f"Seu agendamento no Pet & Gato foi confirmado com sucesso.\n\n"
        f"📅 Data: {item['data_agendamento']}\n"
        f"⏰ Horário: {item['horario']}\n"
        f"🐶 Pet: {item['pet_nome']}\n"
        f"🛁 Serviço: {item.get('servico') or ''}\n"
        f"💰 Valor: R$ {float(item.get('valor') or 0):.2f}\n\n"
        f"Qualquer dúvida, estamos à disposição."
    )
    reminder_msg = (
        f"Olá, {item['cliente_nome']}! 🐾\n\n"
        f"Lembrete do seu agendamento no Pet & Gato.\n\n"
        f"📅 Data: {item['data_agendamento']}\n"
        f"⏰ Horário: {item['horario']}\n"
        f"🐶 Pet: {item['pet_nome']}\n"
        f"🛁 Serviço: {item.get('servico') or ''}\n\n"
        f"Pedimos, por gentileza, que esteja disponível no horário combinado."
    )
    cancel_msg = (
        f"Olá, {item['cliente_nome']}.\n\n"
        f"Informamos que o agendamento abaixo foi cancelado / remarcado.\n\n"
        f"📅 Data: {item['data_agendamento']}\n"
        f"⏰ Horário: {item['horario']}\n"
        f"🐶 Pet: {item['pet_nome']}\n"
        f"🛁 Serviço: {item.get('servico') or ''}\n\n"
        f"Se precisar, fale conosco para reagendarmos."
    )
    item["wa_confirm"] = wa_link(item.get("cliente_telefone"), confirm_msg)
    item["wa_reminder"] = wa_link(item.get("cliente_telefone"), reminder_msg)
    item["wa_cancel"] = wa_link(item.get("cliente_telefone"), cancel_msg)
    return item



def generate_receipt_pdf(receipt_data: dict, company: dict | None, output_path: Path):
    styles = getSampleStyleSheet()
    story = []

    title = company["nome_fantasia"] if company and company["nome_fantasia"] else "Pet & Gato"
    story.append(Paragraph(f"<b>{title}</b>", styles["Title"]))
    story.append(Spacer(1, 4 * mm))
    story.append(Paragraph("<b>RECIBO DE SERVIÇO</b>", styles["Heading2"]))
    story.append(Spacer(1, 4 * mm))

    company_lines = []
    if company:
        if company["nome_empresa"]:
            company_lines.append(company["nome_empresa"])
        if company["cnpj"]:
            company_lines.append(f"CNPJ: {company['cnpj']}")
        contact_parts = []
        if company["telefone"]:
            contact_parts.append(f"Tel: {company['telefone']}")
        if company["whatsapp"]:
            contact_parts.append(f"WhatsApp: {company['whatsapp']}")
        if company["email"]:
            contact_parts.append(company["email"])
        if contact_parts:
            company_lines.append(" | ".join(contact_parts))
        address_parts = [company.get("endereco") or "", company.get("cidade") or "", company.get("estado") or "", company.get("cep") or ""]
        address_text = " - ".join([p for p in address_parts if p])
        if address_text:
            company_lines.append(address_text)

    if company_lines:
        story.append(Paragraph("<br/>".join(company_lines), styles["BodyText"]))
        story.append(Spacer(1, 5 * mm))

    info_data = [
        ["Recibo nº", str(receipt_data["id"])],
        ["Data", receipt_data["data_agendamento"]],
        ["Hora", receipt_data["horario"]],
        ["Cliente", receipt_data["cliente_nome"]],
        ["Pet", receipt_data["pet_nome"]],
        ["Serviço", receipt_data.get("servico") or ""],
        ["Pagamento", receipt_data.get("forma_pagamento") or ""],
        ["Status", receipt_data.get("status") or ""],
    ]
    info_table = Table(info_data, colWidths=[35 * mm, 130 * mm])
    info_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#ede9fe")),
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#7c3aed")),
        ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#c4b5fd")),
        ("PADDING", (0, 0), (-1, -1), 6),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(info_table)
    story.append(Spacer(1, 6 * mm))

    total = float(receipt_data.get("valor") or 0)
    payment_rows = [["Descrição", "Valor (R$)"], [receipt_data.get("servico") or "Serviço prestado", f"{total:.2f}"]]
    payment_table = Table(payment_rows, colWidths=[120 * mm, 45 * mm])
    payment_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#7c3aed")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#7c3aed")),
        ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#d8b4fe")),
        ("ALIGN", (1, 1), (1, -1), "RIGHT"),
        ("PADDING", (0, 0), (-1, -1), 6),
    ]))
    story.append(payment_table)
    story.append(Spacer(1, 6 * mm))
    story.append(Paragraph(f"<b>Total recebido: R$ {total:.2f}</b>", styles["Heading3"]))

    if company and company.get("pix_chave"):
        pix_label = company.get("pix_tipo") or "Chave PIX"
        story.append(Spacer(1, 4 * mm))
        story.append(Paragraph(f"<b>{pix_label} PIX:</b> {company['pix_chave']}", styles["BodyText"]))

    if receipt_data.get("observacoes"):
        story.append(Spacer(1, 6 * mm))
        story.append(Paragraph("<b>Observações</b>", styles["Heading3"]))
        story.append(Paragraph(receipt_data["observacoes"], styles["BodyText"]))

    story.append(Spacer(1, 12 * mm))
    story.append(Paragraph("Assinatura / confirmação de recebimento: ____________________________________", styles["BodyText"]))

    doc = SimpleDocTemplate(
        str(output_path),
        pagesize=A4,
        leftMargin=15 * mm,
        rightMargin=15 * mm,
        topMargin=15 * mm,
        bottomMargin=15 * mm,
    )
    doc.build(story)


PERMISSION_OPTIONS = [
    ("dashboard", "Dashboard"),
    ("clients", "Clientes"),
    ("pets", "Pets"),
    ("appointments", "Agendamentos"),
    ("agenda", "Agenda visual"),
    ("financeiro", "Financeiro"),
    ("relatorios", "Relatórios"),
    ("motorista", "Painel motorista"),
    ("users", "Usuários"),
    ("profiles", "Perfis"),
    ("company", "Empresa"),
    ("backup", "Backup"),
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="Pet & Gato Web", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(ROOT_DIR)), name="static")


@app.get("/", response_class=HTMLResponse)
def home():
    return RedirectResponse(url="/login", status_code=302)


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})


@app.post("/login")
def login(username: str = Form(...), password: str = Form(...)):
    conn = get_conn()
    user = conn.execute("SELECT * FROM users WHERE username=? AND active=1", (username,)).fetchone()
    conn.close()

    if not user or user["password_hash"] != hash_password(password):
        return RedirectResponse(url="/login", status_code=303)

    response = RedirectResponse(url="/dashboard", status_code=303)
    response.set_cookie("username", username, httponly=True, samesite="lax")
    return response


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "dashboard")
    if blocked:
        return blocked

    today = datetime.now().strftime("%Y-%m-%d")
    month_prefix = datetime.now().strftime("%Y-%m")
    conn = get_conn()
    total_clients = conn.execute("SELECT COUNT(*) AS total FROM clients").fetchone()["total"]
    total_pets = conn.execute("SELECT COUNT(*) AS total FROM pets").fetchone()["total"]
    agendados_hoje = conn.execute("SELECT COUNT(*) AS total FROM appointments WHERE data_agendamento=?", (today,)).fetchone()["total"]
    em_andamento_hoje = conn.execute(
        "SELECT COUNT(*) AS total FROM appointments WHERE data_agendamento=? AND status IN ('Em coleta','Em atendimento','Finalizado','Entregue')",
        (today,),
    ).fetchone()["total"]
    concluidos_mes = conn.execute(
        "SELECT COUNT(*) AS total FROM appointments WHERE status='Concluído' AND substr(data_agendamento,1,7)=?",
        (month_prefix,),
    ).fetchone()["total"]
    faturamento_mes = conn.execute(
        "SELECT COALESCE(SUM(valor),0) AS total FROM appointments WHERE status='Concluído' AND substr(data_agendamento,1,7)=?",
        (month_prefix,),
    ).fetchone()["total"]
    proximos = conn.execute(
        '''
        SELECT a.data_agendamento, a.horario, a.status, c.nome AS cliente_nome, p.nome AS pet_nome, a.servico
        FROM appointments a
        JOIN clients c ON c.id = a.client_id
        JOIN pets p ON p.id = a.pet_id
        ORDER BY a.data_agendamento DESC, a.horario DESC
        LIMIT 8
        '''
    ).fetchall()
    conn.close()

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "user": user,
            "current_section": "dashboard",
            "total_clients": total_clients,
            "total_pets": total_pets,
            "agendados_hoje": agendados_hoje,
            "em_andamento_hoje": em_andamento_hoje,
            "concluidos_mes": concluidos_mes,
            "faturamento_mes": float(faturamento_mes or 0),
            "proximos": proximos,
            "today": today,
        },
    )


@app.get("/clients", response_class=HTMLResponse)
def clients_page(request: Request, edit_id: int | None = None, q: str = ""):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "clients")
    if blocked:
        return blocked

    conn = get_conn()
    if q.strip():
        like = f"%{q.strip()}%"
        clients = conn.execute(
            "SELECT * FROM clients WHERE nome LIKE ? OR telefone LIKE ? OR email LIKE ? ORDER BY nome",
            (like, like, like),
        ).fetchall()
    else:
        clients = conn.execute("SELECT * FROM clients ORDER BY nome").fetchall()
    edit_client = conn.execute("SELECT * FROM clients WHERE id=?", (edit_id,)).fetchone() if edit_id else None
    conn.close()

    return templates.TemplateResponse(
        "clients.html",
        {"request": request, "user": user, "current_section": "clients", "clients": clients, "edit_client": edit_client, "q": q},
    )


@app.post("/clients")
def create_client(request: Request, nome: str = Form(...), telefone: str = Form(""), email: str = Form(""), endereco: str = Form(""), observacoes: str = Form("")):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "clients")
    if blocked:
        return blocked
    conn = get_conn()
    conn.execute(
        "INSERT INTO clients(nome, telefone, email, endereco, observacoes) VALUES (?, ?, ?, ?, ?)",
        (nome, telefone or None, email or None, endereco or None, observacoes or None),
    )
    conn.commit()
    conn.close()
    return RedirectResponse(url="/clients", status_code=303)


@app.post("/clients/{client_id}/edit")
def edit_client(client_id: int, request: Request, nome: str = Form(...), telefone: str = Form(""), email: str = Form(""), endereco: str = Form(""), observacoes: str = Form("")):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "clients")
    if blocked:
        return blocked
    conn = get_conn()
    conn.execute(
        "UPDATE clients SET nome=?, telefone=?, email=?, endereco=?, observacoes=? WHERE id=?",
        (nome, telefone or None, email or None, endereco or None, observacoes or None, client_id),
    )
    conn.commit()
    conn.close()
    return RedirectResponse(url="/clients", status_code=303)


@app.post("/clients/{client_id}/delete")
def delete_client(client_id: int, request: Request):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "clients")
    if blocked:
        return blocked
    conn = get_conn()
    pets_count = conn.execute("SELECT COUNT(*) AS total FROM pets WHERE client_id=?", (client_id,)).fetchone()["total"]
    appt_count = conn.execute("SELECT COUNT(*) AS total FROM appointments WHERE client_id=?", (client_id,)).fetchone()["total"]
    if pets_count or appt_count:
        conn.close()
        return RedirectResponse(url="/clients", status_code=303)
    conn.execute("DELETE FROM clients WHERE id=?", (client_id,))
    conn.commit()
    conn.close()
    return RedirectResponse(url="/clients", status_code=303)


@app.get("/pets", response_class=HTMLResponse)
def pets_page(request: Request, edit_id: int | None = None, q: str = "", tutor_id: int | None = None):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "pets")
    if blocked:
        return blocked
    conn = get_conn()
    clients = conn.execute("SELECT * FROM clients ORDER BY nome").fetchall()
    sql = '''
        SELECT p.*, c.nome AS cliente_nome
        FROM pets p
        JOIN clients c ON c.id = p.client_id
        WHERE 1=1
    '''
    params = []
    if q.strip():
        like = f"%{q.strip()}%"
        sql += " AND (p.nome LIKE ? OR p.especie LIKE ? OR p.raca LIKE ? OR c.nome LIKE ?)"
        params.extend([like, like, like, like])
    if tutor_id:
        sql += " AND p.client_id = ?"
        params.append(tutor_id)
    sql += " ORDER BY p.nome"
    pets = conn.execute(sql, params).fetchall()
    edit_pet = conn.execute("SELECT * FROM pets WHERE id=?", (edit_id,)).fetchone() if edit_id else None
    conn.close()
    return templates.TemplateResponse(
        "pets.html",
        {"request": request, "user": user, "current_section": "pets", "pets": pets, "clients": clients, "edit_pet": edit_pet, "q": q, "tutor_id": tutor_id},
    )


@app.post("/pets")
def create_pet(request: Request, client_id: int = Form(...), nome: str = Form(...), especie: str = Form(""), raca: str = Form(""), porte: str = Form(""), idade: str = Form(""), observacoes: str = Form("")):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "pets")
    if blocked:
        return blocked
    conn = get_conn()
    conn.execute(
        "INSERT INTO pets(client_id, nome, especie, raca, porte, idade, observacoes) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (client_id, nome, especie or None, raca or None, porte or None, idade or None, observacoes or None),
    )
    conn.commit()
    conn.close()
    return RedirectResponse(url="/pets", status_code=303)


@app.post("/pets/{pet_id}/edit")
def edit_pet(pet_id: int, request: Request, client_id: int = Form(...), nome: str = Form(...), especie: str = Form(""), raca: str = Form(""), porte: str = Form(""), idade: str = Form(""), observacoes: str = Form("")):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "pets")
    if blocked:
        return blocked
    conn = get_conn()
    conn.execute(
        "UPDATE pets SET client_id=?, nome=?, especie=?, raca=?, porte=?, idade=?, observacoes=? WHERE id=?",
        (client_id, nome, especie or None, raca or None, porte or None, idade or None, observacoes or None, pet_id),
    )
    conn.commit()
    conn.close()
    return RedirectResponse(url="/pets", status_code=303)


@app.post("/pets/{pet_id}/delete")
def delete_pet(pet_id: int, request: Request):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "pets")
    if blocked:
        return blocked
    conn = get_conn()
    appt_count = conn.execute("SELECT COUNT(*) AS total FROM appointments WHERE pet_id=?", (pet_id,)).fetchone()["total"]
    if appt_count:
        conn.close()
        return RedirectResponse(url="/pets", status_code=303)
    conn.execute("DELETE FROM pets WHERE id=?", (pet_id,))
    conn.commit()
    conn.close()
    return RedirectResponse(url="/pets", status_code=303)


@app.get("/appointments", response_class=HTMLResponse)
def appointments_page(request: Request, edit_id: int | None = None, q: str = "", status_filter: str = "", data_inicio: str = "", data_fim: str = ""):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "appointments")
    if blocked:
        return blocked
    conn = get_conn()
    clients = conn.execute("SELECT * FROM clients ORDER BY nome").fetchall()
    pets = conn.execute(
        '''
        SELECT p.*, c.nome AS cliente_nome
        FROM pets p
        JOIN clients c ON c.id = p.client_id
        ORDER BY c.nome, p.nome
        '''
    ).fetchall()
    sql = '''
        SELECT a.*, c.nome AS cliente_nome, c.telefone AS cliente_telefone, c.endereco AS cliente_endereco, p.nome AS pet_nome
        FROM appointments a
        JOIN clients c ON c.id = a.client_id
        JOIN pets p ON p.id = a.pet_id
        WHERE 1=1
    '''
    params = []
    if q.strip():
        like = f"%{q.strip()}%"
        sql += " AND (c.nome LIKE ? OR p.nome LIKE ? OR a.servico LIKE ?)"
        params.extend([like, like, like])
    if status_filter.strip():
        sql += " AND a.status = ?"
        params.append(status_filter.strip())
    if data_inicio.strip():
        sql += " AND a.data_agendamento >= ?"
        params.append(data_inicio.strip())
    if data_fim.strip():
        sql += " AND a.data_agendamento <= ?"
        params.append(data_fim.strip())
    sql += " ORDER BY a.data_agendamento DESC, a.horario DESC"
    raw_appointments = conn.execute(sql, params).fetchall()
    edit_appointment = conn.execute("SELECT * FROM appointments WHERE id=?", (edit_id,)).fetchone() if edit_id else None
    conn.close()
    appointments = [build_wa_messages(dict(row)) for row in raw_appointments]
    return templates.TemplateResponse(
        "appointments.html",
        {
            "request": request,
            "user": user,
            "current_section": "appointments",
            "appointments": appointments,
            "clients": clients,
            "pets": pets,
            "edit_appointment": edit_appointment,
            "q": q,
            "status_filter": status_filter,
            "data_inicio": data_inicio,
            "data_fim": data_fim,
        },
    )


@app.post("/appointments")
def create_appointment(request: Request, client_id: int = Form(...), pet_id: int = Form(...), data_agendamento: str = Form(...), horario: str = Form(...), servico: str = Form(""), valor: str = Form(""), forma_pagamento: str = Form(""), status: str = Form("Agendado"), observacoes: str = Form("")):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "appointments")
    if blocked:
        return blocked
    conn = get_conn()
    conn.execute(
        '''
        INSERT INTO appointments(client_id, pet_id, data_agendamento, horario, servico, valor, forma_pagamento, status, observacoes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (client_id, pet_id, data_agendamento, horario, servico or None, to_float_br(valor), forma_pagamento or None, status or "Agendado", observacoes or None),
    )
    conn.commit()
    conn.close()
    return RedirectResponse(url="/appointments", status_code=303)


@app.post("/appointments/{appointment_id}/edit")
def edit_appointment(appointment_id: int, request: Request, client_id: int = Form(...), pet_id: int = Form(...), data_agendamento: str = Form(...), horario: str = Form(...), servico: str = Form(""), valor: str = Form(""), forma_pagamento: str = Form(""), status: str = Form("Agendado"), observacoes: str = Form("")):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "appointments")
    if blocked:
        return blocked
    conn = get_conn()
    conn.execute(
        '''
        UPDATE appointments
        SET client_id=?, pet_id=?, data_agendamento=?, horario=?, servico=?, valor=?, forma_pagamento=?, status=?, observacoes=?
        WHERE id=?
        ''',
        (client_id, pet_id, data_agendamento, horario, servico or None, to_float_br(valor), forma_pagamento or None, status or "Agendado", observacoes or None, appointment_id),
    )
    conn.commit()
    conn.close()
    return RedirectResponse(url="/appointments", status_code=303)


@app.post("/appointments/{appointment_id}/status")
def update_appointment_status(appointment_id: int, request: Request, new_status: str = Form(...)):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "appointments")
    if blocked:
        return blocked
    conn = get_conn()
    conn.execute("UPDATE appointments SET status=? WHERE id=?", (new_status, appointment_id))
    conn.commit()
    conn.close()
    return RedirectResponse(url="/appointments", status_code=303)


@app.post("/appointments/{appointment_id}/delete")
def delete_appointment(appointment_id: int, request: Request):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "appointments")
    if blocked:
        return blocked
    conn = get_conn()
    conn.execute("DELETE FROM appointments WHERE id=?", (appointment_id,))
    conn.commit()
    conn.close()
    return RedirectResponse(url="/appointments", status_code=303)


@app.get("/agenda", response_class=HTMLResponse)
def agenda_page(request: Request, year: int | None = None, month: int | None = None):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "agenda")
    if blocked:
        return blocked
    now = datetime.now()
    year = year or now.year
    month = month or now.month
    conn = get_conn()
    rows = conn.execute(
        '''
        SELECT a.id, a.data_agendamento, a.horario, a.status, c.nome AS cliente_nome, p.nome AS pet_nome
        FROM appointments a
        JOIN clients c ON c.id = a.client_id
        JOIN pets p ON p.id = a.pet_id
        WHERE substr(a.data_agendamento,1,7)=?
        ORDER BY a.data_agendamento, a.horario
        ''',
        (f"{year:04d}-{month:02d}",),
    ).fetchall()
    conn.close()
    grouped = {}
    grouped_preview = {}
    grouped_count = {}
    for r in rows:
        key = r["data_agendamento"]
        grouped.setdefault(key, []).append(r)
    for key, items in grouped.items():
        grouped_preview[key] = list(items[:4])
        grouped_count[key] = len(items)
    prev_year, prev_month = (year - 1, 12) if month == 1 else (year, month - 1)
    next_year, next_month = (year + 1, 1) if month == 12 else (year, month + 1)
    month_names = ["", "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]
    return templates.TemplateResponse(
        "agenda.html",
        {"request": request, "user": user, "current_section": "agenda", "year": year, "month": month, "month_name": month_names[month], "weeks": month_matrix(year, month), "grouped_preview": grouped_preview, "grouped_count": grouped_count, "prev_year": prev_year, "prev_month": prev_month, "next_year": next_year, "next_month": next_month},
    )


@app.get("/financeiro", response_class=HTMLResponse)
def financeiro_page(request: Request, start: str = "", end: str = ""):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "financeiro")
    if blocked:
        return blocked
    conn = get_conn()
    conditions = ["status='Concluído'"]
    params = []
    if start:
        conditions.append("data_agendamento >= ?")
        params.append(start)
    if end:
        conditions.append("data_agendamento <= ?")
        params.append(end)
    where = " AND ".join(conditions)
    resumo = conn.execute(f"SELECT COUNT(*) AS qtd, COALESCE(SUM(valor),0) AS total FROM appointments WHERE {where}", params).fetchone()
    detalhes = conn.execute(
        f'''
        SELECT a.*, c.nome AS cliente_nome, p.nome AS pet_nome
        FROM appointments a
        JOIN clients c ON c.id = a.client_id
        JOIN pets p ON p.id = a.pet_id
        WHERE {where}
        ORDER BY a.data_agendamento DESC, a.horario DESC
        ''',
        params,
    ).fetchall()
    por_pagamento = conn.execute(
        f'''
        SELECT COALESCE(forma_pagamento, 'Não informado') AS forma, COUNT(*) AS qtd, COALESCE(SUM(valor),0) AS total
        FROM appointments
        WHERE {where}
        GROUP BY COALESCE(forma_pagamento, 'Não informado')
        ORDER BY total DESC
        ''',
        params,
    ).fetchall()
    conn.close()
    total = float(resumo["total"] or 0)
    qtd = int(resumo["qtd"] or 0)
    ticket = total / qtd if qtd else 0
    return templates.TemplateResponse(
        "financeiro.html",
        {"request": request, "user": user, "current_section": "financeiro", "start": start, "end": end, "qtd": qtd, "total": total, "ticket": ticket, "detalhes": detalhes, "por_pagamento": por_pagamento},
    )


@app.get("/motorista", response_class=HTMLResponse)
def motorista_page(request: Request, data: str = ""):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "motorista")
    if blocked:
        return blocked
    if not data:
        data = datetime.now().strftime("%Y-%m-%d")
    conn = get_conn()
    items = conn.execute(
        '''
        SELECT a.*, c.nome AS cliente_nome, c.telefone AS cliente_telefone, c.endereco AS cliente_endereco, p.nome AS pet_nome
        FROM appointments a
        JOIN clients c ON c.id = a.client_id
        JOIN pets p ON p.id = a.pet_id
        WHERE a.data_agendamento = ?
        ORDER BY a.horario
        ''',
        (data,),
    ).fetchall()
    conn.close()
    rota_msg_lines = [f"Rota do dia - {data}", ""]
    items_enriched = []
    for idx, item in enumerate(items, start=1):
        d = dict(item)
        rota_msg_lines.append(f"{idx}. {d['horario']} - {d['cliente_nome']} / {d['pet_nome']}")
        rota_msg_lines.append(f"   Endereço: {d['cliente_endereco'] or ''}")
        rota_msg_lines.append(f"   Serviço: {d['servico'] or ''}")
        rota_msg_lines.append("")
        cliente_msg = (
            f"Olá, {d['cliente_nome']}!\n\n"
            f"Estamos a caminho para o atendimento do Pet & Gato.\n"
            f"⏰ Horário previsto: {d['horario']}\n"
            f"🐶 Pet: {d['pet_nome']}\n"
            f"🛁 Serviço: {d.get('servico') or ''}"
        )
        d["wa_cliente"] = wa_link(d.get("cliente_telefone"), cliente_msg)
        items_enriched.append(d)
    rota_msg = "\n".join(rota_msg_lines).strip()
    return templates.TemplateResponse(
        "motorista.html",
        {"request": request, "user": user, "current_section": "motorista", "data": data, "items": items_enriched, "rota_msg": rota_msg},
    )


@app.get("/motorista/maps")
def motorista_maps(request: Request, data: str = ""):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "motorista")
    if blocked:
        return blocked
    if not data:
        data = datetime.now().strftime("%Y-%m-%d")
    conn = get_conn()
    items = conn.execute(
        '''
        SELECT c.endereco AS endereco
        FROM appointments a
        JOIN clients c ON c.id = a.client_id
        WHERE a.data_agendamento = ? AND c.endereco IS NOT NULL AND c.endereco != ''
        ORDER BY a.horario
        ''',
        (data,),
    ).fetchall()
    conn.close()
    enderecos = [item["endereco"] for item in items]
    if not enderecos:
        return RedirectResponse(url=f"/motorista?data={data}", status_code=303)
    if len(enderecos) == 1:
        url = f"https://www.google.com/maps/search/?api=1&query={urllib.parse.quote(enderecos[0])}"
    else:
        origin = enderecos[0]
        destination = enderecos[-1]
        waypoints = "|".join(enderecos[1:-1]) if len(enderecos) > 2 else ""
        url = f"https://www.google.com/maps/dir/?api=1&origin={urllib.parse.quote(origin)}&destination={urllib.parse.quote(destination)}&travelmode=driving"
        if waypoints:
            url += f"&waypoints={urllib.parse.quote(waypoints)}"
    return RedirectResponse(url=url, status_code=302)


@app.get("/relatorios", response_class=HTMLResponse)
def relatorios_page(request: Request, start: str = "", end: str = "", status_filter: str = ""):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "relatorios")
    if blocked:
        return blocked
    conn = get_conn()
    conditions = ["1=1"]
    params = []
    if start.strip():
        conditions.append("a.data_agendamento >= ?")
        params.append(start.strip())
    if end.strip():
        conditions.append("a.data_agendamento <= ?")
        params.append(end.strip())
    if status_filter.strip():
        conditions.append("a.status = ?")
        params.append(status_filter.strip())
    where = " AND ".join(conditions)
    detalhes = conn.execute(
        f'''
        SELECT a.id, a.data_agendamento, a.horario, a.servico, a.valor, a.forma_pagamento, a.status, c.nome AS cliente_nome, p.nome AS pet_nome
        FROM appointments a
        JOIN clients c ON c.id = a.client_id
        JOIN pets p ON p.id = a.pet_id
        WHERE {where}
        ORDER BY a.data_agendamento DESC, a.horario DESC
        ''',
        params,
    ).fetchall()
    resumo = conn.execute(
        f'''
        SELECT COUNT(*) AS qtd, COALESCE(SUM(CASE WHEN a.status='Concluído' THEN a.valor ELSE 0 END),0) AS total_concluido
        FROM appointments a
        WHERE {where}
        ''',
        params,
    ).fetchone()
    conn.close()
    return templates.TemplateResponse(
        "relatorios.html",
        {"request": request, "user": user, "current_section": "relatorios", "start": start, "end": end, "status_filter": status_filter, "detalhes": detalhes, "qtd": int(resumo["qtd"] or 0), "total_concluido": float(resumo["total_concluido"] or 0)},
    )


@app.get("/relatorios/export/clientes")
def export_clientes(request: Request):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "relatorios")
    if blocked:
        return blocked
    conn = get_conn()
    rows = conn.execute("SELECT id, nome, telefone, email, endereco, observacoes FROM clients ORDER BY nome").fetchall()
    conn.close()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "nome", "telefone", "email", "endereco", "observacoes"])
    for r in rows:
        writer.writerow([r["id"], r["nome"], r["telefone"] or "", r["email"] or "", r["endereco"] or "", r["observacoes"] or ""])
    return Response(content=output.getvalue(), media_type="text/csv; charset=utf-8", headers={"Content-Disposition": "attachment; filename=clientes.csv"})


@app.get("/relatorios/export/pets")
def export_pets(request: Request):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "relatorios")
    if blocked:
        return blocked
    conn = get_conn()
    rows = conn.execute(
        '''
        SELECT p.id, p.nome, c.nome AS tutor, p.especie, p.raca, p.porte, p.idade, p.observacoes
        FROM pets p
        JOIN clients c ON c.id = p.client_id
        ORDER BY p.nome
        '''
    ).fetchall()
    conn.close()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "nome", "tutor", "especie", "raca", "porte", "idade", "observacoes"])
    for r in rows:
        writer.writerow([r["id"], r["nome"], r["tutor"], r["especie"] or "", r["raca"] or "", r["porte"] or "", r["idade"] or "", r["observacoes"] or ""])
    return Response(content=output.getvalue(), media_type="text/csv; charset=utf-8", headers={"Content-Disposition": "attachment; filename=pets.csv"})


@app.get("/relatorios/export/agendamentos")
def export_agendamentos(request: Request, start: str = "", end: str = "", status_filter: str = ""):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "relatorios")
    if blocked:
        return blocked
    conn = get_conn()
    conditions = ["1=1"]
    params = []
    if start.strip():
        conditions.append("a.data_agendamento >= ?")
        params.append(start.strip())
    if end.strip():
        conditions.append("a.data_agendamento <= ?")
        params.append(end.strip())
    if status_filter.strip():
        conditions.append("a.status = ?")
        params.append(status_filter.strip())
    where = " AND ".join(conditions)
    rows = conn.execute(
        f'''
        SELECT a.id, a.data_agendamento, a.horario, c.nome AS cliente, p.nome AS pet, a.servico, a.valor, a.forma_pagamento, a.status, a.observacoes
        FROM appointments a
        JOIN clients c ON c.id = a.client_id
        JOIN pets p ON p.id = a.pet_id
        WHERE {where}
        ORDER BY a.data_agendamento DESC, a.horario DESC
        ''',
        params,
    ).fetchall()
    conn.close()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "data", "hora", "cliente", "pet", "servico", "valor", "forma_pagamento", "status", "observacoes"])
    for r in rows:
        writer.writerow([r["id"], r["data_agendamento"], r["horario"], r["cliente"], r["pet"], r["servico"] or "", r["valor"] or "", r["forma_pagamento"] or "", r["status"] or "", r["observacoes"] or ""])
    return Response(content=output.getvalue(), media_type="text/csv; charset=utf-8", headers={"Content-Disposition": "attachment; filename=agendamentos.csv"})


@app.get("/users", response_class=HTMLResponse)
def users_page(request: Request, edit_id: int | None = None, msg: str = ""):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "users")
    if blocked:
        return blocked
    conn = get_conn()
    users = conn.execute("SELECT * FROM users ORDER BY full_name, username").fetchall()
    edit_user = conn.execute("SELECT * FROM users WHERE id=?", (edit_id,)).fetchone() if edit_id else None
    profiles = conn.execute("SELECT name FROM profiles ORDER BY name").fetchall()
    conn.close()
    return templates.TemplateResponse(
        "users.html",
        {"request": request, "user": user, "current_section": "users", "users": users, "edit_user": edit_user, "profiles": profiles, "msg": msg},
    )


@app.post("/users")
def create_user(request: Request, username: str = Form(...), full_name: str = Form(...), password: str = Form(...), password_confirm: str = Form(...), profile_name: str = Form(...), active: str = Form("1")):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "users")
    if blocked:
        return blocked
    if password.strip() != password_confirm.strip():
        return RedirectResponse(url="/users?msg=senhas_nao_conferem", status_code=303)
    conn = get_conn()
    conn.execute(
        "INSERT INTO users(username, full_name, password_hash, profile_name, active) VALUES (?, ?, ?, ?, ?)",
        (username.strip(), full_name.strip(), hash_password(password.strip()), profile_name.strip(), 1 if active == "1" else 0),
    )
    conn.commit()
    conn.close()
    return RedirectResponse(url="/users?msg=usuario_criado", status_code=303)


@app.post("/users/{user_id}/edit")
def edit_user(user_id: int, request: Request, username: str = Form(...), full_name: str = Form(...), password: str = Form(""), password_confirm: str = Form(""), profile_name: str = Form(...), active: str = Form("1")):
    current = current_user(request)
    if not current:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(current, "users")
    if blocked:
        return blocked
    if password.strip() and password.strip() != password_confirm.strip():
        return RedirectResponse(url=f"/users?edit_id={user_id}&msg=senhas_nao_conferem", status_code=303)
    conn = get_conn()
    existing = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
    if not existing:
        conn.close()
        return RedirectResponse(url="/users", status_code=303)
    password_hash = existing["password_hash"] if not password.strip() else hash_password(password.strip())
    conn.execute(
        "UPDATE users SET username=?, full_name=?, password_hash=?, profile_name=?, active=? WHERE id=?",
        (username.strip(), full_name.strip(), password_hash, profile_name.strip(), 1 if active == "1" else 0, user_id),
    )
    conn.commit()
    conn.close()
    return RedirectResponse(url="/users?msg=usuario_atualizado", status_code=303)
    password_hash = existing["password_hash"] if not password.strip() else hash_password(password.strip())
    conn.execute(
        "UPDATE users SET username=?, full_name=?, password_hash=?, profile_name=?, active=? WHERE id=?",
        (username.strip(), full_name.strip(), password_hash, profile_name.strip(), 1 if active == "1" else 0, user_id),
    )
    conn.commit()
    conn.close()
    return RedirectResponse(url="/users", status_code=303)


@app.post("/users/{user_id}/delete")
def delete_user(user_id: int, request: Request):
    current = current_user(request)
    if not current:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(current, "users")
    if blocked:
        return blocked
    if int(current["id"]) == int(user_id):
        return RedirectResponse(url="/users", status_code=303)
    conn = get_conn()
    conn.execute("DELETE FROM users WHERE id=?", (user_id,))
    conn.commit()
    conn.close()
    return RedirectResponse(url="/users", status_code=303)


@app.get("/profiles", response_class=HTMLResponse)
def profiles_page(request: Request, edit_id: int | None = None):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "profiles")
    if blocked:
        return blocked
    conn = get_conn()
    profiles = conn.execute("SELECT * FROM profiles ORDER BY name").fetchall()
    edit_profile = conn.execute("SELECT * FROM profiles WHERE id=?", (edit_id,)).fetchone() if edit_id else None
    conn.close()
    selected_permissions = set()
    if edit_profile and edit_profile["permissions"]:
        selected_permissions = {p.strip() for p in str(edit_profile["permissions"]).split(",") if p.strip()}
    return templates.TemplateResponse(
        "profiles.html",
        {"request": request, "user": user, "current_section": "profiles", "profiles": profiles, "edit_profile": edit_profile, "permission_options": PERMISSION_OPTIONS, "selected_permissions": selected_permissions},
    )


@app.post("/profiles")
def create_profile(request: Request, name: str = Form(...), permissions: list[str] = Form(default=[])):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "profiles")
    if blocked:
        return blocked
    perms = ",".join(sorted(set([p.strip() for p in permissions if p.strip()])))
    conn = get_conn()
    conn.execute("INSERT INTO profiles(name, permissions) VALUES (?, ?)", (name.strip(), perms))
    conn.commit()
    conn.close()
    return RedirectResponse(url="/profiles", status_code=303)


@app.post("/profiles/{profile_id}/edit")
def edit_profile(profile_id: int, request: Request, name: str = Form(...), permissions: list[str] = Form(default=[])):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "profiles")
    if blocked:
        return blocked
    perms = ",".join(sorted(set([p.strip() for p in permissions if p.strip()])))
    conn = get_conn()
    current = conn.execute("SELECT * FROM profiles WHERE id=?", (profile_id,)).fetchone()
    if not current:
        conn.close()
        return RedirectResponse(url="/profiles", status_code=303)
    old_name = current["name"]
    conn.execute("UPDATE profiles SET name=?, permissions=? WHERE id=?", (name.strip(), perms, profile_id))
    conn.execute("UPDATE users SET profile_name=? WHERE profile_name=?", (name.strip(), old_name))
    conn.commit()
    conn.close()
    return RedirectResponse(url="/profiles", status_code=303)


@app.post("/profiles/{profile_id}/delete")
def delete_profile(profile_id: int, request: Request):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "profiles")
    if blocked:
        return blocked
    conn = get_conn()
    profile = conn.execute("SELECT * FROM profiles WHERE id=?", (profile_id,)).fetchone()
    if not profile:
        conn.close()
        return RedirectResponse(url="/profiles", status_code=303)
    if profile["name"] == "Administrador":
        conn.close()
        return RedirectResponse(url="/profiles", status_code=303)
    in_use = conn.execute("SELECT COUNT(*) AS total FROM users WHERE profile_name=?", (profile["name"],)).fetchone()["total"]
    if in_use:
        conn.close()
        return RedirectResponse(url="/profiles", status_code=303)
    conn.execute("DELETE FROM profiles WHERE id=?", (profile_id,))
    conn.commit()
    conn.close()
    return RedirectResponse(url="/profiles", status_code=303)


@app.get("/company", response_class=HTMLResponse)
def company_page(request: Request):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "company")
    if blocked:
        return blocked

    conn = get_conn()
    company = conn.execute("SELECT * FROM company_settings WHERE id=1").fetchone()
    conn.close()

    return templates.TemplateResponse(
        "company.html",
        {
            "request": request,
            "user": user,
            "current_section": "company",
            "company": company,
        },
    )


@app.post("/company")
def save_company(
    request: Request,
    nome_empresa: str = Form(""),
    nome_fantasia: str = Form(""),
    cnpj: str = Form(""),
    telefone: str = Form(""),
    whatsapp: str = Form(""),
    email: str = Form(""),
    endereco: str = Form(""),
    cidade: str = Form(""),
    estado: str = Form(""),
    cep: str = Form(""),
    pix_chave: str = Form(""),
    pix_tipo: str = Form(""),
    logo_url: str = Form(""),
    observacoes: str = Form(""),
):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "company")
    if blocked:
        return blocked

    conn = get_conn()
    conn.execute(
        '''
        UPDATE company_settings
        SET nome_empresa=?, nome_fantasia=?, cnpj=?, telefone=?, whatsapp=?, email=?, endereco=?,
            cidade=?, estado=?, cep=?, pix_chave=?, pix_tipo=?, logo_url=?, observacoes=?
        WHERE id=1
        ''',
        (
            nome_empresa or None,
            nome_fantasia or None,
            cnpj or None,
            telefone or None,
            whatsapp or None,
            email or None,
            endereco or None,
            cidade or None,
            estado or None,
            cep or None,
            pix_chave or None,
            pix_tipo or None,
            logo_url or None,
            observacoes or None,
        ),
    )
    conn.commit()
    conn.close()
    return RedirectResponse(url="/company", status_code=303)

@app.get("/appointments/{appointment_id}/receipt")
def appointment_receipt(appointment_id: int, request: Request):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "appointments")
    if blocked:
        return blocked

    conn = get_conn()
    item = conn.execute(
        '''
        SELECT a.*, c.nome AS cliente_nome, p.nome AS pet_nome
        FROM appointments a
        JOIN clients c ON c.id = a.client_id
        JOIN pets p ON p.id = a.pet_id
        WHERE a.id=?
        ''',
        (appointment_id,),
    ).fetchone()
    company = conn.execute("SELECT * FROM company_settings WHERE id=1").fetchone()
    conn.close()

    if not item:
        return RedirectResponse(url="/appointments", status_code=303)

    receipts_dir = ROOT_DIR / "recibos"
    receipts_dir.mkdir(exist_ok=True)
    filename = f"recibo_{appointment_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    pdf_path = receipts_dir / filename

    generate_receipt_pdf(dict(item), dict(company) if company else None, pdf_path)

    pdf_bytes = pdf_path.read_bytes()
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="{filename}"'},
    )

@app.get("/backup", response_class=HTMLResponse)
def backup_page(request: Request, msg: str = ""):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "backup")
    if blocked:
        return blocked
    return templates.TemplateResponse(
        "backup.html",
        {
            "request": request,
            "user": user,
            "current_section": "backup",
            "backup_files": list_backup_items(),
            "s3_enabled": has_s3_config(),
            "using_postgres": is_postgres(),
            "msg": msg,
        },
    )

@app.post("/backup/create")
def create_backup(request: Request):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "backup")
    if blocked:
        return blocked
    filename = backup_filename()
    content = generate_app_backup_bytes()
    save_backup_local_and_s3(content, filename)
    return RedirectResponse(url="/backup?msg=backup_criado", status_code=303)

@app.get("/backup/download/{filename}")
def download_backup(filename: str, request: Request):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "backup")
    if blocked:
        return blocked
    data = download_backup_content(filename)
    return Response(content=data, media_type="application/json", headers={"Content-Disposition": f'attachment; filename="{filename}"'})

@app.post("/backup/restore")
async def restore_backup(request: Request, backup_file: UploadFile = File(...)):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "backup")
    if blocked:
        return blocked
    if not backup_file.filename.lower().endswith(".json"):
        return RedirectResponse(url="/backup?msg=formato_invalido", status_code=303)
    content = await backup_file.read()
    restore_backup_content(content)
    return RedirectResponse(url="/backup?msg=backup_restaurado", status_code=303)

@app.post("/backup/restore-s3")
def restore_backup_from_s3(request: Request, filename: str = Form(...)):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    blocked = guard_section(user, "backup")
    if blocked:
        return blocked
    content = download_backup_content(filename)
    restore_backup_content(content)
    return RedirectResponse(url="/backup?msg=backup_restaurado", status_code=303)

@app.get("/logout")
def logout():
    response = RedirectResponse(url="/login", status_code=303)
    response.delete_cookie("username")
    return response
