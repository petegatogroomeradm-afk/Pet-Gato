from pathlib import Path
import shutil
import re

ROOT = Path.cwd()
APP = ROOT / 'app.py'
TEMPLATES = ROOT / 'templates'
BASE = TEMPLATES / 'base.html'

if not APP.exists():
    raise SystemExit('ERRO: app.py não encontrado. Rode dentro da pasta do projeto.')

backup = ROOT / 'backup_antes_multiempresa'
backup.mkdir(exist_ok=True)
shutil.copy2(APP, backup / 'app.py')
if BASE.exists():
    shutil.copy2(BASE, backup / 'base.html')

app = APP.read_text(encoding='utf-8')

MULTI_BLOCK = r'''

# =========================================================
# Multiempresa / SaaS - Pet & Gatô Ponto
# =========================================================
_MULTIEMPRESA_DB_READY = False

PLAN_LIMITS = {
    "BASICO": {"name": "Básico", "price": "R$29/mês", "employees": 5},
    "PRO": {"name": "Profissional", "price": "R$59/mês", "employees": 20},
    "PREMIUM": {"name": "Premium", "price": "R$99/mês", "employees": 9999},
}


def _column_exists(table_name: str, column_name: str) -> bool:
    if os.getenv("DATABASE_URL"):
        row = query_db(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = ? AND column_name = ?
            """,
            (table_name, column_name),
            one=True,
        )
        return bool(row)
    row = query_db(f"PRAGMA table_info({table_name})")
    return any(r["name"] == column_name for r in row)


def _add_company_column(table_name: str) -> None:
    if _column_exists(table_name, "company_id"):
        return
    execute_db(f"ALTER TABLE {table_name} ADD COLUMN company_id INTEGER")


def ensure_multiempresa_tables() -> None:
    """Prepara o sistema para vender para várias empresas sem apagar dados antigos."""
    if os.getenv("DATABASE_URL"):
        execute_db("""
            CREATE TABLE IF NOT EXISTS companies (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                slug TEXT NOT NULL UNIQUE,
                responsible_name TEXT,
                email TEXT,
                phone TEXT,
                plan TEXT NOT NULL DEFAULT 'BASICO',
                status TEXT NOT NULL DEFAULT 'ATIVO',
                trial_until TEXT,
                created_at TEXT NOT NULL
            )
        """)
        execute_db("""
            CREATE TABLE IF NOT EXISTS billing_events (
                id SERIAL PRIMARY KEY,
                company_id INTEGER,
                event_type TEXT NOT NULL,
                description TEXT,
                amount TEXT,
                status TEXT NOT NULL DEFAULT 'PENDENTE',
                created_at TEXT NOT NULL
            )
        """)
    else:
        execute_db("""
            CREATE TABLE IF NOT EXISTS companies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                slug TEXT NOT NULL UNIQUE,
                responsible_name TEXT,
                email TEXT,
                phone TEXT,
                plan TEXT NOT NULL DEFAULT 'BASICO',
                status TEXT NOT NULL DEFAULT 'ATIVO',
                trial_until TEXT,
                created_at TEXT NOT NULL
            )
        """)
        execute_db("""
            CREATE TABLE IF NOT EXISTS billing_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER,
                event_type TEXT NOT NULL,
                description TEXT,
                amount TEXT,
                status TEXT NOT NULL DEFAULT 'PENDENTE',
                created_at TEXT NOT NULL
            )
        """)

    for table in [
        "users", "employees", "schedules", "employee_schedule_days", "time_records",
        "time_adjustments", "system_logs", "holidays", "notification_logs"
    ]:
        try:
            _add_company_column(table)
        except Exception as exc:
            print(f"Aviso multiempresa: não foi possível ajustar {table}: {exc}")

    default_company = query_db("SELECT * FROM companies WHERE slug = ?", ("pet-gato",), one=True)
    if not default_company:
        execute_db(
            """
            INSERT INTO companies (name, slug, responsible_name, email, phone, plan, status, trial_until, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("Pet & Gatô", "pet-gato", "Diego Poppe", "", "", "PREMIUM", "ATIVO", "", now_iso()),
        )

    default_company = query_db("SELECT * FROM companies WHERE slug = ?", ("pet-gato",), one=True)
    default_id = default_company["id"]

    for table in ["users", "employees", "schedules", "employee_schedule_days", "time_records", "time_adjustments", "system_logs"]:
        try:
            execute_db(f"UPDATE {table} SET company_id = ? WHERE company_id IS NULL", (default_id,))
        except Exception as exc:
            print(f"Aviso multiempresa update {table}: {exc}")


def ensure_multiempresa_ready() -> None:
    global _MULTIEMPRESA_DB_READY
    if not _MULTIEMPRESA_DB_READY:
        ensure_multiempresa_tables()
        _MULTIEMPRESA_DB_READY = True


@app.before_request
def multiempresa_bootstrap():
    if request.endpoint not in ("static", "health", "service_worker"):
        try:
            ensure_multiempresa_ready()
        except Exception as exc:
            print("Erro ao inicializar multiempresa:", exc)


def current_company_id() -> Optional[int]:
    cid = session.get("company_id")
    try:
        return int(cid) if cid else None
    except Exception:
        return None


def current_company():
    cid = current_company_id()
    if not cid:
        return None
    return query_db("SELECT * FROM companies WHERE id = ?", (cid,), one=True)


def is_super_admin() -> bool:
    return session.get("role") in ("super_admin", "master") or session.get("user_name") == "Administrador"


def company_plan_limit(company_id: Optional[int] = None) -> int:
    company = query_db("SELECT * FROM companies WHERE id = ?", (company_id or current_company_id(),), one=True)
    if not company:
        return 0
    plan = (company.get("plan") or "BASICO").upper()
    return PLAN_LIMITS.get(plan, PLAN_LIMITS["BASICO"])["employees"]


def active_employee_count(company_id: Optional[int] = None) -> int:
    row = query_db(
        "SELECT COUNT(*) AS total FROM employees WHERE active = 1 AND company_id = ?",
        (company_id or current_company_id(),),
        one=True,
    )
    return int(row["total"] or 0)


def check_employee_limit() -> bool:
    cid = current_company_id()
    if not cid:
        return True
    return active_employee_count(cid) < company_plan_limit(cid)


@app.context_processor
def inject_company_globals():
    return {
        "current_company": current_company,
        "PLAN_LIMITS": PLAN_LIMITS,
        "is_super_admin": is_super_admin,
    }


@app.route("/empresas", methods=["GET", "POST"])
@admin_required
def companies_page():
    ensure_multiempresa_ready()
    if not is_super_admin():
        flash("Acesso restrito ao administrador principal.", "danger")
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        name = request.form.get("name", "").strip()
        slug = request.form.get("slug", "").strip().lower().replace(" ", "-")
        responsible_name = request.form.get("responsible_name", "").strip()
        email = request.form.get("email", "").strip()
        phone = request.form.get("phone", "").strip()
        plan = request.form.get("plan", "BASICO").strip().upper()
        status = request.form.get("status", "ATIVO").strip().upper()
        trial_until = request.form.get("trial_until", "").strip()

        if not name or not slug:
            flash("Nome e identificador da empresa são obrigatórios.", "danger")
        else:
            execute_db(
                """
                INSERT INTO companies (name, slug, responsible_name, email, phone, plan, status, trial_until, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(slug) DO UPDATE SET
                    name = excluded.name,
                    responsible_name = excluded.responsible_name,
                    email = excluded.email,
                    phone = excluded.phone,
                    plan = excluded.plan,
                    status = excluded.status,
                    trial_until = excluded.trial_until
                """,
                (name, slug, responsible_name, email, phone, plan, status, trial_until, now_iso()),
            )
            flash("Empresa salva com sucesso.", "success")
            return redirect(url_for("companies_page"))

    companies = query_db("SELECT * FROM companies ORDER BY created_at DESC, id DESC")
    rows = []
    for c in companies:
        row = dict(c)
        row["employees_count"] = active_employee_count(row["id"])
        row["limit"] = company_plan_limit(row["id"])
        rows.append(row)
    return render_template("companies.html", companies=rows)


@app.route("/empresas/<int:company_id>/entrar")
@admin_required
def switch_company(company_id: int):
    if not is_super_admin():
        flash("Acesso restrito.", "danger")
        return redirect(url_for("dashboard"))
    company = query_db("SELECT * FROM companies WHERE id = ?", (company_id,), one=True)
    if company:
        session["company_id"] = company["id"]
        session["company_name"] = company["name"]
        flash(f"Empresa ativa: {company['name']}", "success")
    return redirect(url_for("dashboard"))


@app.route("/planos")
@admin_required
def plans_page():
    ensure_multiempresa_ready()
    return render_template("plans.html", company=current_company())


@app.route("/assinatura")
@admin_required
def subscription_page():
    ensure_multiempresa_ready()
    company = current_company()
    events = []
    if company:
        events = query_db("SELECT * FROM billing_events WHERE company_id = ? ORDER BY created_at DESC", (company["id"],))
    return render_template("subscription.html", company=company, events=events)
'''

if '# Multiempresa / SaaS - Pet & Gatô Ponto' not in app:
    marker = '# Inicializa as tabelas tanto no Render/PostgreSQL quanto no Windows/SQLite.'
    if marker in app:
        app = app.replace(marker, MULTI_BLOCK + '\n' + marker)
    else:
        app += MULTI_BLOCK

# security paths
app = app.replace('"/notificacoes",\n        "/logout",', '"/notificacoes",\n        "/empresas",\n        "/planos",\n        "/assinatura",\n        "/logout",')

# set company session on login after user_name
old = 'session["user_name"] = user["name"]\n            log_action("LOGIN", f"Usuário {username} entrou no sistema")'
new = 'session["user_name"] = user["name"]\n            session["role"] = user.get("role", "admin") if hasattr(user, "get") else user["role"]\n            session["company_id"] = user.get("company_id") if hasattr(user, "get") else user["company_id"]\n            if session.get("company_id"):\n                empresa_login = query_db("SELECT name FROM companies WHERE id = ?", (session["company_id"],), one=True)\n                session["company_name"] = empresa_login["name"] if empresa_login else ""\n            log_action("LOGIN", f"Usuário {username} entrou no sistema")'
if old in app and 'session["company_id"]' not in app[app.find(old)-200:app.find(old)+500]:
    app = app.replace(old, new)

# make admin user have company_id column assigned after init via ensure already done; for future insert default user add role okay
APP.write_text(app, encoding='utf-8')

TEMPLATES.mkdir(exist_ok=True)

(TEMPLATES / 'companies.html').write_text(r'''{% extends "base.html" %}
{% block content %}
<div class="page-header">
  <div>
    <h1>Empresas</h1>
    <p>Cadastro de clientes para vender o sistema como SaaS multiempresa.</p>
  </div>
</div>

<section class="card">
  <h2>Nova empresa / cliente</h2>
  <form method="post" class="form-grid">
    <label>Nome da empresa<input name="name" required placeholder="Ex: Clínica Modelo"></label>
    <label>Identificador / slug<input name="slug" required placeholder="clinica-modelo"></label>
    <label>Responsável<input name="responsible_name" placeholder="Nome do responsável"></label>
    <label>E-mail<input name="email" type="email" placeholder="cliente@email.com"></label>
    <label>WhatsApp<input name="phone" placeholder="5531999999999"></label>
    <label>Plano
      <select name="plan">
        <option value="BASICO">Básico - até 5 funcionários</option>
        <option value="PRO">Profissional - até 20 funcionários</option>
        <option value="PREMIUM">Premium - ilimitado</option>
      </select>
    </label>
    <label>Status
      <select name="status">
        <option value="ATIVO">Ativo</option>
        <option value="TESTE">Teste</option>
        <option value="BLOQUEADO">Bloqueado</option>
      </select>
    </label>
    <label>Teste grátis até<input name="trial_until" type="date"></label>
    <button class="btn primary" type="submit">Salvar empresa</button>
  </form>
</section>

<section class="card">
  <h2>Clientes cadastrados</h2>
  <div class="table-wrap">
    <table>
      <thead><tr><th>Empresa</th><th>Plano</th><th>Status</th><th>Funcionários</th><th>Responsável</th><th>Ação</th></tr></thead>
      <tbody>
      {% for c in companies %}
        <tr>
          <td><strong>{{ c.name }}</strong><br><small>{{ c.slug }}</small></td>
          <td>{{ c.plan }}</td>
          <td>{{ c.status }}</td>
          <td>{{ c.employees_count }} / {{ c.limit }}</td>
          <td>{{ c.responsible_name or '-' }}<br><small>{{ c.phone or '' }}</small></td>
          <td><a class="btn small" href="{{ url_for('switch_company', company_id=c.id) }}">Entrar</a></td>
        </tr>
      {% else %}
        <tr><td colspan="6">Nenhuma empresa cadastrada.</td></tr>
      {% endfor %}
      </tbody>
    </table>
  </div>
</section>
{% endblock %}
''', encoding='utf-8')

(TEMPLATES / 'plans.html').write_text(r'''{% extends "base.html" %}
{% block content %}
<div class="page-header"><h1>Planos comerciais</h1></div>
<div class="grid grid-3">
  {% for key, p in PLAN_LIMITS.items() %}
  <section class="card">
    <h2>{{ p.name }}</h2>
    <h1>{{ p.price }}</h1>
    <p>Limite: {% if p.employees > 9000 %}Ilimitado{% else %}{{ p.employees }} funcionários{% endif %}</p>
  </section>
  {% endfor %}
</div>
{% if company %}
<section class="card"><h2>Empresa atual</h2><p>{{ company.name }} — Plano {{ company.plan }}</p></section>
{% endif %}
{% endblock %}
''', encoding='utf-8')

(TEMPLATES / 'subscription.html').write_text(r'''{% extends "base.html" %}
{% block content %}
<div class="page-header"><h1>Assinatura</h1><p>Controle comercial para cobrança mensal.</p></div>
<section class="card">
  {% if company %}
    <h2>{{ company.name }}</h2>
    <p>Plano atual: <strong>{{ company.plan }}</strong></p>
    <p>Status: <strong>{{ company.status }}</strong></p>
    <p>Teste grátis até: {{ company.trial_until or '-' }}</p>
  {% else %}
    <p>Nenhuma empresa ativa na sessão.</p>
  {% endif %}
</section>
<section class="card">
  <h2>Histórico financeiro</h2>
  <div class="table-wrap"><table><thead><tr><th>Data</th><th>Tipo</th><th>Descrição</th><th>Valor</th><th>Status</th></tr></thead><tbody>
  {% for e in events %}<tr><td>{{ e.created_at }}</td><td>{{ e.event_type }}</td><td>{{ e.description }}</td><td>{{ e.amount }}</td><td>{{ e.status }}</td></tr>
  {% else %}<tr><td colspan="5">Nenhum evento financeiro.</td></tr>{% endfor %}
  </tbody></table></div>
</section>
{% endblock %}
''', encoding='utf-8')

if BASE.exists():
    base = BASE.read_text(encoding='utf-8')
    if "url_for('companies_page')" not in base:
        base = base.replace("<a href=\"{{ url_for('dashboard') }}\">Dashboard</a>", "<a href=\"{{ url_for('dashboard') }}\">Dashboard</a>\n                <a href=\"{{ url_for('companies_page') }}\">Empresas</a>\n                <a href=\"{{ url_for('plans_page') }}\">Planos</a>")
    BASE.write_text(base, encoding='utf-8')

print('✅ Multiempresa aplicado com sucesso.')
print('Backup salvo em:', backup)
print('Agora rode: git add . && git commit -m "Adiciona multiempresa SaaS" && git push')
