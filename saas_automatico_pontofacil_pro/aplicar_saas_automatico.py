from pathlib import Path
import re
import textwrap

BASE = Path.cwd()
APP = BASE / 'app.py'
TEMPLATES = BASE / 'templates'

if not APP.exists():
    raise SystemExit('ERRO: app.py não encontrado. Rode este script dentro da pasta do projeto.')

app = APP.read_text(encoding='utf-8')
backup = BASE / 'app_backup_antes_saas_automatico.py'
if not backup.exists():
    backup.write_text(app, encoding='utf-8')

# 1) Libera rotas públicas necessárias para pagamento/webhook e evita bloqueio de IP em /assinatura.
new_security = r'''
@app.before_request
def security_gate():
    allowed_public = ["static", "service_worker", "health"]

    if request.endpoint in allowed_public:
        return None

    # Rotas públicas necessárias para cobrança, retorno e webhook Mercado Pago.
    public_paths = [
        "/login",
        "/assinatura",
        "/assinatura/pagar",
        "/webhook/mercadopago",
        "/pagamento/sucesso",
        "/pagamento/falha",
        "/pagamento/pendente",
        "/saas/status",
    ]
    if any(request.path.startswith(path) for path in public_paths):
        return None

    if request.path.startswith("/ponto") or request.path == "/":
        if not is_store_network() and not has_admin_key():
            return render_template("blocked.html", ip=get_client_ip()), 403

    admin_paths = [
        "/dashboard",
        "/funcionarios",
        "/registros",
        "/relatorios",
        "/jornadas",
        "/configuracoes",
        "/notificacoes",
        "/empresas",
        "/planos",
        "/financeiro-saas",
        "/logout",
    ]

    if any(request.path.startswith(path) for path in admin_paths):
        if not has_admin_key() and not session.get("user_id"):
            return render_template("blocked.html", ip=get_client_ip()), 403

    return None
'''
app = re.sub(r'@app\.before_request\s*\ndef security_gate\(\):.*?\n\s*return None\n\n# ---------------------------\n# Database helpers', new_security + '\n\n# ---------------------------\n# Database helpers', app, flags=re.S)

# 2) Torna admin_required SaaS-aware: cliente bloqueado vai para assinatura, super admin entra sempre.
new_admin_required = r'''
def admin_required(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            flash("Faça login para acessar o painel.", "warning")
            return redirect(url_for("login"))

        # Super admin sempre entra.
        try:
            if is_super_admin():
                return func(*args, **kwargs)
        except Exception:
            pass

        # Cliente comum precisa estar ativo/pago/teste válido.
        try:
            company = current_company()
            if company and not company_access_allowed(company):
                if request.endpoint != "subscription_page" and not request.path.startswith("/assinatura"):
                    flash("Assinatura pendente ou vencida. Regularize para liberar o painel.", "warning")
                    return redirect(url_for("subscription_page"))
        except Exception as exc:
            print("Aviso verificação assinatura:", exc)

        return func(*args, **kwargs)
    return wrapper
'''
app = re.sub(r'def admin_required\(func\):.*?\n\s*return wrapper\n\n\ndef log_action', new_admin_required + '\n\ndef log_action', app, flags=re.S)

# 3) Adiciona automação SaaS: bloqueio, liberação, verificação manual, painel financeiro e WhatsApp de cobrança.
block = r'''

# =========================================================
# Automação SaaS - cobrança, bloqueio, liberação e avisos
# =========================================================
_SAAS_AUTOMATION_READY = False


def _safe_get(row, key, default=""):
    if not row:
        return default
    try:
        return row.get(key, default)
    except Exception:
        try:
            return row[key]
        except Exception:
            return default


def company_access_allowed(company) -> bool:
    """Regra central: ativo, pago ou teste válido libera acesso."""
    if not company:
        return False
    status = str(_safe_get(company, "status", "")).upper()
    payment_status = str(_safe_get(company, "payment_status", "")).upper()
    paid_until = str(_safe_get(company, "paid_until", "") or "")
    trial_until = str(_safe_get(company, "trial_until", "") or "")
    today = today_str()

    if status == "ATIVO" and (not paid_until or paid_until >= today):
        return True
    if payment_status == "PAGO" and paid_until and paid_until >= today:
        return True
    if status == "TESTE" and trial_until and trial_until >= today:
        return True
    return False


def ensure_saas_automation_ready() -> None:
    """Garante colunas para automação sem apagar dados."""
    global _SAAS_AUTOMATION_READY
    if _SAAS_AUTOMATION_READY:
        return
    try:
        ensure_billing_ready()
    except Exception:
        try:
            ensure_multiempresa_ready()
        except Exception:
            pass

    for column in ["blocked_at", "last_billing_alert_at", "next_billing_at"]:
        try:
            if not _column_exists("companies", column):
                execute_db(f"ALTER TABLE companies ADD COLUMN {column} TEXT")
        except Exception as exc:
            print(f"Aviso SaaS: não foi possível criar companies.{column}: {exc}")

    for column in ["notified_at", "webhook_payload"]:
        try:
            if not _column_exists("billing_events", column):
                execute_db(f"ALTER TABLE billing_events ADD COLUMN {column} TEXT")
        except Exception as exc:
            print(f"Aviso SaaS: não foi possível criar billing_events.{column}: {exc}")

    _SAAS_AUTOMATION_READY = True


@app.before_request
def saas_automation_bootstrap():
    if request.endpoint not in ("static", "health", "service_worker"):
        try:
            ensure_saas_automation_ready()
            auto_block_overdue_companies()
        except Exception as exc:
            print("Aviso automação SaaS:", exc)


def auto_block_overdue_companies() -> int:
    """Bloqueia empresas vencidas automaticamente, exceto super admin/Pet & Gatô interno."""
    today = today_str()
    changed = 0
    rows = query_db("SELECT * FROM companies")
    for company in rows:
        cid = _safe_get(company, "id")
        slug = str(_safe_get(company, "slug", ""))
        status = str(_safe_get(company, "status", "")).upper()
        paid_until = str(_safe_get(company, "paid_until", "") or "")
        trial_until = str(_safe_get(company, "trial_until", "") or "")

        # Mantém sua empresa base liberada, se desejar administrar sem cobrança.
        if slug == "pet-gato":
            continue

        if status == "ATIVO" and paid_until and paid_until < today:
            execute_db(
                "UPDATE companies SET status='BLOQUEADO', payment_status='VENCIDO', blocked_at=? WHERE id=?",
                (now_iso(), cid),
            )
            changed += 1
        elif status == "TESTE" and trial_until and trial_until < today:
            execute_db(
                "UPDATE companies SET status='BLOQUEADO', payment_status='TESTE_EXPIRADO', blocked_at=? WHERE id=?",
                (now_iso(), cid),
            )
            changed += 1
    return changed


def activate_company_payment(company_id: int, plan_key: str, payment_id: str, event_id: Optional[int] = None) -> None:
    """Libera empresa por 30 dias após pagamento aprovado."""
    paid_until = (date.today() + timedelta(days=30)).strftime("%Y-%m-%d")
    next_billing = (date.today() + timedelta(days=27)).strftime("%Y-%m-%d")
    execute_db(
        """
        UPDATE companies
        SET plan = ?, status = 'ATIVO', payment_status = 'PAGO', paid_until = ?,
            last_payment_at = ?, next_billing_at = ?, blocked_at = NULL
        WHERE id = ?
        """,
        (plan_key.upper(), paid_until, now_iso(), next_billing, company_id),
    )
    if event_id:
        execute_db(
            """
            UPDATE billing_events
            SET status = 'PAGO', mp_payment_id = ?, paid_at = ?
            WHERE id = ?
            """,
            (str(payment_id), now_iso(), event_id),
        )
    try:
        send_billing_whatsapp_notice(company_id, f"✅ Pagamento aprovado! Seu acesso ao PontoFácil Pro foi liberado até {paid_until}.")
    except Exception as exc:
        print("Aviso WhatsApp pagamento aprovado:", exc)


def send_billing_whatsapp_notice(company_id: int, message: str) -> Optional[str]:
    company = query_db("SELECT * FROM companies WHERE id = ?", (company_id,), one=True)
    phone = _safe_get(company, "phone", "")
    if not phone:
        return None
    link = build_whatsapp_link(phone, message)
    try:
        execute_db(
            "INSERT INTO system_logs (user_name, action, details, created_at) VALUES (?, ?, ?, ?)",
            ("Sistema", "WHATSAPP_COBRANCA", f"Empresa {company_id}: {message}", now_iso()),
        )
    except Exception:
        pass
    return link


def billing_reminder_message(company) -> str:
    name = _safe_get(company, "name", "Cliente")
    plan = plan_label(_safe_get(company, "plan", "BASICO")) if "plan_label" in globals() else _safe_get(company, "plan", "BASICO")
    paid_until = _safe_get(company, "paid_until", "-") or "-"
    return (
        f"Olá, {name}!%0A%0A"
        f"Sua assinatura do PontoFácil Pro está pendente ou próxima do vencimento.%0A"
        f"Plano: {plan}%0A"
        f"Pago até: {paid_until}%0A%0A"
        f"Acesse o painel e clique em Assinatura para regularizar.%0A"
        f"{app_public_url()}/assinatura"
    )


@app.route("/financeiro-saas")
@admin_required
def financeiro_saas():
    if not is_super_admin():
        flash("Acesso restrito ao administrador master.", "danger")
        return redirect(url_for("dashboard"))
    ensure_saas_automation_ready()
    rows = query_db("SELECT * FROM companies ORDER BY id ASC")
    companies = []
    total_mrr = 0.0
    for c in rows:
        row = dict(c)
        plan = str(row.get("plan") or "BASICO").upper()
        row["price"] = plan_price(plan) if "plan_price" in globals() else 0
        row["access_ok"] = company_access_allowed(row)
        row["billing_whatsapp"] = build_whatsapp_link(row.get("phone", ""), billing_reminder_message(row))
        if row.get("status") == "ATIVO":
            total_mrr += float(row["price"])
        companies.append(row)
    events = query_db("SELECT * FROM billing_events ORDER BY created_at DESC, id DESC LIMIT 100")
    return render_template("financeiro_saas.html", companies=companies, events=events, total_mrr=total_mrr)


@app.route("/financeiro-saas/verificar-pagamentos", methods=["POST"])
@admin_required
def financeiro_verificar_pagamentos():
    if not is_super_admin():
        flash("Acesso restrito.", "danger")
        return redirect(url_for("dashboard"))
    ensure_saas_automation_ready()
    pending = query_db("SELECT * FROM billing_events WHERE status IN ('AGUARDANDO_PAGAMENTO','PENDENTE') ORDER BY id DESC LIMIT 50")
    checked = 0
    approved = 0
    for event in pending:
        payment_id = _safe_get(event, "mp_payment_id", "")
        if not payment_id:
            continue
        try:
            payment = fetch_mercadopago_payment(str(payment_id))
            checked += 1
            if payment.get("status") == "approved":
                ref = parse_external_reference(payment.get("external_reference", ""))
                activate_company_payment(int(ref.get("company", 0)), ref.get("plan", "BASICO"), str(payment_id), int(_safe_get(event, "id")))
                approved += 1
        except Exception as exc:
            print("Erro verificação manual pagamento:", exc)
    flash(f"Verificação concluída. Consultados: {checked}. Aprovados: {approved}.", "success")
    return redirect(url_for("financeiro_saas"))


@app.route("/saas/status")
def saas_status():
    return {"status": "ok", "system": "PontoFácil Pro", "time": now_iso()}, 200

# =========================================================
# Fim Automação SaaS
# =========================================================
'''

if '# Automação SaaS - cobrança, bloqueio, liberação e avisos' not in app:
    # Se já existir activate_company_payment antigo, renomeia para não dar rota duplicada/função duplicada não é problema, mas última definição vence.
    marker = "# Inicializa as tabelas tanto no Render/PostgreSQL quanto no Windows/SQLite."
    if marker in app:
        app = app.replace(marker, block + "\n\n" + marker)
    else:
        app += block

# 4) Acrescenta menu Financeiro SaaS no base.html, se existir.
TEMPLATES.mkdir(exist_ok=True)
base = TEMPLATES / 'base.html'
if base.exists():
    html = base.read_text(encoding='utf-8')
    if 'financeiro_saas' not in html:
        html = html.replace(
            '<a href="{{ url_for(\'notificacoes\') }}">Notificações</a>',
            '<a href="{{ url_for(\'notificacoes\') }}">Notificações</a>\n                {% if session.get(\'role\') in [\'super_admin\', \'master\'] or session.get(\'user_name\') == \'Administrador\' %}\n                <a href="{{ url_for(\'financeiro_saas\') }}">Financeiro SaaS</a>\n                {% endif %}'
        )
        base.write_text(html, encoding='utf-8')

# 5) Template financeiro SaaS.
(TEMPLATES / 'financeiro_saas.html').write_text(r'''{% extends "base.html" %}
{% block content %}
<div class="page-header">
  <div>
    <h1>Financeiro SaaS</h1>
    <p>Automação de cobrança, status de clientes, bloqueios e WhatsApp de renovação.</p>
  </div>
</div>

<section class="card">
  <h2>Resumo comercial</h2>
  <div class="grid grid-3">
    <div class="metric-card"><span>MRR estimado</span><strong>R$ {{ '%.2f'|format(total_mrr) }}</strong></div>
    <div class="metric-card"><span>Clientes</span><strong>{{ companies|length }}</strong></div>
    <div class="metric-card"><span>Eventos financeiros</span><strong>{{ events|length }}</strong></div>
  </div>
  <form method="post" action="{{ url_for('financeiro_verificar_pagamentos') }}">
    <button class="btn primary" type="submit">Verificar pagamentos pendentes</button>
  </form>
</section>

<section class="card">
  <h2>Clientes e assinaturas</h2>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Empresa</th><th>Plano</th><th>Valor</th><th>Status</th><th>Pago até</th><th>Acesso</th><th>WhatsApp</th>
        </tr>
      </thead>
      <tbody>
      {% for c in companies %}
        <tr>
          <td><strong>{{ c.name }}</strong><br><small>{{ c.slug }}</small></td>
          <td>{{ c.plan }}</td>
          <td>R$ {{ '%.2f'|format(c.price) }}</td>
          <td>{{ c.status }} / {{ c.payment_status or '-' }}</td>
          <td>{{ c.paid_until or '-' }}</td>
          <td>{% if c.access_ok %}<span class="pill success">Liberado</span>{% else %}<span class="pill danger">Bloqueado</span>{% endif %}</td>
          <td>{% if c.billing_whatsapp %}<a class="btn small" target="_blank" href="{{ c.billing_whatsapp }}">Cobrar</a>{% else %}-{% endif %}</td>
        </tr>
      {% else %}
        <tr><td colspan="7">Nenhuma empresa cadastrada.</td></tr>
      {% endfor %}
      </tbody>
    </table>
  </div>
</section>

<section class="card">
  <h2>Últimos eventos financeiros</h2>
  <div class="table-wrap">
    <table>
      <thead><tr><th>Data</th><th>Empresa</th><th>Tipo</th><th>Plano</th><th>Valor</th><th>Status</th><th>Pagamento</th></tr></thead>
      <tbody>
      {% for e in events %}
        <tr>
          <td>{{ e.created_at }}</td><td>{{ e.company_id }}</td><td>{{ e.event_type }}</td><td>{{ e.plan or '-' }}</td><td>R$ {{ e.amount }}</td><td>{{ e.status }}</td><td>{{ e.mp_payment_id or '-' }}</td>
        </tr>
      {% else %}
        <tr><td colspan="7">Nenhum evento financeiro.</td></tr>
      {% endfor %}
      </tbody>
    </table>
  </div>
</section>
{% endblock %}
''', encoding='utf-8')

# 6) server.py robusto
server = BASE / 'server.py'
server.write_text("""import os\nimport sys\n\nsys.path.insert(0, os.path.dirname(__file__))\n\nfrom app import app\n\nif __name__ == \"__main__\":\n    port = int(os.environ.get(\"PORT\", 8080))\n    app.run(host=\"0.0.0.0\", port=port)\n""", encoding='utf-8')

APP.write_text(app, encoding='utf-8')
print('OK - Automação SaaS aplicada com sucesso.')
print('Arquivos alterados: app.py, server.py, templates/financeiro_saas.html')
