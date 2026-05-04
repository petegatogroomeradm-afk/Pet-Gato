from pathlib import Path
import shutil

ROOT = Path.cwd()
APP = ROOT / 'app.py'
TEMPLATES = ROOT / 'templates'
REQ = ROOT / 'requirements.txt'

if not APP.exists():
    raise SystemExit('ERRO: app.py não encontrado. Rode este script dentro da pasta do projeto.')

backup = ROOT / 'backup_antes_cobranca_mercadopago'
backup.mkdir(exist_ok=True)
shutil.copy2(APP, backup / 'app.py')
if (TEMPLATES / 'subscription.html').exists():
    shutil.copy2(TEMPLATES / 'subscription.html', backup / 'subscription.html')
if REQ.exists():
    shutil.copy2(REQ, backup / 'requirements.txt')

app = APP.read_text(encoding='utf-8')

if 'import requests' not in app:
    app = app.replace('import psycopg2\n', 'import psycopg2\nimport requests\n')

if 'SYSTEM_COMMERCIAL_NAME' not in app:
    marker = 'PLAN_LIMITS = {'
    insert = '''SYSTEM_COMMERCIAL_NAME = "PontoFácil Pro"
SYSTEM_COMMERCIAL_SUBTITLE = "Controle de ponto online com banco de horas, alertas e relatórios."

PLAN_PRICES = {
    "BASICO": 29.00,
    "PRO": 59.00,
    "PREMIUM": 99.00,
}

'''
    app = app.replace(marker, insert + marker, 1)

if '"/assinatura",' not in app:
    app = app.replace('"/notificacoes",\n    "/logout",', '"/notificacoes",\n    "/assinatura",\n    "/logout",')

BILLING_BLOCK = r'''

# =========================================================
# Cobrança automática Mercado Pago - PontoFácil Pro
# =========================================================
_BILLING_DB_READY = False


def mp_access_token() -> str:
    return os.getenv("MP_ACCESS_TOKEN", "").strip()


def app_public_url() -> str:
    env_url = os.getenv("APP_PUBLIC_URL", "").strip().rstrip("/")
    if env_url:
        return env_url
    try:
        return request.url_root.rstrip("/")
    except Exception:
        return ""


def plan_price(plan_key: str) -> float:
    plan_key = (plan_key or "BASICO").upper()
    return float(PLAN_PRICES.get(plan_key, PLAN_PRICES["BASICO"]))


def plan_label(plan_key: str) -> str:
    plan_key = (plan_key or "BASICO").upper()
    data = PLAN_LIMITS.get(plan_key, PLAN_LIMITS["BASICO"])
    return data["name"]


def ensure_billing_tables() -> None:
    """Adiciona campos financeiros sem apagar dados existentes."""
    ensure_multiempresa_ready()

    for column in ["paid_until", "last_payment_at", "payment_status"]:
        try:
            if not _column_exists("companies", column):
                execute_db(f"ALTER TABLE companies ADD COLUMN {column} TEXT")
        except Exception as exc:
            print(f"Aviso cobrança: não foi possível criar companies.{column}: {exc}")

    for column in ["external_reference", "mp_preference_id", "mp_payment_id", "checkout_url", "plan", "paid_at"]:
        try:
            if not _column_exists("billing_events", column):
                execute_db(f"ALTER TABLE billing_events ADD COLUMN {column} TEXT")
        except Exception as exc:
            print(f"Aviso cobrança: não foi possível criar billing_events.{column}: {exc}")


def ensure_billing_ready() -> None:
    global _BILLING_DB_READY
    if not _BILLING_DB_READY:
        ensure_billing_tables()
        _BILLING_DB_READY = True


@app.before_request
def billing_bootstrap():
    if request.endpoint not in ("static", "health", "service_worker"):
        try:
            ensure_billing_ready()
        except Exception as exc:
            print("Erro ao inicializar cobrança:", exc)


def create_billing_event(company_id: int, plan_key: str) -> int:
    amount = f"{plan_price(plan_key):.2f}"
    description = f"Assinatura {SYSTEM_COMMERCIAL_NAME} - Plano {plan_label(plan_key)}"
    event_id = execute_db(
        """
        INSERT INTO billing_events (company_id, event_type, description, amount, status, created_at, plan)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (company_id, "ASSINATURA", description, amount, "AGUARDANDO_PAGAMENTO", now_iso(), plan_key.upper()),
    )
    external_reference = f"company:{company_id}:plan:{plan_key.upper()}:event:{event_id}"
    execute_db("UPDATE billing_events SET external_reference = ? WHERE id = ?", (external_reference, event_id))
    return int(event_id)


def create_mercadopago_preference(company: Any, plan_key: str, event_id: int) -> Dict[str, Any]:
    token = mp_access_token()
    if not token:
        raise RuntimeError("MP_ACCESS_TOKEN não configurado no Render.")
    base_url = app_public_url()
    if not base_url:
        raise RuntimeError("APP_PUBLIC_URL não configurado no Render.")

    event = query_db("SELECT * FROM billing_events WHERE id = ?", (event_id,), one=True)
    amount = plan_price(plan_key)
    payer_email = (company.get("email") if hasattr(company, "get") else company["email"]) or "cliente@email.com"
    payer_name = (company.get("responsible_name") if hasattr(company, "get") else company["responsible_name"]) or "Cliente"
    external_reference = event["external_reference"]

    payload = {
        "items": [{
            "title": f"{SYSTEM_COMMERCIAL_NAME} - Plano {plan_label(plan_key)}",
            "quantity": 1,
            "currency_id": "BRL",
            "unit_price": amount,
            "description": "Assinatura mensal do sistema de ponto online",
        }],
        "payer": {"email": payer_email, "name": payer_name},
        "external_reference": external_reference,
        "notification_url": f"{base_url}/webhook/mercadopago",
        "back_urls": {
            "success": f"{base_url}/pagamento/sucesso",
            "failure": f"{base_url}/pagamento/falha",
            "pending": f"{base_url}/pagamento/pendente",
        },
        "auto_return": "approved",
        "statement_descriptor": "PONTOFACILPRO",
    }

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    response = requests.post("https://api.mercadopago.com/checkout/preferences", json=payload, headers=headers, timeout=30)
    data = response.json()
    if response.status_code >= 400:
        raise RuntimeError(f"Mercado Pago retornou erro: {data}")
    return data


def fetch_mercadopago_payment(payment_id: str) -> Dict[str, Any]:
    token = mp_access_token()
    if not token:
        raise RuntimeError("MP_ACCESS_TOKEN não configurado.")
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(f"https://api.mercadopago.com/v1/payments/{payment_id}", headers=headers, timeout=30)
    data = response.json()
    if response.status_code >= 400:
        raise RuntimeError(f"Erro ao consultar pagamento Mercado Pago: {data}")
    return data


def parse_external_reference(reference: str) -> Dict[str, str]:
    parts = (reference or "").split(":")
    result = {}
    for i in range(0, len(parts) - 1, 2):
        result[parts[i]] = parts[i + 1]
    return result


def activate_company_payment(company_id: int, plan_key: str, payment_id: str, event_id: Optional[int] = None) -> None:
    paid_until = (date.today() + timedelta(days=30)).strftime("%Y-%m-%d")
    execute_db(
        """
        UPDATE companies
        SET plan = ?, status = 'ATIVO', payment_status = 'PAGO', paid_until = ?, last_payment_at = ?
        WHERE id = ?
        """,
        (plan_key.upper(), paid_until, now_iso(), company_id),
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


def block_overdue_companies() -> None:
    ensure_billing_ready()
    today = today_str()
    companies = query_db("SELECT * FROM companies WHERE status = 'ATIVO'")
    for company in companies:
        paid_until = company.get("paid_until") if hasattr(company, "get") else company["paid_until"]
        trial_until = company.get("trial_until") if hasattr(company, "get") else company["trial_until"]
        if paid_until and paid_until < today:
            execute_db("UPDATE companies SET status = 'BLOQUEADO', payment_status = 'VENCIDO' WHERE id = ?", (company["id"],))
        elif not paid_until and trial_until and trial_until < today:
            execute_db("UPDATE companies SET status = 'BLOQUEADO', payment_status = 'TESTE_EXPIRADO' WHERE id = ?", (company["id"],))


@app.route("/assinatura/pagar/<plan_key>", methods=["POST"])
@admin_required
def billing_create_payment(plan_key: str):
    ensure_billing_ready()
    company = current_company()
    if not company:
        flash("Nenhuma empresa ativa para gerar cobrança.", "danger")
        return redirect(url_for("subscription_page"))

    plan_key = (plan_key or "BASICO").upper()
    if plan_key not in PLAN_LIMITS:
        flash("Plano inválido.", "danger")
        return redirect(url_for("subscription_page"))

    try:
        event_id = create_billing_event(company["id"], plan_key)
        preference = create_mercadopago_preference(company, plan_key, event_id)
        checkout_url = preference.get("init_point") or preference.get("sandbox_init_point")
        execute_db(
            """
            UPDATE billing_events
            SET mp_preference_id = ?, checkout_url = ?
            WHERE id = ?
            """,
            (preference.get("id", ""), checkout_url or "", event_id),
        )
        if checkout_url:
            return redirect(checkout_url)
        flash("Cobrança criada, mas o Mercado Pago não retornou link de checkout.", "danger")
    except Exception as exc:
        flash(f"Erro ao gerar cobrança: {exc}", "danger")
    return redirect(url_for("subscription_page"))


@app.route("/webhook/mercadopago", methods=["GET", "POST"])
def mercadopago_webhook():
    ensure_billing_ready()
    payload = request.get_json(silent=True) or {}
    topic = request.args.get("topic") or payload.get("type") or payload.get("topic")
    payment_id = request.args.get("id")

    if not payment_id and isinstance(payload.get("data"), dict):
        payment_id = payload["data"].get("id")
    if topic and "payment" not in str(topic):
        return {"status": "ignored", "topic": topic}, 200
    if not payment_id:
        return {"status": "ignored", "reason": "missing payment id"}, 200

    try:
        payment = fetch_mercadopago_payment(str(payment_id))
        status = payment.get("status", "")
        external_reference = payment.get("external_reference", "")
        ref = parse_external_reference(external_reference)
        company_id = int(ref.get("company", "0") or 0)
        plan_key = ref.get("plan", "BASICO")
        event_id = int(ref.get("event", "0") or 0)
        if status == "approved" and company_id:
            activate_company_payment(company_id, plan_key, str(payment_id), event_id or None)
        elif event_id:
            execute_db("UPDATE billing_events SET status = ?, mp_payment_id = ? WHERE id = ?", (status.upper() or "PENDENTE", str(payment_id), event_id))
        return {"status": "ok"}, 200
    except Exception as exc:
        print("Erro webhook Mercado Pago:", exc)
        return {"status": "error", "message": str(exc)}, 200


@app.route("/pagamento/sucesso")
def payment_success():
    flash("Pagamento recebido ou em processamento. A ativação automática ocorre após confirmação do Mercado Pago.", "success")
    return redirect(url_for("subscription_page"))


@app.route("/pagamento/falha")
def payment_failure():
    flash("Pagamento não aprovado. Tente novamente ou escolha outro meio de pagamento.", "danger")
    return redirect(url_for("subscription_page"))


@app.route("/pagamento/pendente")
def payment_pending():
    flash("Pagamento pendente. Assim que o Mercado Pago confirmar, o plano será ativado automaticamente.", "warning")
    return redirect(url_for("subscription_page"))

'''

if '# Cobrança automática Mercado Pago - PontoFácil Pro' not in app:
    marker = '# Inicializa as tabelas tanto no Render/PostgreSQL quanto no Windows/SQLite.'
    if marker not in app:
        raise SystemExit('ERRO: marcador de inicialização não encontrado no app.py')
    app = app.replace(marker, BILLING_BLOCK + '\n' + marker, 1)

TEMPLATES.mkdir(exist_ok=True)
(TEMPLATES / 'subscription.html').write_text(r'''{% extends "base.html" %}
{% block content %}
<div class="page-header">
  <div>
    <h1>PontoFácil Pro</h1>
    <p>Assinatura, cobrança automática via Mercado Pago, Pix e cartão.</p>
  </div>
</div>

<section class="card">
  {% if company %}
    <h2>{{ company.name }}</h2>
    <p>Plano atual: <strong>{{ company.plan }}</strong></p>
    <p>Status: <strong>{{ company.status }}</strong></p>
    <p>Pagamento: <strong>{{ company.payment_status or '-' }}</strong></p>
    <p>Pago até: <strong>{{ company.paid_until or '-' }}</strong></p>
    <p>Teste grátis até: {{ company.trial_until or '-' }}</p>
  {% else %}
    <p>Nenhuma empresa ativa na sessão.</p>
  {% endif %}
</section>

<section class="card">
  <h2>Escolher plano</h2>
  <div class="grid grid-3">
    {% for key, p in PLAN_LIMITS.items() %}
      <div class="card" style="box-shadow:none; border:1px solid rgba(139,92,246,.18);">
        <h3>{{ p.name }}</h3>
        <h1>{{ p.price }}</h1>
        <p>Limite: {% if p.employees > 9000 %}Ilimitado{% else %}{{ p.employees }} funcionários{% endif %}</p>
        <form method="post" action="{{ url_for('billing_create_payment', plan_key=key) }}">
          <button class="btn primary" type="submit">Pagar com Pix ou cartão</button>
        </form>
      </div>
    {% endfor %}
  </div>
  <p class="muted">O pagamento é processado pelo Mercado Pago. Após aprovação, a empresa é ativada automaticamente por 30 dias.</p>
</section>

<section class="card">
  <h2>Histórico financeiro</h2>
  <div class="table-wrap">
    <table>
      <thead><tr><th>Data</th><th>Tipo</th><th>Descrição</th><th>Valor</th><th>Status</th><th>Checkout</th></tr></thead>
      <tbody>
      {% for e in events %}
        <tr>
          <td>{{ e.created_at }}</td><td>{{ e.event_type }}</td><td>{{ e.description }}</td><td>R$ {{ e.amount }}</td><td>{{ e.status }}</td>
          <td>{% if e.checkout_url %}<a class="btn small" target="_blank" href="{{ e.checkout_url }}">Abrir</a>{% else %}-{% endif %}</td>
        </tr>
      {% else %}<tr><td colspan="6">Nenhum evento financeiro.</td></tr>{% endfor %}
      </tbody>
    </table>
  </div>
</section>
{% endblock %}
''', encoding='utf-8')

(TEMPLATES / 'plans.html').write_text(r'''{% extends "base.html" %}
{% block content %}
<div class="page-header"><div><h1>PontoFácil Pro</h1><p>Planos comerciais para controle de ponto online.</p></div></div>
<div class="grid grid-3">
  {% for key, p in PLAN_LIMITS.items() %}
  <section class="card">
    <h2>{{ p.name }}</h2><h1>{{ p.price }}</h1>
    <p>Limite: {% if p.employees > 9000 %}Ilimitado{% else %}{{ p.employees }} funcionários{% endif %}</p>
    <a class="btn primary" href="{{ url_for('subscription_page') }}">Assinar</a>
  </section>
  {% endfor %}
</div>
{% if company %}<section class="card"><h2>Empresa atual</h2><p>{{ company.name }} — Plano {{ company.plan }}</p></section>{% endif %}
{% endblock %}
''', encoding='utf-8')

if REQ.exists():
    req = REQ.read_text(encoding='utf-8')
    if 'requests' not in req.lower():
        req = req.rstrip() + '\nrequests\n'
    REQ.write_text(req, encoding='utf-8')
else:
    REQ.write_text('Flask\nrequests\npsycopg2-binary\n', encoding='utf-8')

APP.write_text(app, encoding='utf-8')

print('✅ Cobrança Mercado Pago aplicada com sucesso.')
print('Backup salvo em:', backup)
print('Variáveis necessárias no Render: MP_ACCESS_TOKEN e APP_PUBLIC_URL')
print('Agora rode: git add . && git commit -m "Adiciona cobranca Mercado Pago" && git push')
