import os
import logging
from pathlib import Path
from functools import wraps

from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import check_password_hash

from database import init_db, query_db, execute_db, now_iso
from modules.clientes import clientes_bp
from modules.crm import crm_bp
from modules.fidelidade import fidelidade_bp
from modules.pets import pets_bp
from modules.agenda import agenda_bp
from modules.banho_tosa import banho_tosa_bp
from modules.funcionarios import funcionarios_bp
from modules.jornadas import jornadas_bp
from modules.ponto import ponto_bp
from modules.resumo_ponto import resumo_ponto_bp
from modules.rh import rh_bp
from modules.financeiro import financeiro_bp
from modules.estoque import estoque_bp
from modules.motorista import motorista_bp
from modules.configuracoes import configuracoes_bp
from modules.relatorios import relatorios_bp
from services.dashboard_service import obter_dashboard
from services.global_service import buscar_global, obter_notificacoes
from services.event_subscribers import register_default_subscribers
from core.logging_config import configure_logging


BASE_DIR = Path(__file__).resolve().parent
configure_logging(BASE_DIR)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)

IS_PRODUCTION = os.environ.get("APP_ENV", "development").lower() == "production"
secret_key = os.environ.get("SECRET_KEY")
if IS_PRODUCTION and not secret_key:
    raise RuntimeError("SECRET_KEY é obrigatória em produção.")

app.config.update(
    SECRET_KEY=secret_key or "petegato-development-only",
    MAX_CONTENT_LENGTH=int(os.environ.get("MAX_CONTENT_LENGTH", 16 * 1024 * 1024)),
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=IS_PRODUCTION,
    PERMANENT_SESSION_LIFETIME=int(os.environ.get("SESSION_LIFETIME_SECONDS", "28800")),
)

init_db()
register_default_subscribers()

app.register_blueprint(clientes_bp)
app.register_blueprint(crm_bp)
app.register_blueprint(fidelidade_bp)
app.register_blueprint(pets_bp)
app.register_blueprint(agenda_bp)
app.register_blueprint(banho_tosa_bp)
app.register_blueprint(funcionarios_bp)
app.register_blueprint(jornadas_bp)
app.register_blueprint(ponto_bp)
app.register_blueprint(resumo_ponto_bp)
app.register_blueprint(rh_bp)
app.register_blueprint(financeiro_bp)
app.register_blueprint(estoque_bp)
app.register_blueprint(motorista_bp)
app.register_blueprint(configuracoes_bp)
app.register_blueprint(relatorios_bp)


PUBLIC_ENDPOINTS = {"login", "health", "readiness", "static"}


@app.before_request
def proteger_rotas_globalmente():
    """Garante autenticação inclusive nos blueprints antigos."""
    endpoint = request.endpoint or ""
    if endpoint in PUBLIC_ENDPOINTS or endpoint.startswith("static"):
        return None
    if not session.get("user_id"):
        if request.path.startswith("/api/"):
            return jsonify({"erro": "autenticacao_necessaria"}), 401
        return redirect(url_for("login", next=request.full_path if request.query_string else request.path))
    session.permanent = True
    return None


@app.after_request
def cabecalhos_seguranca(response):
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
    if IS_PRODUCTION:
        response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
    return response


@app.route("/health")
def health():
    return jsonify({"status": "ok", "app": "petegato-business"}), 200


@app.route("/readiness")
def readiness():
    try:
        query_db("SELECT 1", one=True)
        return jsonify({"status": "ready", "database": "ok"}), 200
    except Exception:
        logger.exception("Falha no readiness check")
        return jsonify({"status": "not_ready", "database": "error"}), 503


def login_required(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("login"))
        return func(*args, **kwargs)
    return wrapper


@app.route("/")
def index():
    if session.get("user_id"):
        return redirect(url_for("dashboard"))
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        user = query_db(
            "SELECT * FROM users WHERE username = ? AND active = 1",
            (username,),
            one=True,
        )

        if user and check_password_hash(user["password_hash"], password):
            session["user_id"] = user["id"]
            session["user_name"] = user["name"]
            session["role"] = user["role"]
            flash("Login realizado com sucesso.", "success")
            next_url = request.args.get("next", "")
            if next_url.startswith("/") and not next_url.startswith("//"):
                return redirect(next_url)
            return redirect(url_for("dashboard"))

        flash("Usuário ou senha inválidos.", "danger")

    return render_template("login.html")


@app.route("/dashboard")
@login_required
def dashboard():
    dados = obter_dashboard()
    return render_template("dashboard.html", **dados)


@app.route("/api/dashboard")
@login_required
def dashboard_api():
    force_refresh = request.args.get("refresh") == "1"
    dados = obter_dashboard(force_refresh=force_refresh)

    def json_ready(value):
        if isinstance(value, dict):
            return {key: json_ready(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [json_ready(item) for item in value]
        if hasattr(value, "keys") and not isinstance(value, str):
            return {key: json_ready(value[key]) for key in value.keys()}
        return value

    return jsonify(json_ready(dados))


@app.route("/api/busca-global")
@login_required
def api_busca_global():
    termo = request.args.get("q", "")
    return jsonify({"resultados": buscar_global(termo)})


@app.route("/api/notificacoes")
@login_required
def api_notificacoes():
    itens = obter_notificacoes()
    return jsonify({"total": len(itens), "itens": itens})

@app.route("/logout")
def logout():
    session.clear()
    flash("Sessão encerrada.", "info")
    return redirect(url_for("login"))


@app.errorhandler(404)
def not_found(error):
    return render_template("error.html", code=404, message="Página não encontrada."), 404


@app.errorhandler(500)
def internal_error(error):
    logger.exception("Erro interno não tratado: %s", error)
    return render_template("error.html", code=500, message="Ocorreu um erro interno."), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)