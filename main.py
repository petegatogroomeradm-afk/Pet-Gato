import os
import logging
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from functools import wraps

from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify, g, send_file
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import check_password_hash, generate_password_hash

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
from modules.portal_cliente import portal_cliente_bp
from modules.pdv import pdv_bp
from modules.comunicacao import comunicacao_bp
from modules.auditoria import auditoria_bp
from modules.comissoes import comissoes_bp
from services.dashboard_service import obter_dashboard
from services.global_service import buscar_global, obter_notificacoes
from services.event_subscribers import register_default_subscribers
from services.integration_service import obter_integridade_integracoes, reprocessar_atendimento, reprocessar_pendencias
from core.logging_config import configure_logging
from core.permissions import has_permission, module_for_request, deny_access, ROLE_LABELS, is_superadmin
from core.version import APP_VERSION
from services.auditoria_service import registrar_auditoria


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
app.register_blueprint(portal_cliente_bp)
app.register_blueprint(pdv_bp)
app.register_blueprint(comunicacao_bp)
app.register_blueprint(auditoria_bp)
app.register_blueprint(comissoes_bp)


@app.context_processor
def inject_app_metadata():
    """Disponibiliza a versão atual para todas as telas sem duplicação."""
    return {"app_version": APP_VERSION}


PUBLIC_ENDPOINTS = {"login", "health", "readiness", "static", "alterar_senha_primeiro_acesso"}


@app.before_request
def proteger_rotas_globalmente():
    """Garante autenticação inclusive nos blueprints antigos."""
    endpoint = request.endpoint or ""
    if endpoint in PUBLIC_ENDPOINTS or endpoint.startswith("static") or endpoint.startswith("portal_cliente."):
        return None
    if not session.get("user_id"):
        if request.path.startswith("/api/"):
            return jsonify({"erro": "autenticacao_necessaria"}), 401
        return redirect(url_for("login", next=request.full_path if request.query_string else request.path))
    session.permanent = True
    if session.get("must_change_password") and endpoint not in {"alterar_senha_primeiro_acesso", "logout", "static"}:
        return redirect(url_for("alterar_senha_primeiro_acesso"))

    required_module = module_for_request()
    if required_module and not has_permission(required_module):
        return deny_access()
    return None


@app.after_request
def cabecalhos_seguranca(response):
    if (request.method in {"POST", "PUT", "PATCH", "DELETE"}
            and response.status_code < 400
            and session.get("user_id")
            and not getattr(g, "audit_recorded", False)
            and request.endpoint not in {"login", "logout", "static"}):
        try:
            registrar_auditoria(session.get("user_name"), "alteracao_sistema", request.blueprint or "sistema", None, f"{request.method} {request.path}")
        except Exception:
            logger.exception("Falha ao registrar auditoria automática")
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
    if IS_PRODUCTION:
        response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
    return response


@app.route("/health")
def health():
    return jsonify({"status": "ok", "app": "petegato-business", "version": APP_VERSION}), 200


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
        username = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "")
        ip = request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",")[0].strip()
        user_agent = request.headers.get("User-Agent", "")[:500]
        user = query_db("SELECT * FROM users WHERE username = ?", (username,), one=True)
        now = datetime.now()
        locked = False
        if user and user["locked_until"]:
            try:
                locked = datetime.strptime(user["locked_until"], "%Y-%m-%d %H:%M:%S") > now
            except (TypeError, ValueError):
                locked = False
        if user and not user["active"]:
            execute_db("INSERT INTO user_access_logs (user_id,username,success,ip_address,user_agent,details,created_at) VALUES (?,?,?,?,?,?,?)", (user["id"],username,0,ip,user_agent,"Usuário inativo",now_iso()))
            flash("Este usuário está inativo. Procure o administrador.", "danger")
        elif user and locked:
            execute_db("INSERT INTO user_access_logs (user_id,username,success,ip_address,user_agent,details,created_at) VALUES (?,?,?,?,?,?,?)", (user["id"],username,0,ip,user_agent,"Conta bloqueada",now_iso()))
            flash(f"Conta temporariamente bloqueada até {user['locked_until']}.", "danger")
        elif user and check_password_hash(user["password_hash"], password):
            execute_db("UPDATE users SET failed_attempts=0,locked_until=NULL,last_login_at=?,last_login_ip=?,last_login_user_agent=?,login_count=COALESCE(login_count,0)+1 WHERE id=?", (now_iso(),ip,user_agent,user["id"]))
            execute_db("INSERT INTO user_access_logs (user_id,username,success,ip_address,user_agent,details,created_at) VALUES (?,?,?,?,?,?,?)", (user["id"],username,1,ip,user_agent,"Login realizado",now_iso()))
            session.clear(); session["user_id"]=user["id"]; session["user_name"]=user["name"]; session["role"]=user["role"]
            session["role_label"] = ROLE_LABELS.get(user["role"], user["role"].title())
            session["must_change_password"] = bool(user["must_change_password"])
            if session["must_change_password"]:
                flash("Por segurança, crie uma nova senha antes de continuar.", "warning")
                return redirect(url_for("alterar_senha_primeiro_acesso"))
            flash("Login realizado com sucesso.", "success")
            next_url=request.args.get("next","")
            return redirect(next_url if next_url.startswith("/") and not next_url.startswith("//") else url_for("dashboard"))
        else:
            attempts = (user["failed_attempts"] or 0) + 1 if user else 1
            details = f"Senha inválida - tentativa {attempts}" if user else "Usuário inexistente"
            if user:
                locked_until = (now + timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S") if attempts >= 5 else None
                execute_db("UPDATE users SET failed_attempts=?,locked_until=? WHERE id=?", (0 if locked_until else attempts,locked_until,user["id"]))
            execute_db("INSERT INTO user_access_logs (user_id,username,success,ip_address,user_agent,details,created_at) VALUES (?,?,?,?,?,?,?)", (user["id"] if user else None,username,0,ip,user_agent,details,now_iso()))
            flash("Usuário ou senha inválidos." if attempts < 5 else "Conta bloqueada por 30 minutos após 5 tentativas.", "danger")
    return render_template("login.html")


@app.route("/primeiro-acesso/senha", methods=["GET", "POST"])
def alterar_senha_primeiro_acesso():
    if not session.get("user_id"):
        return redirect(url_for("login"))
    if request.method == "POST":
        password=request.form.get("password",""); confirm=request.form.get("confirm_password","")
        if len(password)<8:
            flash("A senha deve ter pelo menos 8 caracteres.","danger")
        elif password != confirm:
            flash("As senhas não conferem.","danger")
        else:
            execute_db("UPDATE users SET password_hash=?,must_change_password=0,updated_at=? WHERE id=?",(generate_password_hash(password),now_iso(),session["user_id"]))
            execute_db("INSERT INTO audit_logs (user_name,action,entity_type,entity_id,details,created_at) VALUES (?,?,?,?,?,?)",(session.get("user_name"),"senha_primeiro_acesso","user",session["user_id"],"Senha definida no primeiro acesso",now_iso()))
            session["must_change_password"]=False
            flash("Senha atualizada com sucesso.","success")
            return redirect(url_for("dashboard"))
    return render_template("primeiro_acesso_senha.html")


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


@app.route("/sistema/integracoes")
@login_required
def central_integracoes():
    dados = obter_integridade_integracoes()
    return render_template("central_integracoes.html", **dados)


@app.route("/sistema/integracoes/reprocessar/<int:atendimento_id>", methods=["POST"])
@login_required
def reprocessar_integracao(atendimento_id):
    resultado = reprocessar_atendimento(atendimento_id, session.get("user_name", "Sistema"))
    flash(resultado["mensagem"], "success" if resultado["ok"] else "danger")
    return redirect(url_for("central_integracoes"))


@app.route("/sistema/integracoes/reprocessar-pendencias", methods=["POST"])
@login_required
def reprocessar_integracoes_pendentes():
    resultado = reprocessar_pendencias(session.get("user_name", "Sistema"))
    flash(f"{resultado['processados']} atendimento(s) verificado(s) e {resultado['corrigidos']} corrigido(s).", "success")
    return redirect(url_for("central_integracoes"))


def _sqlite_database_path():
    path = BASE_DIR / "instance" / "petegato_business_v3.db"
    return path if path.exists() else None


def _backup_directory():
    path = BASE_DIR / "backups" / "database"
    path.mkdir(parents=True, exist_ok=True)
    return path


@app.route("/sistema/backup/criar", methods=["POST"])
@login_required
def criar_backup_sistema():
    if not has_permission("configuracoes"):
        return deny_access()
    db_path = _sqlite_database_path()
    if not db_path:
        flash("Backup manual local está disponível para a base SQLite. Em PostgreSQL use o backup gerenciado do provedor.", "warning")
        return redirect(url_for("central_integracoes"))
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    destino = _backup_directory() / f"petegato_business_v3_manual_{timestamp}.db"
    shutil.copy2(db_path, destino)
    registrar_auditoria(session.get("user_name"), "backup_criado", "sistema", None, destino.name)
    flash(f"Backup criado com sucesso: {destino.name}", "success")
    return redirect(url_for("central_integracoes"))


@app.route("/sistema/backup/baixar/<path:nome>")
@login_required
def baixar_backup_sistema(nome):
    if not has_permission("configuracoes"):
        return deny_access()
    seguro = Path(nome).name
    arquivo = _backup_directory() / seguro
    if not arquivo.exists() or arquivo.suffix.lower() != ".db":
        flash("Backup não encontrado.", "danger")
        return redirect(url_for("central_integracoes"))
    return send_file(arquivo, as_attachment=True, download_name=arquivo.name)


@app.route("/sistema/backup/restaurar/<path:nome>", methods=["POST"])
@login_required
def restaurar_backup_sistema(nome):
    if not is_superadmin():
        flash("Somente o Super Administrador pode restaurar o banco local.", "danger")
        return redirect(url_for("central_integracoes"))
    db_path = _sqlite_database_path()
    if not db_path:
        flash("Restauração local é permitida somente para SQLite.", "warning")
        return redirect(url_for("central_integracoes"))
    seguro = Path(nome).name
    origem = _backup_directory() / seguro
    if not origem.exists() or origem.suffix.lower() != ".db":
        flash("Arquivo de backup não encontrado.", "danger")
        return redirect(url_for("central_integracoes"))
    if request.form.get("confirmacao", "").strip().upper() != "RESTAURAR":
        flash("Confirmação inválida. Digite RESTAURAR para executar a operação.", "danger")
        return redirect(url_for("central_integracoes"))
    seguranca = _backup_directory() / f"petegato_business_v3_antes_restore_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    shutil.copy2(db_path, seguranca)
    shutil.copy2(origem, db_path)
    registrar_auditoria(session.get("user_name"), "backup_restaurado", "sistema", None, f"Origem: {origem.name}; segurança: {seguranca.name}")
    flash("Banco restaurado. Reinicie o sistema antes de continuar utilizando.", "warning")
    return redirect(url_for("central_integracoes"))

@app.route("/logout")
def logout():
    session.clear()
    flash("Sessão encerrada.", "info")
    return redirect(url_for("login"))


@app.context_processor
def inject_permissions():
    return {
        "can_access": has_permission,
        "role_labels": ROLE_LABELS,
    }


@app.errorhandler(403)
def forbidden(error):
    return render_template("error.html", code=403, message="Você não possui permissão para acessar esta área."), 403


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