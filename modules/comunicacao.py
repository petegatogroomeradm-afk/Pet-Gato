from urllib.parse import quote

from flask import Blueprint, flash, redirect, render_template, request, session, url_for

from database import execute_db, now_iso, query_db
from core.db_compat import auto_pk_sql, ensure_columns, ensure_table

comunicacao_bp = Blueprint("comunicacao", __name__, url_prefix="/comunicacao")


def _usuario():
    return session.get("user_name") or "Administrador"


def _telefone_limpo(valor):
    numero = "".join(ch for ch in (valor or "") if ch.isdigit())
    if numero and not numero.startswith("55"):
        numero = "55" + numero
    return numero


def _ensure_schema():
    """Garante a estrutura usando a camada compartilhada de banco."""
    primary_key = auto_pk_sql()
    ensure_columns("clients", {"telefone": "TEXT", "whatsapp": "TEXT"})
    ensure_table("message_templates", f"""
        id {primary_key},
        name TEXT NOT NULL,
        category TEXT DEFAULT 'Geral',
        content TEXT NOT NULL,
        active INTEGER DEFAULT 1,
        created_by TEXT,
        created_at TEXT,
        updated_at TEXT
    """)
    ensure_table("communication_logs", f"""
        id {primary_key},
        client_id INTEGER,
        pet_id INTEGER,
        template_id INTEGER,
        channel TEXT DEFAULT 'WhatsApp',
        recipient TEXT,
        subject TEXT,
        message TEXT NOT NULL,
        status TEXT DEFAULT 'Preparada',
        user_name TEXT,
        created_at TEXT
    """)


def _clientes_ativos():
    """Compatibilidade com bancos que ainda não possuem a coluna whatsapp."""
    try:
        return query_db("""
            SELECT id, nome,
                   COALESCE(NULLIF(whatsapp,''), NULLIF(telefone,''), '') AS contato
            FROM clients
            WHERE COALESCE(ativo,1)=1
            ORDER BY nome
        """)
    except Exception:
        return query_db("""
            SELECT id, nome, COALESCE(NULLIF(telefone,''), '') AS contato
            FROM clients
            WHERE COALESCE(ativo,1)=1
            ORDER BY nome
        """)


AGENDA_MESSAGES = {
    "confirmacao": {
        "subject": "Confirmação de agendamento",
        "status": "Confirmação aberta no WhatsApp",
        "message": "Olá, {cliente}! Confirmamos o agendamento de {pet} para {data} às {hora}, para o serviço de {servico}. Até lá! Pet & Gatô 🐾",
    },
    "lembrete": {
        "subject": "Lembrete de agendamento",
        "status": "Lembrete aberto no WhatsApp",
        "message": "Olá, {cliente}! Passando para lembrar que {pet} tem horário na Pet & Gatô em {data}, às {hora}, para {servico}. Esperamos vocês! 🐾",
    },
    "atraso": {
        "subject": "Aviso de atraso",
        "status": "Aviso de atraso aberto no WhatsApp",
        "message": "Olá, {cliente}! O atendimento de {pet} teve um pequeno atraso. Estamos cuidando de tudo com carinho e avisaremos assim que estiver pronto. Obrigado pela compreensão!",
    },
    "pronto": {
        "subject": "Pet pronto",
        "status": "Aviso de pet pronto aberto no WhatsApp",
        "message": "Olá, {cliente}! {pet} já está pronto e esperando por você na Pet & Gatô. 🐾",
    },
    "cancelamento": {
        "subject": "Cancelamento de agendamento",
        "status": "Cancelamento aberto no WhatsApp",
        "message": "Olá, {cliente}. O agendamento de {pet} para {data} às {hora} foi cancelado. Fale conosco para escolhermos um novo horário.",
    },
    "avaliacao": {
        "subject": "Pedido de avaliação",
        "status": "Pedido de avaliação aberto no WhatsApp",
        "message": "Olá, {cliente}! Esperamos que tenha gostado do atendimento de {pet}. Sua avaliação é muito importante para a Pet & Gatô. Conte para nós como foi! 💜",
    },
}


def preparar_whatsapp_agendamento(agendamento_id, action):
    """Monta, registra e retorna a URL do WhatsApp para um agendamento."""
    _ensure_schema()
    config = AGENDA_MESSAGES.get(action)
    if not config:
        raise ValueError("Ação de comunicação inválida.")
    item = query_db("""
        SELECT a.id, a.client_id, a.pet_id, a.data_agendamento, a.horario, a.servico,
               c.nome AS cliente_nome, COALESCE(NULLIF(c.whatsapp,''), NULLIF(c.telefone,''), '') AS contato,
               p.nome AS pet_nome
        FROM appointments a
        LEFT JOIN clients c ON c.id=a.client_id
        LEFT JOIN pets p ON p.id=a.pet_id
        WHERE a.id=?
    """, (agendamento_id,), one=True)
    if not item:
        raise ValueError("Agendamento não encontrado.")
    telefone = _telefone_limpo(item["contato"] or "")
    if not telefone:
        raise ValueError("O tutor não possui WhatsApp ou telefone cadastrado.")
    mensagem = config["message"].format(
        cliente=item["cliente_nome"] or "cliente", pet=item["pet_nome"] or "seu pet",
        data=item["data_agendamento"] or "", hora=item["horario"] or "", servico=item["servico"] or "atendimento",
    )
    execute_db("""
        INSERT INTO communication_logs
        (client_id,pet_id,channel,recipient,subject,message,status,user_name,created_at)
        VALUES(?,?,?,?,?,?,?,?,?)
    """, (item["client_id"], item["pet_id"], "WhatsApp", telefone, config["subject"], mensagem, config["status"], _usuario(), now_iso()))
    try:
        execute_db("""
            INSERT INTO crm_contact_history
            (client_id,channel,subject,message,result,user_name,created_at)
            VALUES(?,?,?,?,?,?,?)
        """, (item["client_id"], "WhatsApp", config["subject"], mensagem, config["status"], _usuario(), now_iso()))
    except Exception:
        pass
    return f"https://wa.me/{telefone}?text={quote(mensagem)}"


BANHO_MESSAGES = {
    "andamento": {
        "subject": "Atendimento em andamento",
        "status": "Aviso de atendimento em andamento aberto no WhatsApp",
        "message": "Olá, {cliente}! O atendimento de {pet} já começou e nossa equipe está cuidando dele com muito carinho. Pet & Gatô 🐾",
    },
    "atraso": AGENDA_MESSAGES["atraso"],
    "pronto": AGENDA_MESSAGES["pronto"],
    "avaliacao": AGENDA_MESSAGES["avaliacao"],
    "cancelamento": {
        "subject": "Cancelamento do atendimento",
        "status": "Cancelamento do atendimento aberto no WhatsApp",
        "message": "Olá, {cliente}. O atendimento de {pet} foi cancelado. Entre em contato conosco para mais informações ou para marcar um novo horário.",
    },
}


def preparar_whatsapp_banho(atendimento_id, action):
    """Monta, registra e retorna a URL do WhatsApp para banho e tosa."""
    _ensure_schema()
    config = BANHO_MESSAGES.get(action)
    if not config:
        raise ValueError("Ação de comunicação inválida.")
    item = query_db("""
        SELECT g.id, g.client_id, g.pet_id, g.data, g.hora_entrada, g.servico,
               c.nome AS cliente_nome, COALESCE(NULLIF(c.whatsapp,''), NULLIF(c.telefone,''), '') AS contato,
               p.nome AS pet_nome
        FROM grooming_services g
        LEFT JOIN clients c ON c.id=g.client_id
        LEFT JOIN pets p ON p.id=g.pet_id
        WHERE g.id=?
    """, (atendimento_id,), one=True)
    if not item:
        raise ValueError("Atendimento não encontrado.")
    telefone = _telefone_limpo(item["contato"] or "")
    if not telefone:
        raise ValueError("O tutor não possui WhatsApp ou telefone cadastrado.")
    mensagem = config["message"].format(
        cliente=item["cliente_nome"] or "cliente", pet=item["pet_nome"] or "seu pet",
        data=item["data"] or "", hora=item["hora_entrada"] or "", servico=item["servico"] or "atendimento",
    )
    execute_db("""
        INSERT INTO communication_logs
        (client_id,pet_id,channel,recipient,subject,message,status,user_name,created_at)
        VALUES(?,?,?,?,?,?,?,?,?)
    """, (item["client_id"], item["pet_id"], "WhatsApp", telefone, config["subject"], mensagem, config["status"], _usuario(), now_iso()))
    try:
        execute_db("""
            INSERT INTO crm_contact_history
            (client_id,channel,subject,message,result,user_name,created_at)
            VALUES(?,?,?,?,?,?,?)
        """, (item["client_id"], "WhatsApp", config["subject"], mensagem, config["status"], _usuario(), now_iso()))
    except Exception:
        pass
    return f"https://wa.me/{telefone}?text={quote(mensagem)}"


@comunicacao_bp.route("/")
def central():
    _ensure_schema()
    clientes = _clientes_ativos()
    templates = query_db(
        "SELECT * FROM message_templates WHERE COALESCE(active,1)=1 ORDER BY category,name"
    )
    historico = query_db("""
        SELECT l.*, c.nome AS cliente_nome
        FROM communication_logs l
        LEFT JOIN clients c ON c.id=l.client_id
        ORDER BY l.id DESC
        LIMIT 50
    """)
    resumo = query_db("""
        SELECT COUNT(*) AS total,
               COALESCE(SUM(CASE WHEN channel='WhatsApp' THEN 1 ELSE 0 END),0) AS whatsapp,
               COALESCE(SUM(CASE WHEN substr(created_at,1,10)=substr(?,1,10) THEN 1 ELSE 0 END),0) AS hoje
        FROM communication_logs
    """, (now_iso(),), one=True) or {"total": 0, "whatsapp": 0, "hoje": 0}
    return render_template(
        "comunicacao.html",
        clientes=clientes,
        templates=templates,
        historico=historico,
        resumo=resumo,
    )


@comunicacao_bp.route("/modelos", methods=["POST"])
def criar_modelo():
    _ensure_schema()
    nome = request.form.get("name", "").strip()
    conteudo = request.form.get("content", "").strip()
    if not nome or not conteudo:
        flash("Informe o nome e o texto do modelo.", "danger")
    else:
        execute_db("""
            INSERT INTO message_templates
            (name,category,content,active,created_by,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?)
        """, (
            nome,
            request.form.get("category", "Geral"),
            conteudo,
            1,
            _usuario(),
            now_iso(),
            now_iso(),
        ))
        flash("Modelo de mensagem criado.", "success")
    return redirect(url_for("comunicacao.central"))


@comunicacao_bp.route("/preparar", methods=["POST"])
def preparar():
    _ensure_schema()
    cliente_id = request.form.get("client_id") or None
    cliente = query_db("SELECT * FROM clients WHERE id=?", (cliente_id,), one=True) if cliente_id else None

    contato_cliente = ""
    if cliente:
        try:
            contato_cliente = cliente["whatsapp"] or cliente["telefone"] or ""
        except (KeyError, IndexError):
            contato_cliente = cliente["telefone"] or ""

    telefone = _telefone_limpo(request.form.get("recipient") or contato_cliente)
    mensagem = request.form.get("message", "").strip()
    pet = request.form.get("pet_name", "").strip()
    nome = cliente["nome"] if cliente else "cliente"
    mensagem = (
        mensagem.replace("{cliente}", nome)
        .replace("{pet}", pet or "seu pet")
        .replace("{empresa}", "Pet & Gatô")
    )

    if not telefone or not mensagem:
        flash("Selecione um cliente com telefone e informe a mensagem.", "danger")
        return redirect(url_for("comunicacao.central"))

    execute_db("""
        INSERT INTO communication_logs
        (client_id,template_id,channel,recipient,subject,message,status,user_name,created_at)
        VALUES(?,?,?,?,?,?,?,?,?)
    """, (
        cliente_id,
        request.form.get("template_id") or None,
        "WhatsApp",
        telefone,
        request.form.get("subject", "Contato"),
        mensagem,
        "Aberta no WhatsApp",
        _usuario(),
        now_iso(),
    ))

    try:
        execute_db("""
            INSERT INTO crm_contact_history
            (client_id,channel,subject,message,result,user_name,created_at)
            VALUES(?,?,?,?,?,?,?)
        """, (
            cliente_id,
            "WhatsApp",
            request.form.get("subject", "Contato"),
            mensagem,
            "Conversa aberta no WhatsApp",
            _usuario(),
            now_iso(),
        ))
    except Exception:
        # O WhatsApp continua funcionando mesmo em bases antigas sem histórico CRM.
        pass

    return redirect(f"https://wa.me/{telefone}?text={quote(mensagem)}")
