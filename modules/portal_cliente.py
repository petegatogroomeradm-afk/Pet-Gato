from __future__ import annotations

from datetime import date
from functools import wraps

from flask import Blueprint, flash, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

from database import execute_db, insert_db, now_iso, query_db
from services.agenda_service import HORAS_AGENDA, SERVICOS_AGENDA

portal_cliente_bp = Blueprint("portal_cliente", __name__, url_prefix="/cliente")


def cliente_login_required(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not session.get("client_portal_id"):
            flash("Entre na sua conta para continuar.", "warning")
            return redirect(url_for("portal_cliente.login_cliente"))
        return func(*args, **kwargs)
    return wrapper


def _conta_atual():
    account_id = session.get("client_portal_id")
    if not account_id:
        return None
    return query_db(
        """
        SELECT a.*, c.nome, c.telefone, c.whatsapp, c.email, c.endereco
        FROM client_portal_accounts a
        JOIN clients c ON c.id = a.client_id
        WHERE a.id = ? AND a.active = 1
        """,
        (account_id,),
        one=True,
    )


@portal_cliente_bp.route("")
def inicio():
    if session.get("client_portal_id"):
        return redirect(url_for("portal_cliente.painel"))
    return render_template("portal_cliente/inicio.html")


@portal_cliente_bp.route("/cadastro", methods=["GET", "POST"])
def cadastro():
    if request.method == "POST":
        nome = request.form.get("nome", "").strip()
        email = request.form.get("email", "").strip().lower()
        telefone = request.form.get("telefone", "").strip()
        whatsapp = request.form.get("whatsapp", "").strip() or telefone
        endereco = request.form.get("endereco", "").strip()
        password = request.form.get("password", "")
        confirm = request.form.get("confirm_password", "")
        consentimento = 1 if request.form.get("consentimento_marketing") else 0

        if not nome or not email or not telefone or not password:
            flash("Preencha nome, e-mail, telefone e senha.", "danger")
        elif len(password) < 8:
            flash("A senha deve ter pelo menos 8 caracteres.", "danger")
        elif password != confirm:
            flash("As senhas não conferem.", "danger")
        elif query_db("SELECT id FROM client_portal_accounts WHERE LOWER(email)=LOWER(?)", (email,), one=True):
            flash("Já existe uma conta cadastrada com este e-mail.", "danger")
        else:
            client_id = insert_db(
                """
                INSERT INTO clients
                (nome, telefone, whatsapp, email, endereco, origem_cadastro,
                 canal_preferido, consentimento_marketing, ativo, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                """,
                (nome, telefone, whatsapp, email, endereco, "Portal do cliente", "WhatsApp", consentimento, now_iso()),
            )
            account_id = insert_db(
                """
                INSERT INTO client_portal_accounts
                (client_id, email, password_hash, active, created_at, updated_at)
                VALUES (?, ?, ?, 1, ?, ?)
                """,
                (client_id, email, generate_password_hash(password), now_iso(), now_iso()),
            )
            session.clear()
            session["client_portal_id"] = account_id
            session["client_id"] = client_id
            session["client_name"] = nome
            flash("Cadastro realizado. Agora cadastre seu pet.", "success")
            return redirect(url_for("portal_cliente.novo_pet"))

    return render_template("portal_cliente/cadastro.html")


@portal_cliente_bp.route("/login", methods=["GET", "POST"])
def login_cliente():
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        account = query_db(
            "SELECT * FROM client_portal_accounts WHERE LOWER(email)=LOWER(?) AND active=1",
            (email,),
            one=True,
        )
        if account and check_password_hash(account["password_hash"], password):
            client = query_db("SELECT nome FROM clients WHERE id=?", (account["client_id"],), one=True)
            execute_db("UPDATE client_portal_accounts SET last_login_at=?, updated_at=? WHERE id=?", (now_iso(), now_iso(), account["id"]))
            session.clear()
            session["client_portal_id"] = account["id"]
            session["client_id"] = account["client_id"]
            session["client_name"] = client["nome"] if client else "Cliente"
            return redirect(url_for("portal_cliente.painel"))
        flash("E-mail ou senha inválidos.", "danger")
    return render_template("portal_cliente/login.html")


@portal_cliente_bp.route("/sair")
def sair():
    session.clear()
    return redirect(url_for("portal_cliente.inicio"))


@portal_cliente_bp.route("/painel")
@cliente_login_required
def painel():
    conta = _conta_atual()
    if not conta:
        session.clear()
        return redirect(url_for("portal_cliente.login_cliente"))
    pets = query_db("SELECT * FROM pets WHERE client_id=? AND COALESCE(ativo,1)=1 ORDER BY nome", (conta["client_id"],))
    agendamentos = query_db(
        """
        SELECT a.*, p.nome AS pet_nome
        FROM appointments a
        JOIN pets p ON p.id=a.pet_id
        WHERE a.client_id=?
        ORDER BY a.data_agendamento DESC, a.horario DESC
        LIMIT 30
        """,
        (conta["client_id"],),
    )
    return render_template("portal_cliente/painel.html", conta=conta, pets=pets, agendamentos=agendamentos)


@portal_cliente_bp.route("/pets/novo", methods=["GET", "POST"])
@cliente_login_required
def novo_pet():
    conta = _conta_atual()
    if request.method == "POST":
        nome = request.form.get("nome", "").strip()
        if not nome:
            flash("Informe o nome do pet.", "danger")
        else:
            insert_db(
                """
                INSERT INTO pets
                (client_id, nome, especie, raca, porte, data_nascimento, sexo, peso,
                 castrado, alergias, medicamentos, temperamento, preferencia_tosa,
                 observacoes, ativo, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                """,
                (
                    conta["client_id"], nome, request.form.get("especie"), request.form.get("raca"),
                    request.form.get("porte"), request.form.get("data_nascimento"), request.form.get("sexo"),
                    request.form.get("peso") or None, 1 if request.form.get("castrado") else 0,
                    request.form.get("alergias", "").strip(), request.form.get("medicamentos", "").strip(),
                    request.form.get("temperamento", "").strip(), request.form.get("preferencia_tosa", "").strip(),
                    request.form.get("observacoes", "").strip(), now_iso(),
                ),
            )
            flash("Pet cadastrado com sucesso.", "success")
            return redirect(url_for("portal_cliente.painel"))
    return render_template("portal_cliente/pet_form.html", conta=conta)


@portal_cliente_bp.route("/agendar", methods=["GET", "POST"])
@cliente_login_required
def agendar():
    conta = _conta_atual()
    pets = query_db("SELECT id,nome FROM pets WHERE client_id=? AND COALESCE(ativo,1)=1 ORDER BY nome", (conta["client_id"],))
    if request.method == "POST":
        pet_id = request.form.get("pet_id", "").strip()
        data_agendamento = request.form.get("data", "").strip()
        horario = request.form.get("horario", "").strip()
        servico = request.form.get("servico", "").strip()
        pet = query_db("SELECT id FROM pets WHERE id=? AND client_id=? AND COALESCE(ativo,1)=1", (pet_id, conta["client_id"]), one=True)
        if not pet or not data_agendamento or not horario or not servico:
            flash("Preencha pet, serviço, data e horário.", "danger")
        elif data_agendamento < date.today().isoformat():
            flash("Escolha uma data futura.", "danger")
        else:
            insert_db(
                """
                INSERT INTO appointments
                (client_id, pet_id, data_agendamento, horario, duration_minutes, servico,
                 valor, status, transport_required, observacoes, requested_online,
                 created_at, updated_at)
                VALUES (?, ?, ?, ?, 60, ?, 0, 'Aguardando aprovação', ?, ?, 1, ?, ?)
                """,
                (
                    conta["client_id"], pet_id, data_agendamento, horario, servico,
                    1 if request.form.get("transport_required") else 0,
                    request.form.get("observacoes", "").strip(), now_iso(), now_iso(),
                ),
            )
            flash("Solicitação enviada. A loja confirmará o horário após análise.", "success")
            return redirect(url_for("portal_cliente.painel"))
    return render_template(
        "portal_cliente/agendar.html", conta=conta, pets=pets,
        servicos=SERVICOS_AGENDA, horas=HORAS_AGENDA, hoje=date.today().isoformat(),
    )
