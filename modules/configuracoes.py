from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from werkzeug.security import generate_password_hash
from database import query_db, execute_db, now_iso
from services.auditoria_service import registrar_auditoria
from core.permissions import ROLE_LABELS, normalize_role, require_permission, is_superadmin

configuracoes_bp = Blueprint("configuracoes", __name__)


def audit(action, user_id=None, details=""):
    registrar_auditoria(session.get("user_name"), action, "user", user_id, details)


@configuracoes_bp.route("/configuracoes", methods=["GET", "POST"])
def configuracoes():
    config = query_db("SELECT * FROM settings LIMIT 1", one=True)
    agenda_config = query_db("SELECT * FROM agenda_capacity_settings ORDER BY id LIMIT 1", one=True)
    if request.method == "POST":
        if request.form.get("form_type") == "agenda_capacity":
            try:
                capacidade = max(1, min(int(request.form.get("default_capacity") or 3), 50))
            except (TypeError, ValueError):
                capacidade = 3
            values = (
                capacidade,
                1 if request.form.get("allow_admin_override") else 0,
                1 if request.form.get("allow_recepcao_override") else 0,
                now_iso(),
            )
            if agenda_config:
                execute_db("UPDATE agenda_capacity_settings SET default_capacity=?,allow_admin_override=?,allow_recepcao_override=?,updated_at=? WHERE id=?", values + (agenda_config["id"],))
            else:
                execute_db("INSERT INTO agenda_capacity_settings (default_capacity,allow_admin_override,allow_recepcao_override,created_at,updated_at) VALUES (?,?,?,?,?)", values[:3] + (now_iso(), now_iso()))
            flash("Capacidade da agenda atualizada.", "success")
        else:
            fields = [request.form.get(k, "").strip() for k in ("company_name","cnpj","phone","whatsapp","email","address","pix_key","notes")]
            execute_db("DELETE FROM settings")
            execute_db("INSERT INTO settings (company_name,cnpj,phone,whatsapp,email,address,pix_key,notes,created_at) VALUES (?,?,?,?,?,?,?,?,?)", (*fields, now_iso()))
            flash("Configurações salvas com sucesso.", "success")
        return redirect(url_for("configuracoes.configuracoes"))
    return render_template("configuracoes.html", config=config, agenda_config=agenda_config)


@configuracoes_bp.route("/usuarios", methods=["GET", "POST"])
@require_permission("usuarios")
def usuarios():
    if request.method == "POST":
        data = {k: request.form.get(k, "").strip() for k in ("name","username","cpf","phone","email","job_title","admission_date")}
        password = request.form.get("password", "")
        role = normalize_role(request.form.get("role"))
        if role == "superadmin" and not is_superadmin():
            role = "admin"
        force_change = 1 if request.form.get("must_change_password") else 0
        if not data["name"] or not data["username"] or len(password) < 8:
            flash("Informe nome, usuário e senha com pelo menos 8 caracteres.", "danger")
        elif query_db("SELECT id FROM users WHERE username = ?", (data["username"].lower(),), one=True):
            flash("Já existe um usuário com esse login.", "danger")
        else:
            execute_db("""INSERT INTO users (name,username,password_hash,role,active,created_at,cpf,phone,email,job_title,admission_date,must_change_password,failed_attempts,login_count,created_by)
                          VALUES (?,?,?,?,1,?,?,?,?,?,?,?,0,0,?)""",
                       (data["name"],data["username"].lower(),generate_password_hash(password),role,now_iso(),data["cpf"],data["phone"],data["email"],data["job_title"],data["admission_date"],force_change,session.get("user_name")))
            new = query_db("SELECT id FROM users WHERE username=?", (data["username"].lower(),), one=True)
            audit("usuario_criado", new["id"] if new else None, f"Perfil: {role}")
            flash("Usuário criado com sucesso.", "success")
            return redirect(url_for("configuracoes.usuarios"))

    q = request.args.get("q", "").strip()
    role_filter = request.args.get("role", "").strip()
    status_filter = request.args.get("status", "").strip()
    sql = "SELECT * FROM users WHERE 1=1"; params=[]
    if q:
        sql += " AND (LOWER(name) LIKE ? OR LOWER(username) LIKE ? OR LOWER(COALESCE(email,'')) LIKE ?)"
        like=f"%{q.lower()}%"; params += [like,like,like]
    if role_filter:
        sql += " AND role = ?"; params.append(role_filter)
    if status_filter in {"active","inactive","locked"}:
        if status_filter == "active": sql += " AND active = 1 AND (locked_until IS NULL OR locked_until <= ?)"; params.append(now_iso())
        elif status_filter == "inactive": sql += " AND active = 0"
        else: sql += " AND locked_until > ?"; params.append(now_iso())
    users = query_db(sql + " ORDER BY name", tuple(params))
    logs = query_db("SELECT * FROM user_access_logs ORDER BY id DESC LIMIT 30")
    stats = {"total": len(query_db("SELECT id FROM users")), "active": len(query_db("SELECT id FROM users WHERE active=1")), "locked": len(query_db("SELECT id FROM users WHERE locked_until > ?", (now_iso(),)))}
    return render_template("usuarios.html", users=users, roles=ROLE_LABELS, logs=logs, stats=stats, filters={"q":q,"role":role_filter,"status":status_filter}, current_now=now_iso())


@configuracoes_bp.route("/usuarios/<int:user_id>/editar", methods=["POST"])
@require_permission("usuarios")
def editar_usuario(user_id):
    user = query_db("SELECT * FROM users WHERE id=?", (user_id,), one=True)
    if not user:
        flash("Usuário não encontrado.", "danger"); return redirect(url_for("configuracoes.usuarios"))
    name=request.form.get("name","").strip(); role=normalize_role(request.form.get("role")); password=request.form.get("password","")
    if role == "superadmin" and not is_superadmin(): role = user["role"]
    if user["username"]=="admin" or user["role"] == "superadmin": role="superadmin"
    if not name: flash("O nome é obrigatório.","danger")
    elif password and len(password)<8: flash("A nova senha deve ter pelo menos 8 caracteres.","danger")
    else:
        fields=(name,role,request.form.get("cpf","").strip(),request.form.get("phone","").strip(),request.form.get("email","").strip(),request.form.get("job_title","").strip(),request.form.get("admission_date","").strip(),now_iso(),user_id)
        execute_db("UPDATE users SET name=?,role=?,cpf=?,phone=?,email=?,job_title=?,admission_date=?,updated_at=? WHERE id=?", fields)
        if password:
            execute_db("UPDATE users SET password_hash=?,must_change_password=?,failed_attempts=0,locked_until=NULL WHERE id=?", (generate_password_hash(password),1 if request.form.get("must_change_password") else 0,user_id))
        audit("usuario_atualizado", user_id, f"Perfil: {role}")
        flash("Usuário atualizado com sucesso.","success")
    return redirect(url_for("configuracoes.usuarios"))


@configuracoes_bp.route("/usuarios/<int:user_id>/status", methods=["POST"])
@require_permission("usuarios")
def alternar_status_usuario(user_id):
    user=query_db("SELECT id,username,role,active FROM users WHERE id=?",(user_id,),one=True)
    if not user: flash("Usuário não encontrado.","danger")
    elif user["username"]=="admin" or user["role"] == "superadmin": flash("O Super Administrador não pode ser desativado.","danger")
    elif session.get("user_id")==user_id: flash("Você não pode desativar seu próprio usuário.","danger")
    else:
        execute_db("UPDATE users SET active=?,updated_at=? WHERE id=?",(0 if user["active"] else 1,now_iso(),user_id)); audit("status_usuario_alterado",user_id)
        flash("Status do usuário atualizado.","success")
    return redirect(url_for("configuracoes.usuarios"))


@configuracoes_bp.route("/usuarios/<int:user_id>/desbloquear", methods=["POST"])
@require_permission("usuarios")
def desbloquear_usuario(user_id):
    execute_db("UPDATE users SET failed_attempts=0,locked_until=NULL,updated_at=? WHERE id=?",(now_iso(),user_id)); audit("usuario_desbloqueado",user_id)
    flash("Usuário desbloqueado.","success")
    return redirect(url_for("configuracoes.usuarios"))


@configuracoes_bp.route("/usuarios/<int:user_id>/resetar-senha", methods=["POST"])
@require_permission("usuarios")
def resetar_senha_usuario(user_id):
    user = query_db("SELECT id, username, role FROM users WHERE id=?", (user_id,), one=True)
    password = request.form.get("temporary_password", "").strip()
    if not user:
        flash("Usuário não encontrado.", "danger")
    elif user["role"] == "superadmin" and not is_superadmin():
        flash("Somente o Super Administrador pode redefinir esta senha.", "danger")
    elif len(password) < 8:
        flash("A senha temporária deve ter pelo menos 8 caracteres.", "danger")
    else:
        execute_db(
            "UPDATE users SET password_hash=?,must_change_password=1,failed_attempts=0,locked_until=NULL,updated_at=? WHERE id=?",
            (generate_password_hash(password), now_iso(), user_id),
        )
        audit("senha_usuario_redefinida", user_id, f"Login: {user['username']}")
        flash("Senha redefinida. O usuário deverá trocá-la no próximo acesso.", "success")
    return redirect(url_for("configuracoes.usuarios"))


@configuracoes_bp.route("/usuarios/<int:user_id>/excluir", methods=["POST"])
@require_permission("usuarios")
def excluir_usuario(user_id):
    user = query_db("SELECT id, username, role FROM users WHERE id=?", (user_id,), one=True)
    if not user:
        flash("Usuário não encontrado.", "danger")
    elif not is_superadmin():
        flash("Somente o Super Administrador pode excluir usuários definitivamente.", "danger")
    elif user["role"] == "superadmin" or user["username"] == "admin":
        flash("O Super Administrador principal não pode ser excluído.", "danger")
    elif session.get("user_id") == user_id:
        flash("Você não pode excluir o próprio usuário.", "danger")
    else:
        execute_db("DELETE FROM users WHERE id=?", (user_id,))
        audit("usuario_excluido", user_id, f"Login: {user['username']}")
        flash("Usuário excluído definitivamente.", "success")
    return redirect(url_for("configuracoes.usuarios"))
