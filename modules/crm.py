from datetime import date
from flask import Blueprint, flash, redirect, render_template, request, session, url_for
from database import execute_db, now_iso, query_db
from services.crm_service import get_dashboard

crm_bp = Blueprint("crm", __name__, url_prefix="/crm")

@crm_bp.route("/")
def dashboard():
    try:
        days = max(1, int(request.args.get("dias_inativos", 45)))
    except (TypeError, ValueError):
        days = 45
    return render_template("crm.html", inactive_days=days,
                           today=date.today().isoformat(), **get_dashboard(days))

@crm_bp.route("/tarefas/adicionar", methods=["POST"])
def add_task():
    title = request.form.get("title","").strip()
    if not title:
        flash("Informe o título da tarefa.","danger")
        return redirect(url_for("crm.dashboard"))
    execute_db("""INSERT INTO crm_tasks
        (client_id,pet_id,task_type,title,message,due_date,priority,status,contact_channel,created_at)
        VALUES (?,?,?,?,?,?,?,'Pendente',?,?)""",
        (request.form.get("client_id") or None, request.form.get("pet_id") or None,
         request.form.get("task_type","Contato"), title,
         request.form.get("message","").strip(), request.form.get("due_date") or None,
         request.form.get("priority","Média"),
         request.form.get("contact_channel","WhatsApp"), now_iso()))
    flash("Tarefa CRM criada.","success")
    return redirect(url_for("crm.dashboard"))

@crm_bp.route("/tarefas/<int:task_id>/concluir", methods=["POST"])
def complete_task(task_id):
    task = query_db("SELECT * FROM crm_tasks WHERE id=?",(task_id,),one=True)
    if not task:
        flash("Tarefa não encontrada.","danger")
        return redirect(url_for("crm.dashboard"))
    result = request.form.get("result","Contato realizado").strip()
    user = session.get("user_name","Sistema")
    execute_db("UPDATE crm_tasks SET status='Concluída',completed_at=?,completed_by=? WHERE id=?",
               (now_iso(),user,task_id))
    execute_db("""INSERT INTO crm_contact_history
        (client_id,pet_id,task_id,channel,subject,message,result,user_name,created_at)
        VALUES (?,?,?,?,?,?,?,?,?)""",
        (task["client_id"],task["pet_id"],task_id,task["contact_channel"],task["title"],
         task["message"],result,user,now_iso()))
    flash("Tarefa concluída e contato registrado.","success")
    return redirect(url_for("crm.dashboard"))

@crm_bp.route("/contatos/registrar", methods=["POST"])
def register_contact():
    subject = request.form.get("subject","").strip()
    if not subject:
        flash("Informe o assunto do contato.","danger")
        return redirect(url_for("crm.dashboard"))
    execute_db("""INSERT INTO crm_contact_history
        (client_id,pet_id,channel,subject,message,result,user_name,created_at)
        VALUES (?,?,?,?,?,?,?,?)""",
        (request.form.get("client_id") or None,request.form.get("pet_id") or None,
         request.form.get("channel","WhatsApp"),subject,
         request.form.get("message","").strip(),request.form.get("result","").strip(),
         session.get("user_name","Sistema"),now_iso()))
    flash("Contato registrado no histórico.","success")
    return redirect(url_for("crm.dashboard"))
