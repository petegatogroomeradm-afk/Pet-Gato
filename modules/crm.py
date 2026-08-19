from datetime import date, datetime, timedelta
import csv
import io
from urllib.parse import quote

from flask import Blueprint, Response, flash, redirect, render_template, request, session, url_for

from core.db_compat import auto_pk_sql, ensure_columns, ensure_table
from database import execute_db, now_iso, query_db
from services.crm_service import get_dashboard

crm_bp = Blueprint("crm", __name__, url_prefix="/crm")


def _usuario():
    return session.get("user_name") or "Sistema"


def _telefone_limpo(valor):
    numero = "".join(ch for ch in str(valor or "") if ch.isdigit())
    if numero and not numero.startswith("55"):
        numero = "55" + numero
    return numero


def _ensure_schema():
    """Garante as estruturas usadas pelas automações CRM nos dois bancos."""
    primary_key = auto_pk_sql()
    ensure_table("crm_tasks", f"""
        id {primary_key},
        client_id INTEGER,
        pet_id INTEGER,
        task_type TEXT DEFAULT 'Contato',
        title TEXT NOT NULL,
        message TEXT,
        due_date TEXT,
        priority TEXT DEFAULT 'Média',
        status TEXT DEFAULT 'Pendente',
        contact_channel TEXT DEFAULT 'WhatsApp',
        source TEXT DEFAULT 'Manual',
        completed_at TEXT,
        completed_by TEXT,
        created_at TEXT
    """)
    ensure_columns("crm_tasks", {
        "source": "TEXT DEFAULT 'Manual'",
        "completed_at": "TEXT",
        "completed_by": "TEXT",
    })
    ensure_table("crm_contact_history", f"""
        id {primary_key},
        client_id INTEGER,
        pet_id INTEGER,
        task_id INTEGER,
        channel TEXT,
        subject TEXT,
        message TEXT,
        result TEXT,
        user_name TEXT,
        created_at TEXT
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


def _task_exists(client_id, pet_id, task_type, title):
    return query_db("""
        SELECT id FROM crm_tasks
        WHERE COALESCE(client_id,0)=COALESCE(?,0)
          AND COALESCE(pet_id,0)=COALESCE(?,0)
          AND task_type=? AND title=?
          AND status IN ('Pendente','Preparada','Aberta no WhatsApp')
        LIMIT 1
    """, (client_id, pet_id, task_type, title), one=True)


def _create_auto_task(client_id, pet_id, task_type, title, message, due_date, priority="Média"):
    if _task_exists(client_id, pet_id, task_type, title):
        return False
    execute_db("""
        INSERT INTO crm_tasks
        (client_id,pet_id,task_type,title,message,due_date,priority,status,contact_channel,source,created_at)
        VALUES (?,?,?,?,?,?,?,'Pendente','WhatsApp','Automática',?)
    """, (client_id, pet_id, task_type, title, message, due_date, priority, now_iso()))
    return True


@crm_bp.route("/")
def dashboard():
    _ensure_schema()
    try:
        days = max(1, int(request.args.get("dias_inativos", 45)))
    except (TypeError, ValueError):
        days = 45
    return render_template("crm.html", inactive_days=days,
                           today=date.today().isoformat(), **get_dashboard(days))


@crm_bp.route("/automacoes/gerar", methods=["POST"])
def generate_automations():
    """Converte alertas inteligentes do painel em tarefas CRM sem duplicar pendências."""
    _ensure_schema()
    try:
        days = max(1, int(request.form.get("dias_inativos", 45)))
    except (TypeError, ValueError):
        days = 45
    data = get_dashboard(days)
    created = 0
    today = date.today().isoformat()

    for item in data["inactive_clients"]:
        pet_text = "seu pet"
        message = f"Olá, {item['name']}! Sentimos sua falta na Pet & Gatô. Que tal agendar o próximo cuidado de {pet_text}?"
        created += _create_auto_task(item["id"], None, "Retorno", "Cliente sem retorno", message, today, "Alta")

    for item in data["birthdays"]:
        message = f"Olá, {item['name']}! A equipe Pet & Gatô deseja um feliz aniversário! 🎉"
        created += _create_auto_task(item["id"], None, "Aniversário", "Aniversário do tutor", message, item["next_date"], "Média")

    for item in data["pet_birthdays"]:
        message = f"Olá, {item['client_name']}! O aniversário de {item['pet_name']} está chegando. Muitas alegrias e lambeijos! 🐾🎂"
        created += _create_auto_task(item["client_id"], item["pet_id"], "Aniversário", "Aniversário do pet", message, item["next_date"], "Média")

    for item in data["vaccine_alerts"]:
        message = f"Olá, {item['client_name']}! A vacina {item['vaccine']} de {item['pet_name']} está {item['status'].lower()} ({item['due_date']})."
        priority = "Urgente" if item["status"] == "Vencida" else "Alta"
        created += _create_auto_task(item["client_id"], item["pet_id"], "Vacina", f"Vacina: {item['vaccine']}", message, item["due_date"], priority)

    if created:
        flash(f"{created} tarefa(s) automática(s) criada(s) sem duplicar pendências.", "success")
    else:
        flash("Nenhuma nova tarefa foi necessária. As pendências já estão atualizadas.", "info")
    return redirect(url_for("crm.dashboard", dias_inativos=days))


@crm_bp.route("/tarefas/adicionar", methods=["POST"])
def add_task():
    _ensure_schema()
    title = request.form.get("title", "").strip()
    if not title:
        flash("Informe o título da tarefa.", "danger")
        return redirect(url_for("crm.dashboard"))
    execute_db("""INSERT INTO crm_tasks
        (client_id,pet_id,task_type,title,message,due_date,priority,status,contact_channel,source,created_at)
        VALUES (?,?,?,?,?,?,?,'Pendente',?,'Manual',?)""",
        (request.form.get("client_id") or None, request.form.get("pet_id") or None,
         request.form.get("task_type", "Contato"), title,
         request.form.get("message", "").strip(), request.form.get("due_date") or None,
         request.form.get("priority", "Média"),
         request.form.get("contact_channel", "WhatsApp"), now_iso()))
    flash("Tarefa CRM criada.", "success")
    return redirect(url_for("crm.dashboard"))


@crm_bp.route("/tarefas/<int:task_id>/whatsapp")
def open_task_whatsapp(task_id):
    """Abre o WhatsApp e registra a tentativa de contato da tarefa."""
    _ensure_schema()
    task = query_db("""
        SELECT t.*, c.nome AS client_name,
               COALESCE(NULLIF(c.whatsapp,''),NULLIF(c.telefone,''),'') AS contact,
               p.nome AS pet_name
        FROM crm_tasks t
        LEFT JOIN clients c ON c.id=t.client_id
        LEFT JOIN pets p ON p.id=t.pet_id
        WHERE t.id=?
    """, (task_id,), one=True)
    if not task:
        flash("Tarefa não encontrada.", "danger")
        return redirect(url_for("crm.dashboard"))
    number = _telefone_limpo(task["contact"])
    if not number:
        flash("O cliente não possui WhatsApp ou telefone cadastrado.", "danger")
        return redirect(url_for("crm.dashboard"))
    message = (task["message"] or task["title"] or "Olá!").strip()
    result = "Tarefa aberta no WhatsApp"
    execute_db("UPDATE crm_tasks SET status='Aberta no WhatsApp' WHERE id=?", (task_id,))
    execute_db("""
        INSERT INTO communication_logs
        (client_id,pet_id,channel,recipient,subject,message,status,user_name,created_at)
        VALUES(?,?,?,?,?,?,?,?,?)
    """, (task["client_id"], task["pet_id"], "WhatsApp", number, task["title"], message, result, _usuario(), now_iso()))
    execute_db("""
        INSERT INTO crm_contact_history
        (client_id,pet_id,task_id,channel,subject,message,result,user_name,created_at)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, (task["client_id"], task["pet_id"], task_id, "WhatsApp", task["title"], message, result, _usuario(), now_iso()))
    return redirect(f"https://wa.me/{number}?text={quote(message)}")


@crm_bp.route("/tarefas/<int:task_id>/concluir", methods=["POST"])
def complete_task(task_id):
    _ensure_schema()
    task = query_db("SELECT * FROM crm_tasks WHERE id=?", (task_id,), one=True)
    if not task:
        flash("Tarefa não encontrada.", "danger")
        return redirect(url_for("crm.dashboard"))
    result = request.form.get("result", "Contato realizado").strip()
    user = _usuario()
    execute_db("UPDATE crm_tasks SET status='Concluída',completed_at=?,completed_by=? WHERE id=?",
               (now_iso(), user, task_id))
    execute_db("""INSERT INTO crm_contact_history
        (client_id,pet_id,task_id,channel,subject,message,result,user_name,created_at)
        VALUES (?,?,?,?,?,?,?,?,?)""",
        (task["client_id"], task["pet_id"], task_id, task["contact_channel"], task["title"],
         task["message"], result, user, now_iso()))
    flash("Tarefa concluída e contato registrado.", "success")
    return redirect(url_for("crm.dashboard"))


@crm_bp.route("/contatos/registrar", methods=["POST"])
def register_contact():
    _ensure_schema()
    subject = request.form.get("subject", "").strip()
    if not subject:
        flash("Informe o assunto do contato.", "danger")
        return redirect(url_for("crm.dashboard"))
    execute_db("""INSERT INTO crm_contact_history
        (client_id,pet_id,channel,subject,message,result,user_name,created_at)
        VALUES (?,?,?,?,?,?,?,?)""",
        (request.form.get("client_id") or None, request.form.get("pet_id") or None,
         request.form.get("channel", "WhatsApp"), subject,
         request.form.get("message", "").strip(), request.form.get("result", "").strip(),
         _usuario(), now_iso()))
    flash("Contato registrado no histórico.", "success")
    return redirect(url_for("crm.dashboard"))



def _parse_date(value, default):
    try:
        return datetime.strptime(value or "", "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return default


def _agenda_task_rows():
    rows = query_db("""SELECT t.*, c.nome AS client_name, p.nome AS pet_name,
        COALESCE(NULLIF(c.whatsapp,''),NULLIF(c.telefone,''),'') AS contact
        FROM crm_tasks t LEFT JOIN clients c ON c.id=t.client_id
        LEFT JOIN pets p ON p.id=t.pet_id
        WHERE t.status NOT IN ('Concluída','Cancelada')
        ORDER BY COALESCE(t.due_date,'9999-12-31'), t.id DESC""")
    result = []
    today = date.today()
    for row in rows:
        item = dict(row)
        due = _parse_date(item.get("due_date"), None) if item.get("due_date") else None
        item["deadline_label"] = "Sem prazo" if not due else (("Vencida há %d dia(s)" % (today-due).days) if due < today else ("Hoje" if due == today else due.strftime("%d/%m/%Y")))
        item["whatsapp_url"] = _wa(item.get("contact"), item.get("message") or item.get("title") or "Olá!")
        result.append(item)
    return result


@crm_bp.route("/agenda")
def task_agenda():
    _ensure_schema()
    start = _parse_date(request.args.get("inicio"), date.today())
    try:
        days_count = int(request.args.get("dias", 7))
    except (TypeError, ValueError):
        days_count = 7
    days_count = days_count if days_count in {7, 14, 30} else 7
    end = start + timedelta(days=days_count - 1)
    tasks = _agenda_task_rows()
    overdue = [t for t in tasks if t.get("due_date") and _parse_date(t["due_date"], start) < start]
    undated = [t for t in tasks if not t.get("due_date")]
    agenda_days = []
    weekdays = ["segunda-feira", "terça-feira", "quarta-feira", "quinta-feira", "sexta-feira", "sábado", "domingo"]
    for offset in range(days_count):
        current = start + timedelta(days=offset)
        agenda_days.append({"date": current.isoformat(), "label": current.strftime("%d/%m"), "weekday": weekdays[current.weekday()], "tasks": [t for t in tasks if t.get("due_date") == current.isoformat()]})
    return render_template("crm_agenda.html", agenda_start=start.isoformat(), agenda_end=end.isoformat(),
        agenda_days_count=days_count, previous_start=(start-timedelta(days=days_count)).isoformat(),
        next_start=(start+timedelta(days=days_count)).isoformat(), today=date.today().isoformat(),
        agenda_days=agenda_days, overdue_tasks=overdue, undated_tasks=undated,
        agenda_metrics={"overdue":len(overdue), "today":sum(1 for t in tasks if t.get("due_date")==date.today().isoformat()),
                        "upcoming":sum(1 for t in tasks if t.get("due_date") and start <= _parse_date(t["due_date"], start) <= end), "undated":len(undated)})


@crm_bp.route("/tarefas/<int:task_id>/adiar", methods=["POST"])
def postpone_task(task_id):
    _ensure_schema()
    try:
        days = max(1, min(90, int(request.form.get("days", 7))))
    except (TypeError, ValueError):
        days = 7
    task = query_db("SELECT due_date FROM crm_tasks WHERE id=?", (task_id,), one=True)
    if not task:
        flash("Tarefa não encontrada.", "danger")
    else:
        base = _parse_date(task["due_date"], date.today())
        execute_db("UPDATE crm_tasks SET due_date=? WHERE id=?", ((base + timedelta(days=days)).isoformat(), task_id))
        flash(f"Tarefa adiada por {days} dia(s).", "success")
    return redirect(request.referrer or url_for("crm.task_agenda"))


@crm_bp.route("/resumo-diario")
def daily_brief():
    _ensure_schema()
    brief = _parse_date(request.args.get("data"), date.today())
    tasks = _agenda_task_rows()
    overdue = [t for t in tasks if t.get("due_date") and _parse_date(t["due_date"], brief) < brief]
    today_tasks = [t for t in tasks if t.get("due_date") == brief.isoformat()]
    undated = [t for t in tasks if not t.get("due_date")]
    week_start = brief - timedelta(days=6)
    contacts = query_db("SELECT result FROM crm_contact_history WHERE substr(created_at,1,10) BETWEEN ? AND ?", (week_start.isoformat(), brief.isoformat()))
    results = [str(r["result"] or "").lower() for r in contacts]
    scheduled = sum("agend" in r for r in results); interested = sum("interess" in r for r in results); no_response = sum("sem resposta" in r for r in results)
    completed_today = query_db("SELECT COUNT(*) total FROM crm_tasks WHERE status='Concluída' AND substr(completed_at,1,10)=?", (brief.isoformat(),), one=True)
    return render_template("crm_resumo_diario.html", brief_date=brief.isoformat(), brief_date_label=brief.strftime("%d/%m/%Y"),
        previous_date=(brief-timedelta(days=1)).isoformat(), next_date=(brief+timedelta(days=1)).isoformat(),
        overdue_tasks=overdue, today_tasks=today_tasks, undated_tasks=undated,
        brief_metrics={"overdue":len(overdue), "today":len(today_tasks), "completed_today":int(completed_today["total"] or 0),
                       "next_7_days":sum(1 for t in tasks if t.get("due_date") and brief < _parse_date(t["due_date"], brief) <= brief+timedelta(days=7))},
        week_performance={"start":week_start.strftime("%d/%m"), "end":brief.strftime("%d/%m"), "contacts":len(results), "scheduled":scheduled,
                          "interested":interested, "no_response":no_response, "conversion_rate":round((scheduled/len(results)*100),1) if results else 0})


@crm_bp.route("/relatorios")
def reports():
    _ensure_schema()
    end = _parse_date(request.args.get("fim"), date.today()); start = _parse_date(request.args.get("inicio"), end-timedelta(days=29))
    rows = query_db("""SELECT h.*, c.nome client_name, p.nome pet_name FROM crm_contact_history h
        LEFT JOIN clients c ON c.id=h.client_id LEFT JOIN pets p ON p.id=h.pet_id
        WHERE substr(h.created_at,1,10) BETWEEN ? AND ? ORDER BY h.created_at DESC""", (start.isoformat(), end.isoformat()))
    results=[str(r["result"] or "").lower() for r in rows]; scheduled=sum("agend" in r for r in results); interested=sum("interess" in r for r in results); no_response=sum("sem resposta" in r for r in results)
    completed=query_db("SELECT COUNT(*) total FROM crm_tasks WHERE status='Concluída' AND substr(completed_at,1,10) BETWEEN ? AND ?",(start.isoformat(),end.isoformat()),one=True)
    pending=query_db("SELECT COUNT(*) total FROM crm_tasks WHERE status IN ('Pendente','Aberta no WhatsApp') AND COALESCE(due_date,?) BETWEEN ? AND ?",(end.isoformat(),start.isoformat(),end.isoformat()),one=True)
    by_day={}
    for r in rows:
        key=str(r["created_at"] or "")[:10]; by_day[key]=by_day.get(key,0)+1
    maximum=max(by_day.values(), default=1)
    daily=[{"label":_parse_date(k,start).strftime("%d/%m"),"completed":v,"scheduled":0,"height":max(8,round(v/maximum*100))} for k,v in sorted(by_day.items())]
    return render_template("crm_relatorios.html", report_start=start.isoformat(), report_end=end.isoformat(), report_start_label=start.strftime("%d/%m/%Y"), report_end_label=end.strftime("%d/%m/%Y"),
        report_metrics={"contacts":len(rows),"completed":int(completed["total"] or 0),"scheduled":scheduled,"conversion_rate":round(scheduled/len(rows)*100,1) if rows else 0,"response_rate":round((len(rows)-no_response)/len(rows)*100,1) if rows else 0,"pending":int(pending["total"] or 0)},
        outcomes={"Agendou":scheduled,"Interessado":interested,"Sem resposta":no_response,"Outros":max(0,len(rows)-scheduled-interested-no_response)}, daily_performance=daily,
        team_performance=[], campaign_performance=[], recent_contacts=rows[:50])


@crm_bp.route("/relatorios/exportar.csv")
def export_report_csv():
    _ensure_schema()
    end = _parse_date(request.args.get("fim"), date.today()); start = _parse_date(request.args.get("inicio"), end-timedelta(days=29))
    rows = query_db("""SELECT h.created_at,c.nome client_name,p.nome pet_name,h.channel,h.subject,h.message,h.result,h.user_name
        FROM crm_contact_history h LEFT JOIN clients c ON c.id=h.client_id LEFT JOIN pets p ON p.id=h.pet_id
        WHERE substr(h.created_at,1,10) BETWEEN ? AND ? ORDER BY h.created_at DESC""", (start.isoformat(), end.isoformat()))
    output=io.StringIO(); writer=csv.writer(output,delimiter=';'); writer.writerow(["Data","Cliente","Pet","Canal","Assunto","Mensagem","Resultado","Usuário"])
    for r in rows: writer.writerow([r["created_at"],r["client_name"] or "",r["pet_name"] or "",r["channel"] or "",r["subject"] or "",r["message"] or "",r["result"] or "",r["user_name"] or ""])
    return Response("\ufeff"+output.getvalue(), mimetype="text/csv; charset=utf-8", headers={"Content-Disposition":f"attachment; filename=crm_{start}_{end}.csv"})
