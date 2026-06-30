from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path

ROOT = Path.cwd()
APP = ROOT / "app.py"
TPL = ROOT / "templates" / "employees.html"
CSS = ROOT / "static" / "css" / "style.css"

EMPLOYEES_HTML = '<!doctype html>\n<html lang="pt-BR">\n<head>\n  <meta charset="utf-8">\n  <meta name="viewport" content="width=device-width, initial-scale=1">\n  <title>Funcionários</title>\n  <link rel="stylesheet" href="{{ url_for(\'static\', filename=\'css/style.css\') }}">\n</head>\n<body>\n  {% include \'nav.html\' ignore missing %}\n\n  <main class="page-wrap saas-v2 employee-layout employees-v2">\n    <section class="card-pro employee-form-card">\n      <div class="section-head">\n        <span class="eyebrow">Gestão de equipe</span>\n        <h2>Cadastrar Funcionário</h2>\n        <p class="muted">Cadastre novos colaboradores, matrícula, PIN e jornada.</p>\n      </div>\n\n      {% with messages = get_flashed_messages(with_categories=true) %}\n        {% if messages %}\n          <div class="flash-stack">\n            {% for cat,msg in messages %}\n              <div class="flash {{cat}}">{{msg}}</div>\n            {% endfor %}\n          </div>\n        {% endif %}\n      {% endwith %}\n\n      <form method="post" class="grid-form employee-create-form">\n        <label>Nome<input name="name" required></label>\n        <label>CPF<input name="cpf"></label>\n        <label>Matrícula<input name="registration" required></label>\n        <label>Cargo<input name="role_name"></label>\n        <label>Telefone com DDD/55<input name="phone" placeholder="5531999999999"></label>\n        <label>Data de admissão<input type="date" name="admission_date"></label>\n        <label>PIN<input type="password" name="pin" required></label>\n\n        <label>Jornada padrão\n          <select name="schedule_id">\n            <option value="">Sem jornada padrão</option>\n            {% for s in schedules %}<option value="{{s.id}}">{{s.description}}</option>{% endfor %}\n          </select>\n        </label>\n\n        <label>Jornada de terça a sexta\n          <select name="schedule_tue_fri">\n            <option value="">Não definir</option>\n            {% for s in schedules %}<option value="{{s.id}}">{{s.description}}</option>{% endfor %}\n          </select>\n        </label>\n\n        <label>Jornada de sábado\n          <select name="schedule_sat">\n            <option value="">Não definir</option>\n            {% for s in schedules %}<option value="{{s.id}}">{{s.description}}</option>{% endfor %}\n          </select>\n        </label>\n\n        <button class="btn-main employee-submit">Salvar funcionário</button>\n      </form>\n    </section>\n\n    <section class="card-pro employee-list-card">\n      <div class="section-head employee-list-head">\n        <div>\n          <span class="eyebrow">Controle</span>\n          <h2>Funcionários Cadastrados</h2>\n          <p class="muted">Inative colaboradores desligados e exclua somente quem ainda não possui histórico de ponto.</p>\n        </div>\n\n        <div class="employee-mini-stats">\n          <div><strong>{{ active_count or 0 }}</strong><span>Ativos</span></div>\n          <div><strong>{{ inactive_count or 0 }}</strong><span>Inativos</span></div>\n        </div>\n      </div>\n\n      <form method="get" class="employee-filters">\n        <label>Pesquisar\n          <input name="q" value="{{ search_query or \'\' }}" placeholder="Nome, matrícula, cargo ou telefone">\n        </label>\n        <label>Status\n          <select name="status">\n            <option value="ativos" {% if selected_status == \'ativos\' %}selected{% endif %}>Ativos</option>\n            <option value="inativos" {% if selected_status == \'inativos\' %}selected{% endif %}>Inativos</option>\n            <option value="todos" {% if selected_status == \'todos\' %}selected{% endif %}>Todos</option>\n          </select>\n        </label>\n        <button class="btn-secondary">Filtrar</button>\n        <a class="btn-mini" href="{{ url_for(\'employees\') }}">Limpar</a>\n      </form>\n\n      <div class="table-responsive mobile-cards">\n        <table>\n          <thead>\n            <tr>\n              <th>Nome</th>\n              <th>Matrícula</th>\n              <th>Telefone</th>\n              <th>Jornada</th>\n              <th>Pontos</th>\n              <th>Status</th>\n              <th>Ação</th>\n            </tr>\n          </thead>\n          <tbody>\n            {% for e in employees %}\n              <tr>\n                <td data-label="Nome">\n                  <strong>{{e.name}}</strong>\n                  {% if e.role_name %}<small class="employee-role">{{e.role_name}}</small>{% endif %}\n                </td>\n                <td data-label="Matrícula">{{e.registration}}</td>\n                <td data-label="Telefone">{{e.phone or \'-\'}}</td>\n                <td data-label="Jornada">{{e.schedule_description or \'-\'}}</td>\n                <td data-label="Pontos">{{e.record_count or 0}}</td>\n                <td data-label="Status">\n                  {% if e.active %}\n                    <span class="status-pill active">Ativo</span>\n                  {% else %}\n                    <span class="status-pill inactive">Inativo</span>\n                  {% endif %}\n                </td>\n                <td data-label="Ação" class="actions-cell employee-actions">\n                  <a class="btn-mini" href="{{ url_for(\'toggle_employee\', employee_id=e.id) }}">{% if e.active %}Inativar{% else %}Ativar{% endif %}</a>\n                  {% if (e.record_count or 0) == 0 %}\n                    <form method="post" action="{{ url_for(\'delete_employee\', employee_id=e.id) }}" onsubmit="return confirm(\'Deseja excluir este funcionário? Esta ação só é permitida porque ele não possui registros de ponto.\');">\n                      <button class="btn-mini danger">Excluir</button>\n                    </form>\n                  {% else %}\n                    <button class="btn-mini danger disabled" type="button" title="Funcionário com histórico não pode ser excluído. Inative para preservar os pontos.">Protegido</button>\n                  {% endif %}\n                </td>\n              </tr>\n            {% else %}\n              <tr><td colspan="7">Nenhum funcionário encontrado.</td></tr>\n            {% endfor %}\n          </tbody>\n        </table>\n      </div>\n    </section>\n  </main>\n</body>\n</html>\n'

CSS_APPEND = '\n/* =========================================================\n   Funcionários V2 - filtros, proteção de histórico e mobile\n   ========================================================= */\n.employees-v2 .section-head { margin-bottom: 18px; }\n.employees-v2 .section-head h2 { margin: 6px 0 6px; }\n.employee-list-head {\n    display: flex;\n    justify-content: space-between;\n    gap: 18px;\n    align-items: flex-start;\n    flex-wrap: wrap;\n}\n.employee-mini-stats {\n    display: flex;\n    gap: 10px;\n    flex-wrap: wrap;\n}\n.employee-mini-stats div {\n    min-width: 96px;\n    padding: 12px 14px;\n    border-radius: 16px;\n    background: #f7efff;\n    border: 1px solid var(--pf-line, #eadcfb);\n    text-align: center;\n}\n.employee-mini-stats strong {\n    display: block;\n    font-size: 22px;\n    color: var(--pf-purple, #6d42ad);\n}\n.employee-mini-stats span {\n    display: block;\n    color: var(--pf-muted, #746b88);\n    font-size: 12px;\n    font-weight: 800;\n}\n.employee-filters {\n    display: grid;\n    grid-template-columns: minmax(220px, 1fr) 180px auto auto;\n    gap: 12px;\n    align-items: end;\n    margin: 18px 0;\n}\n.employee-filters label {\n    display: flex;\n    flex-direction: column;\n    gap: 8px;\n    font-weight: 800;\n}\n.employee-role {\n    display: block;\n    margin-top: 4px;\n    color: var(--pf-muted, #746b88);\n    font-weight: 700;\n}\n.status-pill {\n    display: inline-flex;\n    align-items: center;\n    justify-content: center;\n    padding: 7px 11px;\n    border-radius: 999px;\n    font-size: 12px;\n    font-weight: 900;\n}\n.status-pill.active {\n    background: #dcfce7;\n    color: #166534;\n}\n.status-pill.inactive {\n    background: #fee2e2;\n    color: #991b1b;\n}\n.btn-mini.disabled,\n.btn-mini:disabled {\n    opacity: .55;\n    cursor: not-allowed;\n    box-shadow: none;\n}\n.employee-actions form { margin: 0; }\n.employee-submit { align-self: end; }\n\n@media(max-width: 900px) {\n    .employee-filters { grid-template-columns: 1fr; }\n    .employee-filters .btn-secondary,\n    .employee-filters .btn-mini { width: 100%; }\n    .employee-mini-stats { width: 100%; }\n    .employee-mini-stats div { flex: 1; }\n    .employee-actions { justify-content: flex-end; }\n    .employee-actions .btn-mini,\n    .employee-actions form { width: auto; }\n}\n'

NEW_EMPLOYEES_FUNCTION = '@app.route("/funcionarios", methods=["GET", "POST"])\n@admin_required\ndef employees():\n    selected_status = request.args.get("status", "ativos").strip().lower()\n    if selected_status not in ("ativos", "inativos", "todos"):\n        selected_status = "ativos"\n\n    search_query = request.args.get("q", "").strip()\n\n    if request.method == "POST":\n        name = request.form.get("name", "").strip()\n        cpf = request.form.get("cpf", "").strip()\n        registration = request.form.get("registration", "").strip()\n        role_name = request.form.get("role_name", "").strip()\n        phone = request.form.get("phone", "").strip()\n        admission_date = request.form.get("admission_date", "").strip() or None\n        pin = request.form.get("pin", "").strip()\n        schedule_id = request.form.get("schedule_id", type=int)\n        schedule_tue_fri = request.form.get("schedule_tue_fri", type=int)\n        schedule_sat = request.form.get("schedule_sat", type=int)\n\n        if not all([name, registration, pin]):\n            flash("Nome, matrícula e PIN são obrigatórios.", "danger")\n        else:\n            try:\n                employee_id = execute_db(\n                    """\n                    INSERT INTO employees (name, cpf, registration, role_name, phone, admission_date, pin_hash, schedule_id, active, created_at)\n                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)\n                    """,\n                    (name, cpf, registration, role_name, phone, admission_date, generate_password_hash(pin), schedule_id, now_iso()),\n                )\n\n                for weekday in [1, 2, 3, 4]:\n                    if schedule_tue_fri:\n                        set_employee_week_schedule(employee_id, weekday, schedule_tue_fri)\n                if schedule_sat:\n                    set_employee_week_schedule(employee_id, 5, schedule_sat)\n\n                log_action("CADASTRO_FUNCIONARIO", f"Funcionário {name} cadastrado")\n                flash("Funcionário cadastrado com sucesso.", "success")\n                return redirect(url_for("employees"))\n            except Exception as exc:\n                message = str(exc).lower()\n                if "unique" in message or "duplicate" in message or "matr" in message:\n                    flash("A matrícula já existe. Use outra matrícula.", "danger")\n                else:\n                    print("Erro ao cadastrar funcionário:", exc)\n                    flash("Erro ao cadastrar funcionário. Verifique os dados e tente novamente.", "danger")\n\n    where = []\n    params = []\n\n    if selected_status == "ativos":\n        where.append("e.active = ?")\n        params.append(1)\n    elif selected_status == "inativos":\n        where.append("e.active = ?")\n        params.append(0)\n\n    if search_query:\n        like = f"%{search_query.lower()}%"\n        where.append(\n            """\n            (\n                LOWER(COALESCE(e.name, \'\')) LIKE ?\n                OR LOWER(COALESCE(e.registration, \'\')) LIKE ?\n                OR LOWER(COALESCE(e.role_name, \'\')) LIKE ?\n                OR LOWER(COALESCE(e.phone, \'\')) LIKE ?\n            )\n            """\n        )\n        params.extend([like, like, like, like])\n\n    where_sql = "WHERE " + " AND ".join(where) if where else ""\n\n    employee_list = query_db(\n        f"""\n        SELECT\n            e.*,\n            s.description AS schedule_description,\n            COALESCE((\n                SELECT COUNT(*)\n                FROM time_records tr\n                WHERE tr.employee_id = e.id\n            ), 0) AS record_count\n        FROM employees e\n        LEFT JOIN schedules s ON s.id = e.schedule_id\n        {where_sql}\n        ORDER BY e.active DESC, e.name\n        """,\n        tuple(params),\n    )\n\n    counts = query_db(\n        """\n        SELECT\n            SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) AS active_count,\n            SUM(CASE WHEN active = 0 THEN 1 ELSE 0 END) AS inactive_count\n        FROM employees\n        """,\n        one=True,\n    )\n\n    schedules = query_db("SELECT * FROM schedules WHERE active = 1 ORDER BY description")\n    week_maps = {emp["id"]: get_employee_week_schedule_map(emp["id"]) for emp in employee_list}\n\n    return render_template(\n        "employees.html",\n        employees=employee_list,\n        schedules=schedules,\n        week_maps=week_maps,\n        weekday_name=weekday_name,\n        selected_status=selected_status,\n        search_query=search_query,\n        active_count=int((counts or {}).get("active_count") or 0),\n        inactive_count=int((counts or {}).get("inactive_count") or 0),\n    )\n\n\n'

def backup() -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = ROOT / f"backup_antes_funcionarios_v2_{stamp}"
    dest.mkdir(exist_ok=True)
    for path in (APP, TPL, CSS):
        if path.exists():
            rel = path.relative_to(ROOT)
            out = dest / rel
            out.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, out)
    return dest

def replace_employees_function(text: str) -> str:
    start_marker = '@app.route("/funcionarios", methods=["GET", "POST"])'
    end_marker = '@app.route("/funcionarios/<int:employee_id>/toggle")'
    start = text.find(start_marker)
    end = text.find(end_marker, start)
    if start == -1 or end == -1:
        raise RuntimeError("Não encontrei o bloco da rota /funcionarios para substituir.")
    return text[:start] + NEW_EMPLOYEES_FUNCTION + text[end:]

def ensure_delete_route(text: str) -> str:
    if 'def delete_employee(employee_id: int):' in text and '/funcionarios/<int:employee_id>/excluir' in text:
        return text
    insert_before = text.find('@app.route("/notificacoes"')
    if insert_before == -1:
        insert_before = len(text)
    block = """
@app.route("/funcionarios/<int:employee_id>/excluir", methods=["POST"])
@admin_required
def delete_employee(employee_id: int):
    employee = query_db("SELECT * FROM employees WHERE id = ?", (employee_id,), one=True)
    if not employee:
        flash("Funcionário não encontrado.", "danger")
        return redirect(url_for("employees"))

    total = query_db("SELECT COUNT(*) AS total FROM time_records WHERE employee_id = ?", (employee_id,), one=True)
    try:
        count = int(_safe_get(total, "total", 0) or 0)
    except Exception:
        count = int((total or {}).get("total", 0) or 0)

    if count > 0:
        execute_db("UPDATE employees SET active = 0 WHERE id = ?", (employee_id,))
        flash("Funcionário possui histórico de ponto. Ele foi inativado para preservar os registros.", "warning")
    else:
        try:
            execute_db("DELETE FROM employee_schedule_days WHERE employee_id = ?", (employee_id,))
        except Exception:
            pass
        execute_db("DELETE FROM employees WHERE id = ?", (employee_id,))
        flash("Funcionário excluído com sucesso.", "success")

    return redirect(url_for("employees"))


"""
    return text[:insert_before] + block + text[insert_before:]

def main() -> int:
    if not APP.exists():
        print("ERRO: app.py não encontrado. Execute dentro da pasta do projeto.")
        return 1
    if not CSS.exists():
        print("ERRO: static/css/style.css não encontrado.")
        return 1

    dest = backup()

    app_text = APP.read_text(encoding="utf-8", errors="replace")
    app_text = replace_employees_function(app_text)
    app_text = ensure_delete_route(app_text)
    APP.write_text(app_text, encoding="utf-8")

    TPL.parent.mkdir(parents=True, exist_ok=True)
    TPL.write_text(EMPLOYEES_HTML, encoding="utf-8")

    css_text = CSS.read_text(encoding="utf-8", errors="replace")
    if "Funcionários V2 - filtros" not in css_text:
        CSS.write_text(css_text.rstrip() + "\n\n" + CSS_APPEND.strip() + "\n", encoding="utf-8")

    print("OK: Funcionários V2 aplicado.")
    print(f"Backup criado em: {dest}")
    print("Arquivos atualizados: app.py, templates/employees.html e static/css/style.css")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
