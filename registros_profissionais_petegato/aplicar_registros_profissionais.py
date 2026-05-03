from pathlib import Path
import re
import shutil

ROOT = Path.cwd()
APP = ROOT / "app.py"
TEMPLATES = ROOT / "templates"
RECORDS_TEMPLATE = TEMPLATES / "records.html"
SOURCE_TEMPLATE = Path(__file__).resolve().parent / "records.html"

if not APP.exists():
    raise SystemExit("app.py não encontrado. Rode este script dentro da pasta principal do projeto.")
if not SOURCE_TEMPLATE.exists():
    raise SystemExit("records.html do pacote não encontrado. Extraia o ZIP completo antes de rodar.")

backup_app = ROOT / "app_backup_antes_registros_profissionais.py"
backup_records = TEMPLATES / "records_backup_antes_profissional.html"
shutil.copy2(APP, backup_app)
TEMPLATES.mkdir(exist_ok=True)
if RECORDS_TEMPLATE.exists():
    shutil.copy2(RECORDS_TEMPLATE, backup_records)
shutil.copy2(SOURCE_TEMPLATE, RECORDS_TEMPLATE)

text = APP.read_text(encoding="utf-8")

# Remove versão anterior deste bloco, se existir
text = re.sub(
    r"\n# =========================================================\n# Registros profissionais.*?# =========================================================\n# Fim registros profissionais\n",
    "\n",
    text,
    flags=re.S,
)

professional_block = r'''
# =========================================================
# Registros profissionais: visão por dia + ajuste seguro
# =========================================================

def _time_value_from_record(value: Optional[str]) -> str:
    if not value:
        return ""
    try:
        if isinstance(value, datetime):
            return value.strftime("%H:%M")
        return parse_dt(str(value)).strftime("%H:%M")
    except Exception:
        text_value = str(value)
        if len(text_value) >= 16:
            return text_value[11:16]
        return ""


def _parse_time_input(ref_date: str, time_value: str) -> Optional[str]:
    time_value = (time_value or "").strip()
    if not time_value:
        return None
    try:
        if len(time_value) == 5:
            datetime.strptime(time_value, "%H:%M")
            return f"{ref_date} {time_value}:00"
        if len(time_value) == 8:
            datetime.strptime(time_value, "%H:%M:%S")
            return f"{ref_date} {time_value}"
    except ValueError:
        return None
    return None


def _validate_day_times(values: Dict[str, Optional[str]]) -> Optional[str]:
    parsed = {key: parse_dt(value) for key, value in values.items() if value}
    entrada = parsed.get("ENTRADA")
    saida_intervalo = parsed.get("SAIDA_INTERVALO")
    retorno_intervalo = parsed.get("RETORNO_INTERVALO")
    saida = parsed.get("SAIDA")

    if saida and entrada and saida <= entrada:
        return "A saída final precisa ser maior que a entrada."
    if entrada and saida_intervalo and saida_intervalo <= entrada:
        return "A saída para intervalo precisa ser maior que a entrada."
    if saida_intervalo and retorno_intervalo and retorno_intervalo <= saida_intervalo:
        return "O retorno do intervalo precisa ser maior que a saída para intervalo."
    if retorno_intervalo and saida and saida <= retorno_intervalo:
        return "A saída final precisa ser maior que o retorno do intervalo."
    return None


def _get_record_by_type(employee_id: int, ref_date: str, record_type: str):
    return query_db(
        """
        SELECT * FROM time_records
        WHERE employee_id = ? AND date(record_time) = ? AND record_type = ?
        ORDER BY id ASC
        LIMIT 1
        """,
        (employee_id, ref_date, record_type),
        one=True,
    )


def _build_records_group(employee: Any, ref_date: str) -> Dict[str, Any]:
    summary = calculate_day_summary(employee["id"], ref_date)
    sequence = summary.get("sequence") or {}
    times = {
        "ENTRADA": _time_value_from_record(sequence.get("ENTRADA")),
        "SAIDA_INTERVALO": _time_value_from_record(sequence.get("SAIDA_INTERVALO")),
        "RETORNO_INTERVALO": _time_value_from_record(sequence.get("RETORNO_INTERVALO")),
        "SAIDA": _time_value_from_record(sequence.get("SAIDA")),
    }
    return {
        "employee": employee,
        "schedule": summary.get("schedule"),
        "weekday_name": summary.get("weekday_name"),
        "times": times,
        "worked_h": format_minutes(int(summary.get("worked_minutes") or 0)),
        "delay_h": format_minutes(int(summary.get("delay_minutes") or 0)),
        "extra_h": format_minutes(int(summary.get("extra_minutes") or 0)),
        "delay_minutes": int(summary.get("delay_minutes") or 0),
        "extra_minutes": int(summary.get("extra_minutes") or 0),
        "inconsistency": bool(summary.get("inconsistency")),
    }


@app.route("/registros")
@admin_required
def records():
    employee_id = request.args.get("employee_id", type=int)
    ref_date = request.args.get("ref_date", today_str())

    try:
        parse_date(ref_date)
    except Exception:
        ref_date = today_str()

    employees = query_db("SELECT id, name, registration FROM employees WHERE active = 1 ORDER BY name")
    selected_employees = employees
    if employee_id:
        selected_employees = [emp for emp in employees if int(emp["id"]) == int(employee_id)]

    grouped_days = [_build_records_group(emp, ref_date) for emp in selected_employees]

    params = [ref_date]
    where = ["date(tr.record_time) = ?"]
    if employee_id:
        where.append("tr.employee_id = ?")
        params.append(employee_id)

    record_list = query_db(
        f"""
        SELECT tr.*, e.name AS employee_name, e.registration
        FROM time_records tr
        JOIN employees e ON e.id = tr.employee_id
        WHERE {' AND '.join(where)}
        ORDER BY e.name ASC, tr.record_time ASC, tr.id ASC
        """,
        tuple(params),
    )

    return render_template(
        "records.html",
        records=record_list,
        employees=employees,
        grouped_days=grouped_days,
        ref_date=ref_date,
        ref_date_br=parse_date(ref_date).strftime("%d/%m/%Y"),
        employee_id=employee_id,
        format_dt=format_dt,
    )


@app.route("/registros/dia/<int:employee_id>/<ref_date>/ajustar", methods=["POST"])
@admin_required
def update_day_records(employee_id: int, ref_date: str):
    employee = query_db("SELECT * FROM employees WHERE id = ?", (employee_id,), one=True)
    if not employee:
        flash("Funcionário não encontrado.", "danger")
        return redirect(url_for("records", ref_date=ref_date))

    try:
        parse_date(ref_date)
    except Exception:
        flash("Data inválida para ajuste.", "danger")
        return redirect(url_for("records"))

    reason = request.form.get("reason", "").strip()
    if not reason:
        flash("Informe o motivo do ajuste.", "danger")
        return redirect(url_for("records", ref_date=ref_date, employee_id=employee_id))

    values: Dict[str, Optional[str]] = {}
    for record_type in record_sequence():
        raw_time = request.form.get(record_type, "").strip()
        if raw_time:
            parsed_time = _parse_time_input(ref_date, raw_time)
            if not parsed_time:
                flash(f"Horário inválido em {record_type}. Use HH:MM.", "danger")
                return redirect(url_for("records", ref_date=ref_date, employee_id=employee_id))
            values[record_type] = parsed_time
        else:
            values[record_type] = None

    validation_error = _validate_day_times(values)
    if validation_error:
        flash(validation_error, "danger")
        return redirect(url_for("records", ref_date=ref_date, employee_id=employee_id))

    changed = 0
    for record_type in record_sequence():
        new_value = values.get(record_type)
        if not new_value:
            continue

        existing = _get_record_by_type(employee_id, ref_date, record_type)
        if existing:
            old_value = existing["record_time"]
            if str(old_value) == str(new_value):
                continue
            execute_db("UPDATE time_records SET record_time = ?, notes = ? WHERE id = ?", (new_value, "Ajuste manual", existing["id"]))
            execute_db(
                """
                INSERT INTO time_adjustments (record_id, employee_id, admin_user_id, old_value, new_value, reason, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (existing["id"], employee_id, session["user_id"], str(old_value), new_value, reason, now_iso()),
            )
            changed += 1
        else:
            new_id = execute_db(
                """
                INSERT INTO time_records (employee_id, record_type, record_time, notes, ip_origin, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (employee_id, record_type, new_value, "Criado por ajuste manual", get_client_ip(), now_iso()),
            )
            execute_db(
                """
                INSERT INTO time_adjustments (record_id, employee_id, admin_user_id, old_value, new_value, reason, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (new_id, employee_id, session["user_id"], "", new_value, reason, now_iso()),
            )
            changed += 1

    log_action("AJUSTE_JORNADA_DIA", f"{employee['name']} em {ref_date}. Alterações: {changed}. Motivo: {reason}")
    flash(f"Jornada ajustada com sucesso. Alterações aplicadas: {changed}.", "success")
    return redirect(url_for("records", ref_date=ref_date, employee_id=employee_id))

# =========================================================
# Fim registros profissionais
# =========================================================
'''

pattern = r'@app\.route\("/registros"\)\s*\n@admin_required\s*\ndef records\(\):.*?(?=\n@app\.route\("/registros/<int:record_id>/ajustar")'
if not re.search(pattern, text, flags=re.S):
    raise SystemExit("Não consegui localizar a rota antiga /registros no app.py. Envie o arquivo app.py atual para ajustar manualmente.")

text = re.sub(pattern, professional_block.strip(), text, flags=re.S)
APP.write_text(text, encoding="utf-8")

print("✅ Registros profissionais aplicados com sucesso.")
print(f"Backup do app.py: {backup_app}")
print(f"Template aplicado: {RECORDS_TEMPLATE}")
