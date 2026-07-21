from collections import defaultdict
from datetime import date, datetime

from flask import Blueprint, render_template, request

from database import query_db

resumo_ponto_bp = Blueprint("resumo_ponto", __name__)


def _minutos(hora):
    if not hora or hora == "-":
        return None
    try:
        h, m = hora[-8:-3].split(":") if " " in hora else hora[:5].split(":")
        return int(h) * 60 + int(m)
    except Exception:
        return None


@resumo_ponto_bp.route("/rh/resumo-ponto")
def resumo_ponto():
    data_filtro = request.args.get("data", date.today().isoformat())
    funcionario = request.args.get("funcionario", "")
    params = [f"{data_filtro}%"]
    extra = ""
    if funcionario:
        extra = " AND e.id = ?"
        params.append(funcionario)
    registros = query_db(f"""
        SELECT e.id employee_id, e.name employee_name, e.registration, e.schedule_id,
               w.entrada prevista_entrada, w.saida_intervalo prevista_intervalo,
               w.retorno_intervalo previsto_retorno, w.saida_final prevista_saida,
               tr.id record_id, tr.record_type, tr.record_time, tr.notes
        FROM employees e
        LEFT JOIN work_schedules w ON w.id=e.schedule_id
        LEFT JOIN time_records tr ON tr.employee_id=e.id AND tr.record_time LIKE ?
        WHERE e.active=1 {extra}
        ORDER BY e.name, tr.record_time
    """, tuple(params))
    mapa = {}
    for r in registros:
        eid = r["employee_id"]
        if eid not in mapa:
            mapa[eid] = {"id": eid, "name": r["employee_name"], "registration": r["registration"], "entrada": "-", "saida_intervalo": "-", "retorno_intervalo": "-", "saida": "-", "horas": 0, "saldo": 0}
        tipo = r["record_type"]
        if tipo == "Entrada": mapa[eid]["entrada"] = r["record_time"]
        elif tipo == "Saída intervalo": mapa[eid]["saida_intervalo"] = r["record_time"]
        elif tipo == "Retorno intervalo": mapa[eid]["retorno_intervalo"] = r["record_time"]
        elif tipo == "Saída": mapa[eid]["saida"] = r["record_time"]
        mapa[eid]["prevista_entrada"] = r["prevista_entrada"]
        mapa[eid]["prevista_saida"] = r["prevista_saida"]

    for item in mapa.values():
        e, si, ri, s = map(_minutos, [item["entrada"], item["saida_intervalo"], item["retorno_intervalo"], item["saida"]])
        trabalhado = 0
        if e is not None and si is not None: trabalhado += max(0, si-e)
        if ri is not None and s is not None: trabalhado += max(0, s-ri)
        elif e is not None and s is not None: trabalhado = max(0, s-e)
        item["horas"] = trabalhado
        pe, ps = _minutos(item.get("prevista_entrada")), _minutos(item.get("prevista_saida"))
        previsto = max(0, ps-pe-60) if pe is not None and ps is not None else 480
        item["saldo"] = trabalhado-previsto if trabalhado else 0

    ausencias = query_db("""SELECT a.*, e.name employee_name FROM employee_absences a LEFT JOIN employees e ON e.id=a.employee_id WHERE a.start_date<=? AND COALESCE(NULLIF(a.end_date,''),a.start_date)>=? ORDER BY e.name""", (data_filtro, data_filtro))
    funcionarios = query_db("SELECT id,name,registration FROM employees WHERE active=1 ORDER BY name")
    return render_template("resumo_ponto.html", resumo=mapa.values(), data=data_filtro, funcionario=funcionario, funcionarios=funcionarios, ausencias=ausencias)
