from __future__ import annotations

import csv
import io
from datetime import date

from flask import Blueprint, Response, flash, redirect, render_template, request, session, url_for

from database import execute_db, insert_db, now_iso, query_db

comissoes_bp = Blueprint("comissoes", __name__, url_prefix="/financeiro/comissoes")

STATUS_VALIDOS = {"Pendente", "Aprovada", "Paga", "Cancelada"}


def _mes_atual() -> str:
    return date.today().strftime("%Y-%m")


def _valor(texto: str | None) -> float:
    valor = (texto or "0").strip().replace("R$", "").replace(" ", "")
    if "," in valor:
        valor = valor.replace(".", "").replace(",", ".")
    try:
        return round(float(valor), 2)
    except ValueError:
        return 0.0


def _filtros():
    mes = request.args.get("mes", _mes_atual()).strip() or _mes_atual()
    funcionario_id = request.args.get("funcionario_id", "").strip()
    status = request.args.get("status", "").strip()
    busca = request.args.get("busca", "").strip()
    return mes, funcionario_id, status, busca


def _where_comissoes(mes: str, funcionario_id: str, status: str, busca: str):
    clauses = ["substr(COALESCE(g.finished_at,g.data,c.created_at),1,7)=?"]
    params: list[object] = [mes]
    if funcionario_id:
        clauses.append("c.employee_id=?")
        params.append(funcionario_id)
    if status:
        clauses.append("c.status=?")
        params.append(status)
    if busca:
        clauses.append("(e.name LIKE ? OR p.nome LIKE ? OR cl.nome LIKE ? OR g.servico LIKE ?)")
        termo = f"%{busca}%"
        params.extend([termo, termo, termo, termo])
    return " AND ".join(clauses), tuple(params)


@comissoes_bp.route("/")
def painel():
    mes, funcionario_id, status, busca = _filtros()
    where, params = _where_comissoes(mes, funcionario_id, status, busca)

    comissoes = query_db(f"""
        SELECT c.*, e.name AS funcionario_nome, e.role_name,
               g.data AS atendimento_data, g.servico, g.valor AS valor_servico,
               p.nome AS pet_nome, cl.nome AS cliente_nome
        FROM employee_commissions c
        JOIN employees e ON e.id=c.employee_id
        LEFT JOIN grooming_services g ON g.id=c.grooming_id
        LEFT JOIN pets p ON p.id=g.pet_id
        LEFT JOIN clients cl ON cl.id=g.client_id
        WHERE {where}
        ORDER BY COALESCE(g.finished_at,g.data,c.created_at) DESC, c.id DESC
    """, params)

    resumo = query_db(f"""
        SELECT COUNT(*) AS total_lancamentos,
          COALESCE(SUM(c.amount),0) AS total_gerado,
          COALESCE(SUM(CASE WHEN c.status='Pendente' THEN c.amount ELSE 0 END),0) AS pendente,
          COALESCE(SUM(CASE WHEN c.status='Aprovada' THEN c.amount ELSE 0 END),0) AS aprovada,
          COALESCE(SUM(CASE WHEN c.status='Paga' THEN c.amount ELSE 0 END),0) AS paga,
          COALESCE(SUM(CASE WHEN c.status='Cancelada' THEN c.amount ELSE 0 END),0) AS cancelada
        FROM employee_commissions c
        LEFT JOIN grooming_services g ON g.id=c.grooming_id
        LEFT JOIN employees e ON e.id=c.employee_id
        LEFT JOIN pets p ON p.id=g.pet_id
        LEFT JOIN clients cl ON cl.id=g.client_id
        WHERE {where}
    """, params, one=True)

    ranking = query_db("""
        SELECT e.id, e.name, e.role_name, e.commission_rate,
               COUNT(c.id) AS atendimentos,
               COALESCE(SUM(CASE WHEN c.status<>'Cancelada' THEN c.amount ELSE 0 END),0) AS total,
               COALESCE(SUM(CASE WHEN c.status='Paga' THEN c.amount ELSE 0 END),0) AS pago
        FROM employees e
        LEFT JOIN employee_commissions c ON c.employee_id=e.id
          AND substr(c.created_at,1,7)=?
        WHERE e.active=1
        GROUP BY e.id,e.name,e.role_name,e.commission_rate
        ORDER BY total DESC,e.name
    """, (mes,))

    ajustes = query_db("""
        SELECT a.*,e.name AS funcionario_nome
        FROM employee_commission_adjustments a
        JOIN employees e ON e.id=a.employee_id
        WHERE a.reference_month=?
          AND (?='' OR CAST(a.employee_id AS TEXT)=?)
          AND (?='' OR a.status=?)
        ORDER BY a.id DESC
    """, (mes, funcionario_id, funcionario_id, status, status))

    total_ajustes = sum(float(a["amount"] or 0) for a in ajustes if a["status"] != "Cancelada")
    funcionarios = query_db("SELECT id,name,role_name,commission_rate FROM employees WHERE active=1 ORDER BY name")
    return render_template(
        "comissoes.html", comissoes=comissoes, resumo=resumo, ranking=ranking,
        ajustes=ajustes, total_ajustes=total_ajustes, funcionarios=funcionarios,
        mes=mes, funcionario_id=funcionario_id, status=status, busca=busca,
        status_opcoes=sorted(STATUS_VALIDOS),
    )


@comissoes_bp.route("/<int:comissao_id>/status", methods=["POST"])
def alterar_status(comissao_id: int):
    novo_status = request.form.get("status", "").strip()
    if novo_status not in STATUS_VALIDOS:
        flash("Status de comissão inválido.", "danger")
        return redirect(request.referrer or url_for("comissoes.painel"))
    campos = ["status=?", "updated_at=?"]
    params: list[object] = [novo_status, now_iso()]
    if novo_status == "Aprovada":
        campos += ["approved_at=?", "approved_by=?"]
        params += [now_iso(), session.get("user_name", "Sistema")]
    elif novo_status == "Paga":
        campos += ["approved_at=COALESCE(approved_at,?)", "paid_at=?", "paid_by=?"]
        params += [now_iso(), now_iso(), session.get("user_name", "Sistema")]
    params.append(comissao_id)
    execute_db(f"UPDATE employee_commissions SET {', '.join(campos)} WHERE id=?", tuple(params))
    flash(f"Comissão atualizada para {novo_status}.", "success")
    return redirect(request.referrer or url_for("comissoes.painel"))


@comissoes_bp.route("/lote", methods=["POST"])
def alterar_lote():
    novo_status = request.form.get("status", "").strip()
    ids = [int(v) for v in request.form.getlist("commission_ids") if v.isdigit()]
    if novo_status not in STATUS_VALIDOS or not ids:
        flash("Selecione ao menos uma comissão e uma ação válida.", "warning")
        return redirect(request.referrer or url_for("comissoes.painel"))
    marks = ",".join("?" for _ in ids)
    execute_db(f"UPDATE employee_commissions SET status=?,updated_at=? WHERE id IN ({marks})", (novo_status, now_iso(), *ids))
    if novo_status == "Aprovada":
        execute_db(f"UPDATE employee_commissions SET approved_at=?,approved_by=? WHERE id IN ({marks})", (now_iso(), session.get("user_name", "Sistema"), *ids))
    elif novo_status == "Paga":
        execute_db(f"UPDATE employee_commissions SET approved_at=COALESCE(approved_at,?),paid_at=?,paid_by=? WHERE id IN ({marks})", (now_iso(), now_iso(), session.get("user_name", "Sistema"), *ids))
    flash(f"{len(ids)} comissão(ões) atualizada(s) para {novo_status}.", "success")
    return redirect(request.referrer or url_for("comissoes.painel"))


@comissoes_bp.route("/ajuste", methods=["POST"])
def adicionar_ajuste():
    funcionario_id = request.form.get("employee_id", "").strip()
    mes = request.form.get("reference_month", _mes_atual()).strip()
    descricao = request.form.get("description", "").strip()
    valor = _valor(request.form.get("amount"))
    tipo = request.form.get("adjustment_type", "Bônus").strip()
    if not funcionario_id or not descricao or valor == 0:
        flash("Preencha funcionário, descrição e um valor diferente de zero.", "danger")
        return redirect(request.referrer or url_for("comissoes.painel"))
    insert_db("""INSERT INTO employee_commission_adjustments
      (employee_id,reference_month,adjustment_type,description,amount,status,notes,created_by,created_at,updated_at)
      VALUES (?,?,?,?,?,'Pendente',?,?,?,?)""",
      (funcionario_id,mes,tipo,descricao,valor,request.form.get("notes","").strip(),session.get("user_name","Sistema"),now_iso(),now_iso()))
    flash("Ajuste lançado com sucesso.", "success")
    return redirect(url_for("comissoes.painel", mes=mes, funcionario_id=funcionario_id))


@comissoes_bp.route("/ajuste/<int:ajuste_id>/status", methods=["POST"])
def alterar_status_ajuste(ajuste_id: int):
    novo_status = request.form.get("status", "").strip()
    if novo_status not in STATUS_VALIDOS:
        flash("Status inválido.", "danger")
        return redirect(request.referrer or url_for("comissoes.painel"))
    campos = ["status=?", "updated_at=?"]
    params: list[object] = [novo_status, now_iso()]
    if novo_status == "Aprovada": campos += ["approved_at=?"]; params += [now_iso()]
    if novo_status == "Paga": campos += ["approved_at=COALESCE(approved_at,?)", "paid_at=?"]; params += [now_iso(), now_iso()]
    params.append(ajuste_id)
    execute_db(f"UPDATE employee_commission_adjustments SET {', '.join(campos)} WHERE id=?", tuple(params))
    flash("Ajuste atualizado.", "success")
    return redirect(request.referrer or url_for("comissoes.painel"))


@comissoes_bp.route("/taxa/<int:employee_id>", methods=["POST"])
def atualizar_taxa(employee_id: int):
    taxa = max(0.0, min(100.0, _valor(request.form.get("commission_rate"))))
    execute_db("UPDATE employees SET commission_rate=?,updated_at=? WHERE id=?", (taxa, now_iso(), employee_id))
    flash("Percentual de comissão atualizado.", "success")
    return redirect(request.referrer or url_for("comissoes.painel"))


@comissoes_bp.route("/exportar.csv")
def exportar_csv():
    mes, funcionario_id, status, busca = _filtros()
    where, params = _where_comissoes(mes, funcionario_id, status, busca)
    rows = query_db(f"""SELECT c.id,e.name AS funcionario,g.data,g.servico,cl.nome AS cliente,p.nome AS pet,
      g.valor AS valor_servico,c.rate,c.amount,c.status,c.approved_at,c.paid_at
      FROM employee_commissions c JOIN employees e ON e.id=c.employee_id
      LEFT JOIN grooming_services g ON g.id=c.grooming_id LEFT JOIN clients cl ON cl.id=g.client_id
      LEFT JOIN pets p ON p.id=g.pet_id WHERE {where} ORDER BY g.data,c.id""", params)
    out = io.StringIO(); writer = csv.writer(out, delimiter=";")
    writer.writerow(["ID","Funcionário","Data","Serviço","Cliente","Pet","Valor do serviço","Taxa %","Comissão","Status","Aprovada em","Paga em"])
    for r in rows:
        writer.writerow([r["id"],r["funcionario"],r["data"],r["servico"],r["cliente"],r["pet"],f'{float(r["valor_servico"] or 0):.2f}'.replace('.',','),f'{float(r["rate"] or 0):.2f}'.replace('.',','),f'{float(r["amount"] or 0):.2f}'.replace('.',','),r["status"],r["approved_at"],r["paid_at"]])
    content = "\ufeff" + out.getvalue()
    return Response(content, mimetype="text/csv; charset=utf-8", headers={"Content-Disposition": f'attachment; filename="comissoes_{mes}.csv"'})
