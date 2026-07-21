from __future__ import annotations

import csv
import io
from datetime import date

from flask import Blueprint, Response, render_template, request

from database import query_db

relatorios_bp = Blueprint("relatorios", __name__)


def _periodo():
    hoje = date.today()
    inicio = request.args.get("inicio", hoje.replace(day=1).isoformat())
    fim = request.args.get("fim", hoje.isoformat())
    return inicio, fim


@relatorios_bp.route("/relatorios")
def relatorios():
    inicio, fim = _periodo()

    financeiro = query_db(
        """
        SELECT
            COALESCE(SUM(CASE WHEN type='Entrada' AND status='Pago' THEN amount ELSE 0 END),0) AS entradas,
            COALESCE(SUM(CASE WHEN type='Saída' AND status='Pago' THEN amount ELSE 0 END),0) AS saidas,
            COALESCE(SUM(CASE WHEN status IN ('Pendente','Vencido') AND type='Entrada' THEN amount ELSE 0 END),0) AS receber,
            COALESCE(SUM(CASE WHEN status IN ('Pendente','Vencido') AND type='Saída' THEN amount ELSE 0 END),0) AS pagar
        FROM financial_transactions
        WHERE transaction_date >= ?
          AND transaction_date <= ?
        """,
        (inicio, fim),
        one=True,
    )

    operacao = query_db(
        """
        SELECT
            COUNT(*) AS total,
            COALESCE(SUM(valor),0) AS valor,
            COALESCE(AVG(valor),0) AS ticket
        FROM grooming_services
        WHERE data >= ?
          AND data <= ?
          AND status NOT IN ('Cancelado')
        """,
        (inicio, fim),
        one=True,
    )

    servicos = query_db(
        """
        SELECT servico, COUNT(*) AS total, COALESCE(SUM(valor),0) AS valor
        FROM grooming_services
        WHERE data >= ?
          AND data <= ?
          AND status NOT IN ('Cancelado')
        GROUP BY servico
        ORDER BY total DESC
        """,
        (inicio, fim),
    )

    clientes_novos = query_db(
        """
        SELECT COUNT(*) AS total
        FROM clients
        WHERE created_at >= ?
          AND created_at < ?
        """,
        (f"{inicio} 00:00:00", f"{fim} 23:59:59"),
        one=True,
    )

    pets_novos = query_db(
        """
        SELECT COUNT(*) AS total
        FROM pets
        WHERE created_at >= ?
          AND created_at < ?
        """,
        (f"{inicio} 00:00:00", f"{fim} 23:59:59"),
        one=True,
    )

    estoque_baixo = query_db(
        """
        SELECT name, quantity, min_quantity, unit
        FROM stock_products
        WHERE COALESCE(active,1)=1
          AND COALESCE(quantity,0) <= COALESCE(min_quantity,0)
        ORDER BY quantity, name
        """
    )

    funcionarios = query_db(
        """
        SELECT
            e.id,
            e.name,
            COUNT(t.id) AS marcacoes
        FROM employees e
        LEFT JOIN time_records t
          ON t.employee_id=e.id
         AND t.record_time >= ?
         AND t.record_time < ?
        WHERE COALESCE(e.active,1)=1
        GROUP BY e.id, e.name
        ORDER BY e.name
        """,
        (f"{inicio} 00:00:00", f"{fim} 23:59:59"),
    )

    return render_template(
        "relatorios.html",
        inicio=inicio,
        fim=fim,
        financeiro=financeiro,
        operacao=operacao,
        servicos=servicos,
        clientes_novos=clientes_novos["total"] if clientes_novos else 0,
        pets_novos=pets_novos["total"] if pets_novos else 0,
        estoque_baixo=estoque_baixo,
        funcionarios=funcionarios,
    )


@relatorios_bp.route("/relatorios/financeiro.csv")
def exportar_financeiro():
    inicio, fim = _periodo()
    linhas = query_db(
        """
        SELECT
            transaction_date,
            type,
            category,
            description,
            amount,
            payment_method,
            status,
            due_date,
            source_type
        FROM financial_transactions
        WHERE transaction_date >= ?
          AND transaction_date <= ?
        ORDER BY transaction_date, id
        """,
        (inicio, fim),
    )

    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter=";")
    writer.writerow([
        "Data", "Tipo", "Categoria", "Descrição", "Valor",
        "Pagamento", "Status", "Vencimento", "Origem",
    ])
    for item in linhas:
        writer.writerow([
            item["transaction_date"],
            item["type"],
            item["category"],
            item["description"],
            f"{float(item['amount'] or 0):.2f}".replace(".", ","),
            item["payment_method"],
            item["status"],
            item["due_date"],
            item["source_type"],
        ])

    return Response(
        "\ufeff" + buffer.getvalue(),
        mimetype="text/csv; charset=utf-8",
        headers={
            "Content-Disposition":
                f"attachment; filename=financeiro_{inicio}_{fim}.csv"
        },
    )


@relatorios_bp.route("/relatorios/atendimentos.csv")
def exportar_atendimentos():
    inicio, fim = _periodo()
    linhas = query_db(
        """
        SELECT
            g.data,
            g.servico,
            g.status,
            g.valor,
            c.nome AS cliente,
            p.nome AS pet,
            e.name AS funcionario
        FROM grooming_services g
        LEFT JOIN clients c ON c.id=g.client_id
        LEFT JOIN pets p ON p.id=g.pet_id
        LEFT JOIN employees e ON e.id=g.employee_id
        WHERE g.data >= ?
          AND g.data <= ?
        ORDER BY g.data, g.id
        """,
        (inicio, fim),
    )

    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter=";")
    writer.writerow([
        "Data", "Cliente", "Pet", "Serviço",
        "Funcionário", "Status", "Valor",
    ])
    for item in linhas:
        writer.writerow([
            item["data"],
            item["cliente"],
            item["pet"],
            item["servico"],
            item["funcionario"],
            item["status"],
            f"{float(item['valor'] or 0):.2f}".replace(".", ","),
        ])

    return Response(
        "\ufeff" + buffer.getvalue(),
        mimetype="text/csv; charset=utf-8",
        headers={
            "Content-Disposition":
                f"attachment; filename=atendimentos_{inicio}_{fim}.csv"
        },
    )
