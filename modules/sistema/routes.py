from datetime import datetime
from flask import Blueprint, render_template, current_app
from database import query_db
from core.module_registry import get_modules, get_route_groups
from core.version import version_info

sistema_bp = Blueprint("sistema", __name__, url_prefix="/sistema")


@sistema_bp.route("/status")
def status():
    checks = []
    try:
        query_db("SELECT 1", one=True)
        checks.append({"nome": "Banco de dados", "status": "ok", "detalhe": "Conexão validada"})
    except Exception as exc:
        checks.append({"nome": "Banco de dados", "status": "erro", "detalhe": str(exc)})

    rules = list(current_app.url_map.iter_rules())
    endpoints = {rule.endpoint for rule in rules}

    modules = get_modules()
    for module in modules:
        module["disponivel"] = module["endpoint"] in endpoints

    route_groups = get_route_groups()
    total_expected = 0
    total_available = 0
    for group in route_groups:
        route_items = []
        for label, endpoint in group["routes"]:
            available = endpoint in endpoints
            route_items.append({"label": label, "endpoint": endpoint, "disponivel": available})
            total_expected += 1
            total_available += int(available)
        group["routes"] = route_items
        group["available_count"] = sum(1 for item in route_items if item["disponivel"])
        group["total_count"] = len(route_items)
        group["integridade"] = "ok" if group["available_count"] == group["total_count"] else "erro"

    checks.append(
        {
            "nome": "Integridade das abas críticas",
            "status": "ok" if total_available == total_expected else "erro",
            "detalhe": f"{total_available} de {total_expected} rotas internas disponíveis",
        }
    )

    return render_template(
        "sistema/status.html",
        app_info=version_info(),
        checks=checks,
        modules=modules,
        route_groups=route_groups,
        route_count=len(rules),
        generated_at=datetime.now(),
    )
