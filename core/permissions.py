"""Controle de acesso por perfil (RBAC) do Pet & Gatô Business."""
from __future__ import annotations

from functools import wraps
from typing import Callable, Iterable

from flask import abort, jsonify, request, session

ROLE_LABELS = {
    "superadmin": "Super Administrador",
    "admin": "Administrador",
    "gerente": "Gerente",
    "recepcao": "Recepção",
    "banho_tosa": "Banho e Tosa",
    "motorista": "Motorista",
    "financeiro": "Financeiro",
    "estoque": "Estoque",
}

# Permissões por módulo. O administrador sempre possui acesso total.
ROLE_PERMISSIONS = {
    "superadmin": {"*"},
    "admin": {"*"},
    "gerente": {"dashboard", "clientes", "crm", "fidelidade", "pets", "agenda", "banho_tosa", "rh", "ponto", "pdv", "comunicacao", "financeiro", "estoque", "motorista", "relatorios", "configuracoes", "auditoria", "busca_global", "notificacoes"},
    "recepcao": {
        "dashboard", "clientes", "crm", "fidelidade", "pets", "agenda",
        "banho_tosa", "pdv", "comunicacao", "busca_global", "notificacoes",
    },
    "banho_tosa": {
        "dashboard", "pets", "agenda", "banho_tosa", "pdv", "comunicacao", "busca_global", "notificacoes",
    },
    "motorista": {"dashboard", "motorista", "busca_global", "notificacoes"},
    "financeiro": {
        "dashboard", "financeiro", "pdv", "relatorios", "busca_global", "notificacoes",
    },
    "estoque": {"dashboard", "estoque", "pdv", "busca_global", "notificacoes"},
}

# Blueprint/endpoint -> módulo de autorização.
BLUEPRINT_MODULES = {
    "clientes": "clientes",
    "crm": "crm",
    "fidelidade": "fidelidade",
    "pets": "pets",
    "agenda": "agenda",
    "banho_tosa": "banho_tosa",
    "funcionarios": "rh",
    "jornadas": "rh",
    "ponto": "ponto",
    "resumo_ponto": "rh",
    "rh": "rh",
    "financeiro": "financeiro",
    "comissoes": "financeiro",
    "estoque": "estoque",
    "pdv": "pdv",
    "comunicacao": "comunicacao",
    "motorista": "motorista",
    "configuracoes": "configuracoes",
    "relatorios": "relatorios",
    "auditoria": "auditoria",
}

ENDPOINT_MODULES = {
    "dashboard": "dashboard",
    "dashboard_api": "dashboard",
    "api_busca_global": "busca_global",
    "api_notificacoes": "notificacoes",
}


def normalize_role(role: str | None) -> str:
    value = (role or "").strip().lower()
    return value if value in ROLE_PERMISSIONS else "recepcao"


def is_superadmin(role: str | None = None) -> bool:
    return normalize_role(role if role is not None else session.get("role")) == "superadmin"


def has_permission(permission: str, role: str | None = None) -> bool:
    current_role = normalize_role(role if role is not None else session.get("role"))
    allowed = ROLE_PERMISSIONS.get(current_role, set())
    return "*" in allowed or permission in allowed


def module_for_request() -> str | None:
    endpoint = request.endpoint or ""
    if endpoint in ENDPOINT_MODULES:
        return ENDPOINT_MODULES[endpoint]
    blueprint = request.blueprint
    return BLUEPRINT_MODULES.get(blueprint) if blueprint else None


def deny_access():
    if request.path.startswith("/api/") or request.accept_mimetypes.best == "application/json":
        return jsonify({"erro": "acesso_negado", "mensagem": "Você não possui permissão para esta ação."}), 403
    abort(403)


def require_permission(permission: str):
    """Decorator para rotas que exigem uma permissão específica."""
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not has_permission(permission):
                return deny_access()
            return func(*args, **kwargs)
        return wrapper
    return decorator


def role_choices() -> Iterable[tuple[str, str]]:
    return ROLE_LABELS.items()
