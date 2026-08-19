import importlib

ESSENTIAL_ENDPOINTS = {
    "dashboard", "clientes.clientes", "pets.pets", "agenda.agenda",
    "banho_tosa.banho_tosa", "financeiro.financeiro", "estoque.estoque",
    "crm.dashboard", "fidelidade.dashboard", "sistema.status"
}

def test_essential_routes_registered():
    app = importlib.import_module("main").app
    endpoints = {rule.endpoint for rule in app.url_map.iter_rules()}
    missing = ESSENTIAL_ENDPOINTS - endpoints
    assert not missing, f"Rotas essenciais ausentes: {sorted(missing)}"

def test_health_endpoint():
    app = importlib.import_module("main").app
    client = app.test_client()
    response = client.get("/health")
    assert response.status_code == 200
    payload = response.get_json()
    from core.version import APP_VERSION
    assert payload["version"] == APP_VERSION
