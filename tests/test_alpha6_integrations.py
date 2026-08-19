def test_integration_service_imports():
    from services.integration_service import obter_integridade_integracoes, reprocessar_atendimento
    assert callable(obter_integridade_integracoes)
    assert callable(reprocessar_atendimento)


def test_routes_registered():
    from main import app
    rules = {rule.rule for rule in app.url_map.iter_rules()}
    assert "/sistema/integracoes" in rules
    assert "/sistema/integracoes/reprocessar/<int:atendimento_id>" in rules
    assert "/sistema/integracoes/reprocessar-pendencias" in rules
