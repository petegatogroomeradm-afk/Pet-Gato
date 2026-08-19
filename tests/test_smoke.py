"""Testes básicos de inicialização e proteção das rotas."""
import unittest

import main


class SmokeTests(unittest.TestCase):
    def setUp(self):
        main.app.config.update(TESTING=True)
        self.client = main.app.test_client()

    def test_health(self):
        response = self.client.get('/health')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()['status'], 'ok')


    def test_health_exposes_version(self):
        response = self.client.get('/health')
        payload = response.get_json()
        self.assertEqual(payload['version'], main.APP_VERSION)

    def test_dashboard_contract_is_registered(self):
        rules = {rule.rule for rule in main.app.url_map.iter_rules()}
        self.assertIn('/dashboard', rules)
        self.assertIn('/api/dashboard', rules)

    def test_readiness(self):
        response = self.client.get('/readiness')
        self.assertEqual(response.status_code, 200)

    def test_login_page(self):
        response = self.client.get('/login')
        self.assertEqual(response.status_code, 200)

    def test_private_route_redirects_without_login(self):
        response = self.client.get('/financeiro')
        self.assertEqual(response.status_code, 302)
        self.assertIn('/login', response.headers['Location'])

    def test_api_requires_authentication(self):
        response = self.client.get('/api/dashboard')
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.get_json()['erro'], 'autenticacao_necessaria')


if __name__ == '__main__':
    unittest.main()
