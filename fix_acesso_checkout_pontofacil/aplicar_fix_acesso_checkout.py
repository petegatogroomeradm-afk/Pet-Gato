from pathlib import Path
import shutil
import re

ROOT = Path.cwd()
APP = ROOT / 'app.py'
SERVER = ROOT / 'server.py'

if not APP.exists():
    raise SystemExit('ERRO: app.py não encontrado. Rode este script dentro da pasta do projeto.')

backup = ROOT / 'backup_antes_fix_acesso_checkout'
backup.mkdir(exist_ok=True)
shutil.copy2(APP, backup / 'app.py')
if SERVER.exists():
    shutil.copy2(SERVER, backup / 'server.py')

app = APP.read_text(encoding='utf-8')

new_security_gate = r'''@app.before_request
def security_gate():
    """Segurança corrigida para SaaS + Checkout Pro.

    - /ponto e / continuam protegidos por IP da loja ou chave admin.
    - /login, /assinatura, /assinatura/pagar, /webhook/mercadopago e /pagamento/* ficam livres do bloqueio por IP/chave.
      A proteção de login continua sendo feita pelo @admin_required nas rotas internas.
    - Painel administrativo de clientes usa sessão/login, não bloqueio por IP.
    """
    allowed_endpoints = {"static", "service_worker", "health"}
    if request.endpoint in allowed_endpoints:
        return None

    public_prefixes = (
        "/login",
        "/assinatura",
        "/assinatura/pagar",
        "/webhook/mercadopago",
        "/pagamento",
    )
    if request.path.startswith(public_prefixes):
        return None

    # Tela de ponto pública da loja: só libera na rede autorizada ou com chave segura.
    if request.path.startswith("/ponto") or request.path == "/":
        if not is_store_network() and not has_admin_key():
            return render_template("blocked.html", ip=get_client_ip()), 403

    return None
'''

pattern = r'@app\.before_request\s*def security_gate\(\):.*?\n# ---------------------------\n# Database helpers'
replacement = new_security_gate + '\n\n# ---------------------------\n# Database helpers'
app, count = re.subn(pattern, replacement, app, count=1, flags=re.S)
if count != 1:
    raise SystemExit('ERRO: Não consegui localizar a função security_gate no app.py.')

APP.write_text(app, encoding='utf-8')

# Garante import robusto no Render
SERVER.write_text('''import os\nimport sys\n\nsys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))\n\nfrom app import app\n\nif __name__ == "__main__":\n    port = int(os.environ.get("PORT", 8080))\n    app.run(host="0.0.0.0", port=port)\n''', encoding='utf-8')

print('✅ Fix aplicado com sucesso.')
print('Backup salvo em:', backup)
print('IMPORTANTE no Render: a KEY precisa ser MP_ACCESS_TOKEN e o VALUE precisa ser o Access Token APP_USR-...')
print('Agora rode: git add app.py server.py && git commit -m "Corrige acesso checkout e import app" && git push')
