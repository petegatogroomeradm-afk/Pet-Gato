from pathlib import Path
import shutil

ROOT = Path.cwd()
APP = ROOT / "app.py"
TEMPLATES = ROOT / "templates"
REQ = ROOT / "requirements.txt"
SRC = Path(__file__).resolve().parent

if not APP.exists():
    raise SystemExit("ERRO: app.py não encontrado. Rode este script dentro da pasta do projeto.")

backup = ROOT / "backup_antes_checkout_pro"
backup.mkdir(exist_ok=True)
shutil.copy2(APP, backup / "app.py")
if (TEMPLATES / "subscription.html").exists():
    shutil.copy2(TEMPLATES / "subscription.html", backup / "subscription.html")
if (TEMPLATES / "blocked_subscription.html").exists():
    shutil.copy2(TEMPLATES / "blocked_subscription.html", backup / "blocked_subscription.html")

TEMPLATES.mkdir(exist_ok=True)
shutil.copy2(SRC / "app.py", APP)
shutil.copy2(SRC / "subscription.html", TEMPLATES / "subscription.html")
shutil.copy2(SRC / "blocked_subscription.html", TEMPLATES / "blocked_subscription.html")

if REQ.exists():
    req_text = REQ.read_text(encoding="utf-8")
    if "requests" not in req_text.lower():
        REQ.write_text(req_text.rstrip() + "\nrequests\n", encoding="utf-8")
else:
    REQ.write_text("Flask\npsycopg2-binary\nrequests\n", encoding="utf-8")

print("✅ Checkout Pro Mercado Pago aplicado com sucesso.")
print("Backup salvo em:", backup)
print("Agora rode: git add . && git commit -m \"Ativa Checkout Pro Mercado Pago\" && git push")
