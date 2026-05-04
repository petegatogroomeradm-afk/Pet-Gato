from pathlib import Path
import shutil

ROOT = Path.cwd()
APP = ROOT / "app.py"
TEMPLATES = ROOT / "templates"
SRC = Path(__file__).resolve().parent

if not APP.exists():
    raise SystemExit("ERRO: app.py não encontrado. Rode este script dentro da pasta do projeto.")

backup = ROOT / "backup_antes_cobranca_acesso"
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

print("✅ Cobrança e acesso por assinatura corrigidos com sucesso.")
print("Backup salvo em:", backup)
print("Agora rode: git add . && git commit -m \"Corrige acesso por assinatura\" && git push")
