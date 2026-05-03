from pathlib import Path
import shutil

ROOT = Path.cwd()
APP = ROOT / "app.py"
TEMPLATES = ROOT / "templates"
PACKAGE = Path(__file__).resolve().parent

if not APP.exists():
    raise SystemExit("app.py não encontrado. Rode dentro da pasta principal do projeto.")

backup_app = ROOT / "app_backup_antes_registros_profissionais_corrigido.py"
backup_records = TEMPLATES / "records_backup_antes_profissional_corrigido.html"

shutil.copy2(APP, backup_app)
if (TEMPLATES / "records.html").exists():
    shutil.copy2(TEMPLATES / "records.html", backup_records)

shutil.copy2(PACKAGE / "app.py", APP)
TEMPLATES.mkdir(exist_ok=True)
shutil.copy2(PACKAGE / "records.html", TEMPLATES / "records.html")

print("✅ Registros profissionais corrigidos aplicados.")
print("Backup do app antigo:", backup_app)
print("Backup do template antigo:", backup_records)
