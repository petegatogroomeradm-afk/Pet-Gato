from pathlib import Path
import shutil
import re

ROOT = Path.cwd()
APP = ROOT / "app.py"
TEMPLATES = ROOT / "templates"

BLOCK_START = "# ===========================\n# Notificações profissionais\n# ==========================="
BLOCK_END_MARKER = "@app.context_processor"

if not APP.exists():
    raise SystemExit("app.py não encontrado. Rode este script na raiz do projeto.")

TEMPLATES.mkdir(exist_ok=True)
shutil.copy2(Path(__file__).resolve().parent / "notificacoes.html", TEMPLATES / "notificacoes.html")

app_text = APP.read_text(encoding="utf-8")

# Remove bloco antigo se já existir
if BLOCK_START in app_text:
    start = app_text.index(BLOCK_START)
    end = app_text.index(BLOCK_END_MARKER, start)
    app_text = app_text[:start] + app_text[end:]

block = (Path(__file__).resolve().parent / "notificacoes_block.txt").read_text(encoding="utf-8")

if BLOCK_END_MARKER not in app_text:
    raise SystemExit("Não encontrei @app.context_processor no app.py para inserir o módulo.")

app_text = app_text.replace(BLOCK_END_MARKER, block + "\n\n" + BLOCK_END_MARKER, 1)

# Tenta adicionar link no menu do base.html se existir
base = TEMPLATES / "base.html"
if base.exists():
    base_text = base.read_text(encoding="utf-8")
    if "notifications_page" not in base_text:
        # Insere próximo de configurações, se houver; caso contrário, antes do fechamento do nav.
        link = '<a href="{{ url_for(\'notifications_page\') }}">Notificações</a>'
        if "settings_page" in base_text:
            base_text = re.sub(
                r'(<a[^>]+settings_page[^>]*>.*?</a>)',
                r'\1\n' + link,
                base_text,
                count=1,
                flags=re.DOTALL,
            )
        elif "</nav>" in base_text:
            base_text = base_text.replace("</nav>", link + "\n</nav>", 1)
        base.write_text(base_text, encoding="utf-8")

APP.write_text(app_text, encoding="utf-8")
print("Módulo de notificações aplicado com sucesso.")
print("Agora rode:")
print("git add .")
print('git commit -m "Adiciona notificacoes profissionais"')
print("git push")
