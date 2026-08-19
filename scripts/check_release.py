from __future__ import annotations

import compileall
import sys
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REQUIRED = [
    ROOT / "main.py",
    ROOT / "database.py",
    ROOT / "modules" / "configuracoes.py",
    ROOT / "modules" / "auditoria.py",
    ROOT / "templates" / "usuarios.html",
    ROOT / "templates" / "auditoria.html",
    ROOT / "templates" / "pets_saude.html",
    ROOT / "static" / "css" / "pets_enterprise.css",
    ROOT / "static" / "css" / "v12_design_system.css",
    ROOT / "core" / "permissions.py",
    ROOT / "scripts" / "go_live_check.py",
]

def main() -> int:
    missing = [str(p.relative_to(ROOT)) for p in REQUIRED if not p.exists()]
    if missing:
        print("ARQUIVOS AUSENTES:")
        for item in missing:
            print(f" - {item}")
        return 1
    database_text = (ROOT / "database.py").read_text(encoding="utf-8")
    finance_requirements = [
        "financial_category_budgets",
        "reconciliation_status",
        "reconciled_at",
        "bank_reference",
    ]
    missing_finance = [item for item in finance_requirements if item not in database_text]
    if missing_finance:
        print("SCHEMA FINANCEIRO INCOMPLETO:")
        for item in missing_finance:
            print(f" - {item}")
        return 1

    ok = compileall.compile_dir(str(ROOT), quiet=1, rx=re.compile(r"[\/]\.venv[\/]"))
    if not ok:
        print("Falha na compilação dos arquivos Python.")
        return 1
    route_check = subprocess.run([sys.executable, str(ROOT / "scripts" / "check_templates_routes.py")], cwd=ROOT)
    if route_check.returncode != 0:
        print("Falha na validação de rotas e templates.")
        return 1
    version = (ROOT / "VERSION").read_text(encoding="utf-8").strip() if (ROOT / "VERSION").exists() else "desconhecida"
    print(f"Release {version} validada: Python, templates e rotas conferidos.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
