"""Remove somente arquivos residuais conhecidos de pacotes V11 incompletos.

Não altera banco, .env, uploads, backups ou logs.
"""
from pathlib import Path
import shutil

ROOT = Path(__file__).resolve().parents[1]
TARGETS = [
    ROOT / "modules" / "sistema",
]

print("=" * 60)
print("LIMPEZA SEGURA DE ATUALIZAÇÕES ANTIGAS")
print("=" * 60)
for target in TARGETS:
    if target.exists():
        shutil.rmtree(target)
        print(f"Removido resíduo incompatível: {target.relative_to(ROOT)}")
    else:
        print(f"Sem resíduo: {target.relative_to(ROOT)}")
print("Banco, .env, uploads, backups e logs foram preservados.")
