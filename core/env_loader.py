"""Carregamento simples e opcional do arquivo .env do projeto.

Não sobrescreve variáveis já definidas no Windows/Render.
"""
from __future__ import annotations

import os
from pathlib import Path


def load_project_env(project_root: Path | None = None) -> Path | None:
    root = project_root or Path(__file__).resolve().parent.parent
    env_path = root / ".env"
    if not env_path.exists():
        return None

    for raw_line in env_path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)
    return env_path
