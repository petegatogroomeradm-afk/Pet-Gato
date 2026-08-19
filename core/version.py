"""Informações centralizadas de versão do Pet & Gatô Business."""
from pathlib import Path

APP_NAME = "Pet & Gatô Business"
FALLBACK_VERSION = "12.0.0"
APP_RELEASE = "V12 Stable"


def get_version() -> str:
    version_file = Path(__file__).resolve().parents[1] / "VERSION"
    try:
        value = version_file.read_text(encoding="utf-8").strip()
        return value or FALLBACK_VERSION
    except OSError:
        return FALLBACK_VERSION


APP_VERSION = get_version()


def version_info():
    return {"version": APP_VERSION, "release": APP_RELEASE, "name": APP_NAME}
