from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def configure_logging(base_dir: Path):
    logs_dir = base_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    target = str(logs_dir / "application.log")
    if not any(getattr(h, "baseFilename", None) == target for h in root.handlers):
        handler = RotatingFileHandler(
            target, maxBytes=2_000_000, backupCount=5, encoding="utf-8"
        )
        handler.setFormatter(formatter)
        root.addHandler(handler)

    error_target = str(logs_dir / "errors.log")
    if not any(getattr(h, "baseFilename", None) == error_target for h in root.handlers):
        error_handler = RotatingFileHandler(
            error_target, maxBytes=2_000_000, backupCount=5, encoding="utf-8"
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        root.addHandler(error_handler)
