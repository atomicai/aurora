import contextlib
from pathlib import Path

from loguru import logger

from aurora.configuring.prime import Config


def _ensure_log_dir(log_path: str) -> None:
    path = Path(log_path)
    log_dir = path.parent
    if not log_dir.exists():
        log_dir.mkdir(parents=True, exist_ok=True)
    if not log_dir.is_dir():
        raise RuntimeError(f"Log path parent is not a directory: {log_dir}")
    with contextlib.suppress(PermissionError):
        log_dir.chmod(0o775)


_ensure_log_dir(Config.loguru["LOG_FILE_NAME"])

logger.add(
    Config.loguru["LOG_FILE_NAME"],
    rotation=Config.loguru["LOG_ROTATION"],
    retention=Config.loguru["LOG_RETENTION"],
)
