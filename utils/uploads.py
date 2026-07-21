from pathlib import Path
from uuid import uuid4
from werkzeug.utils import secure_filename

ALLOWED_IMAGE_EXTENSIONS = {"png", "jpg", "jpeg", "webp"}
ALLOWED_DOCUMENT_EXTENSIONS = ALLOWED_IMAGE_EXTENSIONS | {"pdf", "doc", "docx"}


def save_upload(file_storage, destination: Path, allowed_extensions=None):
    if not file_storage or not file_storage.filename:
        return None
    original = secure_filename(file_storage.filename)
    if "." not in original:
        raise ValueError("Arquivo sem extensão.")
    extension = original.rsplit(".", 1)[1].lower()
    allowed = allowed_extensions or ALLOWED_DOCUMENT_EXTENSIONS
    if extension not in allowed:
        raise ValueError("Tipo de arquivo não permitido.")
    destination.mkdir(parents=True, exist_ok=True)
    filename = f"{uuid4().hex}.{extension}"
    file_storage.save(destination / filename)
    return filename
