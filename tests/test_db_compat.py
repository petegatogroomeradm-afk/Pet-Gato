from core import db_compat


def test_auto_pk_sql_sqlite(monkeypatch):
    monkeypatch.setattr(db_compat, "is_postgres", lambda: False)
    assert db_compat.auto_pk_sql() == "INTEGER PRIMARY KEY AUTOINCREMENT"


def test_auto_pk_sql_postgres(monkeypatch):
    monkeypatch.setattr(db_compat, "is_postgres", lambda: True)
    assert db_compat.auto_pk_sql() == "SERIAL PRIMARY KEY"


def test_backend_name(monkeypatch):
    monkeypatch.setattr(db_compat, "is_postgres", lambda: True)
    assert db_compat.backend_name() == "postgresql"
