from __future__ import annotations

from database import query_db
from repositories.base_repository import BaseRepository


class ClienteRepository(BaseRepository):
    table_name = "clients"

    def active(self, search: str = ""):
        where = "WHERE COALESCE(ativo, 1) = 1"
        params = ()
        if search:
            term = f"%{search}%"
            where += " AND (nome LIKE ? OR telefone LIKE ? OR whatsapp LIKE ? OR cpf LIKE ? OR email LIKE ?)"
            params = (term, term, term, term, term)
        return query_db(f"SELECT * FROM clients {where} ORDER BY nome", params)


cliente_repository = ClienteRepository()
