from __future__ import annotations

from database import query_db
from repositories.base_repository import BaseRepository


class PetRepository(BaseRepository):
    table_name = "pets"

    def active(self, search: str = ""):
        where = "WHERE COALESCE(p.ativo, 1) = 1"
        params = ()
        if search:
            term = f"%{search}%"
            where += " AND (p.nome LIKE ? OR c.nome LIKE ? OR p.raca LIKE ? OR p.especie LIKE ?)"
            params = (term, term, term, term)
        return query_db(
            f"""SELECT p.*, c.nome AS cliente_nome FROM pets p
            LEFT JOIN clients c ON c.id = p.client_id
            {where} ORDER BY p.nome""",
            params,
        )


pet_repository = PetRepository()
