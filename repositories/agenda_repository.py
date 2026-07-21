from repositories.base_repository import BaseRepository


class AgendaRepository(BaseRepository):
    table_name = "appointments"


agenda_repository = AgendaRepository()
