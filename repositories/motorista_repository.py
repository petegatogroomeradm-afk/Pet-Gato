from repositories.base_repository import BaseRepository


class MotoristaRepository(BaseRepository):
    table_name = "transport_services"


motorista_repository = MotoristaRepository()
