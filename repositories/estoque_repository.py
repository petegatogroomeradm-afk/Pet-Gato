from repositories.base_repository import BaseRepository


class EstoqueRepository(BaseRepository):
    table_name = "stock_products"


estoque_repository = EstoqueRepository()
