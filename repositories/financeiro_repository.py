from repositories.base_repository import BaseRepository


class FinanceiroRepository(BaseRepository):
    table_name = "financial_transactions"


financeiro_repository = FinanceiroRepository()
