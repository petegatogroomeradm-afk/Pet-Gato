from __future__ import annotations

from database import execute_db, insert_db, query_db


class BaseRepository:
    table_name = ""

    def get(self, item_id: int):
        return query_db(
            f"SELECT * FROM {self.table_name} WHERE id = ?",
            (item_id,),
            one=True,
        )

    def all(self, order_by: str = "id DESC"):
        return query_db(f"SELECT * FROM {self.table_name} ORDER BY {order_by}")

    def delete(self, item_id: int):
        execute_db(f"DELETE FROM {self.table_name} WHERE id = ?", (item_id,))

    def insert(self, fields: dict):
        columns = list(fields)
        placeholders = ", ".join("?" for _ in columns)
        sql = (
            f"INSERT INTO {self.table_name} "
            f"({', '.join(columns)}) VALUES ({placeholders})"
        )
        return insert_db(sql, tuple(fields[column] for column in columns))

    def update(self, item_id: int, fields: dict):
        if not fields:
            return
        columns = list(fields)
        assignments = ", ".join(f"{column} = ?" for column in columns)
        execute_db(
            f"UPDATE {self.table_name} SET {assignments} WHERE id = ?",
            tuple(fields[column] for column in columns) + (item_id,),
        )
