from database import execute_db, now_iso


def registrar_historico(grooming_id, evento, usuario="Sistema"):
    execute_db("""
        INSERT INTO grooming_history
        (grooming_id, event, user_name, created_at)
        VALUES (?, ?, ?, ?)
    """, (
        grooming_id,
        evento,
        usuario,
        now_iso()
    ))