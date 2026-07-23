from __future__ import annotations

from database import execute_db, insert_db, now_iso, query_db

IGNORED_APPOINTMENT_STATUSES = {"Aguardando aprovação", "Recusado"}


def _grooming_status(appointment_status: str | None) -> str:
    status = (appointment_status or "Agendado").strip()
    operational_statuses = {
        "Agendado", "Aguardando coleta", "Em transporte", "Em atendimento",
        "Banho iniciado", "Banho concluído", "Secagem", "Tosa iniciada",
        "Tosa concluída", "Fotos", "Pagamento", "Em entrega", "Entregue",
        "Finalizado", "Cancelado",
    }
    if status in operational_statuses:
        return status
    mapping = {
        "Agendado": "Agendado",
        "Confirmado": "Agendado",
        "Reagendado": "Agendado",
        "Na loja": "Agendado",
        "Em atendimento": "Em atendimento",
        "Pronto": "Em entrega",
        "Entregue": "Entregue",
        "Finalizado": "Finalizado",
        "Cancelado": "Cancelado",
    }
    return mapping.get(status, "Agendado")


def sincronizar_agendamento_com_banho(appointment_id: int, *, criar: bool = True):
    """Cria ou atualiza o atendimento de banho/tosa vinculado a um agendamento."""
    appointment = query_db("SELECT * FROM appointments WHERE id=?", (appointment_id,), one=True)
    if not appointment:
        return None

    status_agenda = (appointment["status"] or "Agendado").strip()
    existente = query_db(
        "SELECT id FROM grooming_services WHERE appointment_id=?",
        (appointment_id,),
        one=True,
    )

    if status_agenda in IGNORED_APPOINTMENT_STATUSES:
        return existente["id"] if existente else None

    status_banho = _grooming_status(status_agenda)
    valores = (
        appointment["client_id"],
        appointment["pet_id"],
        appointment["employee_id"],
        appointment["data_agendamento"],
        appointment["horario"],
        appointment["servico"],
        appointment["valor"] or 0,
        status_banho,
        appointment["observacoes"] or "",
        now_iso(),
    )

    if existente:
        execute_db(
            """
            UPDATE grooming_services
               SET client_id=?, pet_id=?, employee_id=?, data=?, hora_entrada=?,
                   servico=?, valor=?, status=?, observacoes=?, updated_at=?
             WHERE id=?
            """,
            valores + (existente["id"],),
        )
        return existente["id"]

    if not criar:
        return None

    return insert_db(
        """
        INSERT INTO grooming_services
        (appointment_id, client_id, pet_id, employee_id, data, hora_entrada,
         servico, valor, status, observacoes, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            appointment_id,
            appointment["client_id"],
            appointment["pet_id"],
            appointment["employee_id"],
            appointment["data_agendamento"],
            appointment["horario"],
            appointment["servico"],
            appointment["valor"] or 0,
            status_banho,
            appointment["observacoes"] or "",
            now_iso(),
            now_iso(),
        ),
    )


def sincronizar_agendamentos_existentes() -> int:
    """Repara vínculos antigos e mantém Agenda e Banho & Tosa alinhados."""
    rows = query_db(
        """
        SELECT id
          FROM appointments
         WHERE COALESCE(status,'Agendado') NOT IN ('Aguardando aprovação','Recusado')
         ORDER BY id
        """
    )
    total = 0
    for row in rows:
        if sincronizar_agendamento_com_banho(row["id"], criar=True):
            total += 1
    return total
