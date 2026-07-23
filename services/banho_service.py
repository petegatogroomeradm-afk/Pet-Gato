from database import query_db, execute_db, now_iso

FLUXO_STATUS_BANHO = [
    "Agendado", "Aguardando coleta", "Em transporte", "Em atendimento",
    "Banho iniciado", "Banho concluído", "Secagem", "Tosa iniciada",
    "Tosa concluída", "Fotos", "Pagamento", "Em entrega", "Entregue", "Finalizado",
]

KANBAN_COLUNAS = [
    ("recepcao", "Recepção", ["Agendado", "Aguardando coleta", "Em transporte"]),
    ("aguardando", "Aguardando", ["Em atendimento"]),
    ("banho", "Banho", ["Banho iniciado"]),
    ("secagem", "Secagem", ["Banho concluído", "Secagem"]),
    ("tosa", "Tosa", ["Tosa iniciada", "Tosa concluída"]),
    ("fotos", "Fotos", ["Fotos"]),
    ("pagamento", "Pagamento", ["Pagamento"]),
    ("entrega", "Entrega", ["Em entrega", "Entregue"]),
    ("finalizados", "Finalizados", ["Finalizado"]),
    ("cancelado", "Cancelados", ["Cancelado"]),
]

def proximo_status(status_atual):
    atual = status_atual or "Agendado"
    if atual not in FLUXO_STATUS_BANHO:
        return "Agendado"
    indice = FLUXO_STATUS_BANHO.index(atual)
    return atual if indice == len(FLUXO_STATUS_BANHO)-1 else FLUXO_STATUS_BANHO[indice+1]

def obter_atendimento(atendimento_id):
    return query_db("""
        SELECT g.*, c.nome AS cliente_nome, c.whatsapp, p.nome AS pet_nome, p.foto AS pet_foto,
               e.name AS funcionario_nome, e.commission_rate, a.horario AS horario_agendado
        FROM grooming_services g
        LEFT JOIN clients c ON c.id=g.client_id
        LEFT JOIN pets p ON p.id=g.pet_id
        LEFT JOIN employees e ON e.id=g.employee_id
        LEFT JOIN appointments a ON a.id=g.appointment_id
        WHERE g.id=?
    """, (atendimento_id,), one=True)

def atualizar_status_atendimento(atendimento_id, novo_status):
    campos=["status=?", "updated_at=?"]; params=[novo_status, now_iso()]
    if novo_status in ("Em atendimento", "Banho iniciado"):
        campos.append("started_at=COALESCE(started_at, ?)"); params.append(now_iso())
    if novo_status in ("Finalizado", "Entregue"):
        campos.append("finished_at=COALESCE(finished_at, ?)"); params.append(now_iso())
    params.append(atendimento_id)
    execute_db(f"UPDATE grooming_services SET {', '.join(campos)} WHERE id=?", tuple(params))
    atendimento=query_db("SELECT appointment_id FROM grooming_services WHERE id=?",(atendimento_id,),one=True)
    if atendimento and atendimento["appointment_id"]:
        execute_db("UPDATE appointments SET status=?, updated_at=? WHERE id=?",(novo_status,now_iso(),atendimento["appointment_id"]))

def lancar_comissao(atendimento_id):
    atendimento=query_db("""SELECT g.id,g.employee_id,g.valor,e.commission_rate
        FROM grooming_services g LEFT JOIN employees e ON e.id=g.employee_id WHERE g.id=?""",(atendimento_id,),one=True)
    if not atendimento or not atendimento["employee_id"]:
        return False
    existe=query_db("SELECT id FROM employee_commissions WHERE grooming_id=? LIMIT 1",(atendimento_id,),one=True)
    if existe:
        return False
    taxa=float(atendimento["commission_rate"] or 0); valor=float(atendimento["valor"] or 0); total=round(valor*taxa/100,2)
    execute_db("""INSERT INTO employee_commissions
        (grooming_id,employee_id,rate,amount,status,created_at,updated_at) VALUES (?,?,?,?,?,?,?)""",
        (atendimento_id,atendimento["employee_id"],taxa,total,"Pendente",now_iso(),now_iso()))
    execute_db("UPDATE grooming_services SET commission_lancada=1, updated_at=? WHERE id=?",(now_iso(),atendimento_id))
    return True
