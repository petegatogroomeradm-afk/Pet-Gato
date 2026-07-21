from datetime import date, datetime, timedelta
from urllib.parse import quote
from database import query_db

def _date(value):
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(str(value)[:10], fmt).date()
        except ValueError:
            pass
    return None

def _wa(phone, message):
    digits = "".join(c for c in str(phone or "") if c.isdigit())
    if not digits:
        return None
    if len(digits) in (10, 11):
        digits = "55" + digits
    return "https://wa.me/" + digits + "?text=" + quote(message)

def _birthday(value):
    born = _date(value)
    if not born:
        return None
    today = date.today()
    try:
        upcoming = born.replace(year=today.year)
    except ValueError:
        upcoming = date(today.year, 2, 28)
    if upcoming < today:
        try:
            upcoming = born.replace(year=today.year + 1)
        except ValueError:
            upcoming = date(today.year + 1, 2, 28)
    days = (upcoming - today).days
    return (upcoming, days) if days <= 30 else None

def get_dashboard(inactive_days=45):
    today = date.today()
    clients = query_db("SELECT * FROM clients WHERE COALESCE(ativo,1)=1 ORDER BY nome")
    pets = query_db("""SELECT p.*, c.nome client_name, c.whatsapp, c.telefone
                       FROM pets p LEFT JOIN clients c ON c.id=p.client_id
                       WHERE COALESCE(p.ativo,1)=1 ORDER BY p.nome""")
    last_rows = query_db("""SELECT client_id, MAX(COALESCE(data,created_at)) last_service,
                            COUNT(*) services_count, COALESCE(SUM(valor),0) total_spent,
                            COALESCE(AVG(valor),0) average_ticket
                            FROM grooming_services GROUP BY client_id""")
    stats = {r["client_id"]: r for r in last_rows}

    inactive, vip, birthdays, pet_birthdays = [], [], [], []
    for c in clients:
        s = stats.get(c["id"])
        last = _date(s["last_service"]) if s else None
        days = (today-last).days if last else None
        total = float(s["total_spent"] or 0) if s else 0
        count = int(s["services_count"] or 0) if s else 0
        avg = float(s["average_ticket"] or 0) if s else 0
        item = {"id":c["id"],"name":c["nome"],"phone":c["whatsapp"] or c["telefone"],
                "tags":c["tags"] or "","last_service":s["last_service"] if s else None,
                "days_without_return":days,"total_spent":total,
                "services_count":count,"average_ticket":avg}
        if days is None or days >= inactive_days:
            names = [p["nome"] for p in pets if p["client_id"] == c["id"]]
            msg = f"Olá, {c['nome']}! Sentimos sua falta na Pet & Gatô. Que tal agendar o próximo cuidado de {', '.join(names) if names else 'seu pet'}?"
            item["whatsapp_url"] = _wa(item["phone"], msg)
            inactive.append(item)
        if total >= 500 or count >= 8 or "VIP" in item["tags"].upper():
            vip.append(item)
        b = _birthday(c["data_nascimento"])
        if b:
            upcoming, distance = b
            item2 = dict(item)
            item2.update({"next_date":upcoming.isoformat(),"days":distance,
                          "whatsapp_url":_wa(item["phone"],f"Olá, {c['nome']}! A equipe Pet & Gatô deseja um feliz aniversário! 🎉")})
            birthdays.append(item2)

    for p in pets:
        b = _birthday(p["data_nascimento"])
        if b:
            upcoming, distance = b
            pet_birthdays.append({
                "pet_id":p["id"],"client_id":p["client_id"],"pet_name":p["nome"],
                "client_name":p["client_name"],"next_date":upcoming.isoformat(),"days":distance,
                "whatsapp_url":_wa(p["whatsapp"] or p["telefone"],
                    f"Olá, {p['client_name']}! O aniversário de {p['nome']} está chegando. Muitas alegrias e lambeijos! 🐾🎂")
            })

    limit = (today + timedelta(days=30)).isoformat()
    vaccines_raw = query_db("""SELECT v.*, p.nome pet_name, p.client_id,
                               c.nome client_name, c.whatsapp, c.telefone
                               FROM pet_vaccines v
                               LEFT JOIN pets p ON p.id=v.pet_id
                               LEFT JOIN clients c ON c.id=p.client_id
                               WHERE v.proxima_dose IS NOT NULL AND v.proxima_dose<>''
                               AND v.proxima_dose<=? ORDER BY v.proxima_dose""",(limit,))
    vaccines = []
    for v in vaccines_raw:
        due = _date(v["proxima_dose"])
        if not due:
            continue
        d = (due-today).days
        msg = f"Olá, {v['client_name']}! A vacina {v['vacina']} de {v['pet_name']} está {'vencida' if d < 0 else 'próxima do vencimento'} ({v['proxima_dose']})."
        vaccines.append({"pet_id":v["pet_id"],"client_id":v["client_id"],"pet_name":v["pet_name"],
                         "client_name":v["client_name"],"vaccine":v["vacina"],"due_date":v["proxima_dose"],
                         "status":"Vencida" if d < 0 else "Próxima","days":d,
                         "whatsapp_url":_wa(v["whatsapp"] or v["telefone"],msg)})

    tasks = query_db("""SELECT t.*, c.nome client_name, p.nome pet_name
                        FROM crm_tasks t LEFT JOIN clients c ON c.id=t.client_id
                        LEFT JOIN pets p ON p.id=t.pet_id
                        ORDER BY CASE t.status WHEN 'Pendente' THEN 0 ELSE 1 END,
                        COALESCE(t.due_date,'9999-12-31'), t.id DESC LIMIT 100""")
    contacts = query_db("""SELECT h.*, c.nome client_name, p.nome pet_name
                           FROM crm_contact_history h LEFT JOIN clients c ON c.id=h.client_id
                           LEFT JOIN pets p ON p.id=h.pet_id
                           ORDER BY h.created_at DESC, h.id DESC LIMIT 30""")
    inactive.sort(key=lambda x: (-(x["days_without_return"] or 99999),x["name"]))
    vip.sort(key=lambda x: (-x["total_spent"],x["name"]))
    return {"clients":clients,"pets":pets,"inactive_clients":inactive[:50],"vip_clients":vip[:30],
            "birthdays":sorted(birthdays,key=lambda x:x["days"]),
            "pet_birthdays":sorted(pet_birthdays,key=lambda x:x["days"]),
            "vaccine_alerts":vaccines,"tasks":tasks,"contacts":contacts,
            "metrics":{"active_clients":len(clients),"active_pets":len(pets),
                       "inactive_clients":len(inactive),"vip_clients":len(vip),
                       "birthdays":len(birthdays)+len(pet_birthdays),
                       "vaccine_alerts":len(vaccines),
                       "pending_tasks":sum(1 for t in tasks if t["status"]=="Pendente")}}
