from __future__ import annotations
from datetime import date, datetime, timedelta
from database import get_conn, adapt_query, now_iso, query_db, is_postgres


def _row_value(row, key, index=0):
    if hasattr(row, "keys"):
        return row[key]
    return row[index]


def get_settings():
    row = query_db("SELECT * FROM loyalty_settings ORDER BY id LIMIT 1", one=True)
    if row:
        return row
    return {
        "points_per_real": 1, "cashback_bronze": 0, "cashback_prata": 1,
        "cashback_ouro": 2, "cashback_diamante": 3, "prata_min": 300,
        "ouro_min": 1000, "diamante_min": 2500, "points_validity_days": 365,
    }


def level_for(lifetime_points, settings=None):
    s = settings or get_settings()
    total = int(lifetime_points or 0)
    if total >= int(s["diamante_min"] or 0): return "Diamante"
    if total >= int(s["ouro_min"] or 0): return "Ouro"
    if total >= int(s["prata_min"] or 0): return "Prata"
    return "Bronze"


def cashback_rate(level, settings=None):
    s = settings or get_settings()
    return float(s[f"cashback_{str(level).lower()}"] or 0)


def ensure_account(client_id, conn=None):
    own = conn is None
    conn = conn or get_conn()
    cur = conn.cursor()
    try:
        cur.execute(adapt_query("SELECT * FROM loyalty_accounts WHERE client_id=?"), (client_id,))
        row = cur.fetchone()
        if not row:
            cur.execute(adapt_query("""INSERT INTO loyalty_accounts
                (client_id,points_balance,cashback_balance,lifetime_points,level,updated_at)
                VALUES (?,0,0,0,'Bronze',?)"""),(client_id,now_iso()))
            if own: conn.commit()
            cur.execute(adapt_query("SELECT * FROM loyalty_accounts WHERE client_id=?"),(client_id,))
            row=cur.fetchone()
        return row
    finally:
        cur.close()
        if own: conn.close()


def add_purchase(client_id, amount, origin, reference, description, user_name):
    amount=float(amount or 0)
    if amount <= 0: raise ValueError("O valor da compra deve ser maior que zero.")
    conn=get_conn(); cur=conn.cursor()
    try:
        account=ensure_account(client_id, conn)
        settings=get_settings()
        points=max(0, int(amount * float(settings["points_per_real"] or 0)))
        level=_row_value(account,"level",5)
        cashback=round(amount * cashback_rate(level,settings)/100,2)
        validity=int(settings["points_validity_days"] or 0)
        expires=(date.today()+timedelta(days=validity)).isoformat() if validity>0 else None
        current_points=int(_row_value(account,"points_balance",2) or 0)
        current_cash=float(_row_value(account,"cashback_balance",3) or 0)
        lifetime=int(_row_value(account,"lifetime_points",4) or 0)+points
        new_level=level_for(lifetime,settings)
        cur.execute(adapt_query("""UPDATE loyalty_accounts SET points_balance=?, cashback_balance=?,
            lifetime_points=?, level=?, updated_at=? WHERE client_id=?"""),
            (current_points+points,current_cash+cashback,lifetime,new_level,now_iso(),client_id))
        cur.execute(adapt_query("""INSERT INTO loyalty_transactions
            (client_id,movement_type,points,cashback,purchase_amount,origin,reference,
             description,user_name,expires_at,created_at)
            VALUES (?,'Crédito',?,?,?,?,?,?,?,?,?)"""),
            (client_id,points,cashback,amount,origin,reference,description,user_name,expires,now_iso()))
        conn.commit()
        return points,cashback,new_level
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def redeem(client_id, points, cashback, description, user_name):
    points=max(0,int(points or 0)); cashback=max(0,float(cashback or 0))
    if points==0 and cashback==0: raise ValueError("Informe pontos ou cashback para resgatar.")
    conn=get_conn(); cur=conn.cursor()
    try:
        account=ensure_account(client_id,conn)
        pbal=int(_row_value(account,"points_balance",2) or 0)
        cbal=float(_row_value(account,"cashback_balance",3) or 0)
        if points>pbal: raise ValueError("Saldo de pontos insuficiente.")
        if cashback>cbal+0.001: raise ValueError("Saldo de cashback insuficiente.")
        cur.execute(adapt_query("""UPDATE loyalty_accounts SET points_balance=?,
            cashback_balance=?,updated_at=? WHERE client_id=?"""),
            (pbal-points,round(cbal-cashback,2),now_iso(),client_id))
        cur.execute(adapt_query("""INSERT INTO loyalty_transactions
            (client_id,movement_type,points,cashback,purchase_amount,origin,description,user_name,created_at)
            VALUES (?,'Resgate',?,?,?,?,?,?,?)"""),
            (client_id,-points,-cashback,0,'Resgate',description,user_name,now_iso()))
        conn.commit()
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def adjust(client_id, points, cashback, description, user_name):
    points=int(points or 0); cashback=float(cashback or 0)
    if points==0 and abs(cashback)<0.001: raise ValueError("Informe um ajuste diferente de zero.")
    conn=get_conn(); cur=conn.cursor()
    try:
        account=ensure_account(client_id,conn)
        pbal=int(_row_value(account,"points_balance",2) or 0)
        cbal=float(_row_value(account,"cashback_balance",3) or 0)
        lifetime=int(_row_value(account,"lifetime_points",4) or 0)
        if pbal+points<0 or cbal+cashback<0: raise ValueError("O ajuste deixaria o saldo negativo.")
        new_lifetime=max(0,lifetime+max(points,0))
        level=level_for(new_lifetime)
        cur.execute(adapt_query("""UPDATE loyalty_accounts SET points_balance=?,cashback_balance=?,
            lifetime_points=?,level=?,updated_at=? WHERE client_id=?"""),
            (pbal+points,round(cbal+cashback,2),new_lifetime,level,now_iso(),client_id))
        cur.execute(adapt_query("""INSERT INTO loyalty_transactions
            (client_id,movement_type,points,cashback,origin,description,user_name,created_at)
            VALUES (?,'Ajuste',?,?,?,?,?,?)"""),
            (client_id,points,cashback,'Manual',description,user_name,now_iso()))
        conn.commit()
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()


def dashboard_data():
    clients=query_db("SELECT id,nome,telefone,whatsapp FROM clients WHERE COALESCE(ativo,1)=1 ORDER BY nome")
    for c in clients: ensure_account(c["id"])
    accounts=query_db("""SELECT a.*,c.nome client_name,c.telefone,c.whatsapp
        FROM loyalty_accounts a JOIN clients c ON c.id=a.client_id
        WHERE COALESCE(c.ativo,1)=1 ORDER BY a.lifetime_points DESC,c.nome""")
    tx=query_db("""SELECT t.*,c.nome client_name FROM loyalty_transactions t
        JOIN clients c ON c.id=t.client_id ORDER BY t.created_at DESC,t.id DESC LIMIT 100""")
    coupons=query_db("""SELECT cp.*,c.nome client_name FROM loyalty_coupons cp
        LEFT JOIN clients c ON c.id=cp.client_id ORDER BY cp.created_at DESC,cp.id DESC""")
    active=sum(1 for c in coupons if c["status"]=="Ativo")
    return {"clients":clients,"accounts":accounts,"transactions":tx,"coupons":coupons,
        "settings":get_settings(),"metrics":{"members":len(accounts),
        "points":sum(int(a["points_balance"] or 0) for a in accounts),
        "cashback":sum(float(a["cashback_balance"] or 0) for a in accounts),
        "active_coupons":active,"diamond":sum(1 for a in accounts if a["level"]=="Diamante")}}


def validate_coupon(coupon, client_id, purchase_amount):
    today=date.today().isoformat()
    if not coupon: raise ValueError("Cupom não encontrado.")
    if coupon["status"]!="Ativo": raise ValueError("Cupom inativo.")
    if coupon["valid_from"] and coupon["valid_from"]>today: raise ValueError("Cupom ainda não está válido.")
    if coupon["valid_until"] and coupon["valid_until"]<today: raise ValueError("Cupom vencido.")
    if coupon["client_id"] and int(coupon["client_id"])!=int(client_id): raise ValueError("Cupom exclusivo de outro cliente.")
    if int(coupon["used_count"] or 0)>=int(coupon["usage_limit"] or 1): raise ValueError("Limite de uso do cupom atingido.")
    amount=float(purchase_amount or 0)
    if amount<float(coupon["minimum_purchase"] or 0): raise ValueError("Compra abaixo do valor mínimo do cupom.")
    if coupon["discount_type"]=="Percentual":
        return round(amount*float(coupon["discount_value"] or 0)/100,2)
    return min(amount,round(float(coupon["discount_value"] or 0),2))
