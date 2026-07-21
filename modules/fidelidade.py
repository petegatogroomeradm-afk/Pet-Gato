import secrets, string
from flask import Blueprint, flash, redirect, render_template, request, session, url_for
from database import execute_db, now_iso, query_db
from services.loyalty_service import dashboard_data, add_purchase, redeem, adjust, validate_coupon

fidelidade_bp=Blueprint("fidelidade",__name__,url_prefix="/fidelidade")

def _user(): return session.get("user_name","Sistema")
def _float(name, default=0):
    try: return float(request.form.get(name,default) or default)
    except ValueError: return float(default)
def _int(name, default=0):
    try: return int(float(request.form.get(name,default) or default))
    except ValueError: return int(default)

@fidelidade_bp.route("/")
def dashboard(): return render_template("fidelidade.html",**dashboard_data())

@fidelidade_bp.route("/compra",methods=["POST"])
def purchase():
    try:
        points,cashback,level=add_purchase(_int("client_id"),_float("amount"),
            request.form.get("origin","Atendimento"),request.form.get("reference",""),
            request.form.get("description","Compra registrada"),_user())
        flash(f"Compra registrada: +{points} pontos e +R$ {cashback:.2f} de cashback. Nível: {level}.","success")
    except Exception as exc: flash(str(exc),"danger")
    return redirect(url_for("fidelidade.dashboard"))

@fidelidade_bp.route("/resgatar",methods=["POST"])
def redeem_balance():
    try:
        redeem(_int("client_id"),_int("points"),_float("cashback"),
               request.form.get("description","Resgate de benefício"),_user())
        flash("Benefício resgatado com sucesso.","success")
    except Exception as exc: flash(str(exc),"danger")
    return redirect(url_for("fidelidade.dashboard"))

@fidelidade_bp.route("/ajustar",methods=["POST"])
def adjustment():
    try:
        adjust(_int("client_id"),_int("points"),_float("cashback"),
               request.form.get("description","Ajuste manual"),_user())
        flash("Saldo ajustado com sucesso.","success")
    except Exception as exc: flash(str(exc),"danger")
    return redirect(url_for("fidelidade.dashboard"))

@fidelidade_bp.route("/configuracoes",methods=["POST"])
def settings():
    values=(_float("points_per_real"),_float("cashback_bronze"),_float("cashback_prata"),
        _float("cashback_ouro"),_float("cashback_diamante"),_int("prata_min"),
        _int("ouro_min"),_int("diamante_min"),_int("points_validity_days"),_user(),now_iso())
    execute_db("""UPDATE loyalty_settings SET points_per_real=?,cashback_bronze=?,cashback_prata=?,
        cashback_ouro=?,cashback_diamante=?,prata_min=?,ouro_min=?,diamante_min=?,
        points_validity_days=?,updated_by=?,updated_at=? WHERE id=(SELECT id FROM loyalty_settings ORDER BY id LIMIT 1)""",values)
    flash("Regras de fidelidade atualizadas.","success")
    return redirect(url_for("fidelidade.dashboard"))

@fidelidade_bp.route("/cupons/criar",methods=["POST"])
def create_coupon():
    code=request.form.get("code","").strip().upper()
    if not code:
        alphabet=string.ascii_uppercase+string.digits
        code="PET"+"".join(secrets.choice(alphabet) for _ in range(6))
    try:
        execute_db("""INSERT INTO loyalty_coupons
            (code,title,discount_type,discount_value,minimum_purchase,valid_from,valid_until,
             usage_limit,used_count,client_id,status,notes,created_by,created_at)
            VALUES (?,?,?,?,?,?,?,?,0,?,'Ativo',?,?,?)""",
            (code,request.form.get("title","").strip(),request.form.get("discount_type","Percentual"),
             _float("discount_value"),_float("minimum_purchase"),request.form.get("valid_from") or None,
             request.form.get("valid_until") or None,_int("usage_limit",1),
             request.form.get("client_id") or None,request.form.get("notes",""),_user(),now_iso()))
        flash(f"Cupom {code} criado.","success")
    except Exception as exc: flash(f"Não foi possível criar o cupom: {exc}","danger")
    return redirect(url_for("fidelidade.dashboard"))

@fidelidade_bp.route("/cupons/<int:coupon_id>/alternar",methods=["POST"])
def toggle_coupon(coupon_id):
    c=query_db("SELECT status FROM loyalty_coupons WHERE id=?",(coupon_id,),one=True)
    if c:
        execute_db("UPDATE loyalty_coupons SET status=? WHERE id=?",("Inativo" if c["status"]=="Ativo" else "Ativo",coupon_id))
        flash("Status do cupom atualizado.","success")
    return redirect(url_for("fidelidade.dashboard"))

@fidelidade_bp.route("/cupons/resgatar",methods=["POST"])
def redeem_coupon():
    try:
        code=request.form.get("code","").strip().upper(); client_id=_int("client_id"); amount=_float("purchase_amount")
        coupon=query_db("SELECT * FROM loyalty_coupons WHERE code=?",(code,),one=True)
        discount=validate_coupon(coupon,client_id,amount)
        execute_db("""INSERT INTO loyalty_coupon_redemptions
            (coupon_id,client_id,order_reference,discount_amount,user_name,created_at)
            VALUES (?,?,?,?,?,?)""",(coupon["id"],client_id,request.form.get("order_reference",""),discount,_user(),now_iso()))
        execute_db("UPDATE loyalty_coupons SET used_count=used_count+1 WHERE id=?",(coupon["id"],))
        flash(f"Cupom aplicado. Desconto calculado: R$ {discount:.2f}.","success")
    except Exception as exc: flash(str(exc),"danger")
    return redirect(url_for("fidelidade.dashboard"))
