from flask import Blueprint, abort, flash, redirect, render_template, request, session, url_for
from database import query_db
from services.pos_service import abrir_caixa, caixa_aberto, concluir_venda, fechar_caixa, movimentar_caixa, resumo_caixa

pdv_bp=Blueprint('pdv',__name__)
def usuario(): return session.get('user_name') or 'Administrador'

@pdv_bp.route('/pdv')
def pdv():
    produtos=query_db("SELECT id,name,sku,barcode,quantity,unit,sale_price,category FROM stock_products WHERE COALESCE(active,1)=1 AND quantity>0 ORDER BY name")
    clientes=query_db("SELECT id,nome,telefone FROM clients WHERE COALESCE(ativo,1)=1 ORDER BY nome")
    vendas=query_db("""SELECT s.*,c.nome cliente_nome FROM pos_sales s LEFT JOIN clients c ON c.id=s.client_id ORDER BY s.id DESC LIMIT 30""")
    return render_template('pdv.html',produtos=produtos,clientes=clientes,vendas=vendas,caixa=caixa_aberto(),resumo=resumo_caixa())

@pdv_bp.route('/pdv/caixa/abrir',methods=['POST'])
def abrir():
    try: abrir_caixa(request.form.get('opening_amount'),usuario(),request.form.get('notes','')); flash('Caixa aberto com sucesso.','success')
    except ValueError as exc: flash(str(exc),'danger')
    return redirect(url_for('pdv.pdv'))

@pdv_bp.route('/pdv/caixa/movimentar',methods=['POST'])
def movimentar():
    try: movimentar_caixa(request.form.get('movement_type',''),request.form.get('amount'),request.form.get('description',''),usuario()); flash('Movimentação registrada.','success')
    except ValueError as exc: flash(str(exc),'danger')
    return redirect(url_for('pdv.pdv'))

@pdv_bp.route('/pdv/caixa/fechar',methods=['POST'])
def fechar():
    try:
        diff=fechar_caixa(request.form.get('counted_amount'),usuario(),request.form.get('notes',''))
        flash(f'Caixa fechado. Diferença: R$ {diff:.2f}.','success' if diff==0 else 'warning')
    except ValueError as exc: flash(str(exc),'danger')
    return redirect(url_for('pdv.pdv'))



def _venda_completa(sale_id):
    venda=query_db("""SELECT s.*,c.nome cliente_nome,c.telefone cliente_telefone
        FROM pos_sales s LEFT JOIN clients c ON c.id=s.client_id WHERE s.id=?""",(sale_id,),one=True)
    if not venda:
        abort(404)
    itens=query_db("SELECT * FROM pos_sale_items WHERE sale_id=? ORDER BY id",(sale_id,))
    pagamentos=query_db("SELECT * FROM pos_payments WHERE sale_id=? ORDER BY id",(sale_id,))
    empresa=query_db("SELECT * FROM settings ORDER BY id DESC LIMIT 1",one=True) or {}
    return venda,itens,pagamentos,empresa


@pdv_bp.route('/pdv/vendas/<int:sale_id>')
def detalhe_venda(sale_id):
    venda,itens,pagamentos,empresa=_venda_completa(sale_id)
    return render_template('pdv_sale_detail.html',venda=venda,itens=itens,pagamentos=pagamentos,empresa=empresa)


@pdv_bp.route('/pdv/vendas/<int:sale_id>/comprovante')
def comprovante(sale_id):
    venda,itens,pagamentos,empresa=_venda_completa(sale_id)
    return render_template('pdv_receipt.html',venda=venda,itens=itens,pagamentos=pagamentos,empresa=empresa)

@pdv_bp.route('/pdv/vendas',methods=['POST'])
def vender():
    try:
        ids=request.form.getlist('product_id[]'); qtds=request.form.getlist('quantity[]'); precos=request.form.getlist('unit_price[]')
        itens=[{'product_id':a,'quantity':b,'unit_price':c} for a,b,c in zip(ids,qtds,precos)]
        mets=request.form.getlist('payment_method[]'); vals=request.form.getlist('payment_amount[]')
        pagamentos=[{'method':a,'amount':b} for a,b in zip(mets,vals)]
        sale_id=concluir_venda(itens,pagamentos,request.form.get('discount_amount'),request.form.get('client_id'),request.form.get('notes',''),usuario())
        flash(f'Venda #{sale_id} concluída com sucesso.','success')
        return redirect(url_for('pdv.comprovante',sale_id=sale_id,auto_print='1'))
    except (ValueError,TypeError) as exc:
        flash(str(exc),'danger')
        return redirect(url_for('pdv.pdv'))
