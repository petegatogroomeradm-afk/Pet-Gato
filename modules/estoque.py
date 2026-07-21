from __future__ import annotations
import csv, io
from datetime import date, timedelta
from flask import Blueprint, Response, flash, redirect, render_template, request, session, url_for
from database import execute_db, now_iso, query_db
from services.estoque_service import (concluir_inventario, curva_abc, indicadores_estoque,
    iniciar_inventario, numero, produto_por_id, registrar_movimento, salvar_contagem, sugestoes_compra)

estoque_bp=Blueprint('estoque',__name__)

def usuario_atual(): return session.get('user_name') or 'Administrador'

@estoque_bp.route('/estoque',methods=['GET','POST'])
def estoque():
    if request.method=='POST':
        nome=request.form.get('name','').strip()
        if not nome:
            flash('Informe o nome do produto.','danger'); return redirect(url_for('estoque.estoque'))
        execute_db("""INSERT INTO stock_products
        (name,category,subcategory,brand,sku,barcode,quantity,unit,min_quantity,max_quantity,cost_price,sale_price,supplier,expiration_date,location,active,created_at,updated_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",(
            nome,request.form.get('category','').strip(),request.form.get('subcategory','').strip(),request.form.get('brand','').strip(),
            request.form.get('sku','').strip(),request.form.get('barcode','').strip(),numero(request.form.get('quantity')),
            request.form.get('unit','un').strip() or 'un',numero(request.form.get('min_quantity')),numero(request.form.get('max_quantity')),
            numero(request.form.get('cost_price')),numero(request.form.get('sale_price')),request.form.get('supplier','').strip(),
            request.form.get('expiration_date','').strip(),request.form.get('location','').strip(),1,now_iso(),now_iso()))
        flash('Produto cadastrado com sucesso.','success'); return redirect(url_for('estoque.estoque'))
    busca=request.args.get('busca','').strip(); categoria=request.args.get('categoria','').strip(); situacao=request.args.get('situacao','').strip()
    where=['COALESCE(active,1)=1']; params=[]
    if busca:
        termo=f'%{busca}%'; where.append("(name LIKE ? OR sku LIKE ? OR barcode LIKE ? OR supplier LIKE ? OR brand LIKE ?)"); params += [termo]*5
    if categoria: where.append('category=?'); params.append(categoria)
    if situacao=='baixo': where.append('quantity<=min_quantity')
    elif situacao=='zerado': where.append('quantity<=0')
    elif situacao=='vencendo': where.append("expiration_date<>'' AND expiration_date<=?"); params.append((date.today()+timedelta(days=30)).isoformat())
    produtos=query_db(f"SELECT * FROM stock_products WHERE {' AND '.join(where)} ORDER BY name",tuple(params))
    categorias=query_db("SELECT DISTINCT category FROM stock_products WHERE category IS NOT NULL AND category<>'' ORDER BY category")
    movimentos=query_db("""SELECT m.*,p.name product_name,p.unit FROM stock_movements m LEFT JOIN stock_products p ON p.id=m.product_id
      ORDER BY m.movement_date DESC,m.id DESC LIMIT 80""")
    inventarios=query_db('SELECT * FROM stock_inventory_sessions ORDER BY id DESC LIMIT 10')
    return render_template('estoque.html',produtos=produtos,resumo=indicadores_estoque(),categorias=categorias,movimentos=movimentos,
      busca=busca,categoria=categoria,situacao=situacao,sugestoes=sugestoes_compra(),abc=curva_abc()[:12],inventarios=inventarios)

@estoque_bp.route('/estoque/<int:produto_id>/movimentar',methods=['POST'])
def movimentar(produto_id):
    try:
        registrar_movimento(produto_id,request.form.get('movement_type','Entrada'),request.form.get('quantity'),request.form.get('unit_cost'),
          request.form.get('reason','').strip(),request.form.get('reference','').strip(),usuario_atual())
        flash('Movimentação registrada com sucesso.','success')
    except ValueError as exc: flash(str(exc),'danger')
    return redirect(request.referrer or url_for('estoque.estoque'))

@estoque_bp.route('/estoque/<int:produto_id>/editar',methods=['GET','POST'])
def editar_produto(produto_id):
    produto=produto_por_id(produto_id)
    if not produto:
        flash('Produto não encontrado.','danger'); return redirect(url_for('estoque.estoque'))
    if request.method=='POST':
        execute_db("""UPDATE stock_products SET name=?,category=?,subcategory=?,brand=?,sku=?,barcode=?,unit=?,min_quantity=?,max_quantity=?,
          cost_price=?,sale_price=?,supplier=?,expiration_date=?,location=?,updated_at=? WHERE id=?""",(
          request.form.get('name','').strip(),request.form.get('category','').strip(),request.form.get('subcategory','').strip(),
          request.form.get('brand','').strip(),request.form.get('sku','').strip(),request.form.get('barcode','').strip(),request.form.get('unit','un').strip(),
          numero(request.form.get('min_quantity')),numero(request.form.get('max_quantity')),numero(request.form.get('cost_price')),
          numero(request.form.get('sale_price')),request.form.get('supplier','').strip(),request.form.get('expiration_date','').strip(),
          request.form.get('location','').strip(),now_iso(),produto_id))
        flash('Produto atualizado.','success'); return redirect(url_for('estoque.estoque'))
    return render_template('estoque_editar.html',produto=produto)

@estoque_bp.route('/estoque/<int:produto_id>/excluir',methods=['POST'])
def excluir_produto(produto_id):
    execute_db('UPDATE stock_products SET active=0,updated_at=? WHERE id=?',(now_iso(),produto_id)); flash('Produto arquivado.','info')
    return redirect(url_for('estoque.estoque'))

@estoque_bp.route('/estoque/inventario/novo',methods=['POST'])
def novo_inventario():
    ref=request.form.get('reference','').strip() or f"Inventário {date.today().strftime('%d/%m/%Y')}"
    iid=iniciar_inventario(ref,request.form.get('notes','').strip(),usuario_atual())
    flash('Inventário iniciado. Faça a contagem física.','success'); return redirect(url_for('estoque.inventario',inventory_id=iid))

@estoque_bp.route('/estoque/inventario/<int:inventory_id>',methods=['GET','POST'])
def inventario(inventory_id):
    inv=query_db('SELECT * FROM stock_inventory_sessions WHERE id=?',(inventory_id,),one=True)
    if not inv:
        flash('Inventário não encontrado.','danger'); return redirect(url_for('estoque.estoque'))
    if request.method=='POST' and inv['status']=='Aberto':
        contagens={int(k.split('_')[1]):v for k,v in request.form.items() if k.startswith('count_')}
        salvar_contagem(inventory_id,contagens); flash('Contagem salva.','success'); return redirect(url_for('estoque.inventario',inventory_id=inventory_id))
    itens=query_db("""SELECT i.*,p.name,p.sku,p.unit FROM stock_inventory_items i JOIN stock_products p ON p.id=i.product_id
      WHERE i.inventory_id=? ORDER BY p.name""",(inventory_id,))
    return render_template('estoque_inventario.html',inventario=inv,itens=itens)

@estoque_bp.route('/estoque/inventario/<int:inventory_id>/concluir',methods=['POST'])
def finalizar_inventario(inventory_id):
    concluir_inventario(inventory_id,usuario_atual()); flash('Inventário concluído e diferenças ajustadas.','success')
    return redirect(url_for('estoque.inventario',inventory_id=inventory_id))

@estoque_bp.route('/estoque/exportar.csv')
def exportar_csv():
    produtos=query_db('SELECT * FROM stock_products WHERE COALESCE(active,1)=1 ORDER BY name')
    b=io.StringIO(); w=csv.writer(b,delimiter=';')
    w.writerow(['Produto','Categoria','Subcategoria','Marca','SKU','Código de barras','Quantidade','Unidade','Mínimo','Máximo','Custo','Venda','Margem %','Fornecedor','Validade','Localização'])
    for p in produtos:
        custo=float(p['cost_price'] or 0); venda=float(p['sale_price'] or 0); margem=((venda-custo)/venda*100) if venda else 0
        w.writerow([p['name'],p['category'],p['subcategory'],p['brand'],p['sku'],p['barcode'],p['quantity'],p['unit'],p['min_quantity'],p['max_quantity'],custo,venda,round(margem,2),p['supplier'],p['expiration_date'],p['location']])
    return Response('\ufeff'+b.getvalue(),mimetype='text/csv',headers={'Content-Disposition':'attachment; filename=estoque_enterprise.csv'})
