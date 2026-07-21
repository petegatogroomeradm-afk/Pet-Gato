from __future__ import annotations
from datetime import date, timedelta
from database import execute_db, insert_db, now_iso, query_db

def numero(valor):
    texto=str(valor or '0').strip().replace('R$','').replace(' ','')
    if ',' in texto: texto=texto.replace('.','').replace(',','.')
    try: return float(texto)
    except (TypeError,ValueError): return 0.0

def produto_por_id(produto_id):
    return query_db('SELECT * FROM stock_products WHERE id=?',(produto_id,),one=True)

def indicadores_estoque():
    limite=(date.today()+timedelta(days=30)).isoformat()
    return query_db("""SELECT COUNT(*) total,
      COALESCE(SUM(CASE WHEN quantity<=min_quantity THEN 1 ELSE 0 END),0) baixo,
      COALESCE(SUM(CASE WHEN quantity<=0 THEN 1 ELSE 0 END),0) zerado,
      COALESCE(SUM(CASE WHEN expiration_date<>'' AND expiration_date<=? THEN 1 ELSE 0 END),0) vencendo,
      COALESCE(SUM(quantity*cost_price),0) valor_custo,
      COALESCE(SUM(quantity*sale_price),0) valor_venda,
      COALESCE(SUM((sale_price-cost_price)*quantity),0) lucro_potencial
      FROM stock_products WHERE COALESCE(active,1)=1""",(limite,),one=True)

def sugestoes_compra(limite=30):
    return query_db("""SELECT *, CASE WHEN COALESCE(max_quantity,0)>quantity
      THEN max_quantity-quantity ELSE CASE WHEN COALESCE(min_quantity,0)*2-quantity>0 THEN COALESCE(min_quantity,0)*2-quantity ELSE 0 END END suggested_quantity
      FROM stock_products WHERE COALESCE(active,1)=1 AND quantity<=min_quantity
      ORDER BY quantity,name LIMIT ?""",(limite,))

def curva_abc():
    itens=query_db("""SELECT p.id,p.name,p.category,
      COALESCE(SUM(CASE WHEN m.movement_type IN ('Saída','Perda','Consumo') THEN m.quantity ELSE 0 END),0) consumo,
      COALESCE(SUM(CASE WHEN m.movement_type IN ('Saída','Perda','Consumo') THEN m.quantity*COALESCE(m.unit_cost,p.cost_price,0) ELSE 0 END),0) valor_consumido
      FROM stock_products p LEFT JOIN stock_movements m ON m.product_id=p.id AND m.movement_date>=?
      WHERE COALESCE(p.active,1)=1 GROUP BY p.id,p.name,p.category ORDER BY valor_consumido DESC,consumo DESC""",
      ((date.today()-timedelta(days=90)).isoformat(),))
    total=sum(float(x['valor_consumido'] or 0) for x in itens); acumulado=0; saida=[]
    for x in itens:
        acumulado+=float(x['valor_consumido'] or 0); pct=(acumulado/total*100) if total else 0
        classe='A' if pct<=80 else ('B' if pct<=95 else 'C')
        saida.append({**dict(x),'percentual':pct,'classe':classe})
    return saida

def registrar_movimento(produto_id,tipo,quantidade,unit_cost=0,reason='',reference='',user_name='Administrador'):
    p=produto_por_id(produto_id)
    if not p: raise ValueError('Produto não encontrado.')
    qtd=abs(numero(quantidade)); atual=float(p['quantity'] or 0)
    if qtd<=0: raise ValueError('Informe uma quantidade maior que zero.')
    if tipo=='Entrada': novo=atual+qtd
    elif tipo in ('Saída','Perda','Consumo'):
        if qtd>atual: raise ValueError('Quantidade maior que o estoque disponível.')
        novo=atual-qtd
    elif tipo=='Ajuste': novo=qtd
    else: raise ValueError('Tipo de movimentação inválido.')
    execute_db('UPDATE stock_products SET quantity=?,updated_at=? WHERE id=?',(novo,now_iso(),produto_id))
    execute_db("""INSERT INTO stock_movements(product_id,movement_type,quantity,unit_cost,reason,reference,user_name,movement_date,created_at)
      VALUES(?,?,?,?,?,?,?,?,?)""",(produto_id,tipo,qtd,numero(unit_cost) or float(p['cost_price'] or 0),reason,reference,user_name,now_iso()[:10],now_iso()))
    return novo

def iniciar_inventario(reference,notes='',user_name='Administrador'):
    iid=insert_db("INSERT INTO stock_inventory_sessions(reference,status,notes,started_at,created_by) VALUES(?,?,?,?,?)",
      (reference,'Aberto',notes,now_iso(),user_name))
    for p in query_db('SELECT id,quantity FROM stock_products WHERE COALESCE(active,1)=1 ORDER BY name'):
        execute_db("""INSERT INTO stock_inventory_items(inventory_id,product_id,system_quantity,counted_quantity,difference,adjusted,created_at)
          VALUES(?,?,?,?,?,?,?)""",(iid,p['id'],p['quantity'] or 0,p['quantity'] or 0,0,0,now_iso()))
    return iid

def salvar_contagem(inventory_id,contagens):
    for product_id,valor in contagens.items():
        item=query_db('SELECT * FROM stock_inventory_items WHERE inventory_id=? AND product_id=?',(inventory_id,product_id),one=True)
        if item:
            contado=numero(valor); diferenca=contado-float(item['system_quantity'] or 0)
            execute_db('UPDATE stock_inventory_items SET counted_quantity=?,difference=? WHERE id=?',(contado,diferenca,item['id']))

def concluir_inventario(inventory_id,user_name='Administrador'):
    for item in query_db('SELECT * FROM stock_inventory_items WHERE inventory_id=?',(inventory_id,)):
        if float(item['difference'] or 0)!=0:
            registrar_movimento(item['product_id'],'Ajuste',item['counted_quantity'],reason='Ajuste de inventário',reference=f'INVENT-{inventory_id}',user_name=user_name)
        execute_db('UPDATE stock_inventory_items SET adjusted=1 WHERE id=?',(item['id'],))
    execute_db("UPDATE stock_inventory_sessions SET status='Concluído',finished_at=? WHERE id=?",(now_iso(),inventory_id))

def _aplicar_produto(produto_id,quantidade,referencia):
    try: registrar_movimento(produto_id,'Consumo',quantidade,reason='Consumo em Banho e Tosa',reference=referencia,user_name='Sistema'); return True
    except ValueError: return False

def baixar_estoque_banho_tosa(atendimento_id=None):
    if atendimento_id:
        usos=query_db('SELECT * FROM grooming_product_usage WHERE grooming_id=?',(atendimento_id,))
        if usos:
            pend=[u for u in usos if not u['stock_applied']]; aplicados=0
            for u in pend:
                if _aplicar_produto(u['product_id'],u['quantity'],f'ATEND-{atendimento_id}'):
                    execute_db('UPDATE grooming_product_usage SET stock_applied=1 WHERE id=?',(u['id'],)); aplicados+=1
            return aplicados>0 or not pend
    baixou=False
    for nome,qtd in [('Shampoo',20),('Perfume',5),('Laco',1)]:
        p=query_db('SELECT id FROM stock_products WHERE name=? AND COALESCE(active,1)=1 LIMIT 1',(nome,),one=True)
        if p and _aplicar_produto(p['id'],qtd,f"ATEND-{atendimento_id or 'PADRAO'}"): baixou=True
    return baixou
