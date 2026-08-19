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
        aplicar_receita_ao_atendimento(atendimento_id)
        usos=query_db('SELECT * FROM grooming_product_usage WHERE grooming_id=?',(atendimento_id,))
        if usos:
            pend=[u for u in usos if not u['stock_applied']]; aplicados=0
            for u in pend:
                if _aplicar_produto(u['product_id'],u['quantity'],f'ATEND-{atendimento_id}'):
                    execute_db('UPDATE grooming_product_usage SET stock_applied=1 WHERE id=?',(u['id'],)); aplicados+=1
            return aplicados>0 or not pend
    # Sem receita ou produtos vinculados, não cria consumo genérico.
    # Isso evita baixas incorretas em serviços ainda não configurados.
    return False


def receitas_consumo():
    receitas=query_db("""SELECT r.*,COUNT(i.id) item_count FROM service_consumption_recipes r
      LEFT JOIN service_consumption_recipe_items i ON i.recipe_id=r.id
      WHERE COALESCE(r.active,1)=1 GROUP BY r.id ORDER BY r.service_name""")
    saida=[]
    for r in receitas:
        itens=query_db("""SELECT i.*,p.name product_name,p.quantity stock_quantity,p.unit product_unit
          FROM service_consumption_recipe_items i JOIN stock_products p ON p.id=i.product_id
          WHERE i.recipe_id=? ORDER BY p.name""",(r['id'],))
        saida.append({**dict(r),'itens':[dict(x) for x in itens]})
    return saida

def salvar_receita_consumo(service_name,notes=''):
    nome=(service_name or '').strip()
    if not nome: raise ValueError('Informe o nome do serviço.')
    existente=query_db('SELECT id FROM service_consumption_recipes WHERE service_name=?',(nome,),one=True)
    if existente:
        execute_db('UPDATE service_consumption_recipes SET active=1,notes=?,updated_at=? WHERE id=?',(notes,now_iso(),existente['id']))
        return existente['id']
    return insert_db('INSERT INTO service_consumption_recipes(service_name,active,notes,created_at,updated_at) VALUES(?,?,?,?,?)',(nome,1,notes,now_iso(),now_iso()))

def adicionar_item_receita(recipe_id,product_id,quantity):
    qtd=numero(quantity)
    if qtd<=0: raise ValueError('Informe uma quantidade maior que zero.')
    produto=produto_por_id(product_id)
    if not produto: raise ValueError('Produto não encontrado.')
    existente=query_db('SELECT id FROM service_consumption_recipe_items WHERE recipe_id=? AND product_id=?',(recipe_id,product_id),one=True)
    if existente:
        execute_db('UPDATE service_consumption_recipe_items SET quantity=?,unit=? WHERE id=?',(qtd,produto['unit'],existente['id']))
        return existente['id']
    return insert_db('INSERT INTO service_consumption_recipe_items(recipe_id,product_id,quantity,unit,created_at) VALUES(?,?,?,?,?)',(recipe_id,product_id,qtd,produto['unit'],now_iso()))

def aplicar_receita_ao_atendimento(atendimento_id):
    atendimento=query_db('SELECT id,servico FROM grooming_services WHERE id=?',(atendimento_id,),one=True)
    if not atendimento: return 0
    servico=(atendimento['servico'] or '').strip()
    receita=query_db('SELECT id FROM service_consumption_recipes WHERE active=1 AND service_name=?',(servico,),one=True)
    if not receita: return 0
    itens=query_db('SELECT * FROM service_consumption_recipe_items WHERE recipe_id=?',(receita['id'],))
    incluidos=0
    for item in itens:
        existe=query_db('SELECT id FROM grooming_product_usage WHERE grooming_id=? AND product_id=?',(atendimento_id,item['product_id']),one=True)
        if not existe:
            execute_db('INSERT INTO grooming_product_usage(grooming_id,product_id,quantity,unit,stock_applied,created_at) VALUES(?,?,?,?,0,?)',(atendimento_id,item['product_id'],item['quantity'],item['unit'],now_iso()))
            incluidos+=1
    return incluidos
