from __future__ import annotations
from datetime import datetime
from database import execute_db, insert_db, now_iso, query_db


def money(value):
    try:
        return round(float(str(value or 0).replace(',', '.')), 2)
    except (TypeError, ValueError):
        return 0.0


def caixa_aberto():
    return query_db("SELECT * FROM cash_register_sessions WHERE status='Aberto' ORDER BY id DESC LIMIT 1", one=True)


def abrir_caixa(valor, usuario, observacoes=''):
    if caixa_aberto():
        raise ValueError('Já existe um caixa aberto.')
    return insert_db("INSERT INTO cash_register_sessions (opened_at,opening_amount,status,opened_by,notes) VALUES (?,?,?,?,?)",
                     (now_iso(), money(valor), 'Aberto', usuario, observacoes.strip()))


def movimentar_caixa(tipo, valor, descricao, usuario):
    caixa=caixa_aberto()
    if not caixa:
        raise ValueError('Abra o caixa antes de registrar movimentações.')
    valor=money(valor)
    if valor <= 0:
        raise ValueError('Informe um valor maior que zero.')
    if tipo not in {'Sangria','Suprimento'}:
        raise ValueError('Movimentação inválida.')
    insert_db("INSERT INTO cash_register_movements (cash_session_id,movement_type,amount,description,created_by,created_at) VALUES (?,?,?,?,?,?)",
              (caixa['id'],tipo,valor,descricao.strip(),usuario,now_iso()))


def resumo_caixa(cash_id=None):
    caixa = query_db('SELECT * FROM cash_register_sessions WHERE id=?',(cash_id,),one=True) if cash_id else caixa_aberto()
    if not caixa:
        return None
    vendas=query_db("""SELECT p.payment_method,COALESCE(SUM(p.amount),0) total FROM pos_payments p
        JOIN pos_sales s ON s.id=p.sale_id WHERE s.cash_session_id=? AND s.status='Concluída' GROUP BY p.payment_method ORDER BY p.payment_method""",(caixa['id'],))
    mov=query_db("SELECT movement_type,COALESCE(SUM(amount),0) total FROM cash_register_movements WHERE cash_session_id=? GROUP BY movement_type",(caixa['id'],))
    pagamentos={r['payment_method']:float(r['total'] or 0) for r in vendas}
    movimentos={r['movement_type']:float(r['total'] or 0) for r in mov}
    dinheiro=pagamentos.get('Dinheiro',0)
    esperado=float(caixa['opening_amount'] or 0)+dinheiro+movimentos.get('Suprimento',0)-movimentos.get('Sangria',0)
    total_vendido=sum(pagamentos.values())
    return {'caixa':caixa,'pagamentos':pagamentos,'movimentos':movimentos,'esperado':round(esperado,2),'total_vendido':round(total_vendido,2)}


def fechar_caixa(valor_contado, usuario, observacoes=''):
    resumo=resumo_caixa()
    if not resumo:
        raise ValueError('Nenhum caixa aberto.')
    contado=money(valor_contado)
    diff=round(contado-resumo['esperado'],2)
    execute_db("""UPDATE cash_register_sessions SET closed_at=?,expected_amount=?,counted_amount=?,difference=?,status='Fechado',closed_by=?,notes=? WHERE id=?""",
               (now_iso(),resumo['esperado'],contado,diff,usuario,observacoes.strip(),resumo['caixa']['id']))
    return diff


def numero_venda():
    prefix=datetime.now().strftime('%Y%m%d')
    row=query_db("SELECT COUNT(*) qtd FROM pos_sales WHERE sale_number LIKE ?",(f'VEN-{prefix}-%',),one=True)
    return f"VEN-{prefix}-{int(row['qtd'] or 0)+1:04d}"


def concluir_venda(itens, pagamentos, desconto, cliente_id, observacoes, usuario):
    caixa=caixa_aberto()
    if not caixa:
        raise ValueError('Abra o caixa antes de concluir uma venda.')
    valid=[]
    subtotal=0.0
    for item in itens:
        produto_id=int(item.get('product_id') or 0)
        produto=query_db("SELECT * FROM stock_products WHERE id=? AND COALESCE(active,1)=1",(produto_id,),one=True)
        qtd=money(item.get('quantity'))
        if not produto or qtd <= 0:
            continue
        if float(produto['quantity'] or 0) < qtd:
            raise ValueError(f"Estoque insuficiente para {produto['name']}.")
        preco=money(item.get('unit_price') or produto['sale_price'])
        total=round(qtd*preco,2); subtotal+=total
        valid.append((produto,qtd,preco,total))
    if not valid:
        raise ValueError('Adicione pelo menos um produto válido.')
    desconto=min(max(money(desconto),0),subtotal)
    total=round(subtotal-desconto,2)
    pays=[]; pago=0.0
    for pg in pagamentos:
        metodo=(pg.get('method') or '').strip(); valor=money(pg.get('amount'))
        if metodo and valor>0:
            pays.append((metodo,valor)); pago+=valor
    if round(pago,2) != total:
        raise ValueError(f'Os pagamentos devem totalizar R$ {total:.2f}.')
    sale_id=insert_db("""INSERT INTO pos_sales (sale_number,client_id,cash_session_id,subtotal,discount_amount,total_amount,status,notes,created_by,created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?)""",(numero_venda(),client_id or None,caixa['id'],round(subtotal,2),desconto,total,'Concluída',observacoes.strip(),usuario,now_iso()))
    for produto,qtd,preco,item_total in valid:
        insert_db("INSERT INTO pos_sale_items (sale_id,item_type,product_id,description,quantity,unit_price,total_price,stock_applied,created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                  (sale_id,'Produto',produto['id'],produto['name'],qtd,preco,item_total,1,now_iso()))
        execute_db("UPDATE stock_products SET quantity=quantity-?,updated_at=? WHERE id=?",(qtd,now_iso(),produto['id']))
        insert_db("INSERT INTO stock_movements (product_id,movement_type,quantity,unit_cost,reason,reference,user_name,movement_date,created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                  (produto['id'],'Saída',qtd,produto['cost_price'] or 0,'Venda PDV',f'PDV #{sale_id}',usuario,now_iso(),now_iso()))
    for metodo,valor in pays:
        insert_db("INSERT INTO pos_payments (sale_id,payment_method,amount,created_at) VALUES (?,?,?,?)",(sale_id,metodo,valor,now_iso()))
    insert_db("""INSERT INTO financial_transactions (type,category,description,amount,payment_method,transaction_date,status,reference,account,source_type,source_id,created_by,created_at,updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",('Entrada','Venda PDV',f'Venda {numero_por_id(sale_id)}',total,', '.join(m for m,_ in pays),datetime.now().date().isoformat(),'Pago',numero_por_id(sale_id),'Caixa','PDV',sale_id,usuario,now_iso(),now_iso()))
    return sale_id


def numero_por_id(sale_id):
    row=query_db('SELECT sale_number FROM pos_sales WHERE id=?',(sale_id,),one=True)
    return row['sale_number'] if row else str(sale_id)
