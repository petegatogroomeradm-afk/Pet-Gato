from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from flask import Blueprint, flash, jsonify, redirect, render_template, request, url_for
from werkzeug.utils import secure_filename

from database import execute_db, insert_db, now_iso, query_db
from services.historico_service import registrar_historico
from services.financeiro_service import lancar_receita_banho_tosa
from services.estoque_service import baixar_estoque_banho_tosa
from services.event_bus import publish
from services.banho_service import (
    FLUXO_STATUS_BANHO, KANBAN_COLUNAS, proximo_status, obter_atendimento,
    atualizar_status_atendimento, lancar_comissao,
)

banho_tosa_bp=Blueprint("banho_tosa",__name__)
BASE_DIR=Path(__file__).resolve().parents[1]
PHOTO_DIR=BASE_DIR/"static"/"uploads"/"atendimentos"
PHOTO_DIR.mkdir(parents=True,exist_ok=True)
ALLOWED_IMAGES={"png","jpg","jpeg","webp"}

def _valor_decimal(valor):
    texto=(valor or "0").strip().replace("R$","").replace(" ","")
    if "," in texto: texto=texto.replace(".","").replace(",",".")
    try: return float(texto or 0)
    except ValueError: return 0.0

def _mapear_registros(rows, chave="grooming_id"):
    resultado={}
    for row in rows:
        resultado.setdefault(row[chave],[]).append(row)
    return resultado

def _carregar_operacao(atendimentos):
    ids=[a["id"] for a in atendimentos]
    if not ids: return {},{},{},{}
    marks=",".join("?" for _ in ids)
    check_rows=query_db(f"SELECT * FROM grooming_checklists WHERE grooming_id IN ({marks})",tuple(ids))
    checks={r["grooming_id"]:dict(r) for r in check_rows}
    produtos=_mapear_registros(query_db(f"""SELECT u.*,p.name AS product_name,p.unit AS product_unit
        FROM grooming_product_usage u LEFT JOIN stock_products p ON p.id=u.product_id
        WHERE u.grooming_id IN ({marks}) ORDER BY u.id""",tuple(ids)))
    fotos=_mapear_registros(query_db(f"SELECT * FROM grooming_photos WHERE grooming_id IN ({marks}) ORDER BY id DESC",tuple(ids)))
    com_rows=query_db(f"SELECT * FROM employee_commissions WHERE grooming_id IN ({marks})",tuple(ids))
    comissoes={r["grooming_id"]:r for r in com_rows}
    return checks,produtos,fotos,comissoes

@banho_tosa_bp.route("/banho-tosa",methods=["GET","POST"])
def banho_tosa():
    if request.method=="POST":
        client_id=request.form.get("client_id"); pet_id=request.form.get("pet_id"); data=request.form.get("data"); servico=request.form.get("servico")
        if not all([client_id,pet_id,data,servico]):
            flash("Preencha cliente, pet, data e serviço.","danger"); return redirect(url_for("banho_tosa.banho_tosa"))
        atendimento_id=insert_db("""INSERT INTO grooming_services
            (client_id,pet_id,employee_id,data,hora_entrada,hora_saida,servico,valor,payment_method,status,
             pulgas,carrapatos,machucado,no_pelo,agressivo,observacoes,created_at,updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",(client_id,pet_id,request.form.get("employee_id") or None,data,
            request.form.get("hora_entrada"),request.form.get("hora_saida"),servico,_valor_decimal(request.form.get("valor")),
            request.form.get("payment_method") or "A definir",request.form.get("status") or "Em atendimento",
            1 if request.form.get("pulgas") else 0,1 if request.form.get("carrapatos") else 0,1 if request.form.get("machucado") else 0,
            1 if request.form.get("no_pelo") else 0,1 if request.form.get("agressivo") else 0,request.form.get("observacoes","").strip(),now_iso(),now_iso()))
        registrar_historico(atendimento_id,"Atendimento criado","Administrador")
        flash("Atendimento cadastrado com sucesso.","success"); return redirect(url_for("banho_tosa.banho_tosa"))

    data_filtro=request.args.get("data","").strip(); status_filtro=request.args.get("status","").strip(); busca=request.args.get("busca","").strip()
    sql="""SELECT g.*,c.nome AS cliente_nome,c.whatsapp,p.nome AS pet_nome,p.foto AS pet_foto,e.name AS funcionario_nome
        FROM grooming_services g LEFT JOIN clients c ON c.id=g.client_id LEFT JOIN pets p ON p.id=g.pet_id
        LEFT JOIN employees e ON e.id=g.employee_id WHERE 1=1"""; params=[]
    if data_filtro: sql+=" AND g.data=?"; params.append(data_filtro)
    if status_filtro: sql+=" AND g.status=?"; params.append(status_filtro)
    if busca:
        termo=f"%{busca}%"; sql+=" AND (c.nome LIKE ? OR p.nome LIKE ? OR g.servico LIKE ?)"; params.extend([termo]*3)
    sql+=" ORDER BY g.data DESC,g.hora_entrada DESC,g.id DESC"
    atendimentos=query_db(sql,tuple(params))
    resumo=query_db("""SELECT COUNT(*) AS total,
        SUM(CASE WHEN status NOT IN ('Finalizado','Entregue','Cancelado') THEN 1 ELSE 0 END) AS em_andamento,
        SUM(CASE WHEN status='Finalizado' THEN 1 ELSE 0 END) AS finalizados,
        SUM(CASE WHEN status='Entregue' THEN 1 ELSE 0 END) AS entregues,
        COALESCE(SUM(CASE WHEN status<>'Cancelado' THEN valor ELSE 0 END),0) AS valor_total
        FROM grooming_services WHERE (?='' OR data=?)""",(data_filtro,data_filtro),one=True)
    checks,produtos_usados,fotos,comissoes=_carregar_operacao(atendimentos)
    colunas=[]
    for codigo,titulo,statuses in KANBAN_COLUNAS:
        colunas.append({"codigo":codigo,"titulo":titulo,"statuses":statuses,"items":[a for a in atendimentos if a["status"] in statuses]})
    return render_template("banho_tosa.html",clientes=query_db("SELECT id,nome FROM clients WHERE COALESCE(ativo,1)=1 ORDER BY nome"),
        pets=query_db("""SELECT p.id,p.nome,p.client_id,c.nome AS cliente_nome FROM pets p LEFT JOIN clients c ON c.id=p.client_id
            WHERE COALESCE(p.ativo,1)=1 ORDER BY p.nome"""),funcionarios=query_db("SELECT id,name,commission_rate FROM employees WHERE active=1 ORDER BY name"),
        produtos=query_db("SELECT id,name,quantity,unit FROM stock_products WHERE COALESCE(active,1)=1 ORDER BY name"),
        atendimentos=atendimentos,resumo=resumo,fluxo_status=FLUXO_STATUS_BANHO,colunas=colunas,checks=checks,
        produtos_usados=produtos_usados,fotos=fotos,comissoes=comissoes,data_filtro=data_filtro,status_filtro=status_filtro,busca=busca)

@banho_tosa_bp.route("/banho-tosa/<int:atendimento_id>/checkin",methods=["POST"])
def checkin(atendimento_id):
    atendimento=obter_atendimento(atendimento_id)
    if not atendimento: flash("Atendimento não encontrado.","danger"); return redirect(url_for("banho_tosa.banho_tosa"))
    existente=query_db("SELECT id FROM grooming_checklists WHERE grooming_id=?",(atendimento_id,),one=True)
    dados=(_valor_decimal(request.form.get("checkin_weight")),request.form.get("coat_condition","").strip(),
        request.form.get("ears_condition","").strip(),request.form.get("nails_condition","").strip(),
        request.form.get("behavior","").strip(),request.form.get("tutor_requests","").strip(),now_iso(),now_iso())
    if existente:
        execute_db("""UPDATE grooming_checklists SET checkin_weight=?,coat_condition=?,ears_condition=?,nails_condition=?,
            behavior=?,tutor_requests=?,checked_in_at=COALESCE(checked_in_at,?),updated_at=? WHERE grooming_id=?""",dados+(atendimento_id,))
    else:
        execute_db("""INSERT INTO grooming_checklists
            (grooming_id,checkin_weight,coat_condition,ears_condition,nails_condition,behavior,tutor_requests,checked_in_at,updated_at)
            VALUES (?,?,?,?,?,?,?,?,?)""",(atendimento_id,)+dados)
    execute_db("UPDATE grooming_services SET checked_in_at=COALESCE(checked_in_at,?),started_at=COALESCE(started_at,?),status='Em atendimento',updated_at=? WHERE id=?",(now_iso(),now_iso(),now_iso(),atendimento_id))
    registrar_historico(atendimento_id,"Check-in realizado","Administrador"); flash("Check-in registrado.","success")
    return redirect(request.referrer or url_for("banho_tosa.banho_tosa"))

@banho_tosa_bp.route("/banho-tosa/<int:atendimento_id>/checkout",methods=["POST"])
def checkout(atendimento_id):
    existente=query_db("SELECT id FROM grooming_checklists WHERE grooming_id=?",(atendimento_id,),one=True)
    notas=request.form.get("checkout_notes","").strip()
    if existente: execute_db("UPDATE grooming_checklists SET checkout_notes=?,checked_out_at=?,updated_at=? WHERE grooming_id=?",(notas,now_iso(),now_iso(),atendimento_id))
    else: execute_db("INSERT INTO grooming_checklists (grooming_id,checkout_notes,checked_out_at,updated_at) VALUES (?,?,?,?)",(atendimento_id,notas,now_iso(),now_iso()))
    execute_db("UPDATE grooming_services SET checked_out_at=?,updated_at=? WHERE id=?",(now_iso(),now_iso(),atendimento_id))
    registrar_historico(atendimento_id,"Check-out realizado","Administrador")
    return _processar_status(atendimento_id,"Finalizado")

@banho_tosa_bp.route("/banho-tosa/<int:atendimento_id>/produtos",methods=["POST"])
def adicionar_produto(atendimento_id):
    produto_id=request.form.get("product_id"); quantidade=_valor_decimal(request.form.get("quantity"))
    if not produto_id or quantidade<=0: flash("Informe produto e quantidade válidos.","danger")
    else:
        produto=query_db("SELECT unit FROM stock_products WHERE id=?",(produto_id,),one=True)
        execute_db("INSERT INTO grooming_product_usage (grooming_id,product_id,quantity,unit,stock_applied,created_at) VALUES (?,?,?,?,0,?)",(atendimento_id,produto_id,quantidade,(produto["unit"] if produto else ""),now_iso()))
        registrar_historico(atendimento_id,"Produto adicionado ao consumo","Administrador"); flash("Produto adicionado.","success")
    return redirect(request.referrer or url_for("banho_tosa.banho_tosa"))

@banho_tosa_bp.route("/banho-tosa/produtos/<int:uso_id>/excluir",methods=["POST"])
def excluir_produto(uso_id):
    uso=query_db("SELECT * FROM grooming_product_usage WHERE id=?",(uso_id,),one=True)
    if uso and not uso["stock_applied"]: execute_db("DELETE FROM grooming_product_usage WHERE id=?",(uso_id,)); flash("Produto removido.","info")
    else: flash("Não é possível remover um consumo já baixado no estoque.","danger")
    return redirect(request.referrer or url_for("banho_tosa.banho_tosa"))

@banho_tosa_bp.route("/banho-tosa/<int:atendimento_id>/fotos",methods=["POST"])
def adicionar_foto(atendimento_id):
    arquivo=request.files.get("photo"); categoria=request.form.get("category","Durante")
    if not arquivo or not arquivo.filename: flash("Selecione uma foto.","danger"); return redirect(request.referrer or url_for("banho_tosa.banho_tosa"))
    nome=secure_filename(arquivo.filename); ext=nome.rsplit(".",1)[-1].lower() if "." in nome else ""
    if ext not in ALLOWED_IMAGES: flash("Formato de imagem não permitido.","danger"); return redirect(request.referrer or url_for("banho_tosa.banho_tosa"))
    final=f"{uuid4().hex}.{ext}"; arquivo.save(PHOTO_DIR/final)
    execute_db("INSERT INTO grooming_photos (grooming_id,category,filename,description,created_at) VALUES (?,?,?,?,?)",(atendimento_id,categoria,final,request.form.get("description","").strip(),now_iso()))
    registrar_historico(atendimento_id,f"Foto {categoria.lower()} adicionada","Administrador"); flash("Foto adicionada.","success")
    return redirect(request.referrer or url_for("banho_tosa.banho_tosa"))

@banho_tosa_bp.route("/banho-tosa/fotos/<int:foto_id>/excluir",methods=["POST"])
def excluir_foto(foto_id):
    foto=query_db("SELECT * FROM grooming_photos WHERE id=?",(foto_id,),one=True)
    if foto:
        caminho=PHOTO_DIR/foto["filename"]; caminho.unlink(missing_ok=True); execute_db("DELETE FROM grooming_photos WHERE id=?",(foto_id,)); flash("Foto excluída.","info")
    return redirect(request.referrer or url_for("banho_tosa.banho_tosa"))

@banho_tosa_bp.route("/banho-tosa/<int:atendimento_id>/avancar-status",methods=["POST"])
def avancar_status(atendimento_id):
    atendimento=obter_atendimento(atendimento_id)
    if not atendimento: flash("Atendimento não encontrado.","danger"); return redirect(url_for("banho_tosa.banho_tosa"))
    return _processar_status(atendimento_id,proximo_status(atendimento["status"]))

@banho_tosa_bp.route("/banho-tosa/<int:atendimento_id>/status",methods=["POST"])
def definir_status(atendimento_id):
    novo=request.form.get("status")
    if novo not in FLUXO_STATUS_BANHO and novo!="Cancelado": flash("Status inválido.","danger"); return redirect(request.referrer or url_for("banho_tosa.banho_tosa"))
    return _processar_status(atendimento_id,novo)

@banho_tosa_bp.route("/banho-tosa/<int:atendimento_id>/kanban-status",methods=["POST"])
def kanban_status(atendimento_id):
    novo=request.form.get("status")
    if novo not in FLUXO_STATUS_BANHO and novo!="Cancelado": return jsonify({"ok":False,"message":"Status inválido"}),400
    _processar_status(atendimento_id,novo,json_mode=True); return jsonify({"ok":True,"status":novo})

def _processar_status(atendimento_id,novo,json_mode=False):
    atendimento=obter_atendimento(atendimento_id)
    if not atendimento:
        if json_mode: return False
        flash("Atendimento não encontrado.","danger"); return redirect(url_for("banho_tosa.banho_tosa"))
    atualizar_status_atendimento(atendimento_id,novo); registrar_historico(atendimento_id,f"Status alterado de {atendimento['status']} para {novo}","Administrador")
    publish("ATENDIMENTO_STATUS_ALTERADO",{"entity_type":"grooming_service","entity_id":atendimento_id,"status_anterior":atendimento["status"],"novo_status":novo,"user_name":"Administrador"})
    if novo=="Finalizado":
        atual=obter_atendimento(atendimento_id)
        if not atual["financeiro_lancado"] and lancar_receita_banho_tosa(atendimento_id): execute_db("UPDATE grooming_services SET financeiro_lancado=1 WHERE id=?",(atendimento_id,)); registrar_historico(atendimento_id,"Financeiro lançado","Sistema")
        if not atual["estoque_baixado"] and baixar_estoque_banho_tosa(atendimento_id): execute_db("UPDATE grooming_services SET estoque_baixado=1 WHERE id=?",(atendimento_id,)); registrar_historico(atendimento_id,"Estoque baixado","Sistema")
        if not atual["commission_lancada"] and lancar_comissao(atendimento_id): registrar_historico(atendimento_id,"Comissão calculada","Sistema")
        publish("ATENDIMENTO_FINALIZADO",{"entity_type":"grooming_service","entity_id":atendimento_id,"user_name":"Sistema"})
    if json_mode: return True
    flash(f"Status atualizado para {novo}.","success"); return redirect(request.referrer or url_for("banho_tosa.banho_tosa"))

@banho_tosa_bp.route("/banho-tosa/<int:atendimento_id>/editar",methods=["GET","POST"])
def editar_atendimento(atendimento_id):
    atendimento=obter_atendimento(atendimento_id)
    if not atendimento: flash("Atendimento não encontrado.","danger"); return redirect(url_for("banho_tosa.banho_tosa"))
    if request.method=="POST":
        execute_db("""UPDATE grooming_services SET employee_id=?,data=?,hora_entrada=?,hora_saida=?,servico=?,valor=?,payment_method=?,observacoes=?,updated_at=? WHERE id=?""",
            (request.form.get("employee_id") or None,request.form.get("data"),request.form.get("hora_entrada"),request.form.get("hora_saida"),request.form.get("servico"),_valor_decimal(request.form.get("valor")),request.form.get("payment_method") or "A definir",request.form.get("observacoes","").strip(),now_iso(),atendimento_id))
        registrar_historico(atendimento_id,"Dados do atendimento atualizados","Administrador"); flash("Atendimento atualizado.","success"); return redirect(url_for("banho_tosa.banho_tosa"))
    return render_template("banho_tosa_editar.html",atendimento=atendimento,funcionarios=query_db("SELECT id,name FROM employees WHERE active=1 ORDER BY name"),fluxo_status=FLUXO_STATUS_BANHO)

@banho_tosa_bp.route("/banho-tosa/<int:atendimento_id>/historico")
def historico_atendimento(atendimento_id):
    atendimento=obter_atendimento(atendimento_id)
    if not atendimento: flash("Atendimento não encontrado.","danger"); return redirect(url_for("banho_tosa.banho_tosa"))
    return render_template("historico_atendimento.html",atendimento=atendimento,historico=query_db("SELECT * FROM grooming_history WHERE grooming_id=? ORDER BY created_at DESC,id DESC",(atendimento_id,)))
