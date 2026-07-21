from pathlib import Path

from flask import Blueprint, flash, redirect, render_template, request, session, url_for

from database import execute_db, now_iso, query_db
from services.cliente_service import obter_ficha_cliente
from services.event_bus import publish
from utils.uploads import ALLOWED_DOCUMENT_EXTENSIONS, ALLOWED_IMAGE_EXTENSIONS, save_upload

clientes_bp = Blueprint("clientes", __name__)
BASE_DIR = Path(__file__).resolve().parent.parent
CLIENT_UPLOAD_DIR = BASE_DIR / "static" / "uploads" / "clientes"
CLIENT_DOC_DIR = BASE_DIR / "static" / "uploads" / "documentos"
CLIENT_GALLERY_DIR = BASE_DIR / "static" / "uploads" / "clientes" / "galeria"


@clientes_bp.route("/clientes", methods=["GET", "POST"])
def clientes():
    if request.method == "POST":
        nome = request.form.get("nome", "").strip()
        if not nome:
            flash("Informe o nome do cliente.", "danger")
            return redirect(url_for("clientes.clientes"))
        try:
            foto = save_upload(request.files.get("foto"), CLIENT_UPLOAD_DIR, ALLOWED_IMAGE_EXTENSIONS)
        except ValueError as exc:
            flash(str(exc), "danger")
            return redirect(url_for("clientes.clientes"))
        execute_db("""
            INSERT INTO clients
            (nome, cpf, telefone, whatsapp, email, endereco, observacoes, foto, tags,
             contato_emergencia, data_nascimento, origem_cadastro, canal_preferido,
             consentimento_marketing, ativo, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            nome, request.form.get("cpf", "").strip(), request.form.get("telefone", "").strip(),
            request.form.get("whatsapp", "").strip(), request.form.get("email", "").strip(),
            request.form.get("endereco", "").strip(), request.form.get("observacoes", "").strip(),
            foto, request.form.get("tags", "").strip(), request.form.get("contato_emergencia", "").strip(),
            request.form.get("data_nascimento", "").strip(), request.form.get("origem_cadastro", "").strip(),
            request.form.get("canal_preferido", "").strip(), 1 if request.form.get("consentimento_marketing") else 0,
            1, now_iso()
        ))
        publish("CLIENTE_CRIADO", {"entity_type": "client", "nome": nome, "user_name": session.get("user_name", "Sistema")})
        flash("Cliente cadastrado com sucesso.", "success")
        return redirect(url_for("clientes.clientes"))

    busca = request.args.get("busca", "").strip()
    params = ()
    where = "WHERE COALESCE(ativo, 1) = 1"
    if busca:
        where += " AND (nome LIKE ? OR telefone LIKE ? OR whatsapp LIKE ? OR cpf LIKE ? OR email LIKE ?)"
        params = tuple(f"%{busca}%" for _ in range(5))
    clientes_lista = query_db(f"SELECT * FROM clients {where} ORDER BY nome", params)
    return render_template("clientes.html", clientes=clientes_lista, busca=busca)


@clientes_bp.route("/clientes/<int:cliente_id>")
def visualizar_cliente(cliente_id):
    ficha = obter_ficha_cliente(cliente_id)
    if not ficha:
        flash("Cliente não encontrado.", "danger")
        return redirect(url_for("clientes.clientes"))
    return render_template("cliente_detalhes.html", **ficha)


@clientes_bp.route("/clientes/<int:cliente_id>/editar", methods=["GET", "POST"])
def editar_cliente(cliente_id):
    cliente = query_db("SELECT * FROM clients WHERE id=?", (cliente_id,), one=True)
    if not cliente:
        flash("Cliente não encontrado.", "danger")
        return redirect(url_for("clientes.clientes"))
    if request.method == "POST":
        foto = cliente["foto"]
        try:
            nova_foto = save_upload(request.files.get("foto"), CLIENT_UPLOAD_DIR, ALLOWED_IMAGE_EXTENSIONS)
            if nova_foto:
                foto = nova_foto
        except ValueError as exc:
            flash(str(exc), "danger")
            return redirect(url_for("clientes.editar_cliente", cliente_id=cliente_id))
        execute_db("""
            UPDATE clients SET nome=?, cpf=?, telefone=?, whatsapp=?, email=?, endereco=?,
                observacoes=?, foto=?, tags=?, contato_emergencia=?, data_nascimento=?,
                origem_cadastro=?, canal_preferido=?, consentimento_marketing=? WHERE id=?
        """, (
            request.form.get("nome", "").strip(), request.form.get("cpf", "").strip(),
            request.form.get("telefone", "").strip(), request.form.get("whatsapp", "").strip(),
            request.form.get("email", "").strip(), request.form.get("endereco", "").strip(),
            request.form.get("observacoes", "").strip(), foto, request.form.get("tags", "").strip(),
            request.form.get("contato_emergencia", "").strip(), request.form.get("data_nascimento", "").strip(),
            request.form.get("origem_cadastro", "").strip(), request.form.get("canal_preferido", "").strip(),
            1 if request.form.get("consentimento_marketing") else 0, cliente_id
        ))
        publish("CLIENTE_ATUALIZADO", {"entity_type": "client", "entity_id": cliente_id, "user_name": session.get("user_name", "Sistema")})
        flash("Cliente atualizado com sucesso.", "success")
        return redirect(url_for("clientes.visualizar_cliente", cliente_id=cliente_id))
    return render_template("cliente_editar.html", cliente=cliente)


@clientes_bp.route("/clientes/<int:cliente_id>/excluir", methods=["POST"])
def excluir_cliente(cliente_id):
    execute_db("UPDATE clients SET ativo=0 WHERE id=?", (cliente_id,))
    publish("CLIENTE_ARQUIVADO", {"entity_type": "client", "entity_id": cliente_id, "user_name": session.get("user_name", "Sistema")})
    flash("Cliente arquivado com sucesso.", "info")
    return redirect(url_for("clientes.clientes"))


@clientes_bp.route("/clientes/<int:cliente_id>/anotacoes/adicionar", methods=["POST"])
def adicionar_anotacao(cliente_id):
    observacao = request.form.get("observacao", "").strip()
    if observacao:
        execute_db("INSERT INTO client_notes (client_id, observacao, usuario, created_at) VALUES (?, ?, ?, ?)",
                   (cliente_id, observacao, session.get("user_name", "Sistema"), now_iso()))
        flash("Anotação cadastrada.", "success")
    return redirect(url_for("clientes.visualizar_cliente", cliente_id=cliente_id))


@clientes_bp.route("/clientes/<int:cliente_id>/documentos/adicionar", methods=["POST"])
def adicionar_documento(cliente_id):
    try:
        arquivo = save_upload(request.files.get("arquivo"), CLIENT_DOC_DIR, ALLOWED_DOCUMENT_EXTENSIONS)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("clientes.visualizar_cliente", cliente_id=cliente_id))
    if arquivo:
        execute_db("INSERT INTO client_documents (client_id, nome, arquivo, tipo, created_at) VALUES (?, ?, ?, ?, ?)",
                   (cliente_id, request.form.get("nome", "").strip() or arquivo, arquivo,
                    request.form.get("tipo", "").strip(), now_iso()))
        flash("Documento enviado.", "success")
    else:
        flash("Selecione um documento.", "danger")
    return redirect(url_for("clientes.visualizar_cliente", cliente_id=cliente_id))

@clientes_bp.route('/clientes/<int:cliente_id>/anotacoes/<int:item_id>/excluir', methods=['POST'])
def excluir_anotacao(cliente_id, item_id):
    execute_db('DELETE FROM client_notes WHERE id=? AND client_id=?', (item_id, cliente_id))
    publish('CLIENTE_ANOTACAO_EXCLUIDA', {'entity_type': 'client', 'entity_id': cliente_id, 'user_name': session.get('user_name', 'Sistema')})
    flash('Anotação removida.', 'info')
    return redirect(url_for('clientes.visualizar_cliente', cliente_id=cliente_id))


@clientes_bp.route('/clientes/<int:cliente_id>/documentos/<int:item_id>/excluir', methods=['POST'])
def excluir_documento(cliente_id, item_id):
    execute_db('DELETE FROM client_documents WHERE id=? AND client_id=?', (item_id, cliente_id))
    publish('CLIENTE_DOCUMENTO_EXCLUIDO', {'entity_type': 'client', 'entity_id': cliente_id, 'user_name': session.get('user_name', 'Sistema')})
    flash('Documento removido do cadastro.', 'info')
    return redirect(url_for('clientes.visualizar_cliente', cliente_id=cliente_id))


@clientes_bp.route("/clientes/<int:cliente_id>/fotos/adicionar", methods=["POST"])
def adicionar_foto(cliente_id):
    try:
        arquivo = save_upload(request.files.get("arquivo"), CLIENT_GALLERY_DIR, ALLOWED_IMAGE_EXTENSIONS)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("clientes.visualizar_cliente", cliente_id=cliente_id))
    if not arquivo:
        flash("Selecione uma imagem.", "danger")
        return redirect(url_for("clientes.visualizar_cliente", cliente_id=cliente_id))
    execute_db(
        "INSERT INTO client_photos (client_id, arquivo, categoria, descricao, created_at) VALUES (?, ?, ?, ?, ?)",
        (cliente_id, arquivo, request.form.get("categoria", "Geral").strip(),
         request.form.get("descricao", "").strip(), now_iso()),
    )
    publish("CLIENTE_FOTO_ADICIONADA", {"entity_type": "client", "entity_id": cliente_id,
            "user_name": session.get("user_name", "Sistema")})
    flash("Foto adicionada à galeria.", "success")
    return redirect(url_for("clientes.visualizar_cliente", cliente_id=cliente_id))


@clientes_bp.route("/clientes/<int:cliente_id>/fotos/<int:item_id>/excluir", methods=["POST"])
def excluir_foto(cliente_id, item_id):
    execute_db("DELETE FROM client_photos WHERE id=? AND client_id=?", (item_id, cliente_id))
    publish("CLIENTE_FOTO_EXCLUIDA", {"entity_type": "client", "entity_id": cliente_id,
            "user_name": session.get("user_name", "Sistema")})
    flash("Foto removida da galeria.", "info")
    return redirect(url_for("clientes.visualizar_cliente", cliente_id=cliente_id))
