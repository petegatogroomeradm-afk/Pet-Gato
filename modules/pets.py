from pathlib import Path
import csv
import io
from datetime import date, datetime, timedelta

from flask import Blueprint, Response, flash, redirect, render_template, request, session, url_for

from database import execute_db, now_iso, query_db
from services.pet_service import obter_ficha_pet
from services.event_bus import publish
from utils.uploads import ALLOWED_DOCUMENT_EXTENSIONS, ALLOWED_IMAGE_EXTENSIONS, save_upload

pets_bp = Blueprint("pets", __name__)
BASE_DIR = Path(__file__).resolve().parent.parent
PET_UPLOAD_DIR = BASE_DIR / "static" / "uploads" / "pets"
PET_DOC_DIR = BASE_DIR / "static" / "uploads" / "documentos"


def _float_or_none(value):
    value = (value or "").strip().replace(",", ".")
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None




def _parse_date(value):
    if not value:
        return None
    text = str(value)[:10]
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _health_center_data():
    pets = query_db("""
        SELECT p.id, p.nome, p.especie, p.raca, p.foto, p.alergias, p.medicamentos,
               p.peso, p.data_nascimento, c.nome AS cliente_nome, c.whatsapp, c.telefone
        FROM pets p
        LEFT JOIN clients c ON c.id = p.client_id
        WHERE COALESCE(p.ativo, 1) = 1
        ORDER BY p.nome
    """)
    vaccines = query_db("""
        SELECT v.*, p.nome AS pet_nome, c.nome AS cliente_nome
        FROM pet_vaccines v
        JOIN pets p ON p.id = v.pet_id
        LEFT JOIN clients c ON c.id = p.client_id
        WHERE COALESCE(p.ativo, 1) = 1
        ORDER BY v.proxima_dose ASC, v.id DESC
    """)
    treatments = query_db("""
        SELECT t.*, p.nome AS pet_nome, c.nome AS cliente_nome
        FROM pet_health_treatments t
        JOIN pets p ON p.id = t.pet_id
        LEFT JOIN clients c ON c.id = p.client_id
        WHERE COALESCE(p.ativo, 1) = 1
        ORDER BY t.next_date ASC, t.id DESC
    """)
    today = date.today()
    limit = today + timedelta(days=30)
    alerts = []
    pets_with_vaccine = set()
    for item in vaccines:
        pets_with_vaccine.add(item["pet_id"])
        due = _parse_date(item["proxima_dose"])
        if not due or due > limit:
            continue
        days = (due - today).days
        alerts.append({
            "kind": "Vacina", "pet_id": item["pet_id"], "pet_nome": item["pet_nome"],
            "cliente_nome": item["cliente_nome"], "description": item["vacina"],
            "due_date": due.isoformat(), "days": days,
            "status": "Vencida" if days < 0 else ("Hoje" if days == 0 else "Próxima"),
            "priority": 0 if days < 0 else 1,
        })
    for item in treatments:
        due = _parse_date(item["next_date"])
        if not due or due > limit:
            continue
        days = (due - today).days
        alerts.append({
            "kind": item["treatment_type"] or "Tratamento", "pet_id": item["pet_id"],
            "pet_nome": item["pet_nome"], "cliente_nome": item["cliente_nome"],
            "description": item["product_name"] or item["treatment_type"],
            "due_date": due.isoformat(), "days": days,
            "status": "Vencido" if days < 0 else ("Hoje" if days == 0 else "Próximo"),
            "priority": 0 if days < 0 else 1,
        })
    alerts.sort(key=lambda x: (x["priority"], x["due_date"], x["pet_nome"]))
    no_vaccine = [pet for pet in pets if pet["id"] not in pets_with_vaccine]
    clinical_attention = [pet for pet in pets if pet["alergias"] or pet["medicamentos"]]
    return {
        "pets": pets, "alerts": alerts, "no_vaccine": no_vaccine,
        "clinical_attention": clinical_attention,
        "stats": {
            "active_pets": len(pets),
            "overdue": sum(1 for item in alerts if item["days"] < 0),
            "next_7": sum(1 for item in alerts if 0 <= item["days"] <= 7),
            "next_30": sum(1 for item in alerts if 0 <= item["days"] <= 30),
            "no_vaccine": len(no_vaccine),
            "clinical_attention": len(clinical_attention),
        },
    }


@pets_bp.route("/pets/saude")
def central_saude():
    return render_template("pets_saude.html", **_health_center_data())


@pets_bp.route("/pets/saude/exportar.csv")
def exportar_alertas_saude():
    data = _health_center_data()
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, delimiter=";")
    writer.writerow(["Tipo", "Pet", "Tutor", "Descrição", "Vencimento", "Status", "Dias"])
    for item in data["alerts"]:
        writer.writerow([item["kind"], item["pet_nome"], item["cliente_nome"] or "",
                         item["description"], item["due_date"], item["status"], item["days"]])
    filename = f"alertas_saude_pets_{date.today().isoformat()}.csv"
    return Response(output.getvalue(), mimetype="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f"attachment; filename={filename}"})


@pets_bp.route("/pets", methods=["GET", "POST"])
def pets():
    if request.method == "POST":
        client_id = request.form.get("client_id", "").strip()
        nome = request.form.get("nome", "").strip()
        if not client_id or not nome:
            flash("Informe o tutor e o nome do pet.", "danger")
            return redirect(url_for("pets.pets"))

        foto = None
        try:
            foto = save_upload(request.files.get("foto"), PET_UPLOAD_DIR, ALLOWED_IMAGE_EXTENSIONS)
        except ValueError as exc:
            flash(str(exc), "danger")
            return redirect(url_for("pets.pets"))

        fields = (
            client_id, nome, request.form.get("especie", "").strip(), request.form.get("raca", "").strip(),
            request.form.get("porte", "").strip(), request.form.get("idade", "").strip(),
            request.form.get("sexo", "").strip(), request.form.get("cor", "").strip(),
            _float_or_none(request.form.get("peso")), 1 if request.form.get("castrado") == "1" else 0,
            request.form.get("data_nascimento", "").strip(), request.form.get("microchip", "").strip(),
            request.form.get("alergias", "").strip(), request.form.get("medicamentos", "").strip(),
            request.form.get("alimentacao", "").strip(), request.form.get("temperamento", "").strip(),
            request.form.get("preferencia_tosa", "").strip(), foto,
            request.form.get("observacoes", "").strip(), 1, now_iso()
        )
        execute_db("""
            INSERT INTO pets
            (client_id, nome, especie, raca, porte, idade, sexo, cor, peso, castrado,
             data_nascimento, microchip, alergias, medicamentos, alimentacao, temperamento,
             preferencia_tosa, foto, observacoes, ativo, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, fields)
        publish("PET_CRIADO", {"entity_type": "pet", "nome": nome, "user_name": session.get("user_name", "Sistema")})
        flash("Pet cadastrado com sucesso.", "success")
        return redirect(url_for("pets.pets"))

    busca = request.args.get("busca", "").strip()
    status = request.args.get("status", "ativos").strip()
    clientes = query_db("SELECT id, nome FROM clients WHERE COALESCE(ativo, 1) = 1 ORDER BY nome")
    params = ()
    where = "WHERE 1=1"
    if status == "ativos":
        where += " AND COALESCE(p.ativo, 1) = 1"
    elif status == "arquivados":
        where += " AND COALESCE(p.ativo, 1) = 0"
    if busca:
        where += " AND (p.nome LIKE ? OR c.nome LIKE ? OR p.raca LIKE ? OR p.especie LIKE ?)"
        params = tuple(f"%{busca}%" for _ in range(4))
    pets_lista = query_db(f"""
        SELECT p.*, c.nome AS cliente_nome FROM pets p
        LEFT JOIN clients c ON c.id = p.client_id
        {where} ORDER BY p.nome
    """, params)
    return render_template("pets.html", pets=pets_lista, clientes=clientes, busca=busca, status=status)


@pets_bp.route("/pets/<int:pet_id>")
def visualizar_pet(pet_id):
    ficha = obter_ficha_pet(pet_id)
    if not ficha:
        flash("Pet não encontrado.", "danger")
        return redirect(url_for("pets.pets"))
    return render_template("pet_detalhes.html", **ficha)


@pets_bp.route("/pets/<int:pet_id>/prontuario")
def imprimir_prontuario(pet_id):
    ficha = obter_ficha_pet(pet_id)
    if not ficha:
        flash("Pet não encontrado.", "danger")
        return redirect(url_for("pets.pets"))
    return render_template("pet_prontuario_impressao.html", **ficha)


@pets_bp.route("/pets/<int:pet_id>/editar", methods=["GET", "POST"])
def editar_pet(pet_id):
    pet = query_db("SELECT * FROM pets WHERE id = ?", (pet_id,), one=True)
    if not pet:
        flash("Pet não encontrado.", "danger")
        return redirect(url_for("pets.pets"))
    if request.method == "POST":
        foto = pet["foto"]
        try:
            nova_foto = save_upload(request.files.get("foto"), PET_UPLOAD_DIR, ALLOWED_IMAGE_EXTENSIONS)
            if nova_foto:
                foto = nova_foto
        except ValueError as exc:
            flash(str(exc), "danger")
            return redirect(url_for("pets.editar_pet", pet_id=pet_id))

        execute_db("""
            UPDATE pets SET client_id=?, nome=?, especie=?, raca=?, porte=?, idade=?, sexo=?, cor=?,
                peso=?, castrado=?, data_nascimento=?, microchip=?, alergias=?, medicamentos=?,
                alimentacao=?, temperamento=?, preferencia_tosa=?, foto=?, observacoes=?
            WHERE id=?
        """, (
            request.form.get("client_id"), request.form.get("nome", "").strip(), request.form.get("especie", "").strip(),
            request.form.get("raca", "").strip(), request.form.get("porte", "").strip(), request.form.get("idade", "").strip(),
            request.form.get("sexo", "").strip(), request.form.get("cor", "").strip(), _float_or_none(request.form.get("peso")),
            1 if request.form.get("castrado") == "1" else 0, request.form.get("data_nascimento", "").strip(),
            request.form.get("microchip", "").strip(), request.form.get("alergias", "").strip(),
            request.form.get("medicamentos", "").strip(), request.form.get("alimentacao", "").strip(),
            request.form.get("temperamento", "").strip(), request.form.get("preferencia_tosa", "").strip(),
            foto, request.form.get("observacoes", "").strip(), pet_id
        ))
        publish("PET_ATUALIZADO", {"entity_type": "pet", "entity_id": pet_id, "user_name": session.get("user_name", "Sistema")})
        flash("Ficha do pet atualizada.", "success")
        return redirect(url_for("pets.visualizar_pet", pet_id=pet_id))
    clientes = query_db("SELECT id, nome FROM clients WHERE COALESCE(ativo, 1) = 1 ORDER BY nome")
    return render_template("pet_editar.html", pet=pet, clientes=clientes)


@pets_bp.route("/pets/<int:pet_id>/excluir", methods=["POST"])
def excluir_pet(pet_id):
    execute_db("UPDATE pets SET ativo = 0 WHERE id = ?", (pet_id,))
    publish("PET_ARQUIVADO", {"entity_type": "pet", "entity_id": pet_id, "user_name": session.get("user_name", "Sistema")})
    flash("Pet arquivado com sucesso.", "info")
    return redirect(url_for("pets.pets"))


@pets_bp.route("/pets/<int:pet_id>/vacinas/adicionar", methods=["POST"])
def adicionar_vacina(pet_id):
    vacina = request.form.get("vacina", "").strip()
    if not vacina:
        flash("Informe o nome da vacina.", "danger")
    else:
        execute_db("""INSERT INTO pet_vaccines
            (pet_id, vacina, data_aplicacao, proxima_dose, veterinario, observacoes, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)""", (
            pet_id, vacina, request.form.get("data_aplicacao", ""), request.form.get("proxima_dose", ""),
            request.form.get("veterinario", "").strip(), request.form.get("observacoes", "").strip(), now_iso()
        ))
        flash("Vacina cadastrada.", "success")
    return redirect(url_for("pets.visualizar_pet", pet_id=pet_id))


@pets_bp.route("/pets/<int:pet_id>/vacinas/<int:item_id>/excluir", methods=["POST"])
def excluir_vacina(pet_id, item_id):
    execute_db("DELETE FROM pet_vaccines WHERE id=? AND pet_id=?", (item_id, pet_id))
    flash("Vacina removida.", "info")
    return redirect(url_for("pets.visualizar_pet", pet_id=pet_id))


@pets_bp.route("/pets/<int:pet_id>/tratamentos/adicionar", methods=["POST"])
def adicionar_tratamento(pet_id):
    treatment_type = request.form.get("treatment_type", "").strip()
    if treatment_type not in {"Vermífugo", "Antipulgas"}:
        flash("Selecione Vermífugo ou Antipulgas.", "danger")
        return redirect(url_for("pets.visualizar_pet", pet_id=pet_id))

    execute_db(
        """
        INSERT INTO pet_health_treatments
        (pet_id, treatment_type, product_name, application_date, next_date,
         veterinarian, observations, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            pet_id,
            treatment_type,
            request.form.get("product_name", "").strip(),
            request.form.get("application_date", "").strip(),
            request.form.get("next_date", "").strip(),
            request.form.get("veterinarian", "").strip(),
            request.form.get("observations", "").strip(),
            now_iso(),
        ),
    )
    publish(
        "PET_TRATAMENTO_CRIADO",
        {
            "entity_type": "pet",
            "entity_id": pet_id,
            "treatment_type": treatment_type,
            "user_name": session.get("user_name", "Sistema"),
        },
    )
    flash(f"{treatment_type} cadastrado com sucesso.", "success")
    return redirect(url_for("pets.visualizar_pet", pet_id=pet_id))


@pets_bp.route("/pets/<int:pet_id>/tratamentos/<int:item_id>/excluir", methods=["POST"])
def excluir_tratamento(pet_id, item_id):
    execute_db(
        "DELETE FROM pet_health_treatments WHERE id = ? AND pet_id = ?",
        (item_id, pet_id),
    )
    publish(
        "PET_TRATAMENTO_REMOVIDO",
        {
            "entity_type": "pet",
            "entity_id": pet_id,
            "treatment_id": item_id,
            "user_name": session.get("user_name", "Sistema"),
        },
    )
    flash("Tratamento removido.", "info")
    return redirect(url_for("pets.visualizar_pet", pet_id=pet_id))


@pets_bp.route("/pets/<int:pet_id>/pesagens/adicionar", methods=["POST"])
def adicionar_pesagem(pet_id):
    peso = _float_or_none(request.form.get("peso"))
    if peso is None:
        flash("Informe um peso válido.", "danger")
    else:
        execute_db("INSERT INTO pet_weight_history (pet_id, peso, data, created_at) VALUES (?, ?, ?, ?)",
                   (pet_id, peso, request.form.get("data", ""), now_iso()))
        execute_db("UPDATE pets SET peso=? WHERE id=?", (peso, pet_id))
        flash("Pesagem cadastrada.", "success")
    return redirect(url_for("pets.visualizar_pet", pet_id=pet_id))


@pets_bp.route("/pets/<int:pet_id>/anotacoes/adicionar", methods=["POST"])
def adicionar_anotacao(pet_id):
    observacao = request.form.get("observacao", "").strip()
    if observacao:
        execute_db("INSERT INTO pet_notes (pet_id, observacao, usuario, created_at) VALUES (?, ?, ?, ?)",
                   (pet_id, observacao, session.get("user_name", "Sistema"), now_iso()))
        flash("Anotação cadastrada.", "success")
    return redirect(url_for("pets.visualizar_pet", pet_id=pet_id))


@pets_bp.route("/pets/<int:pet_id>/documentos/adicionar", methods=["POST"])
def adicionar_documento(pet_id):
    try:
        arquivo = save_upload(request.files.get("arquivo"), PET_DOC_DIR, ALLOWED_DOCUMENT_EXTENSIONS)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("pets.visualizar_pet", pet_id=pet_id))
    if not arquivo:
        flash("Selecione um documento.", "danger")
    else:
        execute_db("INSERT INTO pet_documents (pet_id, nome, arquivo, tipo, created_at) VALUES (?, ?, ?, ?, ?)",
                   (pet_id, request.form.get("nome", "").strip() or arquivo, arquivo, request.form.get("tipo", "").strip(), now_iso()))
        flash("Documento enviado.", "success")
    return redirect(url_for("pets.visualizar_pet", pet_id=pet_id))


@pets_bp.route("/pets/<int:pet_id>/fotos/adicionar", methods=["POST"])
def adicionar_foto(pet_id):
    try:
        arquivo = save_upload(request.files.get("arquivo"), PET_UPLOAD_DIR, ALLOWED_IMAGE_EXTENSIONS)
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("pets.visualizar_pet", pet_id=pet_id))
    if arquivo:
        categoria = request.form.get("categoria", "Outros")
        execute_db("INSERT INTO pet_photos (pet_id, arquivo, categoria, descricao, created_at) VALUES (?, ?, ?, ?, ?)",
                   (pet_id, arquivo, categoria, request.form.get("descricao", "").strip(), now_iso()))
        if categoria == "Perfil":
            execute_db("UPDATE pets SET foto=? WHERE id=?", (arquivo, pet_id))
        flash("Foto enviada.", "success")
    else:
        flash("Selecione uma foto.", "danger")
    return redirect(url_for("pets.visualizar_pet", pet_id=pet_id))


@pets_bp.route("/pets/<int:pet_id>/historico-medico/adicionar", methods=["POST"])
def adicionar_historico_medico(pet_id):
    descricao = request.form.get("descricao", "").strip()
    if descricao:
        execute_db("""INSERT INTO pet_medical_history
            (pet_id, data, tipo, descricao, veterinario, created_at) VALUES (?, ?, ?, ?, ?, ?)""", (
            pet_id, request.form.get("data", ""), request.form.get("tipo", "").strip(), descricao,
            request.form.get("veterinario", "").strip(), now_iso()
        ))
        flash("Histórico médico cadastrado.", "success")
    return redirect(url_for("pets.visualizar_pet", pet_id=pet_id))

@pets_bp.route('/pets/<int:pet_id>/pesagens/<int:item_id>/excluir', methods=['POST'])
def excluir_pesagem(pet_id, item_id):
    execute_db('DELETE FROM pet_weight_history WHERE id=? AND pet_id=?', (item_id, pet_id))
    ultimo = query_db('SELECT peso FROM pet_weight_history WHERE pet_id=? ORDER BY data DESC, id DESC LIMIT 1', (pet_id,), one=True)
    execute_db('UPDATE pets SET peso=? WHERE id=?', ((ultimo['peso'] if ultimo else None), pet_id))
    flash('Pesagem removida.', 'info')
    return redirect(url_for('pets.visualizar_pet', pet_id=pet_id))


@pets_bp.route('/pets/<int:pet_id>/anotacoes/<int:item_id>/excluir', methods=['POST'])
def excluir_anotacao_pet(pet_id, item_id):
    execute_db('DELETE FROM pet_notes WHERE id=? AND pet_id=?', (item_id, pet_id))
    flash('Anotação removida.', 'info')
    return redirect(url_for('pets.visualizar_pet', pet_id=pet_id))


@pets_bp.route('/pets/<int:pet_id>/historico-medico/<int:item_id>/excluir', methods=['POST'])
def excluir_historico_medico(pet_id, item_id):
    execute_db('DELETE FROM pet_medical_history WHERE id=? AND pet_id=?', (item_id, pet_id))
    flash('Registro médico removido.', 'info')
    return redirect(url_for('pets.visualizar_pet', pet_id=pet_id))


@pets_bp.route('/pets/<int:pet_id>/fotos/<int:item_id>/excluir', methods=['POST'])
def excluir_foto(pet_id, item_id):
    foto = query_db('SELECT arquivo FROM pet_photos WHERE id=? AND pet_id=?', (item_id, pet_id), one=True)
    execute_db('DELETE FROM pet_photos WHERE id=? AND pet_id=?', (item_id, pet_id))
    if foto:
        perfil = query_db('SELECT foto FROM pets WHERE id=?', (pet_id,), one=True)
        if perfil and perfil['foto'] == foto['arquivo']:
            execute_db('UPDATE pets SET foto=NULL WHERE id=?', (pet_id,))
    flash('Foto removida da galeria.', 'info')
    return redirect(url_for('pets.visualizar_pet', pet_id=pet_id))


@pets_bp.route('/pets/<int:pet_id>/documentos/<int:item_id>/excluir', methods=['POST'])
def excluir_documento_pet(pet_id, item_id):
    execute_db('DELETE FROM pet_documents WHERE id=? AND pet_id=?', (item_id, pet_id))
    flash('Documento removido.', 'info')
    return redirect(url_for('pets.visualizar_pet', pet_id=pet_id))

@pets_bp.route("/pets/<int:pet_id>/saude/atualizar", methods=["POST"])
def atualizar_perfil_saude(pet_id):
    pet = query_db("SELECT id FROM pets WHERE id = ?", (pet_id,), one=True)
    if not pet:
        flash("Pet não encontrado.", "danger")
        return redirect(url_for("pets.pets"))

    altura_cm = _float_or_none(request.form.get("altura_cm"))
    valores = (
        altura_cm,
        request.form.get("tipo_sanguineo", "").strip(),
        request.form.get("plano_saude", "").strip(),
        request.form.get("veterinario_nome", "").strip(),
        request.form.get("clinica_nome", "").strip(),
        request.form.get("doencas_cronicas", "").strip(),
        request.form.get("cirurgias", "").strip(),
        request.form.get("restricoes", "").strip(),
        request.form.get("medos", "").strip(),
        request.form.get("cuidados_especiais", "").strip(),
        now_iso(),
    )

    existente = query_db(
        "SELECT id FROM pet_health_profiles WHERE pet_id = ?",
        (pet_id,),
        one=True,
    )

    if existente:
        execute_db(
            """
            UPDATE pet_health_profiles
            SET altura_cm = ?, tipo_sanguineo = ?, plano_saude = ?,
                veterinario_nome = ?, clinica_nome = ?, doencas_cronicas = ?,
                cirurgias = ?, restricoes = ?, medos = ?,
                cuidados_especiais = ?, updated_at = ?
            WHERE pet_id = ?
            """,
            valores + (pet_id,),
        )
    else:
        execute_db(
            """
            INSERT INTO pet_health_profiles
            (altura_cm, tipo_sanguineo, plano_saude, veterinario_nome,
             clinica_nome, doencas_cronicas, cirurgias, restricoes,
             medos, cuidados_especiais, updated_at, pet_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            valores + (pet_id,),
        )

    publish(
        "PET_SAUDE_ATUALIZADA",
        {
            "entity_type": "pet",
            "entity_id": pet_id,
            "user_name": session.get("user_name", "Sistema"),
        },
    )
    flash("Informações de saúde atualizadas.", "success")
    return redirect(url_for("pets.visualizar_pet", pet_id=pet_id) + "#saude")


@pets_bp.route("/pets/<int:pet_id>/restaurar", methods=["POST"])
def restaurar_pet(pet_id):
    execute_db("UPDATE pets SET ativo=1 WHERE id=?", (pet_id,))
    publish("PET_RESTAURADO", {"entity_type": "pet", "entity_id": pet_id, "user_name": session.get("user_name", "Sistema")})
    flash("Pet restaurado com sucesso.", "success")
    return redirect(url_for("pets.pets", status="arquivados"))
