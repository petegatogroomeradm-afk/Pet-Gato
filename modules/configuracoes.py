from flask import Blueprint, render_template, request, redirect, url_for, flash
from database import query_db, execute_db, now_iso

configuracoes_bp = Blueprint("configuracoes", __name__)


@configuracoes_bp.route("/configuracoes", methods=["GET", "POST"])
def configuracoes():
    config = query_db("SELECT * FROM settings LIMIT 1", one=True)

    if request.method == "POST":
        company_name = request.form.get("company_name", "").strip()
        cnpj = request.form.get("cnpj", "").strip()
        phone = request.form.get("phone", "").strip()
        whatsapp = request.form.get("whatsapp", "").strip()
        email = request.form.get("email", "").strip()
        address = request.form.get("address", "").strip()
        pix_key = request.form.get("pix_key", "").strip()
        notes = request.form.get("notes", "").strip()

        execute_db("DELETE FROM settings")

        execute_db("""
            INSERT INTO settings
            (company_name, cnpj, phone, whatsapp, email, address, pix_key, notes, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            company_name, cnpj, phone, whatsapp, email,
            address, pix_key, notes, now_iso()
        ))

        flash("Configurações salvas com sucesso.", "success")
        return redirect(url_for("configuracoes.configuracoes"))

    return render_template("configuracoes.html", config=config)