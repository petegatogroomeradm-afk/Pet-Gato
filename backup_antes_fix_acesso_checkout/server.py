from waitress import serve
from app import app

if __name__ == "__main__":
    print("Servidor profissional iniciado em http://0.0.0.0:8080")
    serve(app, host="0.0.0.0", port=8080, threads=8)