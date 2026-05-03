PACOTE NÍVEL EMPRESA - ARQUIVOS PRONTOS

Substitua no projeto:
- main.py
- backup.html
- login.html
- motorista.html

Melhorias incluídas:
- backup real em JSON
- download e restore funcionando
- suporte local + S3
- acesso ao módulo backup no PostgreSQL
- login sem preencher usuário/senha automaticamente
- painel do motorista com:
  * Enviar para motorista
  * Gerar rota do dia
  * Abrir rota no Maps
  * WhatsApp rota do dia
  * Copiar mensagem

IMPORTANTE:
- confirme no Render:
  APP_DB_MODE=postgres
  DATABASE_URL=...
  AWS_ACCESS_KEY_ID=...
  AWS_SECRET_ACCESS_KEY=...
  AWS_BUCKET_NAME=...
  AWS_REGION=...

Depois faça novo deploy.
