# Pet & Gatô Business — implantação no Render

## 1. Antes do deploy

1. Guarde uma cópia do ZIP e do arquivo `instance/petegato_business_v3.db`.
2. Crie uma senha forte para `ADMIN_PASSWORD`.
3. Não publique o arquivo `.env` nem o banco SQLite em repositório público.

## 2. Deploy com Blueprint

1. Envie esta pasta para um repositório privado no GitHub.
2. No Render, escolha **New > Blueprint** e selecione o repositório.
3. O arquivo `render.yaml` criará:
   - serviço web;
   - PostgreSQL;
   - disco persistente para `static/uploads`.
4. Quando solicitado, preencha `ADMIN_PASSWORD`.
5. Aguarde `/health` retornar `status: ok`.

## 3. Migrar os dados atuais

No computador, com Python e as dependências instaladas, configure a URL externa do PostgreSQL:

```powershell
$env:DATABASE_URL="postgresql://..."
$env:ADMIN_PASSWORD="SUA-SENHA-FORTE"
py scripts/migrate_sqlite_to_postgres.py instance/petegato_business_v3.db
```

Execute uma única vez. O script não apaga dados existentes e ignora IDs duplicados.

## 4. Fotos e documentos

O disco do Render é montado em `static/uploads`. Copie o conteúdo da pasta local `static/uploads` para o disco antes da entrada oficial, caso existam arquivos atuais. Banco e arquivos são backups diferentes.

## 5. Verificação obrigatória

- `/health` responde 200.
- `/readiness` responde 200 e `database: ok`.
- usuário não autenticado é direcionado para `/login`.
- cadastro de cliente e pet persiste após novo deploy/restart.
- upload de foto permanece após restart.
- agenda, banho e tosa e financeiro gravam no PostgreSQL.
- backup do PostgreSQL está ativo no plano escolhido.

## 6. Comando de produção

```text
gunicorn main:app --workers 2 --threads 4 --timeout 120 --access-logfile - --error-logfile -
```

O modo debug está desativado e os cookies são marcados como seguros em produção.
