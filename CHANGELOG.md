# 12.0.0 Stable — 2026-08-10

- Promoção da RC5 para versão estável após pré-flight com 0 falhas.
- Validação de Python, templates, rotas, schema SQLite e diretórios operacionais.
- Financeiro consolidado com orçamento mensal, painel gerencial e conciliação.
- Auditoria, configurações, backup/restore e saúde do sistema consolidados.
- Nenhuma alteração de schema em relação à RC5; promoção de release validada.

# 12.0.0-RC5 — Testes finais e go-live

- Adicionado pré-flight técnico `scripts/go_live_check.py`.
- Checklist de go-live consolidado para operação, financeiro, segurança, backup e produção.
- Nenhuma regra de negócio alterada nesta release.
- Base RC4 preservada para validação final antes da V12 Stable.

# 11.14.0-RC25 — Financeiro Executivo

- Painel executivo independente dos filtros da tabela.
- Indicadores de saldo, resultados diário, semanal e mensal.
- Central de contas vencidas e vencimentos dos próximos sete dias.
- Resumo por origem e últimas movimentações.
- Atualização automática do status de títulos vencidos.
- Sem alteração destrutiva no banco de dados.

# V11.3.0 — Central do Sistema

- Painel de infraestrutura e integrações.
- Teste real do banco e medição de latência.
- Leitura do último backup local.
- Melhorias visuais e responsivas.

# 10.0.0-rc.1

- Corrige painel de Auditoria no PostgreSQL sem comparar TEXT com DATE.
- Usa data ISO parametrizada, compatível com SQLite e PostgreSQL.
- Centraliza os testes na versão real informada pelo arquivo VERSION.
- Consolida a base V10 Beta 10 para a etapa Release Candidate.

# 10.0.0-beta.5 — Compatibilidade de banco

- Nova camada `core/db_compat.py` para diferenças entre SQLite e PostgreSQL.
- Chave primária automática centralizada; módulos não precisam usar `AUTOINCREMENT` ou `SERIAL` diretamente.
- Verificação segura de tabelas e colunas.
- Comunicação migrada para a camada compartilhada.
- Diagnóstico `scripts/check_db_compat.py`.
- Testes unitários para os dois bancos.

# 10.0.0-beta.4.3 — Correção PostgreSQL da Comunicação

- Corrige criação das tabelas da Comunicação no PostgreSQL.
- Usa `SERIAL PRIMARY KEY` no PostgreSQL.
- Mantém `INTEGER PRIMARY KEY AUTOINCREMENT` no SQLite.
- Preserva compatibilidade com bancos antigos e todos os módulos anteriores.

# 10.0.0-beta.4.1

- Corrige Erro 500 ao abrir a Central de Comunicação em bancos antigos.
- Adiciona automaticamente as colunas `telefone` e `whatsapp` à tabela de clientes quando estiverem ausentes.
- Mantém todos os clientes e dados existentes.
- Torna a consulta de contato compatível com números vazios ou nulos.

# Changelog

## 10.0.0-beta.3 — Comprovantes e histórico de vendas

- Comprovante térmico imprimível em 80 mm.
- Abertura automática do comprovante após concluir a venda.
- Tela completa de detalhes da venda.
- Consulta dos itens, pagamentos, cliente, caixa e observações.
- Atalhos para visualizar e reimprimir vendas no histórico do PDV.
- Dados da empresa carregados das Configurações no comprovante.
- Preservação integral do PDV Premium da Beta 2.

## 10.0.0-beta.2 — PDV Premium
- Novo catálogo visual com pesquisa instantânea.
- Filtros por categoria.
- Carrinho touch com controle de quantidade.
- Resumo de subtotal, desconto, total e saldo de pagamento.
- Validação do pagamento antes de concluir.
- Caixa com indicadores e ações recolhíveis.
- Histórico compacto das últimas vendas.
- Layout responsivo para notebook, tablet e monitor touch.

## 10.0.0-alpha.7 — Consumo inteligente por serviço
- Receitas de consumo vinculadas ao nome do serviço.
- Produtos e quantidades configuráveis por atendimento.
- Inclusão automática dos itens ao finalizar/reprocessar.
- Baixa de estoque com proteção contra duplicidade.
- Nova aba Consumo por serviço no Estoque Enterprise.

# Changelog

## 10.0.0-alpha.5 — Dashboard Executivo Integrado

- Consolidado o Dashboard Enterprise como tela principal da V10.
- Indicadores integrados de operação, financeiro, CRM, fidelidade, estoque, transporte e equipe.
- Gráficos de receita, crescimento de clientes e movimentação financeira.
- Atualização automática a cada 60 segundos e atualização manual.
- Estado visual de falha de sincronização.
- Versão centralizada no arquivo `VERSION` e em `core/version.py`.
- Versão exibida nas telas e no endpoint `/health`.
- Testes de contrato das rotas do Dashboard.

## 10.0.0-alpha.4 — Proteção dos módulos

- Verificação de integridade das rotas internas do Financeiro e CRM.

## 10.0.0-alpha.6 — Integração Segura
- Central de Integrações para Banho e Tosa, Financeiro, Estoque e Comissões.
- Diagnóstico automático de atendimentos finalizados com lançamentos pendentes.
- Reprocessamento individual ou em lote com proteção contra duplicidade.
- Baixa de estoque somente quando o consumo foi explicitamente configurado.
- Histórico dos eventos e erros de integração das últimas 24 horas.

## 10.0.0-beta.1
- PDV Enterprise com carrinho, pagamentos divididos e baixa automática no estoque.
- Caixa com abertura, sangria, suprimento, fechamento e conferência.
- Integração automática das vendas com o Financeiro.
- Proteção contra venda sem caixa aberto e estoque insuficiente.

## 10.0.0-beta.4.2
- Corrige Erro 500 persistente na Central de Comunicação.
- Cria e valida as tabelas de modelos e histórico ao abrir o módulo.
- Adiciona compatibilidade com bancos antigos sem a coluna WhatsApp.
- Torna o resumo diário compatível com SQLite e PostgreSQL.
- Mantém a abertura do WhatsApp mesmo quando o histórico do CRM ainda não estiver disponível.

## 10.0.0-beta.6
- Migração centralizada e idempotente para SQLite e PostgreSQL.
- Backup automático do SQLite antes de qualquer atualização estrutural.
- Histórico de versões na tabela `schema_migrations`.
- Diagnóstico corrigido com os nomes reais das colunas do sistema.
- Carregamento opcional do arquivo `.env` sem dependência adicional.
- Exibição clara do banco selecionado e de sua origem.

## 10.0.0-beta.7
- Integração da Agenda com WhatsApp.
- Seis mensagens operacionais prontas.
- Histórico automático em communication_logs e CRM.
- Telefone com fallback entre WhatsApp e telefone.


## 10.0.0-beta.8
- Integração da Comunicação com o Kanban de Banho e Tosa.
- Mensagens rápidas e registro no histórico do CRM.

## 10.0.0-beta.9 — Automações CRM
- Geração inteligente de tarefas de retorno, aniversários e vacinas.
- Deduplicação de pendências automáticas.
- Abertura do WhatsApp pela própria tarefa.
- Registro integrado em `communication_logs` e `crm_contact_history`.
- Campo de origem Manual/Automática e novos estados operacionais.
