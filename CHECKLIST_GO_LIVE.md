# Checklist final — Pet & Gatô Business V12

## 1. Pré-flight técnico
- [ ] `py scripts/check_release.py` sem falhas
- [ ] `py scripts/check_db_compat.py` com estrutura compatível
- [ ] `py scripts/go_live_check.py` sem FALHAS
- [ ] `/health` retorna `status: ok`
- [ ] `/readiness` retorna `status: ready`
- [ ] `logs/errors.log` sem novos tracebacks após os testes

## 2. Segurança e acesso
- [ ] Super Admin acessa todos os módulos
- [ ] Admin da empresa não recebe privilégios de Super Admin
- [ ] usuário comum respeita permissões
- [ ] troca e reset de senha testados
- [ ] `SECRET_KEY` configurada em produção
- [ ] HTTPS e cookies seguros confirmados no ambiente publicado

## 3. Cadastros e operação
- [ ] criar/editar/arquivar/restaurar cliente
- [ ] criar/editar/arquivar/restaurar pet
- [ ] criar/editar funcionário
- [ ] criar/reagendar/cancelar agendamento
- [ ] iniciar, avançar e finalizar Banho & Tosa
- [ ] sincronização Agenda ↔ Banho & Tosa conferida
- [ ] Cliente 360° e Pet 360° conferidos

## 4. Financeiro e PDV
- [ ] entrada e saída manuais
- [ ] pagamento parcial, quitação e estorno
- [ ] conta recorrente
- [ ] orçamento mensal
- [ ] painel gerencial
- [ ] conciliação financeira
- [ ] abrir caixa, venda PDV, sangria/suprimento e fechar caixa
- [ ] comissão gerada e paga sem duplicidade

## 5. Estoque, CRM e RH
- [ ] entrada/saída/ajuste de estoque
- [ ] estoque crítico e inventário
- [ ] CRM, tarefas e comunicação
- [ ] fidelidade/cashback
- [ ] ponto, banco de horas, férias e RH
- [ ] relatórios e exportações

## 6. Backup, restauração e produção
- [ ] backup manual criado pela interface
- [ ] download do backup testado
- [ ] restauração SQLite testada em cópia/ambiente controlado
- [ ] PostgreSQL/Render com backup do provedor confirmado quando aplicável
- [ ] uploads preservados
- [ ] espaço em disco adequado
- [ ] domínio e HTTPS funcionando
- [ ] plano de contingência definido para o primeiro dia

## Critério de promoção para 12.0 Stable
Promover somente quando todos os itens críticos acima estiverem aprovados e não houver novos erros 500 nos fluxos principais.
