MODULES = [
    {"key":"dashboard","label":"Dashboard","group":"Visão geral","icon":"🏠","endpoint":"dashboard"},
    {"key":"clientes","label":"Clientes","group":"Relacionamento","icon":"👥","endpoint":"clientes.clientes"},
    {"key":"crm","label":"CRM Inteligente","group":"Relacionamento","icon":"🎯","endpoint":"crm.dashboard"},
    {"key":"fidelidade","label":"Fidelização","group":"Relacionamento","icon":"🏆","endpoint":"fidelidade.dashboard"},
    {"key":"pets","label":"Pets","group":"Operações","icon":"🐶","endpoint":"pets.pets"},
    {"key":"agenda","label":"Agenda","group":"Operações","icon":"📅","endpoint":"agenda.agenda"},
    {"key":"banho_tosa","label":"Banho e Tosa","group":"Operações","icon":"🚿","endpoint":"banho_tosa.banho_tosa"},
    {"key":"motorista","label":"Táxi Pet / Motorista","group":"Operações","icon":"🚗","endpoint":"motorista.motorista"},
    {"key":"financeiro","label":"Financeiro","group":"Gestão","icon":"💰","endpoint":"financeiro.financeiro"},
    {"key":"estoque","label":"Estoque","group":"Gestão","icon":"📦","endpoint":"estoque.estoque"},
    {"key":"relatorios","label":"Relatórios","group":"Gestão","icon":"📈","endpoint":"relatorios.relatorios"},
    {"key":"rh","label":"Gestão de RH","group":"Equipe","icon":"📊","endpoint":"rh.gestao"},
    {"key":"ponto","label":"Relógio de Ponto","group":"Equipe","icon":"🕒","endpoint":"ponto.ponto"},
    {"key":"configuracoes","label":"Configurações","group":"Sistema","icon":"⚙️","endpoint":"configuracoes.configuracoes"},
]

ROUTE_GROUPS = [
    {
        "key": "financeiro",
        "label": "Financeiro Enterprise",
        "icon": "💰",
        "routes": [
            ("Visão geral", "financeiro.financeiro"),
            ("Painel gerencial", "financeiro.painel_gerencial_financeiro"),
            ("Contas a pagar e receber", "financeiro.contas_pagar_receber"),
            ("Despesas da loja", "financeiro.despesas_loja"),
            ("Fluxo projetado", "financeiro.previsao_fluxo_caixa"),
            ("Fechamento mensal", "financeiro.relatorio_mensal"),
            ("Orçamento mensal", "financeiro.orcamento_mensal"),
            ("Calendário financeiro", "financeiro.calendario_financeiro"),
            ("Comparativo mensal", "financeiro.comparativo_financeiro"),
            ("Conciliação financeira", "financeiro.conciliacao_financeira"),
        ],
    },
    {
        "key": "crm",
        "label": "CRM Inteligente",
        "icon": "🎯",
        "routes": [
            ("Visão geral", "crm.dashboard"),
            ("Resumo diário", "crm.daily_brief"),
            ("Agenda CRM", "crm.task_agenda"),
            ("Relatórios CRM", "crm.reports"),
            ("Exportação de relatórios", "crm.export_report_csv"),
            ("Criar campanha", "crm.add_campaign"),
            ("Gerar tarefas automáticas", "crm.generate_tasks"),
            ("Adicionar tarefa", "crm.add_task"),
            ("Registrar contato", "crm.register_contact"),
        ],
    },
]


def get_modules():
    return [dict(module) for module in MODULES]


def get_route_groups():
    return [
        {
            **group,
            "routes": list(group["routes"]),
        }
        for group in ROUTE_GROUPS
    ]
