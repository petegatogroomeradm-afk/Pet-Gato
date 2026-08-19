(() => {
    const modal = document.getElementById("global-search-modal");
    const input = document.getElementById("global-search-input");
    const results = document.getElementById("global-search-results");
    const trigger = document.getElementById("global-search-trigger");
    const dashboardTrigger = document.getElementById("dashboard-global-search");

    const notificationTrigger = document.getElementById("notification-trigger");
    const notificationPanel = document.getElementById("notification-panel");
    const notificationClose = document.getElementById("notification-close");
    const notificationCount = document.getElementById("notification-count");
    const notificationList = document.getElementById("notification-list");

    let searchTimer = null;

    const RECENT_KEY = "petegato_recent_pages_v1";

    function rememberCurrentPage() {
        const active = document.querySelector(".menu a.active");
        if (!active || !active.href) return;
        const current = {
            title: active.querySelector(".menu-label")?.textContent?.trim() || document.title,
            icon: active.querySelector(".menu-icon")?.textContent?.trim() || "↗",
            url: active.getAttribute("href")
        };
        let recent = [];
        try { recent = JSON.parse(localStorage.getItem(RECENT_KEY) || "[]"); } catch (_) {}
        recent = [current, ...recent.filter((item) => item.url !== current.url)].slice(0, 6);
        localStorage.setItem(RECENT_KEY, JSON.stringify(recent));
    }

    function renderRecentPages() {
        if (!results) return;
        let recent = [];
        try { recent = JSON.parse(localStorage.getItem(RECENT_KEY) || "[]"); } catch (_) {}
        if (!recent.length) {
            results.innerHTML = '<p class="empty-state">Digite pelo menos duas letras para pesquisar.</p>';
            return;
        }
        results.innerHTML = '<div class="global-search-section-title">Acessados recentemente</div>' + recent.map((item) => `
            <a class="global-result" href="${escapeHtml(item.url)}">
                <span class="global-result-icon">${escapeHtml(item.icon)}</span>
                <span><strong>${escapeHtml(item.title)}</strong><small>Abrir módulo</small></span>
                <span class="global-result-type">Recente</span>
            </a>`).join("");
    }

    function escapeHtml(value) {
        const div = document.createElement("div");
        div.textContent = value ?? "";
        return div.innerHTML;
    }

    function openSearch() {
        if (!modal) return;
        modal.hidden = false;
        renderRecentPages();
        window.setTimeout(() => input?.focus(), 20);
    }

    function closeSearch() {
        if (!modal) return;
        modal.hidden = true;
        if (input) input.value = "";
        if (results) {
            renderRecentPages();
        }
    }

    async function performSearch(term) {
        if (!results) return;
        if (term.trim().length < 2) {
            results.innerHTML =
                '<p class="empty-state">Digite pelo menos duas letras para pesquisar.</p>';
            return;
        }

        results.innerHTML = '<p class="empty-state">Pesquisando...</p>';

        try {
            const response = await fetch(
                `/api/busca-global?q=${encodeURIComponent(term.trim())}`,
                {headers: {"Accept": "application/json"}, cache: "no-store"}
            );
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const payload = await response.json();
            const items = payload.resultados || [];

            if (!items.length) {
                results.innerHTML =
                    '<p class="empty-state">Nenhum resultado encontrado.</p>';
                return;
            }

            results.innerHTML = items.map((item) => `
                <a class="global-result" href="${escapeHtml(item.url)}">
                    <span class="global-result-icon">${escapeHtml(item.icone)}</span>
                    <span>
                        <strong>${escapeHtml(item.titulo)}</strong>
                        <small>${escapeHtml(item.subtitulo)}</small>
                    </span>
                    <span class="global-result-type">${escapeHtml(item.tipo)}</span>
                </a>
            `).join("");
        } catch (error) {
            console.error("Erro na busca global:", error);
            results.innerHTML =
                '<p class="empty-state">Não foi possível realizar a pesquisa.</p>';
        }
    }

    trigger?.addEventListener("click", openSearch);
    dashboardTrigger?.addEventListener("click", openSearch);

    modal?.querySelectorAll("[data-close-global-search]").forEach((element) => {
        element.addEventListener("click", closeSearch);
    });

    input?.addEventListener("input", () => {
        window.clearTimeout(searchTimer);
        searchTimer = window.setTimeout(() => performSearch(input.value), 260);
    });

    document.addEventListener("keydown", (event) => {
        if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "k") {
            event.preventDefault();
            openSearch();
        }
        if (event.key === "Escape") {
            closeSearch();
            if (notificationPanel) notificationPanel.hidden = true;
        }
    });

    function renderNotifications(items) {
        if (!notificationList) return;

        if (!items.length) {
            notificationList.innerHTML =
                '<p class="empty-state">Nenhum alerta no momento.</p>';
            return;
        }

        notificationList.innerHTML = items.map((item) => `
            <a class="notification-item ${escapeHtml(item.nivel)}"
               href="${escapeHtml(item.url)}">
                <span class="notification-icon">${escapeHtml(item.icone)}</span>
                <span>
                    <strong>${escapeHtml(item.titulo)}</strong>
                    <small>${escapeHtml(item.mensagem)}</small>
                </span>
            </a>
        `).join("");
    }

    async function loadNotifications() {
        if (!notificationList || !notificationCount) return;

        try {
            const response = await fetch("/api/notificacoes", {
                headers: {"Accept": "application/json"},
                cache: "no-store"
            });
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const payload = await response.json();
            const total = Number(payload.total || 0);

            notificationCount.textContent = String(total > 99 ? "99+" : total);
            notificationCount.hidden = total === 0;
            renderNotifications(payload.itens || []);
        } catch (error) {
            console.error("Erro ao carregar notificações:", error);
            notificationList.innerHTML =
                '<p class="empty-state">Não foi possível carregar os alertas.</p>';
        }
    }

    notificationTrigger?.addEventListener("click", () => {
        if (!notificationPanel) return;
        notificationPanel.hidden = !notificationPanel.hidden;
        if (!notificationPanel.hidden) loadNotifications();
    });

    notificationClose?.addEventListener("click", () => {
        if (notificationPanel) notificationPanel.hidden = true;
    });

    document.addEventListener("click", (event) => {
        if (
            notificationPanel &&
            !notificationPanel.hidden &&
            !notificationPanel.contains(event.target) &&
            !notificationTrigger?.contains(event.target)
        ) {
            notificationPanel.hidden = true;
        }
    });

    rememberCurrentPage();
    loadNotifications();
    window.setInterval(loadNotifications, 60000);
})();