(() => {
    const root = document.documentElement;
    const layout = document.getElementById('app-layout');
    const themeToggle = document.getElementById('theme-toggle');
    const themeIcon = themeToggle?.querySelector('.theme-icon');
    const collapseButton = document.getElementById('sidebar-collapse');
    const mobileTrigger = document.getElementById('mobile-menu-trigger');
    const mobileClose = document.getElementById('sidebar-close');
    const overlay = document.getElementById('mobile-overlay');

    function safeGet(key, fallback) {
        try { return localStorage.getItem(key) ?? fallback; } catch (_) { return fallback; }
    }
    function safeSet(key, value) {
        try { localStorage.setItem(key, value); } catch (_) {}
    }

    function applyTheme(theme) {
        const normalized = theme === 'dark' ? 'dark' : 'light';
        root.dataset.theme = normalized;
        if (themeIcon) themeIcon.textContent = normalized === 'dark' ? '☀️' : '🌙';
        themeToggle?.setAttribute('aria-label', normalized === 'dark' ? 'Ativar tema claro' : 'Ativar tema escuro');
        safeSet('petegato-theme', normalized);
    }

    const storedTheme = safeGet('petegato-theme', '');
    const preferredDark = window.matchMedia?.('(prefers-color-scheme: dark)').matches;
    applyTheme(storedTheme || (preferredDark ? 'dark' : 'light'));

    themeToggle?.addEventListener('click', () => {
        applyTheme(root.dataset.theme === 'dark' ? 'light' : 'dark');
    });

    function applyCollapsed(collapsed) {
        if (!layout) return;
        layout.classList.toggle('sidebar-collapsed', collapsed);
        safeSet('petegato-sidebar-collapsed', collapsed ? '1' : '0');
        collapseButton?.setAttribute('aria-label', collapsed ? 'Expandir menu' : 'Recolher menu');
    }

    if (window.innerWidth > 820) {
        applyCollapsed(safeGet('petegato-sidebar-collapsed', '0') === '1');
    }
    collapseButton?.addEventListener('click', () => applyCollapsed(!layout?.classList.contains('sidebar-collapsed')));

    function openMobileMenu() {
        layout?.classList.add('mobile-menu-open');
        document.body.classList.add('mobile-menu-open');
        if (overlay) overlay.hidden = false;
    }
    function closeMobileMenu() {
        layout?.classList.remove('mobile-menu-open');
        document.body.classList.remove('mobile-menu-open');
        if (overlay) overlay.hidden = true;
    }
    mobileTrigger?.addEventListener('click', openMobileMenu);
    mobileClose?.addEventListener('click', closeMobileMenu);
    overlay?.addEventListener('click', closeMobileMenu);
    document.querySelectorAll('.menu a').forEach((link) => link.addEventListener('click', () => {
        if (window.innerWidth <= 820) closeMobileMenu();
    }));

    document.querySelectorAll('.menu a').forEach((link) => {
        const label = link.querySelector('.menu-label')?.textContent?.trim();
        if (label) link.title = label;
    });

    window.addEventListener('resize', () => {
        if (window.innerWidth > 820) closeMobileMenu();
    });
})();
