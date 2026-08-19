(() => {
    const root = document.getElementById("enterprise-dashboard");
    if (!root) return;

    const apiUrl = root.dataset.api;
    const refreshButton = document.getElementById("refresh-dashboard");
    const updatedLabel = document.getElementById("dashboard-updated");
    const connectionStatus = document.getElementById("dashboard-connection-status");

    const currency = new Intl.NumberFormat("pt-BR", {
        style: "currency",
        currency: "BRL"
    });

    function getPath(obj, path) {
        return path.split(".").reduce((value, key) => {
            if (value === undefined || value === null) return undefined;
            return value[key];
        }, obj);
    }

    function formatValue(value, format) {
        const number = Number(value || 0);
        if (format === "currency") return currency.format(number);
        if (format === "percent") return `${number.toFixed(1)}%`;
        return String(value ?? "");
    }

    function updateSimpleValues(data) {
        document.querySelectorAll("[data-dashboard-value]").forEach((element) => {
            const value = getPath(data, element.dataset.dashboardValue);
            if (value === undefined) return;
            element.textContent = formatValue(value, element.dataset.format);
        });
        if (updatedLabel && data.gerado_em) {
            updatedLabel.textContent = data.gerado_em;
        }
    }

    function drawChart(series) {
        const chart = document.getElementById("financial-chart");
        if (!chart || !Array.isArray(series) || series.length === 0) return;

        chart.dataset.series = JSON.stringify(series);
        const svg = chart.querySelector("svg");
        const grid = svg.querySelector(".chart-grid");
        const labels = svg.querySelector(".chart-labels");
        const incomeLine = svg.querySelector(".income-line");
        const expenseLine = svg.querySelector(".expense-line");

        const width = 900;
        const height = 280;
        const paddingX = 42;
        const paddingTop = 24;
        const paddingBottom = 40;
        const chartHeight = height - paddingTop - paddingBottom;
        const chartWidth = width - paddingX * 2;
        const max = Math.max(
            1,
            ...series.map((item) => Math.max(Number(item.entradas || 0), Number(item.saidas || 0)))
        );

        grid.innerHTML = "";
        labels.innerHTML = "";

        for (let i = 0; i <= 4; i += 1) {
            const y = paddingTop + (chartHeight / 4) * i;
            const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
            line.setAttribute("x1", paddingX);
            line.setAttribute("x2", width - paddingX);
            line.setAttribute("y1", y);
            line.setAttribute("y2", y);
            grid.appendChild(line);
        }

        const xFor = (index) => paddingX + (chartWidth / Math.max(series.length - 1, 1)) * index;
        const yFor = (value) => paddingTop + chartHeight - (Number(value || 0) / max) * chartHeight;

        incomeLine.setAttribute(
            "points",
            series.map((item, index) => `${xFor(index)},${yFor(item.entradas)}`).join(" ")
        );
        expenseLine.setAttribute(
            "points",
            series.map((item, index) => `${xFor(index)},${yFor(item.saidas)}`).join(" ")
        );

        series.forEach((item, index) => {
            if (index % 2 !== 0 && series.length > 8) return;
            const text = document.createElementNS("http://www.w3.org/2000/svg", "text");
            text.setAttribute("x", xFor(index));
            text.setAttribute("y", height - 12);
            text.setAttribute("text-anchor", "middle");
            text.textContent = item.label;
            labels.appendChild(text);
        });
    }

    function drawBarChart(elementId, series, valueFormatter) {
        const chart = document.getElementById(elementId);
        if (!chart || !Array.isArray(series)) return;
        chart.dataset.series = JSON.stringify(series);
        const max = Math.max(1, ...series.map((item) => Number(item.value || 0)));
        chart.innerHTML = series.map((item) => {
            const value = Number(item.value || 0);
            const height = Math.max(value > 0 ? 4 : 0, (value / max) * 100);
            const shown = valueFormatter ? valueFormatter(value) : String(value);
            return `<div class="dashboard-bar-item" title="${item.label}: ${shown}">
                <span class="dashboard-bar-value">${shown}</span>
                <span class="dashboard-bar-track"><i class="dashboard-bar-fill" style="height:${height}%"></i></span>
                <span class="dashboard-bar-label">${item.label}</span>
            </div>`;
        }).join("");
    }

    async function refreshDashboard() {
        if (!apiUrl || root.classList.contains("dashboard-loading")) return;
        root.classList.add("dashboard-loading");
        try {
            const response = await fetch(`${apiUrl}?refresh=1`, {
                headers: {"Accept": "application/json"},
                cache: "no-store"
            });
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const data = await response.json();
            if (connectionStatus) connectionStatus.textContent = "Sistema atualizado";
            root.classList.remove("dashboard-offline");
            updateSimpleValues(data);
            drawChart(data.grafico_financeiro);
            drawBarChart("monthly-revenue-chart", data.grafico_receita_mensal, (value) => currency.format(value));
            drawBarChart("customer-growth-chart", data.grafico_clientes, (value) => String(Math.round(value)));
        } catch (error) {
            console.error("Falha ao atualizar dashboard:", error);
            if (connectionStatus) connectionStatus.textContent = "Falha na atualização";
            root.classList.add("dashboard-offline");
        } finally {
            root.classList.remove("dashboard-loading");
        }
    }

    refreshButton?.addEventListener("click", refreshDashboard);

    try {
        drawChart(JSON.parse(document.getElementById("financial-chart")?.dataset.series || "[]"));
        drawBarChart("monthly-revenue-chart", JSON.parse(document.getElementById("monthly-revenue-chart")?.dataset.series || "[]"), (value) => currency.format(value));
        drawBarChart("customer-growth-chart", JSON.parse(document.getElementById("customer-growth-chart")?.dataset.series || "[]"), (value) => String(Math.round(value)));
    } catch (error) {
        console.error("Falha ao desenhar gráfico:", error);
    }

    window.setInterval(refreshDashboard, 60000);
})();
