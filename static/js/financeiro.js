(() => {
  const canvas = document.getElementById("cash-flow-chart");
  const source = document.getElementById("finance-flow-data");
  if (!canvas || !source || typeof Chart === "undefined") return;

  const payload = JSON.parse(source.textContent || "{}");
  const labels = payload.labels || [];
  const entries = payload.entries || [];
  const exits = payload.exits || [];
  const result = entries.map((value, index) => Number(value || 0) - Number(exits[index] || 0));

  new Chart(canvas, {
    type: "line",
    data: {
      labels,
      datasets: [
        {label: "Entradas", data: entries, borderColor: "#16a34a", backgroundColor: "rgba(22,163,74,.10)", tension: .35, fill: true},
        {label: "Saídas", data: exits, borderColor: "#dc2626", backgroundColor: "rgba(220,38,38,.08)", tension: .35, fill: true},
        {label: "Resultado", data: result, borderColor: "#7c3aed", backgroundColor: "rgba(124,58,237,.08)", tension: .35}
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {mode: "index", intersect: false},
      plugins: {legend: {position: "bottom"}},
      scales: {
        y: {beginAtZero: true, ticks: {callback: value => `R$ ${Number(value).toLocaleString("pt-BR")}`}},
        x: {grid: {display: false}}
      }
    }
  });
})();
