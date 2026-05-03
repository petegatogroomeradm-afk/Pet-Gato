document.addEventListener("DOMContentLoaded", () => {
  const splash = document.getElementById("splash-screen");
  window.setTimeout(() => splash?.classList.add("hidden"), 900);

  const clockEl = document.querySelector('.live-clock');
  if (clockEl) {
    const tick = () => {
      const now = new Date();
      const parts = new Intl.DateTimeFormat('pt-BR', {
        day:'2-digit', month:'2-digit', year:'numeric',
        hour:'2-digit', minute:'2-digit', second:'2-digit'
      }).format(now);
      clockEl.textContent = parts;
    };
    tick();
    setInterval(tick, 1000);
  }

  const form = document.getElementById('punch-form');
  if (form) {
    form.addEventListener('submit', () => {
      const btn = form.querySelector('button[type="submit"]');
      if (btn) {
        btn.disabled = true;
        btn.textContent = 'Registrando...';
      }
      if (navigator.vibrate) navigator.vibrate(45);
    });
  }
});
