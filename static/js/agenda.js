(() => {
  const toast = document.getElementById('agenda-toast');
  let dragged = null;

  function showToast(message, ok = true) {
    if (!toast) return;
    toast.textContent = message;
    toast.className = `agenda-toast show ${ok ? 'ok' : 'error'}`;
    setTimeout(() => toast.classList.remove('show'), 2600);
  }

  document.querySelectorAll('.calendar-event[draggable="true"]').forEach((event) => {
    event.addEventListener('dragstart', () => {
      dragged = event;
      event.classList.add('dragging');
    });
    event.addEventListener('dragend', () => event.classList.remove('dragging'));
  });

  document.querySelectorAll('.drop-zone').forEach((zone) => {
    zone.addEventListener('dragover', (e) => {
      e.preventDefault();
      zone.classList.add('drag-over');
    });
    zone.addEventListener('dragleave', () => zone.classList.remove('drag-over'));
    zone.addEventListener('drop', async (e) => {
      e.preventDefault();
      zone.classList.remove('drag-over');
      if (!dragged) return;
      const id = dragged.dataset.id;
      const date = zone.dataset.date;
      const time = zone.dataset.time || dragged.querySelector('strong')?.textContent?.slice(0, 5) || '09:00';
      const url = window.AGENDA_MOVE_URL.replace('/0/', `/${id}/`);
      try {
        let response = await fetch(url, {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({date, time})
        });
        let data = await response.json();
        if (!response.ok && data.requires_override) {
          const confirmar = window.confirm(`${data.message}

Deseja realizar o encaixe acima da capacidade?`);
          if (confirmar) {
            response = await fetch(url, {
              method: 'POST',
              headers: {'Content-Type': 'application/json'},
              body: JSON.stringify({date, time, force_capacity_override: true})
            });
            data = await response.json();
          }
        }
        if (!response.ok || !data.ok) throw new Error(data.message || 'Não foi possível reagendar.');
        showToast(data.message || 'Agendamento reagendado.');
        setTimeout(() => location.reload(), 550);
      } catch (error) {
        showToast(error.message, false);
      }
    });
  });

  function bindPetFilter(clientId, petId) {
    const client = document.getElementById(clientId);
    const pet = document.getElementById(petId);
    if (!client || !pet) return;
    function filter() {
      const value = client.value;
      [...pet.options].forEach((option, index) => {
        if (index === 0) return;
        option.hidden = Boolean(value) && option.dataset.client !== value;
      });
      if (pet.selectedOptions[0]?.hidden) pet.value = '';
    }
    client.addEventListener('change', filter);
    filter();
  }
  bindPetFilter('agenda-client', 'agenda-pet');
  bindPetFilter('wait-client', 'wait-pet');

  const eventDialog = document.getElementById('agenda-event-dialog');
  const dialogClose = eventDialog?.querySelector('.dialog-close');

  function esc(value) {
    return String(value ?? '').replace(/[&<>'"]/g, (char) => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[char]));
  }

  function statusAction(id, status, label) {
    return `<form method="post" action="/agenda/${id}/status-rapido"><input type="hidden" name="status" value="${esc(status)}"><button>${esc(label)}</button></form>`;
  }

  function smartCheckinAction(id, label = 'Registrar chegada') {
    return `<form method="post" class="js-smart-checkin" action="/agenda/${id}/checkin-inteligente"><button>${esc(label)}</button></form>`;
  }

  async function runSmartCheckin(form) {
    const button = form.querySelector('button');
    const card = form.closest('.calendar-event');
    const original = button?.textContent || 'Chegou';
    if (button) { button.disabled = true; button.textContent = 'Registrando...'; }
    try {
      const response = await fetch(form.action, {
        method: 'POST',
        headers: {'X-Requested-With': 'XMLHttpRequest', 'Accept': 'application/json'}
      });
      const data = await response.json();
      if (!response.ok || !data.ok) throw new Error(data.message || 'Não foi possível registrar o check-in.');
      if (card) {
        card.dataset.status = 'Na loja';
        card.className = card.className.replace(/event-[^\s]+/g, '').trim() + ' event-na-loja';
        const actions = card.querySelector('.calendar-event-actions');
        if (actions) actions.innerHTML = `<form method="post" action="/agenda/${card.dataset.id}/iniciar"><button>Iniciar</button></form><a href="/agenda/${card.dataset.id}/editar">Editar</a>`;
      }
      if (eventDialog?.open) eventDialog.close();
      showToast('Check-in registrado. Pet enviado para a Recepção.');
    } catch (error) {
      showToast(error.message, false);
      if (button) { button.disabled = false; button.textContent = original; }
    }
  }

  document.addEventListener('submit', (event) => {
    const form = event.target.closest?.('.js-smart-checkin');
    if (!form) return;
    event.preventDefault();
    runSmartCheckin(form);
  });

  document.querySelectorAll('.calendar-event').forEach((event) => {
    event.addEventListener('click', (e) => {
      if (e.target.closest('a,button,form')) return;
      if (!eventDialog) return;
      const d = event.dataset;
      document.getElementById('dialog-pet').textContent = d.pet || 'Pet';
      document.getElementById('dialog-status').textContent = d.status || '—';
      document.getElementById('dialog-tutor').textContent = d.tutor || '—';
      document.getElementById('dialog-phone').textContent = d.phone || '—';
      document.getElementById('dialog-service').textContent = `${d.service || '—'} · ${d.duration || 60} min`;
      document.getElementById('dialog-employee').textContent = d.employee || '—';
      document.getElementById('dialog-datetime').textContent = `${d.date || '—'} às ${d.time || '—'}`;
      document.getElementById('dialog-transport').textContent = d.transport || 'Não';
      const actions = document.getElementById('dialog-actions');
      let html = `<a href="/agenda/${d.id}/editar">Editar</a>`;
      if (['Agendado','Confirmado','Reagendado','Aguardando aprovação'].includes(d.status)) html += smartCheckinAction(d.id,'✓ Registrar chegada');
      if (d.status === 'Na loja') html += `<form method="post" action="/agenda/${d.id}/iniciar"><button>Iniciar atendimento</button></form>`;
      if (d.status === 'Em atendimento') html += statusAction(d.id,'Pronto','Marcar como pronto');
      if (d.status === 'Pronto') html += statusAction(d.id,'Entregue','Registrar entrega');
      if (!['Cancelado','Entregue','Finalizado'].includes(d.status)) html += statusAction(d.id,'Cancelado','Cancelar');
      actions.innerHTML = html;
      eventDialog.showModal();
    });
  });
  dialogClose?.addEventListener('click', () => eventDialog.close());
  eventDialog?.addEventListener('click', (e) => { if (e.target === eventDialog) eventDialog.close(); });

})();
