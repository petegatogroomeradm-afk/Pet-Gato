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
        const response = await fetch(url, {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({date, time})
        });
        const data = await response.json();
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
})();
