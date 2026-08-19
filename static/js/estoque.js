(() => {
  const buttons=[...document.querySelectorAll('[data-stock-tab]')];
  const panels=[...document.querySelectorAll('[data-panel]')];
  function open(name){
    const exists=panels.some(p=>p.dataset.panel===name);
    const selected=exists?name:'produtos';
    buttons.forEach(b=>b.classList.toggle('active',b.dataset.stockTab===selected));
    panels.forEach(p=>p.hidden=p.dataset.panel!==selected);
    localStorage.setItem('stockTab',selected);
  }
  buttons.forEach(b=>b.addEventListener('click',()=>open(b.dataset.stockTab)));
  const hash=(window.location.hash||'').replace('#','');
  open(hash || localStorage.getItem('stockTab') || 'produtos');
})();
