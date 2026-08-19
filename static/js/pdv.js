(()=>{
  const root=document.querySelector('.pdv-premium'); if(!root)return;
  const grid=document.querySelector('#product-grid'), search=document.querySelector('#product-search');
  const categoryButtons=[...document.querySelectorAll('.category-chip')];
  const cart=document.querySelector('#sale-items'), tpl=document.querySelector('#cart-item-template');
  const discount=document.querySelector('#discount'), payments=document.querySelector('#payments');
  const brl=v=>new Intl.NumberFormat('pt-BR',{style:'currency',currency:'BRL'}).format(Number(v)||0);
  let selectedCategory='';

  function filterProducts(){
    const term=(search.value||'').trim().toLowerCase(); let visible=0;
    document.querySelectorAll('.product-card').forEach(card=>{
      const okText=!term||card.dataset.search.includes(term);
      const okCat=!selectedCategory||card.dataset.category===selectedCategory;
      card.classList.toggle('hidden',!(okText&&okCat)); if(okText&&okCat)visible++;
    });
    document.querySelector('#catalog-count').textContent=visible;
    document.querySelector('#no-products').classList.toggle('hidden',visible!==0);
  }

  function totals(){
    let subtotal=0;
    cart.querySelectorAll('.cart-item').forEach(item=>{
      const q=Math.max(0,parseFloat(item.querySelector('.qty').value)||0);
      const p=parseFloat(item.querySelector('.price-input').value)||0;
      const total=q*p; subtotal+=total; item.querySelector('.line-total').textContent=brl(total);
    });
    const desc=Math.max(0,parseFloat(discount.value)||0), total=Math.max(0,subtotal-desc);
    document.querySelector('#subtotal').textContent=brl(subtotal);
    document.querySelector('#grand-total').textContent=brl(total);
    document.querySelector('#finish-total').textContent=brl(total);
    let paid=0; document.querySelectorAll('.payment-amount').forEach(i=>paid+=parseFloat(i.value)||0);
    const balance=total-paid, balanceEl=document.querySelector('#payment-balance');
    balanceEl.textContent=brl(Math.abs(balance));
    balanceEl.previousElementSibling.textContent=balance>0.009?'Falta pagar':balance<-0.009?'Troco / excedente':'Pagamento completo';
    document.querySelector('#cart-empty').classList.toggle('hidden',cart.children.length>0);
    return total;
  }

  function addProduct(card){
    const existing=[...cart.querySelectorAll('.cart-item')].find(i=>i.querySelector('.product-id').value===card.dataset.id);
    if(existing){const q=existing.querySelector('.qty');q.value=(parseFloat(q.value)||0)+1;totals();return;}
    const node=tpl.content.cloneNode(true), item=node.querySelector('.cart-item');
    item.querySelector('.product-id').value=card.dataset.id;
    item.querySelector('.price-input').value=card.dataset.price;
    item.querySelector('.item-name').textContent=card.dataset.name;
    item.querySelector('.item-stock').textContent=`Estoque: ${card.dataset.stock} ${card.dataset.unit}`;
    item.querySelector('.unit-price').textContent=`${brl(card.dataset.price)} / ${card.dataset.unit}`;
    const qty=item.querySelector('.qty');
    qty.max=card.dataset.stock; qty.addEventListener('input',totals);
    item.querySelector('.qty-minus').onclick=()=>{qty.value=Math.max(.01,(parseFloat(qty.value)||1)-1);totals()};
    item.querySelector('.qty-plus').onclick=()=>{qty.value=Math.min(parseFloat(card.dataset.stock)||999999,(parseFloat(qty.value)||0)+1);totals()};
    item.querySelector('.remove-item').onclick=()=>{item.remove();totals()};
    cart.appendChild(node); totals();
  }

  document.querySelectorAll('.product-card').forEach(c=>c.addEventListener('click',()=>addProduct(c)));
  search.addEventListener('input',filterProducts);
  document.querySelector('#clear-search').onclick=()=>{search.value='';search.focus();filterProducts()};
  categoryButtons.forEach(btn=>btn.onclick=()=>{categoryButtons.forEach(b=>b.classList.remove('active'));btn.classList.add('active');selectedCategory=btn.dataset.category;filterProducts()});
  document.querySelector('#clear-cart').onclick=()=>{if(cart.children.length&&confirm('Limpar todos os itens do carrinho?')){cart.innerHTML='';totals()}};
  discount.addEventListener('input',totals);
  document.querySelectorAll('.payment-amount').forEach(i=>i.addEventListener('input',totals));
  document.querySelector('#add-payment').onclick=()=>{
    const d=document.createElement('div');d.className='payment-row';
    d.innerHTML='<select name="payment_method[]" class="premium-input"><option>PIX</option><option>Dinheiro</option><option>Débito</option><option>Crédito</option><option>Voucher</option></select><input name="payment_amount[]" class="premium-input payment-amount" type="number" step="0.01" min="0" placeholder="Valor">';
    d.querySelector('input').addEventListener('input',totals);payments.appendChild(d);
  };
  document.querySelector('#pdv-form').addEventListener('submit',e=>{
    const total=totals(); if(!cart.children.length){e.preventDefault();alert('Adicione pelo menos um produto ao carrinho.');return;}
    let paid=0;document.querySelectorAll('.payment-amount').forEach(i=>paid+=parseFloat(i.value)||0);
    if(Math.abs(paid-total)>.01){e.preventDefault();alert(`O pagamento precisa totalizar ${brl(total)}.`);}
  });
  totals();
})();
