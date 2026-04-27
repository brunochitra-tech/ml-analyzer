/* =========================================================
   ML ANALYZER — lógica completa con sincronización Firebase
   ========================================================= */

// ---------- util ----------
const $ = (s,root=document)=>root.querySelector(s);
const $$ = (s,root=document)=>Array.from(root.querySelectorAll(s));
const fmtMoney = n => (n==null||isNaN(n))?'—':'$ '+Number(n).toLocaleString('es-AR',{minimumFractionDigits:0,maximumFractionDigits:0});
const fmtMoney2 = n => (n==null||isNaN(n))?'—':'$ '+Number(n).toLocaleString('es-AR',{minimumFractionDigits:2,maximumFractionDigits:2});
const fmtInt = n => (n==null||isNaN(n))?'—':Number(n).toLocaleString('es-AR');
const fmtPct = n => (n==null||isNaN(n))?'—':(n*100).toFixed(2)+'%';
const safeDiv = (a,b)=> (b&&b!==0) ? a/b : null;
const toast = (msg,type='')=>{
  const el=document.createElement('div');
  el.className='toast '+type; el.textContent=msg;
  $('#toastHost').appendChild(el);
  setTimeout(()=>el.remove(),3500);
};
const normalizeSku = raw => {
  if(raw==null) return '';
  return String(raw).trim().toUpperCase();
};
const normalizeSkuSoft = raw => {
  if(raw==null) return '';
  return String(raw).trim().toUpperCase().replace(/[\s\.\-_]+/g,'');
};
const parseNumber = v => {
  if(v==null||v==='') return null;
  if(typeof v==='number') return isFinite(v)?v:null;
  let s=String(v).trim();
  if(!s) return null;
  if(/[a-zA-Z]/.test(s.replace(/e[+-]?\d/i,''))) return null;
  s=s.replace(/\$/g,'').replace(/\s/g,'');
  if(/,/.test(s) && /\./.test(s)){ s=s.replace(/\./g,'').replace(',','.'); }
  else if(/,/.test(s)){ s=s.replace(',','.'); }
  const n=parseFloat(s);
  return isFinite(n)?n:null;
};
const parseDate = v=>{
  if(v==null||v==='') return null;
  if(v instanceof Date) return v;
  if(typeof v==='number'){
    const d = XLSX.SSF.parse_date_code(v);
    if(d) return new Date(Date.UTC(d.y,d.m-1,d.d,d.H||0,d.M||0,Math.floor(d.S||0)));
  }
  const s=String(v).trim();
  const m=s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?/);
  if(m){
    let y=parseInt(m[3]); if(y<100) y+=2000;
    return new Date(y,parseInt(m[2])-1,parseInt(m[1]),parseInt(m[4]||0),parseInt(m[5]||0),parseInt(m[6]||0));
  }
  const d=new Date(s);
  return isNaN(d)?null:d;
};
const fmtDate = d => !d?'—':d.toLocaleDateString('es-AR');
const fmtDateISO = d => !d?'':d.toISOString().slice(0,10);

// ---------- estado global ----------
const state = {
  priceList: null,
  sales: null,
  filtered: null,
  charts:{},
  thresholdPct: 3,
  sort:{},
  selectedSkuDetail: null,
  pubSort:{k:'neto_total',dir:-1},
};

// ---------- Persistencia localStorage ----------
const LS_PRICE = 'mlanalyzer.priceList.v2';
const LS_HIST  = 'mlanalyzer.history.v2';
const LS_CFG   = 'mlanalyzer.cfg.v2';
const LS_FB    = 'mlanalyzer.firebase.v1';

function savePriceList(){
  if(!state.priceList) return;
  const data = { raw:state.priceList.raw, loadedAt:state.priceList.loadedAt, fileName:state.priceList.fileName, invalid:state.priceList.invalid };
  try{ localStorage.setItem(LS_PRICE, JSON.stringify(data)); }
  catch(e){ toast('No se pudo guardar lista (storage lleno).','error'); }
}
function loadPriceListFromStorage(){
  const s=localStorage.getItem(LS_PRICE);
  if(!s) return;
  try{
    const data=JSON.parse(s);
    state.priceList = buildPriceListFromRaw(data.raw);
    state.priceList.loadedAt = data.loadedAt;
    state.priceList.fileName = data.fileName;
    updatePriceListUI();
  }catch(e){ console.error(e); }
}
function saveHistoryEntry(entry){
  let hist=[];
  try{ hist=JSON.parse(localStorage.getItem(LS_HIST)||'[]'); }catch(e){}
  hist.unshift(entry); hist=hist.slice(0,20);
  try{ localStorage.setItem(LS_HIST,JSON.stringify(hist)); }catch(e){}
}
function loadHistory(){ try{ return JSON.parse(localStorage.getItem(LS_HIST)||'[]'); }catch(e){ return []; } }
function saveCfg(){ localStorage.setItem(LS_CFG,JSON.stringify({thresholdPct:state.thresholdPct})); }
function loadCfg(){
  try{
    const c=JSON.parse(localStorage.getItem(LS_CFG)||'{}');
    if(c.thresholdPct!=null){ state.thresholdPct=c.thresholdPct; $('#thresholdPct').value=c.thresholdPct; }
  }catch(e){}
}

// =====================================================================
//   FIREBASE — sincronización en la nube en tiempo real
// =====================================================================

let _db = null;
let _auth = null;
const fbConnected = () => _db !== null && _auth && _auth.currentUser;

// Campos derivados de enrichSales() y normalizeSku: no se almacenan en Firebase
const DERIVED_FIELDS = new Set([
  '_sku_padre','_modelo','_categoria','_precio_neto_obj','_precio_valido',
  '_match_kind','_ingreso_neto_unit','_dif_abs','_dif_pct','_sku_norm','_sku_soft'
]);

function initFirebase(config){
  try{
    if(!config.databaseURL){ toast('Faltá el campo databaseURL en la configuración','error'); return false; }
    if(firebase.apps && firebase.apps.length>0){
      firebase.apps[0].delete().then(()=>_startFirebase(config)).catch(e=>console.error(e));
    } else {
      _startFirebase(config);
    }
    return true;
  }catch(e){
    console.error('Firebase init error:', e);
    toast('Error al conectar Firebase: '+e.message,'error');
    setFbStatus('error');
    return false;
  }
}

function _startFirebase(config){
  firebase.initializeApp(config);
  _db = firebase.database();
  _auth = firebase.auth();
  localStorage.setItem(LS_FB, JSON.stringify(config));
  
  _auth.onAuthStateChanged(user => {
    if(user) {
      $('#loginOverlay').style.display = 'none';
      $('#currentUserLabel').textContent = user.email;
      $('#btnLogout').style.display = 'block';
      setFbStatus('connecting');
      _db.ref('.info/connected').on('value', snap=>{
        setFbStatus(snap.val()?'ok':'error');
      });
      _setupListeners();
      _setupHistoryListener();
    } else {
      $('#loginOverlay').style.display = 'flex';
      $('#currentUserLabel').textContent = '';
      $('#btnLogout').style.display = 'none';
      setFbStatus('none');
    }
  });
}

function setFbStatus(s){
  const el = $('#fbStatus');
  if(!el) return;
  const map = {
    ok:         { label:'🟢 Conectado — datos en tiempo real para todos', bg:'rgba(47,191,113,.2)' },
    error:      { label:'🔴 Sin conexión a la nube',                      bg:'rgba(239,75,92,.2)'  },
    connecting: { label:'🟡 Conectando con Firebase...',                  bg:'rgba(244,178,59,.2)' },
    none:       { label:'⚫ Firebase no configurado (modo local)',         bg:'rgba(138,148,171,.2)'},
  };
  const st = map[s]||map.none;
  el.textContent = st.label;
  el.style.background = st.bg;
}

function _setupListeners(){
  if(!_db) return;

  // — Lista de precios —
  _db.ref('mlanalyzer/priceList').on('value', snap=>{
    const val = snap.val();
    if(!val || !val.raw){
      // Nube vacía → limpiar price list (alguien la borró)
      state.priceList = null;
      updatePriceListUI();
      return;
    }
    try{
      const raw = JSON.parse(val.raw);
      state.priceList = buildPriceListFromRaw(raw);
      state.priceList.loadedAt = val.loadedAt;
      state.priceList.fileName = val.fileName;
      // Sincronizar también en localStorage como cache offline
      localStorage.setItem(LS_PRICE, JSON.stringify({ raw, loadedAt:val.loadedAt, fileName:val.fileName }));
      updatePriceListUI();
      if(state.sales){ enrichSales(); refreshAll(); }
    }catch(e){ console.error('FB priceList parse error:', e); }
  });

  // — Reporte de ventas —
  _db.ref('mlanalyzer/sales').on('value', snap=>{
    const val = snap.val();
    if(!val || !val.rows){
      // Nube vacía → limpiar estado de ventas
      state.sales = null; state.filtered = null;
      Object.values(state.charts).forEach(c=>c.destroy()); state.charts={};
      $('#kpisRow').innerHTML='<div class="empty">Cargá un reporte para ver el dashboard.</div>';
      ['tblSku','tblPrecios','tblLogistica','tblProv','tblCity','tblAudit'].forEach(id=>{
        const tb = document.querySelector('#'+id+' tbody');
        if(tb) tb.innerHTML='';
      });
      $('#filtersBlock').style.display='none';
      renderStatus();
      return;
    }
    try{
      const rows = JSON.parse(val.rows).map(_deserializeRow);
      const meta = JSON.parse(val.meta);
      if(meta.period){
        if(meta.period.from) meta.period.from = new Date(meta.period.from);
        if(meta.period.to)   meta.period.to   = new Date(meta.period.to);
      }
      state.sales = { rows, meta };
      enrichSales();
      refreshAll();
      $('#filtersBlock').style.display='block';
    }catch(e){ console.error('FB sales parse error:', e); }
  });
}

// Serializa una fila eliminando campos derivados y convirtiendo Dates a ISO string
function _serializeRow(r){
  const s={};
  for(const k in r){
    if(DERIVED_FIELDS.has(k)) continue;
    const v = r[k];
    s[k] = v instanceof Date ? v.toISOString() : v;
  }
  return s;
}

// Restaura una fila desde Firebase: recalcula norms y aliases
function _deserializeRow(r){
  if(r._fecha) r._fecha = new Date(r._fecha);
  if(r.fecha)  r.fecha  = new Date(r.fecha);
  // Recalcular campos derivados básicos
  r._sku_norm = normalizeSku(r._sku_raw);
  r._sku_soft = normalizeSkuSoft(r._sku_raw);
  // Restaurar aliases por si no estaban
  if(!('fecha' in r))         r.fecha         = r._fecha;
  if(!('unidades_n' in r))    r.unidades_n    = r._unidades;
  if(!('total' in r))         r.total         = r._total;
  if(!('facturacion' in r))   r.facturacion   = r._ing_prod;
  if(!('publicidad_flag' in r)) r.publicidad_flag = r._publicidad;
  return r;
}

async function fbSave_priceList(){
  if(!_db || !state.priceList) return;
  try{
    await _db.ref('mlanalyzer/priceList').set({
      raw: JSON.stringify(state.priceList.raw),
      loadedAt: state.priceList.loadedAt,
      fileName: state.priceList.fileName
    });
  }catch(e){ toast('Error guardando lista en nube: '+e.message,'error'); }
}

async function fbSave_sales(){
  if(!_db || !state.sales) return;
  const rows = state.sales.rows.map(_serializeRow);
  const meta = {...state.sales.meta};
  if(meta.period){
    meta.period = {
      from: meta.period.from instanceof Date ? meta.period.from.toISOString() : meta.period.from,
      to:   meta.period.to   instanceof Date ? meta.period.to.toISOString()   : meta.period.to
    };
  }
  try{
    toast('Subiendo reporte a la nube... ☁️','');
    await _db.ref('mlanalyzer/sales').set({
      rows: JSON.stringify(rows),
      meta: JSON.stringify(meta)
    });
    toast('✅ Reporte en la nube — todos los usuarios ven los datos actualizados','ok');
  }catch(e){ toast('Error subiendo a nube: '+e.message,'error'); }
}

async function fbClear_sales(){
  if(!_db) return;
  try{ await _db.ref('mlanalyzer/sales').remove(); }
  catch(e){ toast('Error limpiando ventas en nube: '+e.message,'error'); }
}

async function fbClear_priceList(){
  if(!_db) return;
  try{ await _db.ref('mlanalyzer/priceList').remove(); }
  catch(e){ toast('Error limpiando lista en nube: '+e.message,'error'); }
}

// =====================================================================
//   FIN FIREBASE
// =====================================================================

// ---------- Tabs ----------
$('#tabsNav').addEventListener('click', e=>{
  const b=e.target.closest('button[data-tab]');
  if(!b) return;
  switchTab(b.dataset.tab);
});
document.addEventListener('click', e=>{
  const b=e.target.closest('[data-go-tab]');
  if(b){ switchTab(b.dataset.goTab); }
});
function switchTab(t){
  $$('#tabsNav button').forEach(x=>x.classList.toggle('active',x.dataset.tab===t));
  $$('.tab').forEach(x=>x.classList.toggle('active',x.id==='tab-'+t));
}

// ---------- Lectura de archivos ----------
function readFileAsWorkbook(file){
  return new Promise((res,rej)=>{
    const r=new FileReader();
    r.onload = e=>{
      try{
        const wb=XLSX.read(new Uint8Array(e.target.result),{type:'array',cellDates:true});
        res(wb);
      }catch(err){ rej(err); }
    };
    r.onerror=rej;
    r.readAsArrayBuffer(file);
  });
}

// ---------- LISTA DE PRECIOS ----------
async function handlePriceFile(file){
  try{
    const wb=await readFileAsWorkbook(file);
    const sheetName = wb.SheetNames.find(n=>n.toLowerCase()==='resumen') || wb.SheetNames[0];
    if(!sheetName){ toast('No encontré hoja válida','error'); return; }
    const ws = wb.Sheets[sheetName];
    const aoa = XLSX.utils.sheet_to_json(ws,{header:1,defval:null,raw:true});
    const raw = parsePriceListAOA(aoa);
    state.priceList = buildPriceListFromRaw(raw);
    state.priceList.loadedAt = new Date().toISOString();
    state.priceList.fileName = file.name;
    savePriceList();
    updatePriceListUI();
    if(state.sales){ enrichSales(); refreshAll(); }
    toast('Lista de precios cargada: '+state.priceList.expanded.size+' SKUs','ok');
    // Sincronizar a nube
    if(fbConnected()) fbSave_priceList();
  }catch(err){
    console.error(err);
    toast('Error leyendo lista de precios: '+err.message,'error');
  }
}
function parsePriceListAOA(aoa){
  let headerRow=-1;
  for(let i=0;i<Math.min(aoa.length,15);i++){
    const row=(aoa[i]||[]).map(c=>String(c||'').toLowerCase().trim());
    if(row.includes('sku') && row.some(c=>c.includes('precio neto'))){ headerRow=i;break; }
  }
  if(headerRow<0) throw new Error('No encuentro encabezados (SKU / precio neto) en hoja Resumen');
  const headers = aoa[headerRow].map(c=>String(c||'').trim());
  const idx = {};
  headers.forEach((h,i)=>{
    const lo=h.toLowerCase();
    if(lo==='modelo') idx.modelo=i;
    else if(lo.includes('categoria')||lo.includes('categor')) idx.categoria=i;
    else if(lo==='sku') idx.sku=i;
    else if(lo.startsWith('variante')){ idx.variantes = idx.variantes || []; idx.variantes.push(i); }
    else if(lo.includes('precio neto')) idx.precio=i;
  });
  if(idx.sku==null || idx.precio==null) throw new Error('Faltan columnas SKU o precio neto');
  idx.variantes = idx.variantes || [];

  const out=[];
  for(let r=headerRow+1;r<aoa.length;r++){
    const row = aoa[r]||[];
    const skuRaw = row[idx.sku];
    const sku = skuRaw!=null?String(skuRaw).trim():'';
    if(!sku) continue;
    const precioRaw = row[idx.precio];
    const modelo = idx.modelo!=null?(row[idx.modelo]||''):'';
    const categoria = idx.categoria!=null?(row[idx.categoria]||''):'';
    const variantes = idx.variantes.map(vi=>row[vi]).filter(v=>v!=null && String(v).trim()!=='').map(v=>String(v).trim());
    const parsedPrecio = parseNumber(precioRaw);
    const isSeparator = (parsedPrecio==null) && variantes.length===0 && /\s/.test(sku) && sku.length>15;
    if(isSeparator) continue;
    out.push({ sku_padre:sku, modelo:String(modelo||'').trim(), categoria:String(categoria||'').trim(), variantes, precio_neto_raw:precioRaw, precio_neto:parsedPrecio, valido:parsedPrecio!=null && parsedPrecio>0 });
  }
  return out;
}
function buildPriceListFromRaw(raw){
  const expanded = new Map();
  const byExact = new Map();
  const invalid = [];
  raw.forEach(item=>{
    if(!item.valido) invalid.push(item);
    const parent = item.sku_padre;
    const register = (sku, matchType)=>{
      const keySoft = normalizeSkuSoft(sku);
      const keyExact = normalizeSku(sku);
      if(!keySoft) return;
      const rec = { sku_original:sku, sku_padre:parent, modelo:item.modelo, categoria:item.categoria, precio_neto:item.precio_neto, valido:item.valido, matchKind:matchType };
      if(!expanded.has(keySoft)) expanded.set(keySoft, rec);
      if(!byExact.has(keyExact)) byExact.set(keyExact, rec);
    };
    register(parent,'principal');
    item.variantes.forEach(v=>register(v,'variante'));
  });
  return { raw, expanded, byExact, invalid };
}

function updatePriceListUI(){
  const pl = state.priceList;
  const info = $('#priceListInfo');
  const status = $('#priceListStatus');
  if(!pl){
    info.textContent='Sin lista de precios';
    info.style.background='rgba(239,75,92,.15)';
    if(status) status.innerHTML='<div class="empty">Sin lista cargada.</div>';
    $('#tblPriceList tbody').innerHTML='';
    return;
  }
  info.innerHTML = '📋 '+pl.raw.length+' prod · '+pl.expanded.size+' SKUs';
  info.style.background='rgba(47,191,113,.15)';
  if(status){
    const dt = pl.loadedAt?new Date(pl.loadedAt).toLocaleString('es-AR'):'—';
    status.innerHTML =
      '<div class="chip">Archivo: <b>'+(pl.fileName||'—')+'</b></div>'+
      '<div class="chip">Cargado: <b>'+dt+'</b></div>'+
      '<div class="chip">Productos: <b>'+pl.raw.length+'</b></div>'+
      '<div class="chip">SKUs expandidos: <b>'+pl.expanded.size+'</b></div>'+
      '<div class="chip">Precios inválidos: <b>'+pl.invalid.length+'</b></div>';
  }
  const tb = $('#tblPriceList tbody');
  const items=[];
  pl.raw.forEach(p=>{
    items.push({sku:p.sku_padre,parent:p.sku_padre,modelo:p.modelo,cat:p.categoria,precio:p.precio_neto,val:p.valido});
    p.variantes.forEach(v=>items.push({sku:v,parent:p.sku_padre,modelo:p.modelo,cat:p.categoria,precio:p.precio_neto,val:p.valido}));
  });
  tb.innerHTML = items.map(i=>`
    <tr>
      <td>${escapeHtml(i.sku)}</td><td>${escapeHtml(i.parent)}</td>
      <td>${escapeHtml(i.modelo)}</td><td>${escapeHtml(i.cat)}</td>
      <td class="num">${i.precio!=null?fmtMoney(i.precio):'—'}</td>
      <td>${i.val?'<span class="badge ok">OK</span>':'<span class="badge bad">Inválido</span>'}</td>
    </tr>`).join('');
}

function escapeHtml(s){ return String(s==null?'':s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }

// ---------- REPORTE DE VENTAS ----------
async function handleSalesFile(file){
  try{
    if(!state.priceList){
      toast('Advertencia: no hay lista de precios. El cruce de precios no estará disponible.','error');
    }
    const wb = await readFileAsWorkbook(file);
    const sheetName = wb.SheetNames.find(n=>n.toLowerCase().includes('ventas')) || wb.SheetNames[0];
    const ws=wb.Sheets[sheetName];
    const aoa=XLSX.utils.sheet_to_json(ws,{header:1,defval:null,raw:true});
    const parsed = parseSalesAOA(aoa);
    state.sales = { rows:parsed.rows, meta:{ fileName:file.name, loadedAt:new Date().toISOString(), period:computePeriod(parsed.rows), sheetName } };
    enrichSales();
    const neto = state.sales.rows.reduce((a,r)=>a+(r.total||0),0);
    const per = state.sales.meta.period;
    saveHistoryEntry({ loadedAt:state.sales.meta.loadedAt, fileName:file.name, period:per?(fmtDate(per.from)+' → '+fmtDate(per.to)):'—', rows:state.sales.rows.length, neto });
    refreshAll();
    toast('Reporte cargado: '+state.sales.rows.length+' filas','ok');
    $('#filtersBlock').style.display='block';
    // Subir a Firebase para que todos vean los datos
    if(fbConnected()) fbSave_sales();
  }catch(err){
    console.error(err);
    toast('Error leyendo reporte: '+err.message,'error');
  }
}

function parseSalesAOA(aoa){
  let headerIdx = -1;
  for(let i=0;i<Math.min(aoa.length,15);i++){
    const row=(aoa[i]||[]).map(c=>String(c||'').toLowerCase());
    if(row.some(c=>c.includes('# de venta')||c.includes('número de venta')||c.includes('num_venta'))){ headerIdx=i; break; }
  }
  if(headerIdx<0) throw new Error('No encuentro encabezados del reporte (falta columna "# de venta")');

  const headers = (aoa[headerIdx]||[]).map(c=>String(c||'').toLowerCase().trim());
  const idx = {};
  headers.forEach((h, i) => {
    if(h.includes('# de venta') && idx.num_venta===undefined) idx.num_venta = i;
    else if(h.includes('fecha de venta') && idx.fecha_venta===undefined) idx.fecha_venta = i;
    else if(h === 'estado' && idx.estado_venta===undefined) idx.estado_venta = i;
    else if(h.includes('unidades') && !h.includes('devueltas') && idx.unidades===undefined) idx.unidades = i;
    else if(h.includes('ingresos por producto') && idx.ingresos_productos===undefined) idx.ingresos_productos = i;
    else if(h.includes('cargo por venta') && idx.cargo_venta===undefined) idx.cargo_venta = i;
    else if(h.includes('ingresos por envío') && idx.ingresos_envio===undefined) idx.ingresos_envio = i;
    else if(h.includes('costos de envío') && !h.includes('declarado') && idx.costos_envio===undefined) idx.costos_envio = i;
    else if(h.includes('descuento') && idx.descuentos===undefined) idx.descuentos = i;
    else if((h.includes('anulacion') || h.includes('reembolso')) && idx.anulaciones===undefined) idx.anulaciones = i;
    else if((h.startsWith('total') || h === 'total neto') && idx.total===undefined) idx.total = i;
    else if((h === 'sku' || h.includes('sku de')) && idx.sku===undefined) idx.sku = i;
    else if((h === '# de publicación' || h === 'publicación') && idx.publicacion===undefined) idx.publicacion = i;
    else if(h.includes('título') && idx.titulo===undefined) idx.titulo = i;
    else if(h.includes('variante') && idx.variante===undefined) idx.variante = i;
    else if(h.includes('precio unitario') && idx.precio_unitario===undefined) idx.precio_unitario = i;
    else if(h.includes('publicidad') && idx.publicidad===undefined) idx.publicidad = i;
    else if(h.includes('forma de entrega') && idx.forma_entrega===undefined) idx.forma_entrega = i;
    else if(h === 'provincia' && idx.provincia===undefined) idx.provincia = i;
    else if(h === 'ciudad' && idx.ciudad===undefined) idx.ciudad = i;
    else if(h.includes('cuotas') && idx.cuotas===undefined) idx.cuotas = i;
    else if((h.includes('es pack') || h.includes('es kit')) && idx.es_pack===undefined) idx.es_pack = i;
  });

  const rows=[];
  for(let r=headerIdx+1;r<aoa.length;r++){
    const raw = aoa[r]||[];
    let hasData = false;
    for(let i=0; i<Math.min(raw.length, 15); i++) { if(raw[i]!=null && String(raw[i]).trim()!=='') {hasData=true; break;} }
    if(!hasData) continue;
    if(idx.num_venta!==undefined && String(raw[idx.num_venta]||'').toLowerCase().includes('total')) continue;

    const obj = {};
    obj.num_venta      = idx.num_venta!==undefined      ? raw[idx.num_venta]      : null;
    obj.publicacion    = idx.publicacion!==undefined    ? raw[idx.publicacion]    : null;
    obj.titulo         = idx.titulo!==undefined         ? raw[idx.titulo]         : null;
    obj.variante       = idx.variante!==undefined       ? raw[idx.variante]       : null;
    obj.forma_entrega  = idx.forma_entrega!==undefined  ? raw[idx.forma_entrega]  : null;
    obj.provincia      = idx.provincia!==undefined      ? raw[idx.provincia]      : null;
    obj.ciudad         = idx.ciudad!==undefined         ? raw[idx.ciudad]         : null;
    obj.estado_venta   = idx.estado_venta!==undefined   ? raw[idx.estado_venta]   : null;
    obj.cuotas         = idx.cuotas!==undefined         ? raw[idx.cuotas]         : null;
    obj.es_pack        = idx.es_pack!==undefined        ? raw[idx.es_pack]        : null;

    const estadoLow = String(obj.estado_venta||'').toLowerCase();
    obj._cancelada = estadoLow.includes('cancel') || estadoLow.includes('devuel');

    obj._unidades    = parseNumber(idx.unidades!==undefined           ? raw[idx.unidades]           : null)||0;
    obj._ing_prod    = parseNumber(idx.ingresos_productos!==undefined ? raw[idx.ingresos_productos] : null)||0;
    obj._cargo       = parseNumber(idx.cargo_venta!==undefined        ? raw[idx.cargo_venta]        : null)||0;
    obj._ing_envio   = parseNumber(idx.ingresos_envio!==undefined     ? raw[idx.ingresos_envio]     : null)||0;
    obj._costo_envio = parseNumber(idx.costos_envio!==undefined       ? raw[idx.costos_envio]       : null)||0;
    obj._desc        = parseNumber(idx.descuentos!==undefined         ? raw[idx.descuentos]         : null)||0;
    obj._anul        = parseNumber(idx.anulaciones!==undefined        ? raw[idx.anulaciones]        : null)||0;
    obj._total       = parseNumber(idx.total!==undefined              ? raw[idx.total]              : null)||0;
    obj._precio_unit = parseNumber(idx.precio_unitario!==undefined    ? raw[idx.precio_unitario]    : null);
    obj._fecha       = parseDate(idx.fecha_venta!==undefined          ? raw[idx.fecha_venta]        : null);

    let rawSku = idx.sku!==undefined ? raw[idx.sku] : null;
    obj._sku_raw = rawSku!=null ? String(rawSku).trim() : '';

    let rawPubli = idx.publicidad!==undefined ? raw[idx.publicidad] : null;
    obj._publicidad = /s[ií]/i.test(String(rawPubli||''));

    obj._sku_norm = normalizeSku(obj._sku_raw);
    obj._sku_soft = normalizeSkuSoft(obj._sku_raw);

    obj.fecha         = obj._fecha;
    obj.unidades_n    = obj._unidades;
    obj.total         = obj._total;
    obj.facturacion   = obj._ing_prod;
    obj.publicidad_flag = obj._publicidad;

    rows.push(obj);
  }
  return {rows, headerIdx};
}

function computePeriod(rows){
  let min=null,max=null;
  rows.forEach(r=>{ if(r._fecha){ if(!min||r._fecha<min) min=r._fecha; if(!max||r._fecha>max) max=r._fecha; } });
  if(!min) return null;
  return {from:min,to:max};
}

// ---------- ENRIQUECIMIENTO ----------
function enrichSales(){
  if(!state.sales) return;
  const pl = state.priceList;
  state.sales.rows.forEach(r=>{
    let match=null, matchKind='sin_match';
    if(!r._sku_raw){ matchKind='sin_sku'; }
    else if(pl){
      const ex = pl.byExact.get(r._sku_norm);
      if(ex){ match=ex; matchKind=ex.matchKind==='principal'?'exacto':'variante'; }
      else{
        const soft = pl.expanded.get(r._sku_soft);
        if(soft){ match=soft; matchKind=soft.matchKind==='principal'?'exacto':'variante'; }
      }
    }
    if(match){
      r._sku_padre = match.sku_padre; r._modelo=match.modelo; r._categoria=match.categoria;
      r._precio_neto_obj=match.precio_neto; r._precio_valido=match.valido;
      r._match_kind = matchKind;
      if(!match.valido) r._match_kind = 'precio_invalido';
    } else {
      r._sku_padre=null; r._modelo=null; r._categoria=null;
      r._precio_neto_obj=null; r._precio_valido=false;
      r._match_kind = matchKind;
    }
    r._ingreso_neto_unit = safeDiv(r._total, r._unidades);
    r._dif_abs = (r._ingreso_neto_unit!=null && r._precio_neto_obj!=null) ? r._ingreso_neto_unit - r._precio_neto_obj : null;
    r._dif_pct = (r._ingreso_neto_unit!=null && r._precio_neto_obj) ? (r._ingreso_neto_unit/r._precio_neto_obj - 1) : null;
  });
}

// ---------- FILTROS ----------
function getFiltered(){
  if(!state.sales) return [];
  const from = $('#fDateFrom').value ? new Date($('#fDateFrom').value) : null;
  const to   = $('#fDateTo').value   ? new Date($('#fDateTo').value+'T23:59:59') : null;
  const skuPadre = $('#fSkuPadre').value.trim().toUpperCase();
  const skuOrig  = $('#fSkuOriginal').value.trim().toUpperCase();
  const pub      = $('#fPublicacion').value.trim();
  const modalidad= $('#fModalidad').value;
  const prov     = $('#fProvincia').value;
  const ciudad   = $('#fCiudad').value.trim().toLowerCase();
  const publi    = $('#fPublicidad').value;
  const estado   = $('#fEstado').value;
  return state.sales.rows.filter(r=>{
    if(r._cancelada && !estado) return false;
    if(from && (!r._fecha||r._fecha<from)) return false;
    if(to   && (!r._fecha||r._fecha>to))   return false;
    if(skuPadre && (r._sku_padre||'').toUpperCase()!==skuPadre) return false;
    if(skuOrig  && (r._sku_raw||'').toUpperCase().indexOf(skuOrig)<0) return false;
    if(pub      && String(r.publicacion||'').indexOf(pub)<0) return false;
    if(modalidad && (r.forma_entrega||'')!==modalidad) return false;
    if(prov     && (r.provincia||'')!==prov) return false;
    if(ciudad   && String(r.ciudad||'').toLowerCase().indexOf(ciudad)<0) return false;
    if(publi==='si' && !r._publicidad) return false;
    if(publi==='no' &&  r._publicidad) return false;
    if(estado   && (r.estado_venta||'')!==estado) return false;
    return true;
  });
}
function populateFilters(){
  if(!state.sales) return;
  const rows = state.sales.rows;
  const modalidades = [...new Set(rows.map(r=>r.forma_entrega).filter(Boolean))].sort();
  const provincias  = [...new Set(rows.map(r=>r.provincia).filter(Boolean))].sort();
  const estados     = [...new Set(rows.map(r=>r.estado_venta).filter(Boolean))].sort();
  fillSelect('#fModalidad','Modalidad (todas)',modalidades);
  fillSelect('#fProvincia','Provincia (todas)',provincias);
  fillSelect('#fEstado','Estado (todos)',estados);
  const per=state.sales.meta.period;
  if(per){ $('#fDateFrom').value=fmtDateISO(per.from); $('#fDateTo').value=fmtDateISO(per.to); }
}
function fillSelect(sel, placeholder, values){
  const el=$(sel); const prev=el.value;
  el.innerHTML='<option value="">'+placeholder+'</option>'+values.map(v=>`<option value="${escapeHtml(v)}">${escapeHtml(v)}</option>`).join('');
  if(values.includes(prev)) el.value=prev;
}

// ---------- REFRESH GLOBAL ----------
function refreshAll(){
  if(!state.sales) return;
  state.filtered = getFiltered();
  populateFilters();
  renderStatus();
  renderDashboard();
  renderSkuTab();
  renderPreciosTab();
  renderLogisticaTab();
  renderZonasTab();
  renderExcepcionesTab();
  renderAuditoriaTab();
  renderCalidadTab();
  renderHistory();
}

function renderStatus(){
  const s=state.sales;
  if(!s){ $('#statusBox').textContent='Sin datos'; return; }
  const per = s.meta.period;
  const perStr = per?(fmtDate(per.from)+' → '+fmtDate(per.to)):'—';
  const cloudBadge = fbConnected() ? ' ☁️' : '';
  $('#statusBox').innerHTML = '✅ '+s.rows.length+' filas · '+perStr+(state.filtered?(' · '+state.filtered.length+' filtradas'):'')+cloudBadge;
}

// ---------- DASHBOARD ----------
function renderDashboard(){
  const rows = state.filtered;
  if(!rows || rows.length===0){ $('#kpisRow').innerHTML='<div class="empty">Cargá un reporte para ver el dashboard.</div>'; return; }

  const sum = (k)=>rows.reduce((a,r)=>a+(r[k]||0),0);
  const ventas=rows.length, unidades=sum('_unidades'), fact=sum('_ing_prod');
  const cargo=sum('_cargo'), ing_envio=sum('_ing_envio'), costo_envio=sum('_costo_envio');
  const desc=sum('_desc'), anul=sum('_anul'), neto=sum('_total');
  const ticket=safeDiv(neto,ventas);
  const conPub=rows.filter(r=>r._publicidad).length;
  const pctPub=safeDiv(conPub,ventas);

  $('#kpisRow').innerHTML = [
    kpi('Ventas',fmtInt(ventas)),
    kpi('Unidades',fmtInt(unidades)),
    kpi('Facturación bruta',fmtMoney(fact)),
    kpi('Total neto real',fmtMoney(neto),'Ingreso real (Total ARS)'),
    kpi('Ticket promedio',fmtMoney(ticket)),
    kpi('Cargos ML',fmtMoney(cargo)),
    kpi('Ingreso envío',fmtMoney(ing_envio)),
    kpi('Costo envío',fmtMoney(costo_envio)),
    kpi('Descuentos',fmtMoney(desc)),
    kpi('Anulaciones',fmtMoney(anul)),
    kpi('% con publicidad',fmtPct(pctPub),conPub+' ventas'),
  ].join('');

  const byDay=new Map();
  rows.forEach(r=>{ if(!r._fecha) return; const k=fmtDateISO(r._fecha); byDay.set(k,(byDay.get(k)||0)+r._total); });
  const days=[...byDay.keys()].sort();
  drawChart('chartDaily','line',{labels:days,datasets:[{label:'Neto',data:days.map(d=>byDay.get(d)),borderColor:'#4c9aff',backgroundColor:'rgba(76,154,255,.15)',tension:.3,fill:true}]});

  const byMod=groupSum(rows,r=>r.forma_entrega||'Sin modalidad',r=>1);
  drawChart('chartModalidad','doughnut',{labels:[...byMod.keys()],datasets:[{data:[...byMod.values()],backgroundColor:['#4c9aff','#7c5cff','#2fbf71','#f4b23b','#ef4b5c','#8a94ab']}]});

  const bySku=groupSum(rows.filter(r=>r._sku_padre),r=>r._sku_padre,r=>r._total);
  const topSku=[...bySku.entries()].sort((a,b)=>b[1]-a[1]).slice(0,10);
  drawChart('chartTopSku','bar',{labels:topSku.map(x=>x[0]),datasets:[{label:'Neto',data:topSku.map(x=>x[1]),backgroundColor:'#4c9aff'}]},{indexAxis:'y'});

  const byProv=groupSum(rows,r=>r.provincia||'Sin provincia',r=>r._unidades);
  const topProv=[...byProv.entries()].sort((a,b)=>b[1]-a[1]).slice(0,10);
  drawChart('chartTopProv','bar',{labels:topProv.map(x=>x[0]),datasets:[{label:'Unidades',data:topProv.map(x=>x[1]),backgroundColor:'#7c5cff'}]},{indexAxis:'y'});

  const a=computeAuditStats(rows);
  $('#crossSummary').innerHTML=[
    kpi('Filas totales',fmtInt(a.total)),
    kpi('Con SKU',fmtInt(a.conSku)),
    kpi('Match exacto',fmtInt(a.exacto),'',a.total?a.exacto/a.total:0),
    kpi('Match por variante',fmtInt(a.variante),'',a.total?a.variante/a.total:0),
    kpi('Sin match',fmtInt(a.sinMatch),'',a.total?a.sinMatch/a.total:0),
    kpi('Precio inválido',fmtInt(a.precioInv)),
  ].join('');
}

function kpi(label,value,sub,progress){
  return `<div class="card kpi">
    <div class="label">${label}</div>
    <div class="value">${value}</div>
    ${sub?`<div class="sub">${sub}</div>`:''}
    ${progress!=null?`<div class="progress"><div style="width:${Math.min(100,progress*100).toFixed(1)}%"></div></div>`:''}
  </div>`;
}
function groupSum(rows,keyFn,valFn){
  const m=new Map();
  rows.forEach(r=>{ const k=keyFn(r); if(k==null) return; m.set(k,(m.get(k)||0)+(valFn(r)||0)); });
  return m;
}
function drawChart(id,type,data,options={}){
  const ctx=document.getElementById(id); if(!ctx) return;
  if(state.charts[id]) state.charts[id].destroy();
  state.charts[id]=new Chart(ctx,{type,data,options:Object.assign({responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:'#e6ecf5'}}},scales:type==='doughnut'?{}:{x:{ticks:{color:'#8a94ab'},grid:{color:'#2a3350'}},y:{ticks:{color:'#8a94ab'},grid:{color:'#2a3350'}}}},options)});
}

// ---------- SKU ----------
function computeSkuAggregates(rows){
  const byPadre=new Map();
  rows.filter(r=>r._sku_padre).forEach(r=>{
    let g=byPadre.get(r._sku_padre);
    if(!g){ g={sku_padre:r._sku_padre,modelo:r._modelo,categoria:r._categoria,variantes:new Set(),ventas:0,unidades:0,facturacion_bruta:0,neto_total:0,precios:[],pub_count:0,modalidadMap:new Map(),provinciaMap:new Map(),precio_neto_obj:r._precio_neto_obj,precio_valido:r._precio_valido}; byPadre.set(r._sku_padre,g); }
    if(r._sku_raw) g.variantes.add(r._sku_raw);
    g.ventas+=1; g.unidades+=r._unidades; g.facturacion_bruta+=r._ing_prod; g.neto_total+=r._total;
    if(r._precio_unit!=null) g.precios.push(r._precio_unit);
    if(r._publicidad) g.pub_count+=1;
    const mk=r.forma_entrega||'Sin modalidad'; g.modalidadMap.set(mk,(g.modalidadMap.get(mk)||0)+1);
    const pk=r.provincia||'Sin provincia';     g.provinciaMap.set(pk,(g.provinciaMap.get(pk)||0)+1);
  });
  const thr=state.thresholdPct/100;
  const out=[];
  byPadre.forEach(g=>{
    const neto_unit_prom=safeDiv(g.neto_total,g.unidades);
    const precio_prom=g.precios.length?g.precios.reduce((a,b)=>a+b,0)/g.precios.length:null;
    const precio_min=g.precios.length?Math.min(...g.precios):null;
    const precio_max=g.precios.length?Math.max(...g.precios):null;
    const dominant=m=>{let best=null,bestV=-1; m.forEach((v,k)=>{if(v>bestV){best=k;bestV=v;}}); return best;};
    const dif_abs=(neto_unit_prom!=null && g.precio_neto_obj!=null)?neto_unit_prom-g.precio_neto_obj:null;
    const dif_pct=(neto_unit_prom!=null && g.precio_neto_obj)?(neto_unit_prom/g.precio_neto_obj-1):null;
    let estado='—';
    if(dif_pct!=null){ if(Math.abs(dif_pct)<=thr) estado='igual'; else if(dif_pct>0) estado='arriba'; else estado='abajo'; }
    out.push({sku_padre:g.sku_padre,modelo:g.modelo,categoria:g.categoria,variantes:[...g.variantes].join(', '),ventas:g.ventas,unidades:g.unidades,facturacion_bruta:g.facturacion_bruta,neto_total:g.neto_total,neto_unit_prom,precio_prom,precio_min,precio_max,pub_count:g.pub_count,pub_pct:safeDiv(g.pub_count,g.ventas),modalidad_dom:dominant(g.modalidadMap),provincia_dom:dominant(g.provinciaMap),precio_neto_obj:g.precio_neto_obj,precio_valido:g.precio_valido,dif_abs,dif_pct,estado});
  });
  return out;
}

let skuData=[], skuSort={k:'neto_total',dir:-1};
function renderSkuTab(){ skuData=computeSkuAggregates(state.filtered||[]); bindSortable('#tblSku',skuSort,()=>renderSkuTable()); renderSkuTable(); }
function renderSkuTable(){
  const q=($('#skuSearch').value||'').toLowerCase().trim();
  let data=skuData.filter(r=>!q||[r.sku_padre,r.modelo,r.categoria,r.variantes].join(' ').toLowerCase().includes(q));
  data=sortData(data,skuSort);
  $('#tblSku tbody').innerHTML=data.map(r=>`
    <tr>
      <td><b>${escapeHtml(r.sku_padre)}</b></td><td>${escapeHtml(r.modelo||'')}</td>
      <td>${escapeHtml(r.categoria||'')}</td><td>${escapeHtml(r.variantes)}</td>
      <td class="num">${fmtInt(r.ventas)}</td><td class="num">${fmtInt(r.unidades)}</td>
      <td class="num">${fmtMoney(r.facturacion_bruta)}</td><td class="num">${fmtMoney(r.neto_total)}</td>
      <td class="num">${fmtMoney(r.neto_unit_prom)}</td><td class="num">${fmtMoney(r.precio_prom)}</td>
      <td class="num">${fmtMoney(r.precio_min)}</td><td class="num">${fmtMoney(r.precio_max)}</td>
      <td class="num">${fmtInt(r.pub_count)}</td><td class="num">${fmtPct(r.pub_pct)}</td>
      <td>${escapeHtml(r.modalidad_dom||'')}</td><td>${escapeHtml(r.provincia_dom||'')}</td>
      <td class="num">${r.precio_neto_obj!=null?fmtMoney(r.precio_neto_obj):'<span class="badge muted">sin obj.</span>'}</td>
      <td class="num ${diffCls(r.dif_abs)}">${r.dif_abs!=null?fmtMoney(r.dif_abs):'—'}</td>
      <td class="num ${diffCls(r.dif_pct)}">${r.dif_pct!=null?fmtPct(r.dif_pct):'—'}</td>
      <td>${badgeEstado(r.estado)}</td>
    </tr>`).join('')||`<tr><td colspan="20" class="empty">Sin datos</td></tr>`;
}
function diffCls(n){ if(n==null) return 'diff-zero'; if(n>0) return 'diff-pos'; if(n<0) return 'diff-neg'; return 'diff-zero'; }
function badgeEstado(e){
  if(e==='arriba') return '<span class="badge ok"><span class="dot ok"></span>Arriba</span>';
  if(e==='igual')  return '<span class="badge warn"><span class="dot warn"></span>Cerca</span>';
  if(e==='abajo')  return '<span class="badge bad"><span class="dot bad"></span>Abajo</span>';
  return '<span class="badge muted">—</span>';
}

// ---------- PRECIOS ----------
function renderPreciosTab(){
  const data=skuData.filter(r=>r.precio_neto_obj!=null && r.neto_unit_prom!=null);
  const abajo=data.filter(r=>r.estado==='abajo'), cerca=data.filter(r=>r.estado==='igual'), arriba=data.filter(r=>r.estado==='arriba');
  $('#kpiAbajo').innerHTML=`<div class="label">🔴 Abajo del objetivo</div><div class="value">${abajo.length}</div><div class="sub">SKUs</div>`;
  $('#kpiCerca').innerHTML=`<div class="label">🟡 Cerca del objetivo (±${state.thresholdPct}%)</div><div class="value">${cerca.length}</div><div class="sub">SKUs</div>`;
  $('#kpiArriba').innerHTML=`<div class="label">🟢 Arriba del objetivo</div><div class="value">${arriba.length}</div><div class="sub">SKUs</div>`;
  const peor=[...data].sort((a,b)=>(a.dif_pct??0)-(b.dif_pct??0)).slice(0,15);
  const mejor=[...data].sort((a,b)=>(b.dif_pct??0)-(a.dif_pct??0)).slice(0,15);
  $('#wrapPeor').innerHTML=rankTable(peor);
  $('#wrapMejor').innerHTML=rankTable(mejor);
  const sorted=[...data].sort((a,b)=>(a.dif_pct??0)-(b.dif_pct??0));
  $('#tblPrecios tbody').innerHTML=sorted.map(r=>`
    <tr>
      <td><b>${escapeHtml(r.sku_padre)}</b></td><td>${escapeHtml(r.modelo||'')}</td>
      <td class="num">${fmtMoney(r.neto_unit_prom)}</td><td class="num">${fmtMoney(r.precio_neto_obj)}</td>
      <td class="num ${diffCls(r.dif_abs)}">${fmtMoney(r.dif_abs)}</td>
      <td class="num ${diffCls(r.dif_pct)}">${fmtPct(r.dif_pct)}</td>
      <td>${badgeEstado(r.estado)}</td>
      <td class="num">${fmtInt(r.unidades)}</td><td class="num">${fmtMoney(r.neto_total)}</td>
      <td><button class="btn-link" data-sku-detail="${escapeHtml(r.sku_padre)}">Ver detalle →</button></td>
    </tr>`).join('')||`<tr><td colspan="10" class="empty">Sin datos con precio objetivo</td></tr>`;
  populateSkuDetailSelector();
  autoDetectSelectedSku();
  renderSelectedSkuPriceDetail();
}
function rankTable(arr){
  if(!arr.length) return '<div class="empty">Sin datos</div>';
  return `<table><thead><tr><th>SKU</th><th class="num">Neto unit.</th><th class="num">Objetivo</th><th class="num">Dif %</th></tr></thead><tbody>
    ${arr.map(r=>`<tr>
      <td><b>${escapeHtml(r.sku_padre)}</b> <small style="color:var(--muted)">${escapeHtml(r.modelo||'')}</small></td>
      <td class="num">${fmtMoney(r.neto_unit_prom)}</td><td class="num">${fmtMoney(r.precio_neto_obj)}</td>
      <td class="num ${diffCls(r.dif_pct)}">${fmtPct(r.dif_pct)}</td>
    </tr>`).join('')}
  </tbody></table>`;
}

// ---------- DETALLE SKU ----------
function populateSkuDetailSelector(){
  const sel=$('#selSkuDetail'); if(!sel) return;
  const prev=state.selectedSkuDetail||sel.value;
  const list=[...new Set((state.filtered||[]).filter(r=>r._sku_padre).map(r=>r._sku_padre))].sort();
  sel.innerHTML='<option value="">— Seleccionar SKU —</option>'+list.map(s=>{
    const rec=skuData.find(x=>x.sku_padre===s);
    const label=rec?(s+(rec.modelo?' · '+rec.modelo:'')):s;
    return `<option value="${escapeHtml(s)}">${escapeHtml(label)}</option>`;
  }).join('');
  if(prev && list.includes(prev)) sel.value=prev; else sel.value='';
}
function autoDetectSelectedSku(){
  const globalSkuPadre=($('#fSkuPadre').value||'').trim().toUpperCase();
  if(globalSkuPadre){
    const exist=(state.filtered||[]).some(r=>(r._sku_padre||'').toUpperCase()===globalSkuPadre);
    if(exist){
      state.selectedSkuDetail=globalSkuPadre;
      const sel=$('#selSkuDetail');
      if(sel){
        if(![...sel.options].some(o=>o.value===globalSkuPadre)){ const opt=document.createElement('option'); opt.value=globalSkuPadre; opt.textContent=globalSkuPadre; sel.appendChild(opt); }
        sel.value=globalSkuPadre;
      }
      return;
    }
  }
  if(state.selectedSkuDetail){
    const stillThere=(state.filtered||[]).some(r=>r._sku_padre===state.selectedSkuDetail);
    if(!stillThere) state.selectedSkuDetail=null;
  }
}
function computeSkuPriceDetail(rows,selectedSku){
  const sub=rows.filter(r=>r._sku_padre===selectedSku);
  if(!sub.length) return null;
  const sum=k=>sub.reduce((a,r)=>a+(r[k]||0),0);
  const unidades=sum('_unidades'),ventas=sub.length,vvp_total=sum('_ing_prod'),cargo_total=sum('_cargo');
  const ing_envio_total=sum('_ing_envio'),costo_envio_total=sum('_costo_envio'),desc_total=sum('_desc'),anul_total=sum('_anul'),neto_total=sum('_total');
  const precios=sub.map(r=>r._precio_unit).filter(v=>v!=null);
  const precio_prom=precios.length?precios.reduce((a,b)=>a+b,0)/precios.length:null;
  const precio_min=precios.length?Math.min(...precios):null,precio_max=precios.length?Math.max(...precios):null;
  const variantes=[...new Set(sub.map(r=>r._sku_raw).filter(Boolean))];
  const publicaciones=[...new Set(sub.map(r=>r.publicacion).filter(Boolean))];
  const modMap=new Map(),provMap=new Map();
  sub.forEach(r=>{ const mk=r.forma_entrega||'Sin modalidad'; modMap.set(mk,(modMap.get(mk)||0)+1); const pk=r.provincia||'Sin provincia'; provMap.set(pk,(provMap.get(pk)||0)+1); });
  const dominant=m=>{let b=null,bv=-1;m.forEach((v,k)=>{if(v>bv){b=k;bv=v;}}); return b;};
  const pub_count=sub.filter(r=>r._publicidad).length,pub_pct=safeDiv(pub_count,ventas);
  const cuotas_count=sub.filter(r=>/s[ií]/i.test(String(r.cuotas||''))).length,cuotas_pct=safeDiv(cuotas_count,ventas);
  const muestra=sub[0],precio_neto_obj=muestra._precio_neto_obj,precio_valido=muestra._precio_valido,modelo=muestra._modelo,categoria=muestra._categoria;
  const vvp_unit=safeDiv(vvp_total,unidades),cargo_unit=safeDiv(cargo_total,unidades),neto_unit=safeDiv(neto_total,unidades);
  const dif_abs=(neto_unit!=null && precio_neto_obj!=null)?neto_unit-precio_neto_obj:null;
  const dif_pct=(neto_unit!=null && precio_neto_obj)?(neto_unit/precio_neto_obj-1):null;
  const thr=state.thresholdPct/100; let estado='—';
  if(dif_pct!=null){ if(Math.abs(dif_pct)<=thr) estado='igual'; else if(dif_pct>0) estado='arriba'; else estado='abajo'; }
  return {sku_padre:selectedSku,modelo,categoria,variantes,publicaciones,ventas,unidades,vvp_total,vvp_unit,cargo_total,cargo_unit,ing_envio_total,costo_envio_total,desc_total,anul_total,neto_total,neto_unit,precio_prom,precio_min,precio_max,modalidad_dom:dominant(modMap),provincia_dom:dominant(provMap),pub_count,pub_pct,cuotas_count,cuotas_pct,precio_neto_obj,precio_valido,dif_abs,dif_pct,estado,rows:sub};
}
function computePublicationBreakdown(rows,selectedSku){
  const sub=rows.filter(r=>r._sku_padre===selectedSku);
  const byPub=new Map();
  sub.forEach(r=>{
    const mla=r.publicacion||'(sin #)';
    let g=byPub.get(mla);
    if(!g){ g={mla,titulo:r.titulo||'',variantesSet:new Set(),ventas:0,unidades:0,vvp_total:0,cargo_total:0,ing_envio_total:0,costo_envio_total:0,desc_total:0,anul_total:0,neto_total:0,precios:[],modMap:new Map(),provMap:new Map(),pub_count:0,cuotas_count:0,precio_neto_obj:r._precio_neto_obj,precio_valido:r._precio_valido}; byPub.set(mla,g); }
    if(r._sku_raw) g.variantesSet.add(r._sku_raw);
    if(r.variante) g.variantesSet.add(String(r.variante).trim());
    g.ventas+=1; g.unidades+=r._unidades; g.vvp_total+=r._ing_prod; g.cargo_total+=r._cargo;
    g.ing_envio_total+=r._ing_envio; g.costo_envio_total+=r._costo_envio; g.desc_total+=r._desc; g.anul_total+=r._anul; g.neto_total+=r._total;
    if(r._precio_unit!=null) g.precios.push(r._precio_unit);
    const mk=r.forma_entrega||'Sin modalidad'; g.modMap.set(mk,(g.modMap.get(mk)||0)+1);
    const pk=r.provincia||'Sin provincia';     g.provMap.set(pk,(g.provMap.get(pk)||0)+1);
    if(r._publicidad) g.pub_count+=1;
    if(/s[ií]/i.test(String(r.cuotas||''))) g.cuotas_count+=1;
    if(!g.titulo && r.titulo) g.titulo=r.titulo;
  });
  const thr=state.thresholdPct/100; const out=[];
  const dominant=m=>{let b=null,bv=-1;m.forEach((v,k)=>{if(v>bv){b=k;bv=v;}}); return b;};
  byPub.forEach(g=>{
    const vvp_unit=safeDiv(g.vvp_total,g.unidades),cargo_unit=safeDiv(g.cargo_total,g.unidades),neto_unit=safeDiv(g.neto_total,g.unidades);
    const precio_prom=g.precios.length?g.precios.reduce((a,b)=>a+b,0)/g.precios.length:null;
    const precio_min=g.precios.length?Math.min(...g.precios):null,precio_max=g.precios.length?Math.max(...g.precios):null;
    const dif_abs=(neto_unit!=null && g.precio_neto_obj!=null)?neto_unit-g.precio_neto_obj:null;
    const dif_pct=(neto_unit!=null && g.precio_neto_obj)?(neto_unit/g.precio_neto_obj-1):null;
    let estado='—';
    if(dif_pct!=null){ if(Math.abs(dif_pct)<=thr) estado='igual'; else if(dif_pct>0) estado='arriba'; else estado='abajo'; }
    out.push({mla:g.mla,titulo:g.titulo,variantes:[...g.variantesSet].join(', '),ventas:g.ventas,unidades:g.unidades,vvp_total:g.vvp_total,vvp_unit,cargo_total:g.cargo_total,cargo_unit,ing_envio_total:g.ing_envio_total,costo_envio_total:g.costo_envio_total,desc_total:g.desc_total,anul_total:g.anul_total,neto_total:g.neto_total,neto_unit,precio_prom,precio_min,precio_max,modalidad_dom:dominant(g.modMap),provincia_dom:dominant(g.provMap),pub_count:g.pub_count,pub_pct:safeDiv(g.pub_count,g.ventas),cuotas_count:g.cuotas_count,cuotas_pct:safeDiv(g.cuotas_count,g.ventas),dif_abs,dif_pct,estado,precio_neto_obj:g.precio_neto_obj});
  });
  return out;
}
function renderSelectedSkuPriceDetail(){
  const sku=state.selectedSkuDetail;
  const empty=$('#skuDetailEmpty'),secResumen=$('#skuDetailSummary'),secLeaders=$('#skuDetailPubLeaders'),secPubTable=$('#skuDetailPubTableBox'),secRows=$('#skuDetailRowsBox');
  if(!sku){ if(empty) empty.style.display='block'; [secResumen,secLeaders,secPubTable,secRows].forEach(s=>{if(s) s.style.display='none';}); return; }
  const detail=computeSkuPriceDetail(state.filtered||[],sku);
  if(!detail){ if(empty){empty.style.display='block';empty.textContent='No hay filas para el SKU "'+sku+'" con los filtros actuales.';} [secResumen,secLeaders,secPubTable,secRows].forEach(s=>{if(s) s.style.display='none';}); return; }
  if(empty) empty.style.display='none';
  [secResumen,secLeaders,secPubTable,secRows].forEach(s=>{if(s) s.style.display='block';});
  $('#skuDetailTitle').innerHTML=`<b>${escapeHtml(detail.sku_padre)}</b> <small style="color:var(--muted);font-weight:400">${escapeHtml(detail.modelo||'')} · ${escapeHtml(detail.categoria||'')}</small>`;
  const subs=[]; subs.push(`${detail.publicaciones.length} publicación(es) MLA`);
  if(detail.variantes.length) subs.push(`${detail.variantes.length} variante(s): ${escapeHtml(detail.variantes.slice(0,6).join(', '))}${detail.variantes.length>6?'…':''}`);
  $('#skuDetailSub').innerHTML=subs.join(' · ');
  $('#skuDetailBadge').innerHTML=badgeEstado(detail.estado);
  $('#skuDetailResumen').innerHTML=[
    kpi('SKU padre',escapeHtml(detail.sku_padre)),kpi('Modelo',escapeHtml(detail.modelo||'—')),kpi('Categoría',escapeHtml(detail.categoria||'—')),
    kpi('Variantes vendidas',String(detail.variantes.length),detail.variantes.slice(0,3).join(', ')||''),kpi('Publicaciones MLA',String(detail.publicaciones.length)),
    kpi('Ventas',fmtInt(detail.ventas)),kpi('Unidades',fmtInt(detail.unidades)),kpi('Modalidad dominante',escapeHtml(detail.modalidad_dom||'—')),kpi('Provincia dominante',escapeHtml(detail.provincia_dom||'—')),
    kpi('Precio neto objetivo',detail.precio_neto_obj!=null?fmtMoney(detail.precio_neto_obj):'—',detail.precio_valido?'':'<span class="badge warn">sin objetivo válido</span>'),
  ].join('');
  const diffColor=(n)=>n==null?'':(n>0?'style="color:#6fe09e"':(n<0?'style="color:#ff8a95"':''));
  $('#skuDetailEconomico').innerHTML=[
    kpi('Valor venta público (total)',fmtMoney(detail.vvp_total),'Suma de Ingresos por productos'),kpi('VVP unitario promedio',fmtMoney(detail.vvp_unit)),
    kpi('Cargo ML (total)',fmtMoney(detail.cargo_total)),kpi('Cargo ML unitario',fmtMoney(detail.cargo_unit)),
    kpi('Ingresos por envío',fmtMoney(detail.ing_envio_total)),kpi('Costos de envío',fmtMoney(detail.costo_envio_total)),
    kpi('Descuentos y bonif.',fmtMoney(detail.desc_total)),kpi('Anulaciones y reembolsos',fmtMoney(detail.anul_total)),
    kpi('Ingreso NETO final (total)',fmtMoney(detail.neto_total),'Suma de Total (ARS)'),kpi('NETO unitario promedio',fmtMoney(detail.neto_unit)),
    kpi('Dif. absoluta vs objetivo',detail.dif_abs!=null?`<span ${diffColor(detail.dif_abs)}>${fmtMoney(detail.dif_abs)}</span>`:'—'),
    kpi('Dif. % vs objetivo',detail.dif_pct!=null?`<span ${diffColor(detail.dif_pct)}>${fmtPct(detail.dif_pct)}</span>`:'—',badgeEstado(detail.estado)),
    kpi('Precio prom. vendido',fmtMoney(detail.precio_prom)),
    kpi('Precio mín. / máx.',(detail.precio_min!=null?fmtMoney(detail.precio_min):'—')+' / '+(detail.precio_max!=null?fmtMoney(detail.precio_max):'—')),
    kpi('Ventas con publicidad',fmtInt(detail.pub_count),fmtPct(detail.pub_pct),safeDiv(detail.pub_count,detail.ventas)),
  ].join('');
  $('#skuDetailCuotas').innerHTML=`
    <div class="card kpi"><div class="label">Ventas con cuotas agregadas</div><div class="value">${fmtInt(detail.cuotas_count)}</div><div class="sub">${fmtPct(detail.cuotas_pct)} del total del SKU</div></div>
    <div class="card kpi" style="flex:2 1 400px"><div class="label">Cargo por cuotas</div><div class="value" style="font-size:14px;font-weight:500;color:var(--muted);line-height:1.5">
      <span class="badge muted">No informado por separado en el reporte</span>
      <div style="margin-top:6px">El costo financiero ya viene deducido dentro de <b>Cargo por venta e impuestos</b> y se refleja en el <b>Total (ARS)</b> neto.</div>
    </div></div>`;
  const pubs=computePublicationBreakdown(state.filtered||[],sku);
  renderSkuPublicationTable(pubs); renderPubLeaders(pubs); renderSkuDetailRows(detail.rows);
  $('#skuDetailPubCount').textContent=pubs.length+' publicación'+(pubs.length===1?'':'es');
  $('#skuDetailRowsCount').textContent=detail.rows.length+' fila'+(detail.rows.length===1?'':'s');
}
function renderPubLeaders(pubs){
  const cont=$('#skuDetailLeaders');
  if(!pubs.length){ cont.innerHTML='<div class="empty">Sin publicaciones</div>'; return; }
  const pickBy=(arr,fn,desc=true)=>{ const filt=arr.filter(p=>fn(p)!=null); if(!filt.length) return null; filt.sort((a,b)=>desc?(fn(b)-fn(a)):(fn(a)-fn(b))); return filt[0]; };
  const leaderCard=(title,pub,valueFn,sub)=>{
    if(!pub) return `<div class="card leader-card"><div class="label">${title}</div><div class="value">—</div></div>`;
    return `<div class="card leader-card"><div class="label">${title}</div><div class="value" title="${escapeHtml(pub.titulo||'')}">${escapeHtml(pub.mla)}</div><div class="sub">${valueFn(pub)}</div>${sub?`<div class="sub">${sub(pub)}</div>`:''}</div>`;
  };
  cont.innerHTML=[
    leaderCard('🥇 Más ventas',pickBy(pubs,p=>p.ventas),p=>fmtInt(p.ventas)+' ventas · '+fmtInt(p.unidades)+' uds'),
    leaderCard('💰 Mayor neto total',pickBy(pubs,p=>p.neto_total),p=>fmtMoney(p.neto_total)),
    leaderCard('📈 Mayor neto unitario',pickBy(pubs,p=>p.neto_unit),p=>fmtMoney(p.neto_unit)),
    leaderCard('🏷 Mayor precio prom.',pickBy(pubs,p=>p.precio_prom),p=>fmtMoney(p.precio_prom)),
    leaderCard('⚠️ Peor desvío vs obj.',pickBy(pubs,p=>p.dif_pct,false),p=>p.dif_pct!=null?fmtPct(p.dif_pct):'—',p=>badgeEstado(p.estado)),
  ].join('');
}
function renderSkuPublicationTable(pubs){
  const s=state.pubSort; const data=sortData([...pubs],s);
  let bestMla=null,worstMla=null;
  const withDiff=pubs.filter(p=>p.dif_pct!=null);
  if(withDiff.length){
    const best=[...withDiff].sort((a,b)=>b.dif_pct-a.dif_pct)[0];
    const worst=[...withDiff].sort((a,b)=>a.dif_pct-b.dif_pct)[0];
    if(best) bestMla=best.mla; if(worst) worstMla=worst.mla;
    if(bestMla===worstMla) worstMla=null;
  } else if(pubs.length){
    const best=[...pubs].sort((a,b)=>b.neto_total-a.neto_total)[0];
    const worst=[...pubs].sort((a,b)=>a.neto_total-b.neto_total)[0];
    if(best) bestMla=best.mla; if(worst && worst.mla!==bestMla) worstMla=worst.mla;
  }
  bindSortable('#tblSkuPubs',state.pubSort,()=>renderSkuPublicationTable(pubs));
  $('#tblSkuPubs tbody').innerHTML=data.map(r=>{
    const cls=r.mla===bestMla?'row-best':(r.mla===worstMla?'row-worst':'');
    const marker=r.mla===bestMla?'🟢 ':(r.mla===worstMla?'🔴 ':'');
    const title=r.titulo?escapeHtml(r.titulo):'';
    return `<tr class="${cls}">
      <td>${marker}<b>${escapeHtml(r.mla)}</b></td>
      <td title="${title}" style="max-width:260px;white-space:normal">${title}</td>
      <td style="max-width:200px;white-space:normal">${escapeHtml(r.variantes||'')}</td>
      <td class="num">${fmtInt(r.ventas)}</td><td class="num">${fmtInt(r.unidades)}</td>
      <td class="num">${fmtMoney(r.vvp_total)}</td><td class="num">${fmtMoney(r.vvp_unit)}</td>
      <td class="num">${fmtMoney(r.precio_prom)}</td><td class="num">${fmtMoney(r.precio_min)}</td><td class="num">${fmtMoney(r.precio_max)}</td>
      <td class="num">${fmtMoney(r.cargo_total)}</td><td class="num">${fmtMoney(r.ing_envio_total)}</td>
      <td class="num">${fmtMoney(r.costo_envio_total)}</td><td class="num">${fmtMoney(r.desc_total)}</td><td class="num">${fmtMoney(r.anul_total)}</td>
      <td class="num">${fmtMoney(r.neto_total)}</td><td class="num">${fmtMoney(r.neto_unit)}</td>
      <td class="num" style="background:rgba(255,255,255,0.05)">${r.precio_neto_obj!=null?fmtMoney(r.precio_neto_obj):'—'}</td>
      <td class="num ${diffCls(r.dif_abs)}">${r.dif_abs!=null?fmtMoney(r.dif_abs):'—'}</td>
      <td class="num ${diffCls(r.dif_pct)}">${r.dif_pct!=null?fmtPct(r.dif_pct):'—'}</td>
      <td>${formatModalidadBadge(r.modalidad_dom)}</td><td>${escapeHtml(r.provincia_dom||'')}</td>
      <td class="num">${fmtInt(r.pub_count)}</td><td class="num">${fmtPct(r.pub_pct)}</td>
      <td>${badgeEstado(r.estado)}</td>
    </tr>`;
  }).join('')||`<tr><td colspan="24" class="empty">Sin publicaciones</td></tr>`;
}
function renderSkuDetailRows(rows){
  const thr=state.thresholdPct/100;
  const data=[...rows].sort((a,b)=>{ const da=a._fecha?a._fecha.getTime():0,db=b._fecha?b._fecha.getTime():0; return db-da; }).slice(0,300);
  $('#tblSkuRows tbody').innerHTML=data.map(r=>{
    const neto_unit=safeDiv(r._total,r._unidades); let estado='—';
    if(r._precio_neto_obj && neto_unit!=null){ const d=neto_unit/r._precio_neto_obj-1; if(Math.abs(d)<=thr) estado='igual'; else if(d>0) estado='arriba'; else estado='abajo'; }
    return `<tr>
      <td>${fmtDate(r._fecha)}</td><td>${escapeHtml(r.num_venta||'')}</td><td>${escapeHtml(r.publicacion||'')}</td>
      <td>${escapeHtml(r._sku_raw||'')}</td><td>${escapeHtml(r.variante||'')}</td>
      <td class="num">${fmtInt(r._unidades)}</td><td class="num">${fmtMoney(r._precio_unit)}</td>
      <td class="num">${fmtMoney(r._ing_prod)}</td><td class="num">${fmtMoney(r._cargo)}</td>
      <td class="num">${fmtMoney(r._ing_envio)}</td><td class="num">${fmtMoney(r._costo_envio)}</td>
      <td class="num">${fmtMoney(r._desc)}</td><td class="num">${fmtMoney(r._anul)}</td>
      <td class="num">${fmtMoney(r._total)}</td><td class="num">${fmtMoney(neto_unit)}</td>
      <td>${formatModalidadBadge(r.forma_entrega)}</td><td>${escapeHtml(r.provincia||'')}</td>
      <td>${r._publicidad?'<span class="badge info">Sí</span>':'<span class="badge muted">No</span>'}</td>
      <td>${/s[ií]/i.test(String(r.cuotas||''))?'<span class="badge info">Sí</span>':'<span class="badge muted">No</span>'}</td>
      <td>${badgeEstado(estado)}</td>
    </tr>`;
  }).join('')||`<tr><td colspan="20" class="empty">Sin filas</td></tr>`;
}

// ---------- LOGÍSTICA ----------
function renderLogisticaTab(){
  const rows=state.filtered||[];
  const byMod=new Map();
  rows.forEach(r=>{ const k=r.forma_entrega||'Sin modalidad'; let g=byMod.get(k); if(!g){ g={mod:k,ventas:0,unidades:0,fact:0,neto:0,costo:0,ingreso:0}; byMod.set(k,g); } g.ventas+=1; g.unidades+=r._unidades; g.fact+=r._ing_prod; g.neto+=r._total; g.costo+=r._costo_envio; g.ingreso+=r._ing_envio; });
  const totalV=rows.length;
  const data=[...byMod.values()].sort((a,b)=>b.ventas-a.ventas);
  $('#tblLogistica tbody').innerHTML=data.map(g=>`
    <tr><td><b>${escapeHtml(g.mod)}</b></td><td class="num">${fmtInt(g.ventas)}</td><td class="num">${fmtPct(safeDiv(g.ventas,totalV))}</td>
    <td class="num">${fmtInt(g.unidades)}</td><td class="num">${fmtMoney(g.fact)}</td><td class="num">${fmtMoney(g.neto)}</td><td class="num">${fmtMoney(g.costo)}</td><td class="num">${fmtMoney(g.ingreso)}</td></tr>`).join('')||`<tr><td colspan="8" class="empty">Sin datos</td></tr>`;
  drawChart('chartLogVentas','bar',{labels:data.map(d=>d.mod),datasets:[{label:'Ventas',data:data.map(d=>d.ventas),backgroundColor:'#4c9aff'}]});
  drawChart('chartLogNeto','bar',{labels:data.map(d=>d.mod),datasets:[{label:'Neto',data:data.map(d=>d.neto),backgroundColor:'#7c5cff'}]});
}

// ---------- ZONAS ----------
let provData=[],cityData=[],provSort={k:'envios',dir:-1},citySort={k:'envios',dir:-1};
function renderZonasTab(){
  const rows=state.filtered||[]; const totalEnv=rows.length;
  const pMap=new Map(),cMap=new Map();
  rows.forEach(r=>{
    const pk=r.provincia||'Sin provincia'; let p=pMap.get(pk);
    if(!p){ p={provincia:pk,envios:0,unidades:0,facturacion:0,neto:0}; pMap.set(pk,p); }
    p.envios+=1; p.unidades+=r._unidades; p.facturacion+=r._ing_prod; p.neto+=r._total;
    const ck=(r.ciudad||'Sin ciudad')+'||'+pk; let c=cMap.get(ck);
    if(!c){ c={ciudad:r.ciudad||'Sin ciudad',provincia:pk,envios:0,unidades:0,facturacion:0,neto:0}; cMap.set(ck,c); }
    c.envios+=1; c.unidades+=r._unidades; c.facturacion+=r._ing_prod; c.neto+=r._total;
  });
  provData=[...pMap.values()].map(p=>Object.assign(p,{pct:safeDiv(p.envios,totalEnv)}));
  cityData=[...cMap.values()];
  bindSortable('#tblProv',provSort,()=>renderProvTable());
  bindSortable('#tblCity',citySort,()=>renderCityTable());
  renderProvTable(); renderCityTable();
  const top=[...provData].sort((a,b)=>b.envios-a.envios).slice(0,15);
  drawChart('chartProv','bar',{labels:top.map(x=>x.provincia),datasets:[{label:'Envíos',data:top.map(x=>x.envios),backgroundColor:'#4c9aff'},{label:'Unidades',data:top.map(x=>x.unidades),backgroundColor:'#7c5cff'}]});
}
function renderProvTable(){ const data=sortData([...provData],provSort); $('#tblProv tbody').innerHTML=data.map(p=>`<tr><td><b>${escapeHtml(p.provincia)}</b></td><td class="num">${fmtInt(p.envios)}</td><td class="num">${fmtInt(p.unidades)}</td><td class="num">${fmtMoney(p.facturacion)}</td><td class="num">${fmtMoney(p.neto)}</td><td class="num">${fmtPct(p.pct)}</td></tr>`).join('')||`<tr><td colspan="6" class="empty">Sin datos</td></tr>`; }
function renderCityTable(){ const data=sortData([...cityData],citySort).slice(0,50); $('#tblCity tbody').innerHTML=data.map(c=>`<tr><td><b>${escapeHtml(c.ciudad)}</b></td><td>${escapeHtml(c.provincia)}</td><td class="num">${fmtInt(c.envios)}</td><td class="num">${fmtInt(c.unidades)}</td><td class="num">${fmtMoney(c.facturacion)}</td><td class="num">${fmtMoney(c.neto)}</td></tr>`).join('')||`<tr><td colspan="6" class="empty">Sin datos</td></tr>`; }

// ---------- EXCEPCIONES ----------
function renderExcepcionesTab(){
  const rows=state.filtered||[];
  const sinSku=rows.filter(r=>!r._sku_raw), sinMatch=rows.filter(r=>r._sku_raw && !r._sku_padre), precInv=rows.filter(r=>r._sku_padre && !r._precio_valido);
  // FIX: es_kit no existe como campo separado — ambos "es pack" y "es kit" se mapean a es_pack
  const combos=rows.filter(r=>String(r.es_pack||'').toLowerCase().includes('s'));
  $('#kpiSinSku').innerHTML=`<div class="label">Filas sin SKU</div><div class="value">${sinSku.length}</div>`;
  $('#kpiSinMatch').innerHTML=`<div class="label">SKU sin match</div><div class="value">${sinMatch.length}</div>`;
  $('#kpiPrecioInv').innerHTML=`<div class="label">Precio neto inválido</div><div class="value">${precInv.length}</div>`;
  $('#kpiCombos').innerHTML=`<div class="label">Packs / Kits</div><div class="value">${combos.length}</div>`;
  const agg=new Map();
  sinMatch.forEach(r=>{ const k=r._sku_raw; let g=agg.get(k); if(!g){ g={sku:k,apariciones:0,unidades:0,neto:0,ejemplo:r.titulo||r.publicacion||''}; agg.set(k,g); } g.apariciones+=1; g.unidades+=r._unidades; g.neto+=r._total; });
  const list=[...agg.values()].sort((a,b)=>b.apariciones-a.apariciones);
  $('#tblSinMatch tbody').innerHTML=list.map(r=>`<tr><td><b>${escapeHtml(r.sku)}</b></td><td class="num">${fmtInt(r.apariciones)}</td><td class="num">${fmtInt(r.unidades)}</td><td class="num">${fmtMoney(r.neto)}</td><td>${escapeHtml(r.ejemplo)}</td></tr>`).join('')||`<tr><td colspan="5" class="empty">Sin excepciones</td></tr>`;
  const pl=state.priceList; const invRows=pl?pl.invalid:[];
  $('#tblPrecioInv tbody').innerHTML=invRows.map(i=>`<tr><td><b>${escapeHtml(i.sku_padre)}</b></td><td>${escapeHtml(i.modelo||'')}</td><td>${escapeHtml(i.categoria||'')}</td><td>${escapeHtml(i.precio_neto_raw)}</td></tr>`).join('')||`<tr><td colspan="4" class="empty">Todos válidos</td></tr>`;
  $('#tblSinSku tbody').innerHTML=sinSku.slice(0,200).map(r=>`<tr><td>${escapeHtml(r.num_venta||'')}</td><td>${fmtDate(r._fecha)}</td><td>${escapeHtml(r.titulo||'')}</td><td class="num">${fmtInt(r._unidades)}</td><td class="num">${fmtMoney(r._total)}</td></tr>`).join('')||`<tr><td colspan="5" class="empty">Todas las filas tienen SKU</td></tr>`;
}

// ---------- AUDITORÍA ----------
function computeAuditStats(rows){
  const stats={total:rows.length,conSku:0,exacto:0,variante:0,sinMatch:0,precioInv:0,sinSku:0};
  rows.forEach(r=>{ if(!r._sku_raw){stats.sinSku++;return;} stats.conSku++; if(r._match_kind==='exacto') stats.exacto++; else if(r._match_kind==='variante') stats.variante++; else if(r._match_kind==='precio_invalido') stats.precioInv++; else stats.sinMatch++; });
  return stats;
}
function renderAuditoriaTab(){
  const rows=state.filtered||[]; const a=computeAuditStats(rows);
  $('#auditKpis').innerHTML=[kpi('Filas totales',fmtInt(a.total)),kpi('Con SKU',fmtInt(a.conSku),'',a.total?a.conSku/a.total:0),kpi('Sin SKU',fmtInt(a.sinSku)),kpi('Match exacto',fmtInt(a.exacto),'',a.total?a.exacto/a.total:0),kpi('Match variante',fmtInt(a.variante),'',a.total?a.variante/a.total:0),kpi('Sin match',fmtInt(a.sinMatch),'',a.total?a.sinMatch/a.total:0),kpi('Precio inválido',fmtInt(a.precioInv))].join('');
  const m=new Map();
  rows.forEach(r=>{ const k=r._sku_raw||'(sin sku)'; let g=m.get(k); if(!g){ g={sku_original:k,sku_padre:r._sku_padre||'',tipo:r._match_kind||'sin_match',precio:r._precio_neto_obj,apariciones:0,observacion:describeObs(r)}; m.set(k,g); } g.apariciones+=1; });
  renderAuditTable([...m.values()]);
}
function describeObs(r){
  switch(r._match_kind){
    case 'exacto':          return 'Match directo sobre SKU principal o variante';
    case 'variante':        return 'Match a través de código de variante';
    case 'precio_invalido': return 'SKU encontrado pero precio neto vacío o no numérico';
    case 'sin_sku':         return 'Fila sin SKU en el reporte';
    default:                return 'SKU no encontrado en lista de precios — revisar homologación';
  }
}
function renderAuditTable(data){
  const q=($('#auditSearch').value||'').toLowerCase().trim(), f=$('#auditFilter').value;
  let d=data.filter(r=>{ if(q && !(r.sku_original+' '+r.sku_padre).toLowerCase().includes(q)) return false; if(f && r.tipo!==f) return false; return true; });
  d.sort((a,b)=>b.apariciones-a.apariciones);
  $('#tblAudit tbody').innerHTML=d.slice(0,1000).map(r=>`<tr><td><b>${escapeHtml(r.sku_original)}</b></td><td>${escapeHtml(r.sku_padre||'')}</td><td>${matchBadge(r.tipo)}</td><td class="num">${r.precio!=null?fmtMoney(r.precio):'—'}</td><td class="num">${fmtInt(r.apariciones)}</td><td>${escapeHtml(r.observacion)}</td></tr>`).join('')||`<tr><td colspan="6" class="empty">Sin datos</td></tr>`;
}
function matchBadge(k){
  switch(k){
    case 'exacto':          return '<span class="badge ok">Exacto</span>';
    case 'variante':        return '<span class="badge info">Variante</span>';
    case 'precio_invalido': return '<span class="badge warn">Precio inválido</span>';
    case 'sin_sku':         return '<span class="badge muted">Sin SKU</span>';
    default:                return '<span class="badge bad">Sin match</span>';
  }
}

// ---------- CALIDAD Y DEVOLUCIONES ----------
let calidadData = [];

function renderCalidadTab(){
  const rows = getCalidadFiltered();
  const cancelledRows = rows.filter(r => r._cancelada);
  
  const totalCancel = cancelledRows.length;
  const udsCancel = cancelledRows.reduce((a,r)=>a+(r._unidades||0),0);
  const factPerdida = cancelledRows.reduce((a,r)=>a+(r._ing_prod||r._precio_unit*(r._unidades||1)||0), 0);
  
  const udsTotales = rows.reduce((a,r)=>a+(r._unidades||0), 0);
  const tasaFalloGral = safeDiv(udsCancel, udsTotales);

  $('#calidadKpis').innerHTML=[
    kpi('Ventas Fallidas (Filas)',fmtInt(totalCancel)),
    kpi('Unidades Fallidas',fmtInt(udsCancel)),
    kpi('Facturación Bruta Perdida',fmtMoney(factPerdida)),
    kpi('Tasa de Fallo General',fmtPct(tasaFalloGral), 'Unidades fallidas / Totales')
  ].join('');

  // Grafico Estados
  const byEstado = groupSum(cancelledRows, r=>r.estado_venta||'Sin estado', r=>1);
  drawChart('chartCalidadEstados','doughnut',{
    labels:[...byEstado.keys()],
    datasets:[{data:[...byEstado.values()],backgroundColor:['#ef4b5c','#f4b23b','#7c5cff','#4c9aff','#8a94ab']}]
  });

  // Ranking SKU
  const skuMap = new Map();
  rows.forEach(r => {
    const k = r._sku_padre || r._sku_raw || '(Sin SKU)';
    let g = skuMap.get(k);
    if(!g) { g = {sku:k, fallos:0, uds_vendidas:0, fact_perdida:0}; skuMap.set(k,g); }
    g.uds_vendidas += (r._unidades||0);
    if(r._cancelada) {
      g.fallos += (r._unidades||0);
      g.fact_perdida += (r._ing_prod||r._precio_unit*(r._unidades||1)||0);
    }
  });

  const skuList = [...skuMap.values()].filter(s => s.fallos > 0);
  skuList.forEach(s => s.tasa = safeDiv(s.fallos, s.uds_vendidas));
  skuList.sort((a,b) => b.fallos - a.fallos);

  $('#tblCalidadRank tbody').innerHTML = skuList.map(s => `<tr>
    <td><b>${escapeHtml(s.sku)}</b></td>
    <td class="num">${fmtInt(s.fallos)}</td>
    <td class="num">${fmtInt(s.uds_vendidas)}</td>
    <td class="num">${fmtPct(s.tasa)}</td>
  </tr>`).join('') || `<tr><td colspan="4" class="empty">No hay fallos</td></tr>`;

  // Detalle
  calidadData = cancelledRows.sort((a,b) => (b._fecha?b._fecha.getTime():0) - (a._fecha?a._fecha.getTime():0));
  $('#tblCalidadDetail tbody').innerHTML = calidadData.slice(0,500).map(r => {
    const fact = r._ing_prod || r._precio_unit*(r._unidades||1) || 0;
    return `<tr>
      <td>${fmtDate(r._fecha)}</td>
      <td>${escapeHtml(r.num_venta||'')}</td>
      <td><span class="badge bad">${escapeHtml(r.estado_venta||'')}</span></td>
      <td>${escapeHtml(r._sku_raw||'')}</td>
      <td class="num">${fmtInt(r._unidades)}</td>
      <td class="num">${fmtMoney(fact)}</td>
    </tr>`;
  }).join('') || `<tr><td colspan="6" class="empty">No hay devoluciones ni cancelaciones</td></tr>`;
}

function getCalidadFiltered(){
  if(!state.sales) return [];
  const from = $('#fDateFrom').value ? new Date($('#fDateFrom').value) : null;
  const to   = $('#fDateTo').value   ? new Date($('#fDateTo').value+'T23:59:59') : null;
  const skuPadre = $('#fSkuPadre').value.trim().toUpperCase();
  const skuOrig  = $('#fSkuOriginal').value.trim().toUpperCase();
  const pub      = $('#fPublicacion').value.trim();
  const modalidad= $('#fModalidad').value;
  const prov     = $('#fProvincia').value;
  const ciudad   = $('#fCiudad').value.trim().toLowerCase();
  const publi    = $('#fPublicidad').value;
  // Omitimos fEstado para que no afecte la vista de cancelaciones
  return state.sales.rows.filter(r=>{
    if(from && (!r._fecha||r._fecha<from)) return false;
    if(to   && (!r._fecha||r._fecha>to))   return false;
    if(skuPadre && (r._sku_padre||'').toUpperCase()!==skuPadre) return false;
    if(skuOrig  && (r._sku_raw||'').toUpperCase().indexOf(skuOrig)<0) return false;
    if(pub      && String(r.publicacion||'').indexOf(pub)<0) return false;
    if(modalidad && (r.forma_entrega||'')!==modalidad) return false;
    if(prov     && (r.provincia||'')!==prov) return false;
    if(ciudad   && String(r.ciudad||'').toLowerCase().indexOf(ciudad)<0) return false;
    if(publi==='si' && !r._publicidad) return false;
    if(publi==='no' &&  r._publicidad) return false;
    return true;
  });
}

// ---------- SORT ----------
function bindSortable(selector,sortState,rerender){
  const ths=$$(selector+' thead th[data-k]');
  ths.forEach(th=>{ th.onclick=()=>{ const k=th.dataset.k; if(sortState.k===k) sortState.dir*=-1; else{sortState.k=k;sortState.dir=-1;} ths.forEach(x=>x.textContent=x.textContent.replace(/ [↑↓]$/,'')); th.textContent=th.textContent+(sortState.dir>0?' ↑':' ↓'); rerender(); }; });
}
function sortData(arr,s){
  if(!s||!s.k) return arr;
  const k=s.k,d=s.dir;
  return arr.sort((a,b)=>{ let va=a[k],vb=b[k]; if(typeof va==='string'||typeof vb==='string'){va=String(va||'').toLowerCase();vb=String(vb||'').toLowerCase();if(va<vb)return -1*d;if(va>vb)return 1*d;return 0;} va=va==null?-Infinity:va;vb=vb==null?-Infinity:vb;return(va-vb)*d; });
}

// ---------- HISTORIAL EN LA NUBE ----------
let cloudHistory = [];
function _setupHistoryListener(){
  if(!_db) return;
  _db.ref('mlanalyzer/history').on('value', snap=>{
    const val = snap.val() || {};
    cloudHistory = Object.keys(val).map(k => ({id:k, ...val[k]})).sort((a,b)=>b.loadedAt - a.loadedAt);
    renderHistory();
  });
}

function renderHistory(){
  $('#tblHistory tbody').innerHTML = cloudHistory.map(x => `<tr>
    <td>${new Date(x.loadedAt).toLocaleString('es-AR')}</td>
    <td>${escapeHtml(x.fileName||'')}</td>
    <td>${escapeHtml(x.period||'')}</td>
    <td class="num">${fmtInt(x.rows)}</td>
    <td class="num">${fmtMoney(x.neto)}</td>
    <td>
      <button class="btn primary" style="padding:4px 8px;font-size:12px" onclick="loadCloudReport('${x.id}')">Cargar</button>
      <button class="btn danger" style="padding:4px 8px;font-size:12px;margin-left:4px" onclick="deleteCloudReport('${x.id}')">Borrar</button>
    </td>
  </tr>`).join('') || `<tr><td colspan="6" class="empty">Sin historial guardado en la nube</td></tr>`;
}

window.loadCloudReport = async (id) => {
  if(!_db) return;
  toast('Cargando reporte de la nube...','');
  try {
    const snap = await _db.ref(`mlanalyzer/history/${id}`).once('value');
    const data = snap.val();
    if(!data){ toast('Reporte no encontrado','error'); return; }
    await _db.ref('mlanalyzer/sales').set({ rows: data.rows, meta: data.meta });
    toast('Reporte cargado con éxito','ok');
  } catch(e) {
    toast('Error cargando reporte: '+e.message, 'error');
  }
};

window.deleteCloudReport = async (id) => {
  if(!_db || !confirm('¿Eliminar este reporte permanentemente de la nube?')) return;
  await _db.ref(`mlanalyzer/history/${id}`).remove();
  toast('Reporte eliminado de la nube','ok');
};

$('#btnSaveToCloud').onclick = async ()=>{
  if(!_db || !state.sales){ toast('No hay reporte activo para guardar','error'); return; }
  const s = state.sales;
  const ser = s.rows.map(_serializeRow);
  const rowsStr = JSON.stringify(ser);
  const metaStr = JSON.stringify(s.meta);
  const p = s.meta.period;
  const periodStr = p ? `${fmtDate(p.from)} → ${fmtDate(p.to)}` : 'Sin periodo';
  const neto = s.rows.reduce((a,r)=>a+(r._total||0), 0);
  
  const id = 'rep_' + Date.now();
  toast('Guardando en la nube (puede tardar unos segundos)...','');
  try {
    await _db.ref(`mlanalyzer/history/${id}`).set({
      loadedAt: Date.now(),
      fileName: s.meta.fileName || 'Reporte',
      period: periodStr,
      rows: rowsStr,
      meta: metaStr,
      neto: neto
    });
    toast('Reporte guardado en el historial','ok');
  } catch(e) {
    toast('Error guardando historial: '+e.message, 'error');
  }
};

// ---------- EXPORTACIONES ----------
function exportCSV(filename,rows,columns){
  const header=columns.map(c=>c.label).join(',');
  const lines=rows.map(r=>columns.map(c=>{ let v=c.get(r); if(v==null) v=''; v=String(v).replace(/"/g,'""'); if(/[",\n;]/.test(v)) v='"'+v+'"'; return v; }).join(','));
  const csv=[header].concat(lines).join('\n');
  const blob=new Blob(['﻿'+csv],{type:'text/csv;charset=utf-8'});
  const url=URL.createObjectURL(blob); const a=document.createElement('a'); a.href=url; a.download=filename; a.click(); URL.revokeObjectURL(url);
}
function exportExcel(filename,rows,columns){
  const header=columns.map(c=>c.label);
  const data=rows.map(r=>columns.map(c=>{ let v=c.get(r); if(typeof v==='string') v=v.replace(/<[^>]*>?/gm,''); return v==null?'':v; }));
  const ws=XLSX.utils.aoa_to_sheet([header].concat(data)); const wb=XLSX.utils.book_new(); XLSX.utils.book_append_sheet(wb,ws,"Datos"); XLSX.writeFile(wb,filename);
}
function formatModalidadBadge(raw){
  const s=String(raw||'').toLowerCase();
  if(s.includes('fulfillment'))  return '<span class="badge ok">⚡ FULL</span>';
  if(s.includes('cross docking')) return '<span class="badge info">🚚 Colecta</span>';
  if(s.includes('flex'))          return '<span class="badge warn">🛵 Flex</span>';
  if(s.includes('drop off'))      return '<span class="badge muted">📦 Correo</span>';
  return escapeHtml(raw);
}

// ---------- EVENTOS ----------
$('#fileSales').addEventListener('change',  e=>{ if(e.target.files[0]) handleSalesFile(e.target.files[0]); });
$('#filePrices').addEventListener('change', e=>{ if(e.target.files[0]) handlePriceFile(e.target.files[0]); });
$('#filePrices2').addEventListener('change',e=>{ if(e.target.files[0]) handlePriceFile(e.target.files[0]); });

$('#btnClearReport').onclick = ()=>{
  state.sales=null; state.filtered=null;
  $('#filtersBlock').style.display='none';
  Object.values(state.charts).forEach(c=>c.destroy()); state.charts={};
  $('#kpisRow').innerHTML='<div class="empty">Cargá un reporte para ver el dashboard.</div>';
  if($('#calidadKpis')) $('#calidadKpis').innerHTML='';
  ['tblSku','tblPrecios','tblLogistica','tblProv','tblCity','tblAudit','tblCalidadRank','tblCalidadDetail'].forEach(id=>{ const tb=document.querySelector('#'+id+' tbody'); if(tb) tb.innerHTML=''; });
  renderStatus();
  toast('Reporte limpiado','ok');
  if(fbConnected()) fbClear_sales();
};

$('#btnClearPriceList').onclick = ()=>{
  if(!confirm('¿Borrar lista de precios guardada?')) return;
  localStorage.removeItem(LS_PRICE);
  state.priceList=null; updatePriceListUI();
  if(state.sales){ enrichSales(); refreshAll(); }
  toast('Lista borrada','ok');
  if(fbConnected()) fbClear_priceList();
};



$('#thresholdPct').addEventListener('change', e=>{ state.thresholdPct=parseFloat(e.target.value)||0; saveCfg(); if(state.sales) refreshAll(); });
$('#btnApplyFilters').onclick = ()=>{ if(state.sales) refreshAll(); };
$('#btnResetFilters').onclick = ()=>{
  ['fDateFrom','fDateTo','fSkuPadre','fSkuOriginal','fPublicacion','fCiudad'].forEach(id=>$('#'+id).value='');
  ['fModalidad','fProvincia','fPublicidad','fEstado'].forEach(id=>$('#'+id).value='');
  if(state.sales) refreshAll();
};

$('#skuSearch').addEventListener('input',()=>renderSkuTable());
$('#auditSearch').addEventListener('input',()=>renderAuditoriaTab());
$('#auditFilter').addEventListener('change',()=>renderAuditoriaTab());

$('#btnExportSku').onclick = ()=>exportCSV('sku.csv',skuData,[{label:'SKU padre',get:r=>r.sku_padre},{label:'Modelo',get:r=>r.modelo},{label:'Categoría',get:r=>r.categoria},{label:'Variantes',get:r=>r.variantes},{label:'Ventas',get:r=>r.ventas},{label:'Unidades',get:r=>r.unidades},{label:'Facturación bruta',get:r=>r.facturacion_bruta},{label:'Neto total',get:r=>r.neto_total},{label:'Neto unit. prom.',get:r=>r.neto_unit_prom},{label:'Precio prom.',get:r=>r.precio_prom},{label:'Precio min',get:r=>r.precio_min},{label:'Precio max',get:r=>r.precio_max},{label:'Con publi',get:r=>r.pub_count},{label:'% publi',get:r=>r.pub_pct},{label:'Modalidad dom.',get:r=>r.modalidad_dom},{label:'Provincia dom.',get:r=>r.provincia_dom},{label:'Neto objetivo',get:r=>r.precio_neto_obj},{label:'Dif abs',get:r=>r.dif_abs},{label:'Dif %',get:r=>r.dif_pct},{label:'Estado',get:r=>r.estado}]);
$('#btnExportPrecios').onclick = ()=>{ const data=skuData.filter(r=>r.precio_neto_obj!=null && r.neto_unit_prom!=null); exportCSV('control_precios.csv',data,[{label:'SKU padre',get:r=>r.sku_padre},{label:'Modelo',get:r=>r.modelo},{label:'Neto unit. prom.',get:r=>r.neto_unit_prom},{label:'Neto objetivo',get:r=>r.precio_neto_obj},{label:'Dif abs',get:r=>r.dif_abs},{label:'Dif %',get:r=>r.dif_pct},{label:'Estado',get:r=>r.estado},{label:'Unidades',get:r=>r.unidades},{label:'Neto total',get:r=>r.neto_total}]); };

$('#selSkuDetail').addEventListener('change', e=>{ state.selectedSkuDetail=e.target.value||null; renderSelectedSkuPriceDetail(); });
$('#btnClearSkuDetail').onclick = ()=>{ state.selectedSkuDetail=null; $('#selSkuDetail').value=''; renderSelectedSkuPriceDetail(); };
document.addEventListener('click', e=>{
  const btn=e.target.closest('[data-sku-detail]');
  if(!btn) return;
  const sku=btn.dataset.skuDetail;
  state.selectedSkuDetail=sku;
  const sel=$('#selSkuDetail');
  if(sel){ if(![...sel.options].some(o=>o.value===sku)){ const opt=document.createElement('option'); opt.value=sku; opt.textContent=sku; sel.appendChild(opt); } sel.value=sku; }
  switchTab('precios'); renderSelectedSkuPriceDetail();
  setTimeout(()=>{ const el=$('#skuDetailSummary'); if(el) el.scrollIntoView({behavior:'smooth',block:'start'}); },50);
});

$('#btnExportSkuDetail').onclick = ()=>{
  const sku=state.selectedSkuDetail; if(!sku){toast('Seleccioná un SKU primero','error');return;}
  const d=computeSkuPriceDetail(state.filtered||[],sku); if(!d){toast('Sin datos para exportar','error');return;}
  const rows=[{campo:'SKU padre',valor:d.sku_padre},{campo:'Modelo',valor:d.modelo||''},{campo:'Categoría',valor:d.categoria||''},{campo:'Variantes vendidas',valor:d.variantes.join(', ')},{campo:'Publicaciones MLA',valor:d.publicaciones.length},{campo:'Ventas',valor:d.ventas},{campo:'Unidades',valor:d.unidades},{campo:'VVP total',valor:d.vvp_total},{campo:'VVP unit.',valor:d.vvp_unit},{campo:'Cargo ML total',valor:d.cargo_total},{campo:'Cargo ML unit.',valor:d.cargo_unit},{campo:'Ing. envío',valor:d.ing_envio_total},{campo:'Costo envío',valor:d.costo_envio_total},{campo:'Descuentos',valor:d.desc_total},{campo:'Anulaciones',valor:d.anul_total},{campo:'Neto total',valor:d.neto_total},{campo:'Neto unit.',valor:d.neto_unit},{campo:'Precio prom.',valor:d.precio_prom},{campo:'Precio mín.',valor:d.precio_min},{campo:'Precio máx.',valor:d.precio_max},{campo:'Con publi',valor:d.pub_count},{campo:'% publi',valor:d.pub_pct},{campo:'Con cuotas',valor:d.cuotas_count},{campo:'% cuotas',valor:d.cuotas_pct},{campo:'Modalidad dom.',valor:d.modalidad_dom||''},{campo:'Provincia dom.',valor:d.provincia_dom||''},{campo:'Neto objetivo',valor:d.precio_neto_obj},{campo:'Dif abs',valor:d.dif_abs},{campo:'Dif %',valor:d.dif_pct},{campo:'Estado',valor:d.estado}];
  exportExcel(`detalle_${sku}.xlsx`,rows,[{label:'Campo',get:r=>r.campo},{label:'Valor',get:r=>r.valor}]);
};
$('#btnExportSkuPubs').onclick = ()=>{
  const sku=state.selectedSkuDetail; if(!sku){toast('Seleccioná un SKU primero','error');return;}
  const pubs=computePublicationBreakdown(state.filtered||[],sku); if(!pubs.length){toast('Sin publicaciones','error');return;}
  exportExcel(`publicaciones_${sku}.xlsx`,pubs,[{label:'MLA',get:r=>r.mla},{label:'Título',get:r=>r.titulo},{label:'Variantes',get:r=>r.variantes},{label:'Ventas',get:r=>r.ventas},{label:'Unidades',get:r=>r.unidades},{label:'VVP total',get:r=>r.vvp_total},{label:'VVP unit.',get:r=>r.vvp_unit},{label:'Precio prom.',get:r=>r.precio_prom},{label:'Precio mín.',get:r=>r.precio_min},{label:'Precio máx.',get:r=>r.precio_max},{label:'Cargo ML total',get:r=>r.cargo_total},{label:'Ing. envío',get:r=>r.ing_envio_total},{label:'Costo envío',get:r=>r.costo_envio_total},{label:'Descuentos',get:r=>r.desc_total},{label:'Anulaciones',get:r=>r.anul_total},{label:'Neto total',get:r=>r.neto_total},{label:'Neto unit.',get:r=>r.neto_unit},{label:'Dif abs',get:r=>r.dif_abs},{label:'Dif %',get:r=>r.dif_pct},{label:'Modalidad dom.',get:r=>r.modalidad_dom},{label:'Provincia dom.',get:r=>r.provincia_dom},{label:'Con publi',get:r=>r.pub_count},{label:'% publi',get:r=>r.pub_pct},{label:'Con cuotas',get:r=>r.cuotas_count},{label:'% cuotas',get:r=>r.cuotas_pct},{label:'Estado',get:r=>r.estado}]);
};
$('#btnExportSkuRows').onclick = ()=>{
  const sku=state.selectedSkuDetail; if(!sku){toast('Seleccioná un SKU primero','error');return;}
  const rows=(state.filtered||[]).filter(r=>r._sku_padre===sku); if(!rows.length){toast('Sin filas','error');return;}
  exportExcel(`ventas_${sku}.xlsx`,rows,[{label:'Fecha',get:r=>r._fecha?r._fecha.toISOString().slice(0,10):''},{label:'# Venta',get:r=>r.num_venta},{label:'MLA',get:r=>r.publicacion},{label:'Título',get:r=>r.titulo},{label:'SKU vendido',get:r=>r._sku_raw},{label:'SKU padre',get:r=>r._sku_padre},{label:'Variante',get:r=>r.variante},{label:'Unidades',get:r=>r._unidades},{label:'Precio unit.',get:r=>r._precio_unit},{label:'Ing. productos',get:r=>r._ing_prod},{label:'Cargo ML',get:r=>r._cargo},{label:'Ing. envío',get:r=>r._ing_envio},{label:'Costo envío',get:r=>r._costo_envio},{label:'Descuentos',get:r=>r._desc},{label:'Anulaciones',get:r=>r._anul},{label:'Total (neto)',get:r=>r._total},{label:'Modalidad',get:r=>r.forma_entrega},{label:'Provincia',get:r=>r.provincia},{label:'Ciudad',get:r=>r.ciudad},{label:'Publicidad',get:r=>r._publicidad?'SI':'NO'},{label:'Cuotas',get:r=>/s[ií]/i.test(String(r.cuotas||''))?'SI':'NO'},{label:'Estado venta',get:r=>r.estado_venta},{label:'Precio neto objetivo',get:r=>r._precio_neto_obj}]);
};
$('#btnExportProv').onclick = ()=>exportCSV('provincias.csv',provData,[{label:'Provincia',get:r=>r.provincia},{label:'Envíos',get:r=>r.envios},{label:'Unidades',get:r=>r.unidades},{label:'Facturación',get:r=>r.facturacion},{label:'Neto',get:r=>r.neto},{label:'% envíos',get:r=>r.pct}]);
$('#btnExportCity').onclick = ()=>exportCSV('ciudades.csv',cityData,[{label:'Ciudad',get:r=>r.ciudad},{label:'Provincia',get:r=>r.provincia},{label:'Envíos',get:r=>r.envios},{label:'Unidades',get:r=>r.unidades},{label:'Facturación',get:r=>r.facturacion},{label:'Neto',get:r=>r.neto}]);
$('#btnExportSinMatch').onclick = ()=>{
  const rows=(state.filtered||[]).filter(r=>r._sku_raw && !r._sku_padre); const agg=new Map();
  rows.forEach(r=>{ const k=r._sku_raw; let g=agg.get(k); if(!g){g={sku:k,apariciones:0,unidades:0,neto:0,ejemplo:r.titulo||''}; agg.set(k,g);} g.apariciones+=1; g.unidades+=r._unidades; g.neto+=r._total; });
  exportCSV('sin_match.csv',[...agg.values()],[{label:'SKU original',get:r=>r.sku},{label:'Apariciones',get:r=>r.apariciones},{label:'Unidades',get:r=>r.unidades},{label:'Neto',get:r=>r.neto},{label:'Ejemplo',get:r=>r.ejemplo}]);
};
$('#btnExportAudit').onclick = ()=>{
  const rows=state.filtered||[]; const m=new Map();
  rows.forEach(r=>{ const k=r._sku_raw||'(sin sku)'; let g=m.get(k); if(!g){g={sku_original:k,sku_padre:r._sku_padre||'',tipo:r._match_kind||'sin_match',precio:r._precio_neto_obj,apariciones:0,observacion:describeObs(r)};m.set(k,g);} g.apariciones+=1; });
  exportCSV('auditoria.csv',[...m.values()],[{label:'SKU original',get:r=>r.sku_original},{label:'SKU padre',get:r=>r.sku_padre},{label:'Tipo match',get:r=>r.tipo},{label:'Precio neto',get:r=>r.precio},{label:'Apariciones',get:r=>r.apariciones},{label:'Observación',get:r=>r.observacion}]);
};
$('#btnExportPriceList').onclick = ()=>{
  if(!state.priceList) return;
  const items=[];
  state.priceList.raw.forEach(p=>{ items.push({sku:p.sku_padre,parent:p.sku_padre,modelo:p.modelo,cat:p.categoria,precio:p.precio_neto,val:p.valido}); p.variantes.forEach(v=>items.push({sku:v,parent:p.sku_padre,modelo:p.modelo,cat:p.categoria,precio:p.precio_neto,val:p.valido})); });
  exportCSV('lista_precios_expandida.csv',items,[{label:'SKU',get:r=>r.sku},{label:'SKU padre',get:r=>r.parent},{label:'Modelo',get:r=>r.modelo},{label:'Categoría',get:r=>r.cat},{label:'Precio neto',get:r=>r.precio},{label:'Válido',get:r=>r.val?'SI':'NO'}]);
};

$('#btnExportCalidad').onclick = ()=>{
  if(!calidadData.length){ toast('No hay datos para exportar','error'); return; }
  exportCSV('calidad_devoluciones.csv', calidadData, [
    {label:'Fecha', get:r=>r._fecha?r._fecha.toISOString().slice(0,10):''},
    {label:'# Venta', get:r=>r.num_venta},
    {label:'Estado exacto', get:r=>r.estado_venta},
    {label:'SKU Original', get:r=>r._sku_raw},
    {label:'SKU Padre', get:r=>r._sku_padre},
    {label:'Unidades', get:r=>r._unidades},
    {label:'Facturación Perdida', get:r=>(r._ing_prod || r._precio_unit*(r._unidades||1) || 0)}
  ]);
};

// ---------- FIREBASE BUTTONS ----------
$('#btnFbConnect').onclick = ()=>{
  const raw=($('#fbConfigInput').value||'').trim();
  if(!raw){ toast('Pegá la configuración de Firebase primero','error'); return; }
  try{
    let cfg;
    try { cfg = JSON.parse(raw); } 
    catch(err) { cfg = new Function('return ' + raw)(); }
    if(initFirebase(cfg)) toast('Conectando a Firebase...','');
  }catch(e){ toast('Configuración inválida: '+e.message,'error'); }
};

$('#btnFbDisconnect').onclick = ()=>{
  if(!confirm('¿Eliminar la configuración de Firebase guardada en este navegador?')) return;
  localStorage.removeItem(LS_FB);
  toast('Configuración eliminada. Recargá la página para desconectar.','ok');
};

// ---------- AUTH EVENTOS ----------
$('#btnLoginSubmit').onclick = async ()=>{
  const email = $('#loginEmail').value.trim();
  const pass = $('#loginPassword').value;
  const err = $('#loginError');
  if(!email || !pass){ err.textContent='Completá ambos campos'; err.style.display='block'; return; }
  try {
    $('#btnLoginSubmit').disabled = true;
    err.style.display='none';
    await _auth.signInWithEmailAndPassword(email, pass);
  } catch(e) {
    err.textContent = 'Credenciales inválidas. Verifica tu correo y contraseña.';
    err.style.display = 'block';
    $('#btnLoginSubmit').disabled = false;
  }
};
$('#btnLogout').onclick = ()=>{ if(_auth) _auth.signOut(); };

// ---------- INIT ----------
loadCfg();
loadPriceListFromStorage();  // carga rápida desde localStorage mientras Firebase conecta
renderHistory();
setFbStatus('none');

// Configuración fija de Firebase (Auto-conexión para todos)
const HARDCODED_FB_CFG = {
  apiKey: "AIzaSyCINoebeHHi_pLkihX1A1NIz09CzICLR7A",
  authDomain: "ml-analyzer-4e633.firebaseapp.com",
  databaseURL: "https://ml-analyzer-4e633-default-rtdb.firebaseio.com",
  projectId: "ml-analyzer-4e633",
  storageBucket: "ml-analyzer-4e633.firebasestorage.app",
  messagingSenderId: "800257701972",
  appId: "1:800257701972:web:73e3827622dd6bd15fd5c1"
};

try {
  const textarea = $('#fbConfigInput');
  if(textarea) textarea.value = JSON.stringify(HARDCODED_FB_CFG, null, 2);
  initFirebase(HARDCODED_FB_CFG);
} catch(e) {
  console.error('Error auto-conectando Firebase:', e);
}
