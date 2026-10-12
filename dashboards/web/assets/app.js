// Page logic: filters, measures (same definitions as dashboards/measures.dax) and rendering.
// The site is in French; the data file keeps the warehouse's values, translated here.
(function () {
  'use strict';

  const D = window.DASHBOARD_DATA;
  const C = window.Charts;
  const $ = (s, r = document) => r.querySelector(s);
  const $$ = (s, r = document) => [...r.querySelectorAll(s)];

  function el(tag, cls, text) {
    const n = document.createElement(tag);
    if (cls) n.className = cls;
    if (text != null) n.textContent = text;
    return n;
  }
  // Text with bold runs: rich(node, ['plain ', {b: 'bold'}, ' plain'])
  function rich(node, parts) {
    node.replaceChildren(...parts.map((p) => (typeof p === 'string' ? document.createTextNode(p) : el('b', null, p.b))));
  }
  const cap = (s) => s.charAt(0).toUpperCase() + s.slice(1);

  // ---------- French formats
  const NNBSP = ' ', NBSP = ' ';
  const FR = d3.formatLocale({ decimal: ',', thousands: NNBSP, grouping: [3], currency: ['', NBSP + '€'], percent: NNBSP + '%', minus: '−' });
  const TL = d3.timeFormatLocale({
    dateTime: '%A %e %B %Y à %X', date: '%d/%m/%Y', time: '%H:%M:%S', periods: ['', ''],
    days: ['dimanche', 'lundi', 'mardi', 'mercredi', 'jeudi', 'vendredi', 'samedi'],
    shortDays: ['dim.', 'lun.', 'mar.', 'mer.', 'jeu.', 'ven.', 'sam.'],
    months: ['janvier', 'février', 'mars', 'avril', 'mai', 'juin', 'juillet', 'août', 'septembre', 'octobre', 'novembre', 'décembre'],
    shortMonths: ['janv.', 'févr.', 'mars', 'avr.', 'mai', 'juin', 'juil.', 'août', 'sept.', 'oct.', 'nov.', 'déc.'],
  });
  const eur0 = (v) => FR.format(',.0f')(v) + NBSP + '€';
  const eur2 = (v) => FR.format(',.2f')(v) + NBSP + '€';
  const int = FR.format(',');
  const pct1 = FR.format('.1%');
  const pct2 = FR.format('.2%');
  const dec1 = FR.format('.1f');
  const sig3 = FR.format('.3~');
  // 18,2 M€, 808 k€, 120 €
  const eurShort = (v) => (Math.abs(v) >= 1e6 ? sig3(v / 1e6) + NBSP + 'M€' : Math.abs(v) >= 1e3 ? sig3(v / 1e3) + NBSP + 'k€' : FR.format('.0f')(v) + NBSP + '€');
  const intShort = (v) => (Math.abs(v) >= 1e6 ? sig3(v / 1e6) + NBSP + 'M' : Math.abs(v) >= 1e3 ? sig3(v / 1e3) + NBSP + 'k' : FR.format('.0f')(v));
  const pctAxis = FR.format('.0%');
  const safe = (f) => (v) => (v == null || !Number.isFinite(v) ? '—' : f(v));

  // Dates: "1er oct. 2025", "29 nov. 2025"
  const dayNum = (d) => (d.getUTCDate() === 1 ? '1er' : String(d.getUTCDate()));
  const fmtDay = (d) => `${dayNum(d)} ${TL.utcFormat('%b %Y')(d)}`;
  const fmtDayLong = (d) => cap(`${TL.utcFormat('%A')(d)} ${dayNum(d)} ${TL.utcFormat('%B %Y')(d)}`);
  const fmtMonth = (d) => cap(TL.utcFormat('%B %Y')(d));
  const fmtMonthShort = TL.utcFormat('%b %y');
  const fmtMonthYear = TL.utcFormat('%b %Y');
  const fmtDayMonth = (d) => `${dayNum(d)} ${TL.utcFormat('%b')(d)}`;

  // ---------- warehouse values in French
  const STATUS_LABELS = { created: 'Créée', paid: 'Payée', shipped: 'Expédiée', delivered: 'Livrée', returned: 'Retournée', cancelled: 'Annulée' };
  const PAYMENT_LABELS = { card: 'Carte bancaire', paypal: 'PayPal', apple_pay: 'Apple Pay', installments: 'En plusieurs fois', bank_transfer: 'Virement' };
  const SEGMENT_LABELS = { Loyal: 'Fidèles', Repeat: 'Réguliers', 'One-time': 'Ponctuels', 'No purchase': 'Sans achat' };
  const EVENT_LABELS = { 'Black Friday': 'Black Friday', 'Winter sales': "Soldes d'hiver", 'Summer sales': "Soldes d'été" };
  const DIMENSION_LABELS = { completeness: 'Complétude', consistency: 'Cohérence', freshness: 'Fraîcheur', uniqueness: 'Unicité', validity: 'Validité', volume: 'Volume' };
  const SEVERITY_LABELS = { error: 'Bloquant', warning: 'Avertissement' };
  const CHECK_STATUS_LABELS = { passed: 'Réussi', warning: 'Avertissement', failed: 'Échoué' };
  const REASON_LABELS = { malformed_json: 'JSON mal formé', orphan_status_event: 'Statut sans commande', unknown_product: 'Produit inconnu', invalid_quantity: 'Quantité invalide' };
  const SOURCE_LABELS = { order_events: 'Événements de commande', marketplace_orders: 'Commandes marketplace' };
  const CATEGORY_LABELS = { Unknown: 'Inconnue' };
  const category = (i) => CATEGORY_LABELS[D.categories[i]] || D.categories[i];
  const pctText = (s) => FR.format('.1f')(Number(s)) + NNBSP + '%';
  const DESCRIPTIONS = {
    'an order event was ingested in the last hour': "un événement de commande a été ingéré dans la dernière heure",
    "an order's total is its net amount plus shipping": "le total d'une commande est son montant net plus la livraison",
    'customers were loaded from the API in the last 26 hours': "les clients ont été chargés depuis l'API dans les dernières 26 heures",
    'each customer has exactly one current version': 'chaque client a exactement une version courante',
    'each product has exactly one current version': 'chaque produit a exactement une version courante',
    'email looks like an email address': 'email a la forme d’une adresse e-mail',
    'every line belongs to an order of gold.orders': 'chaque ligne appartient à une commande de gold.orders',
    'every line sells a product of gold.products': 'chaque ligne vend un produit de gold.products',
    'gold holds orders from today or yesterday': "gold contient des commandes d'aujourd'hui ou d'hier",
    'gross amount is quantity times unit price': 'le montant brut est la quantité multipliée par le prix unitaire',
    'net amount is gross amount minus the discount': 'le montant net est le montant brut moins la remise',
    'no order is dated in the future': "aucune commande n'est datée dans le futur",
    'order_id looks like a web shop order number': 'order_id a la forme d’un numéro de commande de la boutique web',
    'postal_code looks like a five-digit postal code': 'postal_code a la forme d’un code postal à cinq chiffres',
    'the computed total matches the total declared by the shop, within one cent per line': 'le total calculé correspond au total déclaré par la boutique, à un centime près par ligne',
    'the table is not empty': "la table n'est pas vide",
    "yesterday's marketplace export has been loaded": "l'export marketplace de la veille a été chargé",
  };
  const DESCRIPTION_RULES = [
    [/^(\S+) is filled on at least ([\d.]+)% of rows$/, (m) => `${m[1]} est renseigné sur au moins ${pctText(m[2])} des lignes`],
    [/^(\S+) is filled$/, (m) => `${m[1]} est renseigné`],
    [/^(\S+) is one of: (.+)$/, (m) => `${m[1]} prend une de ces valeurs : ${m[2]}`],
    [/^(\S+) is never earlier than (\S+)$/, (m) => `${m[1]} n'est jamais antérieur à ${m[2]}`],
    [/^(\S+) is at least (\S+) and at most (\S+)$/, (m) => `${m[1]} est compris entre ${m[2].replace('.', ',')} et ${m[3].replace('.', ',')}`],
    [/^(\S+) is at least (\S+)$/, (m) => `${m[1]} vaut au moins ${m[2].replace('.', ',')}`],
    [/^no two rows share the same (.+)$/, (m) => `deux lignes n'ont jamais le même ${m[1]}`],
    [/^no source has more than ([\d.]+)% of its rows rejected$/, (m) => `aucune source n'a plus de ${m[1]}${NNBSP}% de lignes rejetées`],
  ];
  function describe(text) {
    if (!text) return '';
    if (DESCRIPTIONS[text]) return DESCRIPTIONS[text];
    for (const [re, fn] of DESCRIPTION_RULES) { const m = text.match(re); if (m) return fn(m); }
    return text;
  }
  const DEPARTMENTS = {
    '01': 'Ain', '05': 'Hautes-Alpes', '06': 'Alpes-Maritimes', '13': 'Bouches-du-Rhône', '14': 'Calvados',
    '17': 'Charente-Maritime', '20': 'Corse', '21': "Côte-d'Or", '25': 'Doubs', '29': 'Finistère', '30': 'Gard',
    '31': 'Haute-Garonne', '33': 'Gironde', '34': 'Hérault', '35': 'Ille-et-Vilaine', '37': 'Indre-et-Loire',
    '38': 'Isère', '42': 'Loire', '44': 'Loire-Atlantique', '45': 'Loiret', '49': 'Maine-et-Loire', '51': 'Marne',
    '54': 'Meurthe-et-Moselle', '57': 'Moselle', '59': 'Nord', '63': 'Puy-de-Dôme', '64': 'Pyrénées-Atlantiques',
    '66': 'Pyrénées-Orientales', '67': 'Bas-Rhin', '69': 'Rhône', '72': 'Sarthe', '74': 'Haute-Savoie', '75': 'Paris',
    '76': 'Seine-Maritime', '80': 'Somme', '83': 'Var', '87': 'Haute-Vienne',
  };
  const SEGMENTS = ['Loyal', 'Repeat', 'One-time', 'No purchase'];

  // ---------- days
  const DAY = 864e5;
  const T0 = Date.parse(D.first_day + 'T00:00:00Z');
  const LAST = Math.round((Date.parse(D.last_day + 'T00:00:00Z') - T0) / DAY);
  const N = LAST + 1;
  const dateOf = (i) => new Date(T0 + i * DAY);
  const isoOf = (i) => dateOf(i).toISOString().slice(0, 10);
  const indexOf = (iso) => Math.round((Date.parse(iso + 'T00:00:00Z') - T0) / DAY);

  // Sales events as date ranges, for the shaded bands of the daily chart.
  const EVENTS = [];
  (D.events || []).filter((e) => e.sales_event).forEach((e) => {
    const label = EVENT_LABELS[e.sales_event] || e.sales_event;
    const prev = EVENTS[EVENTS.length - 1];
    if (prev && prev.label === label && prev.toDay === e.day - 1) { prev.toDay = e.day; prev.to = dateOf(e.day); }
    else EVENTS.push({ label, fromDay: e.day, toDay: e.day, from: dateOf(e.day), to: dateOf(e.day) });
  });
  const eventOn = (i) => EVENTS.find((e) => i >= e.fromDay && i <= e.toDay);

  // ---------- state
  const PAGES = ['sales', 'customers', 'operations', 'quality', 'project'];
  const PRESETS = [
    { id: '30d', label: '30 jours', days: 30 },
    { id: '90d', label: '90 jours', days: 90 },
    { id: '6m', label: '6 mois', months: 6 },
    { id: 'all', label: 'Tout' },
  ];
  const state = { page: 'sales', preset: 'all', from: 0, to: LAST, channel: -1, category: -1, segment: 'All', checkStatus: 'all', checkQuery: '' };
  function applyPreset(id) {
    const p = PRESETS.find((x) => x.id === id);
    state.preset = id;
    state.to = LAST;
    if (p.days) state.from = Math.max(0, LAST - p.days + 1);
    else if (p.months) state.from = Math.max(0, Math.round((+d3.utcMonth.offset(dateOf(LAST), -p.months) - T0) / DAY) + 1);
    else state.from = 0;
  }
  const rangeDays = () => state.to - state.from + 1;
  // The period of the same length just before the selected one, when the data covers it.
  const previousRange = () => {
    const L = rangeDays();
    return state.from - L >= 0 ? [state.from - L, state.from - 1] : null;
  };
  const rangeText = () => `${fmtDay(dateOf(state.from))} – ${fmtDay(dateOf(state.to))}`;
  const periodName = () => {
    const p = PRESETS.find((x) => x.id === state.preset);
    return p && p.months ? `les ${p.months} mois précédents` : `les ${rangeDays()} jours précédents`;
  };

  // ---------- sales measures (fct_sales)
  const sum = (arr, a, b) => { let s = 0; for (let i = a; i <= b; i++) s += arr[i]; return s; };
  const salesCache = new Map();
  function salesDaily(ch, cat) {
    const key = ch + '|' + cat;
    if (salesCache.has(key)) return salesCache.get(key);
    const s = {
      net: new Float64Array(N), units: new Float64Array(N), gross: new Float64Array(N),
      disc: new Float64Array(N), orders: new Float64Array(N), present: new Uint8Array(N),
    };
    for (const r of D.sales) {
      if ((ch >= 0 && r[1] !== ch) || (cat >= 0 && r[2] !== cat)) continue;
      const d = r[0];
      s.net[d] += r[3]; s.units[d] += r[4]; s.gross[d] += r[5]; s.disc[d] += r[6];
      if (cat >= 0) s.orders[d] += r[7];
      s.present[d] = 1;
    }
    // Without a category filter, an order holding several categories counts once.
    if (cat < 0) for (const r of D.sales_orders) if (ch < 0 || r[1] === ch) s.orders[r[0]] += r[2];
    salesCache.set(key, s);
    return s;
  }
  function salesTotals(s, a, b) {
    const net = sum(s.net, a, b);
    const orders = sum(s.orders, a, b);
    const gross = sum(s.gross, a, b);
    const disc = sum(s.disc, a, b);
    return {
      net, orders, units: sum(s.units, a, b),
      basket: orders ? net / orders : null,
      discount: gross ? disc / gross : null,
    };
  }
  // Net Sales 7-Day Average: mean over the last 7 days that have sales, blank on a day without.
  function avg7(s, i) {
    if (!s.present[i]) return null;
    let t = 0, n = 0;
    for (let j = Math.max(0, i - 6); j <= i; j++) if (s.present[j]) { t += s.net[j]; n++; }
    return n ? t / n : null;
  }

  // ---------- order measures (fct_orders)
  const opsCache = new Map();
  function opsDaily(ch) {
    if (opsCache.has(ch)) return opsCache.get(ch);
    const k = ['n', 'cancelled', 'returned', 'billed', 'shipping', 'hs', 'hn', 'ds', 'dn'];
    const o = Object.fromEntries(k.map((x) => [x, new Float64Array(N)]));
    for (const r of D.orders) {
      if (ch >= 0 && r[1] !== ch) continue;
      const d = r[0];
      o.n[d] += r[4]; o.cancelled[d] += r[5]; o.returned[d] += r[6]; o.billed[d] += r[7]; o.shipping[d] += r[8];
      o.hs[d] += r[9]; o.hn[d] += r[10]; o.ds[d] += r[11]; o.dn[d] += r[12];
    }
    opsCache.set(ch, o);
    return o;
  }
  function opsTotals(o, a, b) {
    const n = sum(o.n, a, b), c = sum(o.cancelled, a, b), r = sum(o.returned, a, b), billed = sum(o.billed, a, b);
    const hn = sum(o.hn, a, b), dn = sum(o.dn, a, b);
    return {
      placed: n, cancelled: c, returned: r, billed, shipping: sum(o.shipping, a, b),
      cancelRate: n ? c / n : null,
      returnRate: n - c ? r / (n - c) : null,
      aov: n - c ? billed / (n - c) : null,
      hours: hn ? sum(o.hs, a, b) / hn : null,
      days: dn ? sum(o.ds, a, b) / dn : null,
    };
  }

  // ---------- KPI tiles with a change on the previous period and a sparkline
  function buildKpis(container, specs) {
    container.replaceChildren(...specs.map((s) => {
      const k = el('div', 'kpi');
      k.dataset.kpi = s.id;
      const foot = el('div', 'kpi-foot');
      foot.append(el('div', 'delta-wrap'), el('div', 'spark'));
      k.append(el('span', 'kpi-label', s.label), el('span', 'kpi-value', '—'), foot, el('span', 'kpi-def', s.def));
      return k;
    }));
  }
  // buckets: [[a, b], ...] for the sparkline; totals(a, b) -> object read by spec.get
  function drawKpis(container, specs, totals, prev, buckets, sparkColor) {
    const cur = totals(state.from, state.to);
    const before = prev ? totals(prev[0], prev[1]) : null;
    specs.forEach((s) => {
      const k = container.querySelector(`[data-kpi="${s.id}"]`);
      const v = s.get(cur);
      k.querySelector('.kpi-value').textContent = safe(s.fmt)(v);
      const left = k.querySelector('.delta-wrap');
      left.replaceChildren();
      if (before && s.better !== undefined) {
        const p = s.get(before);
        if (v != null && p != null && p !== 0) {
          const change = s.kind === 'pt' ? v - p : (v - p) / Math.abs(p);
          const flat = Math.abs(change) < 0.0005;
          const up = change > 0;
          const good = s.better == null || flat ? 'flat' : (up === (s.better === 'up') ? 'good' : 'bad');
          const txt = s.kind === 'pt' ? `${FR.format('.2f')(Math.abs(change) * 100)}${NBSP}pt` : pct1(Math.abs(change));
          const chip = el('span', 'delta ' + good, (flat ? '■ ' : up ? '▲ ' : '▼ ') + txt);
          chip.title = `${up ? 'En hausse' : 'En baisse'} par rapport à ${periodName()}`;
          left.append(chip, el('span', 'delta-note', 'vs ' + periodName()));
        }
      }
      const spark = k.querySelector('.spark');
      if (buckets) C.spark(spark, buckets.map(([a, b]) => s.get(totals(a, b))), sparkColor);
      else spark.replaceChildren();
    });
  }
  // Daily points up to three months, weekly beyond.
  function sparkBuckets() {
    const step = rangeDays() <= 92 ? 1 : 7;
    const out = [];
    for (let a = state.from; a <= state.to; a += step) out.push([a, Math.min(state.to, a + step - 1)]);
    if (step > 1 && out.length > 1 && out[out.length - 1][1] - out[out.length - 1][0] < step - 1) out.pop();
    return out;
  }

  const SALES_KPIS = [
    { id: 'net', label: 'Ventes nettes', fmt: eur0, get: (t) => t.net, def: 'Après remises, hors annulations', better: 'up' },
    { id: 'orders', label: 'Commandes', fmt: int, get: (t) => t.orders, def: 'Commandes non annulées', better: 'up' },
    { id: 'units', label: 'Unités vendues', fmt: int, get: (t) => t.units, def: 'Hors commandes annulées', better: 'up' },
    { id: 'basket', label: 'Panier moyen', fmt: eur2, get: (t) => t.basket, def: 'Ventes nettes par commande, hors livraison', better: 'up' },
    { id: 'discount', label: 'Taux de remise', fmt: pct2, get: (t) => t.discount, def: 'Remises sur ventes brutes', better: null, kind: 'pt' },
  ];
  const OPS_KPIS = [
    { id: 'placed', label: 'Commandes passées', fmt: int, get: (t) => t.placed, def: 'Tous statuts, annulées comprises', better: 'up' },
    { id: 'cancel', label: "Taux d'annulation", fmt: pct2, get: (t) => t.cancelRate, def: 'Annulées parmi les commandes passées', better: 'down', kind: 'pt' },
    { id: 'return', label: 'Taux de retour', fmt: pct2, get: (t) => t.returnRate, def: 'Retournées parmi les non annulées', better: 'down', kind: 'pt' },
    { id: 'hours', label: 'Heures avant expédition', fmt: dec1, get: (t) => t.hours, def: "Moyenne, de la commande à l'expédition", better: 'down' },
    { id: 'days', label: 'Jours avant livraison', fmt: dec1, get: (t) => t.days, def: 'Moyenne, de la commande à la livraison', better: 'down' },
  ];
  const CUSTOMER_KPIS = [
    { id: 'customers', label: 'Clients', fmt: int, get: (t) => t.customers, def: 'Comptes clients' },
    { id: 'buyers', label: 'Acheteurs', fmt: int, get: (t) => t.buyers, def: 'Au moins une commande non annulée' },
    { id: 'repeat', label: 'Taux de réachat', fmt: pct2, get: (t) => (t.buyers ? t.repeat_buyers / t.buyers : null), def: 'Acheteurs avec deux commandes ou plus' },
    { id: 'perbuyer', label: 'Dépense par acheteur', fmt: eur2, get: (t) => (t.buyers ? t.lifetime_spend / t.buyers : null), def: 'Dépense cumulée divisée par les acheteurs' },
    { id: 'spend', label: 'Dépense cumulée', fmt: eur0, get: (t) => t.lifetime_spend, def: 'Facturée, livraison comprise' },
  ];

  // ---------- filters
  function drawFilters() {
    const presets = $('#presets');
    if (!presets.childElementCount) {
      PRESETS.forEach((p) => {
        const b = el('button', null, p.label);
        b.type = 'button';
        b.dataset.preset = p.id;
        b.addEventListener('click', () => { applyPreset(p.id); render(); });
        presets.append(b);
      });
      const from = $('#from'), to = $('#to');
      [from, to].forEach((i) => { i.min = D.first_day; i.max = D.last_day; });
      const onDate = () => {
        if (!from.value || !to.value || !from.validity.valid || !to.validity.valid) return;
        let a = indexOf(from.value), b = indexOf(to.value);
        if (a > b) [a, b] = [b, a];
        state.from = Math.max(0, a); state.to = Math.min(LAST, b); state.preset = 'custom';
        render();
      };
      from.addEventListener('change', onDate);
      to.addEventListener('change', onDate);
      const ch = $('#channel');
      ch.append(new Option('Tous les canaux', '-1'), ...D.channels.map((c, i) => new Option(c, String(i))));
      ch.addEventListener('change', () => { state.channel = +ch.value; render(); });
      const cat = $('#category');
      cat.append(new Option('Toutes les catégories', '-1'), ...D.categories.map((c, i) => new Option(category(i), String(i))));
      cat.addEventListener('change', () => { state.category = +cat.value; render(); });
      const seg = $('#segments');
      ['All', ...SEGMENTS].forEach((s) => {
        const b = el('button', null, s === 'All' ? 'Tous' : SEGMENT_LABELS[s]);
        b.type = 'button';
        b.dataset.segment = s;
        b.addEventListener('click', () => { state.segment = s; render(); });
        seg.append(b);
      });
      $('#reset').addEventListener('click', () => {
        applyPreset('all'); state.channel = -1; state.category = -1; state.segment = 'All';
        render();
      });
    }
    $$('#presets button').forEach((b) => b.setAttribute('aria-pressed', String(b.dataset.preset === state.preset)));
    $$('#segments button').forEach((b) => b.setAttribute('aria-pressed', String(b.dataset.segment === state.segment)));
    const from = $('#from'), to = $('#to');
    if (document.activeElement !== from) from.value = isoOf(state.from);
    if (document.activeElement !== to) to.value = isoOf(state.to);
    $('#channel').value = String(state.channel);
    $('#category').value = String(state.category);
    $$('[data-for]', $('#filters')).forEach((n) => { n.hidden = !n.dataset.for.split(' ').includes(state.page); });
    $('#filters').hidden = !['sales', 'operations', 'customers'].includes(state.page);
  }
  const toggleChannel = (i) => { state.channel = state.channel === i ? -1 : i; render(); };
  const toggleCategory = (i) => { state.category = state.category === i ? -1 : i; render(); };
  const toggleSegment = (s) => { state.segment = state.segment === s ? 'All' : s; render(); };

  function legend(node, items) {
    node.replaceChildren(...items.map((it) => {
      const s = el('span');
      const sw = el('i', 'sw' + (it.kind ? ' ' + it.kind : ''));
      if (it.color) sw.style.background = it.color;
      s.append(sw, document.createTextNode(it.name));
      return s;
    }));
  }
  function cellBar(value, max, text, color, compact) {
    const wrap = el('div', 'cellbar' + (compact ? ' compact' : ''));
    const track = el('span', 'track');
    const fill = el('span', 'fill');
    fill.style.width = (max ? Math.max(0, Math.min(1, value / max)) * 100 : 0) + '%';
    if (color) fill.style.background = color;
    track.append(fill);
    wrap.append(track, el('span', 'num', text));
    return wrap;
  }
  function td(content, cls) {
    const c = el('td', cls || null);
    if (content instanceof Node) c.append(content); else c.textContent = content;
    return c;
  }
  function emptyRow(tbody, cols, text) {
    const tr = el('tr');
    tr.append(td(text, 'dim'));
    tr.firstChild.colSpan = cols;
    tbody.replaceChildren(tr);
  }
  function clickableRow(tr, onSelect) {
    tr.tabIndex = 0;
    tr.addEventListener('click', onSelect);
    tr.addEventListener('keydown', (ev) => { if (ev.key === 'Enter' || ev.key === ' ') { ev.preventDefault(); onSelect(); } });
  }

  // ---------- Sales page
  buildKpis($('#sales-kpis'), SALES_KPIS);
  function drawSales() {
    const { from: a, to: b, channel: ch, category: cat } = state;
    const s = salesDaily(ch, cat);
    const t = salesTotals(s, a, b);
    drawKpis($('#sales-kpis'), SALES_KPIS, (x, y) => salesTotals(s, x, y), previousRange(), sparkBuckets(), C.color(0));

    // takeaway: total, and the best day of the period
    let best = -1;
    for (let i = a; i <= b; i++) if (s.present[i] && (best < 0 || s.net[i] > s.net[best])) best = i;
    const ev = best >= 0 ? eventOn(best) : null;
    if (t.orders) {
      rich($('#sales-takeaway'), [
        { b: eur0(t.net) }, ' de ventes nettes sur ', { b: int(t.orders) }, ' commandes. Meilleur jour : ',
        { b: fmtDay(dateOf(best)) }, ` (${eur0(s.net[best])}${ev ? ', ' + ev.label : ''}).`,
      ]);
    } else $('#sales-takeaway').textContent = 'Aucune vente pour cette sélection.';
    $('#sales-scope').textContent = [rangeText(), `${int(rangeDays())} jours`,
      ch < 0 ? 'Tous les canaux' : D.channels[ch], cat < 0 ? 'Toutes les catégories' : category(cat)].join(' · ');

    // daily line
    legend($('#lg-daily'), [
      { name: 'Ventes nettes', color: C.color(0), kind: 'line' },
      { name: 'Moyenne sur 7 jours', color: C.color(1), kind: 'line' },
      { name: 'Temps fort commercial', kind: 'band' },
    ]);
    const days = d3.range(a, b + 1);
    C.line($('#ch-daily'), {
      label: 'Ventes nettes par jour et moyenne sur 7 jours', height: 280, yFmt: eurShort, valFmt: eur0, tipX: fmtDayLong,
      area: true, bands: EVENTS.filter((e) => e.toDay >= a && e.fromDay <= b),
      xTickFmt: rangeDays() > 120 ? fmtMonthYear : fmtDayMonth,
      series: [
        { name: 'Ventes nettes', color: C.color(0), width: 1.5, values: days.map((i) => ({ x: dateOf(i), y: s.present[i] ? s.net[i] : null })) },
        { name: 'Moyenne sur 7 jours', color: C.color(1), width: 2.5, values: days.map((i) => ({ x: dateOf(i), y: avg7(s, i) })) },
      ],
    });

    // months, partial ones lighter
    const months = d3.rollups(days, (v) => ({ net: d3.sum(v, (i) => s.net[i]), n: v.length }), (i) => isoOf(i).slice(0, 7))
      .sort((x, y) => d3.ascending(x[0], y[0]));
    C.columns($('#ch-monthly'), {
      label: 'Ventes nettes par mois', yFmt: eurShort, valFmt: eur0, valName: 'Ventes nettes',
      data: months.map(([m, v]) => {
        const start = new Date(m + '-01T00:00:00Z');
        const len = d3.utcDay.count(start, d3.utcMonth.offset(start, 1));
        const partial = v.n < len;
        return {
          key: m, label: fmtMonthShort(start), title: fmtMonth(start), value: v.net, partial,
          tip: partial ? [{ name: `Mois partiel : ${v.n} jours sur ${len}`, note: true }] : null,
        };
      }),
    });

    // channel mix
    const chData = D.channels.map((c, i) => ({ key: i, label: c, value: salesTotals(salesDaily(i, cat), a, b).net, color: C.color(i) }));
    const chTotal = d3.sum(chData, (d) => d.value);
    C.donut($('#ch-channel'), {
      label: 'Ventes nettes par canal', data: chData, selected: ch < 0 ? null : ch, onClick: (d) => toggleChannel(d.key),
      valFmt: eur0, valName: 'Ventes nettes', shareFmt: pct1, centerValue: eurShort(chTotal), centerLabel: 'ventes nettes', height: 250,
    });
    const mix = $('#mix-channel');
    mix.replaceChildren(...chData.flatMap((d) => {
      const sw = el('i', 'sw'); sw.style.background = ch >= 0 && ch !== d.key ? C.css('--axis') : d.color;
      const name = el('span', null, d.label);
      name.style.cursor = 'pointer';
      name.addEventListener('click', () => toggleChannel(d.key));
      return [sw, name, el('span', 'r', eur0(d.value)), el('span', 'm', chTotal ? pct1(d.value / chTotal) : '—')];
    }));

    // categories
    const catData = D.categories.map((c, i) => ({ key: i, label: category(i), value: salesTotals(salesDaily(ch, i), a, b).net }))
      .filter((d) => d.value > 0 || d.key === cat).sort((x, y) => y.value - x.value);
    C.bars($('#ch-category'), {
      label: 'Ventes nettes par catégorie', data: catData, valFmt: eur0, valShort: eurShort, valName: 'Ventes nettes',
      selected: cat < 0 ? null : cat, onClick: (d) => toggleCategory(d.key),
    });

    // top products
    const net = new Float64Array(D.products.length), units = new Float64Array(D.products.length);
    for (const r of D.product_sales) {
      if (r[0] < a || r[0] > b || (ch >= 0 && r[1] !== ch) || (cat >= 0 && D.products[r[2]][1] !== cat)) continue;
      net[r[2]] += r[3]; units[r[2]] += r[4];
    }
    const top = d3.range(D.products.length).filter((p) => units[p] > 0).sort((x, y) => net[y] - net[x]).slice(0, 10);
    const tb = $('#tb-products tbody');
    if (!top.length) emptyRow(tb, 4, 'Aucun produit vendu pour cette sélection.');
    else {
      const max = net[top[0]];
      tb.replaceChildren(...top.map((p, i) => {
        const tr = el('tr');
        const name = el('div', 'product');
        name.append(el('span', null, D.products[p][0]), el('span', 'tag', category(D.products[p][1])));
        tr.append(td(String(i + 1), 'rank'), td(name), td(cellBar(net[p], max, eur0(net[p])), 'n'), td(int(units[p]), 'n'));
        return tr;
      }));
    }
    const tr = el('tr', 'total');
    tr.append(td(''), td('Tous les produits'), td(eur0(t.net), 'n'), td(int(t.units), 'n'));
    $('#tb-products tfoot').replaceChildren(tr);
  }

  // ---------- Customers page
  buildKpis($('#customers-kpis'), CUSTOMER_KPIS);
  function drawCustomers() {
    const segs = Object.fromEntries(D.customers.segments.map((s) => [s.segment, s]));
    const cur = segs[state.segment] || segs.All;
    CUSTOMER_KPIS.forEach((s) => {
      $(`#customers-kpis [data-kpi="${s.id}"] .kpi-value`).textContent = safe(s.fmt)(s.get(cur));
    });
    const all = segs.All, loyal = segs.Loyal;
    rich($('#customers-takeaway'), [
      'Les clients fidèles représentent ', { b: pct1(loyal.customers / all.customers) }, ' des comptes, mais ',
      { b: pct1(loyal.lifetime_spend / all.lifetime_spend) }, ' de la dépense cumulée.',
    ]);

    // segments table
    const tb = $('#tb-segments tbody');
    const maxC = d3.max(SEGMENTS, (s) => segs[s].customers / all.customers);
    const maxS = d3.max(SEGMENTS, (s) => segs[s].lifetime_spend / all.lifetime_spend);
    tb.replaceChildren(...SEGMENTS.map((name, i) => {
      const s = segs[name];
      const tr = el('tr', 'clickable' + (state.segment === name ? ' selected' : ''));
      clickableRow(tr, () => toggleSegment(name));
      const label = el('span', 'status');
      const sw = el('i', 'sw'); sw.style.background = C.color(i);
      label.append(sw, document.createTextNode(SEGMENT_LABELS[name]));
      label.style.color = 'var(--fg)';
      tr.append(
        td(label), td(int(s.customers), 'n'),
        td(cellBar(s.customers / all.customers, maxC, pct1(s.customers / all.customers), C.color(i), true), 'n'),
        td(eur0(s.lifetime_spend), 'n'),
        td(cellBar(s.lifetime_spend / all.lifetime_spend, maxS, pct1(s.lifetime_spend / all.lifetime_spend), C.color(i), true), 'n'),
        td(s.buyers ? eur2(s.lifetime_spend / s.buyers) : '—', 'n'),
      );
      return tr;
    }));

    const segColor = state.segment === 'All' ? C.color(0) : C.color(SEGMENTS.indexOf(state.segment));
    const rec = D.customers.recency.filter((r) => r.segment === state.segment).sort((x, y) => x.days_from - y.days_from);
    C.columns($('#ch-recency'), {
      label: 'Acheteurs selon les jours depuis la dernière commande', yFmt: intShort, valFmt: int, valName: 'Acheteurs', color: segColor,
      empty: "Ces clients n'ont jamais commandé.",
      data: rec.map((r) => ({ key: String(r.days_from), label: String(r.days_from), title: `De ${r.days_from} à ${r.days_from + 29} jours`, value: r.customers })),
    });

    const deps = D.customers.departments.filter((r) => r.segment === state.segment);
    C.bars($('#ch-departments'), {
      label: 'Ventes nettes par département du client', valFmt: eur0, valShort: eurShort, valName: 'Ventes nettes', rowH: 38,
      empty: "Ces clients n'ont pas de ventes.",
      data: deps.map((r) => ({ key: r.department, label: `${r.department} ${DEPARTMENTS[r.department] || ''}`.trim(), value: r.net_sales, color: segColor })),
    });

    const tc = $('#tb-customers tbody');
    const top = D.customers.top.filter((r) => r.segment === state.segment);
    if (!top.length) emptyRow(tc, 7, "Ces clients n'ont encore rien acheté.");
    else {
      tc.replaceChildren(...top.map((r) => {
        const tr = el('tr');
        const who = el('div');
        who.append(el('div', null, r.name), el('span', 'dim mono', r.id));
        tr.append(td(String(r.rank), 'rank'), td(who), td(r.city), td(el('span', 'tag', SEGMENT_LABELS[r.customer_segment] || r.customer_segment)),
          td(int(r.orders), 'n'), td(eur2(r.total_spent), 'n'), td(fmtDay(new Date(r.last_order + 'T00:00:00Z')), 'n dim'));
        return tr;
      }));
    }
  }

  // ---------- Operations page
  buildKpis($('#operations-kpis'), OPS_KPIS);
  function drawOperations() {
    const { from: a, to: b, channel: ch } = state;
    const o = opsDaily(ch);
    const t = opsTotals(o, a, b);
    drawKpis($('#operations-kpis'), OPS_KPIS, (x, y) => opsTotals(o, x, y), previousRange(), sparkBuckets(), C.color(0));

    const byStatus = new Float64Array(D.statuses.length), byPay = new Float64Array(D.payments.length);
    for (const r of D.orders) {
      if (r[0] < a || r[0] > b || (ch >= 0 && r[1] !== ch)) continue;
      byStatus[r[2]] += r[4]; byPay[r[3]] += r[4];
    }
    const delivered = byStatus[D.statuses.indexOf('delivered')] || 0;
    if (t.placed) {
      rich($('#operations-takeaway'), [
        { b: pct1(delivered / t.placed) }, ' des ', { b: int(t.placed) }, ' commandes passées sont livrées, en ',
        { b: dec1(t.days) + NBSP + 'jours' }, ' en moyenne. ', { b: pct2(t.cancelRate) }, ' ont été annulées.',
      ]);
    } else $('#operations-takeaway').textContent = 'Aucune commande pour cette sélection.';
    $('#operations-scope').textContent = [rangeText(), `${int(rangeDays())} jours`, ch < 0 ? 'Tous les canaux' : D.channels[ch]].join(' · ');

    const share = (n) => (t.placed ? [{ name: 'Part', value: pct1(n / t.placed) }] : null);
    C.bars($('#ch-status'), {
      label: 'Commandes par statut', valFmt: int, valShort: int, valName: 'Commandes',
      data: D.statuses.map((s, i) => ({ key: s, label: STATUS_LABELS[s] || s, value: byStatus[i], tip: share(byStatus[i]) })),
    });
    C.columns($('#ch-payment'), {
      label: 'Commandes par moyen de paiement', yFmt: intShort, valFmt: int, valShort: intShort, valName: 'Commandes', showValues: true,
      data: D.payments.map((p, i) => ({ key: p, label: PAYMENT_LABELS[p] || p, value: byPay[i], tip: share(byPay[i]) })),
    });

    const months = d3.rollups(d3.range(a, b + 1), (v) => opsTotals(o, v[0], v[v.length - 1]), (i) => isoOf(i).slice(0, 7))
      .sort((x, y) => d3.ascending(x[0], y[0]));
    const mx = months.map(([m]) => new Date(m + '-01T00:00:00Z'));
    legend($('#lg-rates'), [{ name: "Taux d'annulation", color: C.color(0), kind: 'line' }, { name: 'Taux de retour', color: C.color(1), kind: 'line' }]);
    C.line($('#ch-rates'), {
      label: "Taux d'annulation et de retour par mois", dots: true, yFmt: pctAxis, valFmt: pct2, tipX: fmtMonth,
      xTicks: mx, xTickFmt: fmtMonthShort, height: 240,
      series: [
        { name: "Taux d'annulation", color: C.color(0), values: months.map(([, v], i) => ({ x: mx[i], y: v.cancelRate })) },
        { name: 'Taux de retour', color: C.color(1), values: months.map(([, v], i) => ({ x: mx[i], y: v.returnRate })) },
      ],
    });
    C.line($('#ch-delivery'), {
      label: 'Délai moyen de livraison par mois', dots: true, yFmt: dec1, valFmt: (v) => dec1(v) + NBSP + 'jours',
      tipX: fmtMonth, xTicks: mx, xTickFmt: fmtMonthShort, height: 240,
      series: [{ name: 'Jours avant livraison', color: C.color(0), values: months.map(([, v], i) => ({ x: mx[i], y: v.days })) }],
    });

    const tb = $('#tb-channels tbody');
    const rows = D.channels.map((c, i) => ({ i, name: c, t: opsTotals(opsDaily(i), a, b) })).filter((r) => ch < 0 || r.i === ch);
    if (ch < 0) rows.push({ i: -1, name: 'Tous les canaux', t: opsTotals(opsDaily(-1), a, b) });
    tb.replaceChildren(...rows.map((r) => {
      const tr = el('tr', r.i < 0 ? 'total' : 'clickable' + (ch === r.i ? ' selected' : ''));
      if (r.i >= 0) clickableRow(tr, () => toggleChannel(r.i));
      const name = el('span', 'status');
      if (r.i >= 0) { const sw = el('i', 'sw'); sw.style.background = C.color(r.i); name.append(sw); }
      name.append(document.createTextNode(r.name));
      name.style.color = 'var(--fg)';
      tr.append(td(name), td(int(r.t.placed), 'n'), td(eur0(r.t.billed), 'n'), td(safe(eur2)(r.t.aov), 'n'), td(eur0(r.t.shipping), 'n'),
        td(safe(pct2)(r.t.cancelRate), 'n'), td(safe(pct2)(r.t.returnRate), 'n'), td(safe(dec1)(r.t.days), 'n'));
      return tr;
    }));
  }

  // ---------- Data quality page
  const ICONS = {
    passed: '<svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true"><path d="M3 8.5l3 3 7-7" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>',
    warning: '<svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true"><path d="M8 2l6.5 11.5h-13z" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linejoin="round"/><path d="M8 6.5v3.2M8 11.6v.1" stroke="currentColor" stroke-width="1.6" stroke-linecap="round"/></svg>',
    failed: '<svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true"><path d="M4 4l8 8M12 4l-8 8" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>',
  };
  function statusNode(status) {
    const s = el('span', 'status ' + status);
    s.insertAdjacentHTML('afterbegin', ICONS[status] || '');
    s.append(document.createTextNode(CHECK_STATUS_LABELS[status] || status));
    return s;
  }
  const QUALITY_KPIS = [
    { id: 'checks', label: 'Contrôles', def: 'Sur les tables bronze, silver et gold' },
    { id: 'passed', label: 'Réussis', def: '' },
    { id: 'warning', label: 'En avertissement', def: "Signalés, l'exécution continue" },
    { id: 'failed', label: 'Échoués', def: "Bloquants : l'exécution s'arrête" },
    { id: 'rejected', label: 'Enregistrements rejetés', def: 'Mis de côté avec leur motif' },
  ];
  buildKpis($('#quality-kpis'), QUALITY_KPIS);
  $$('#quality-kpis .kpi-foot').forEach((n) => n.remove());
  function drawQuality() {
    const q = D.quality;
    const checks = q.checks || [];
    const count = (st) => checks.filter((c) => c.status === st).length;
    const rejected = d3.sum(q.rejects || [], (r) => r.rejected);
    const values = { checks: checks.length, passed: count('passed'), warning: count('warning'), failed: count('failed'), rejected };
    QUALITY_KPIS.forEach((s) => {
      const k = $(`#quality-kpis [data-kpi="${s.id}"]`);
      k.querySelector('.kpi-value').textContent = int(values[s.id]);
      if (s.id === 'passed') k.querySelector('.kpi-def').textContent = checks.length ? `${pct1(values.passed / checks.length)} de réussite` : '';
    });
    const ok = values.failed === 0;
    $('#quality-banner').className = 'banner ' + (ok ? 'ok' : 'ko');
    $('#quality-icon').innerHTML = (ok ? ICONS.passed : ICONS.failed).replace(/14/g, '20');
    $('#quality-title').textContent = ok
      ? (values.warning ? `Aucun échec bloquant : ${values.warning} contrôles en avertissement` : `Les ${values.checks} contrôles sont réussis`)
      : `${values.failed} contrôles bloquants en échec`;
    const at = new Date(q.checked_at);
    $('#quality-sub').textContent = `Dernière exécution du job qualité : ${fmtDay(at)} à ${d3.utcFormat('%H:%M')(at)} UTC · exécution ${q.run_id}`;

    const layers = ['bronze', 'silver', 'gold'].filter((l) => checks.some((c) => c.layer === l));
    const dims = d3.rollups(checks, (v) => v.length, (c) => c.dimension).sort((x, y) => y[1] - x[1]).map((d) => d[0]);
    const dimLabel = (d) => DIMENSION_LABELS[d] || d;
    C.heatmap($('#ch-heatmap'), {
      label: 'Contrôles par couche du lac et dimension qualité', valName: 'Contrôles',
      rows: layers.map(cap), cols: dims.map(dimLabel),
      value: (r, c) => checks.filter((x) => x.layer === r.toLowerCase() && dimLabel(x.dimension) === c).length,
    });

    const sources = [...new Set((q.rejects || []).map((r) => r.source))];
    legend($('#lg-rejects'), sources.map((s, i) => ({ name: SOURCE_LABELS[s] || s, color: C.color(i) })));
    C.bars($('#ch-rejects'), {
      label: 'Enregistrements rejetés par motif', valFmt: int, valShort: int, valName: 'Rejetés',
      data: (q.rejects || []).map((r) => ({
        key: r.source + r.reason, label: REASON_LABELS[r.reason] || r.reason, value: r.rejected,
        color: C.color(sources.indexOf(r.source)), tipName: SOURCE_LABELS[r.source] || r.source, tipColor: true,
      })),
    });

    const seg = $('#check-status');
    const opts = [['all', 'Tous', checks.length], ['passed', 'Réussis', values.passed], ['warning', 'Avertissements', values.warning], ['failed', 'Échoués', values.failed]];
    seg.replaceChildren(...opts.map(([id, label, n]) => {
      const b = el('button', null, `${label} ${n}`);
      b.type = 'button';
      b.setAttribute('aria-pressed', String(state.checkStatus === id));
      b.addEventListener('click', () => { state.checkStatus = id; drawQuality(); });
      return b;
    }));
    drawChecks();
  }
  function drawChecks() {
    const checks = D.quality.checks || [];
    const q = state.checkQuery.trim().toLowerCase();
    const rows = checks.filter((c) => (state.checkStatus === 'all' || c.status === state.checkStatus)
      && (!q || [c.table, c.check, DIMENSION_LABELS[c.dimension], describe(c.description)].join(' ').toLowerCase().includes(q)));
    $('#checks-count').textContent = rows.length === checks.length ? `${checks.length} contrôles, classés par table.` : `${rows.length} contrôles sur ${checks.length}.`;
    const tb = $('#tb-checks tbody');
    if (!rows.length) { emptyRow(tb, 6, 'Aucun contrôle ne correspond.'); return; }
    tb.replaceChildren(...rows.map((c) => {
      const tr = el('tr');
      tr.append(td(statusNode(c.status)), td(c.table, 'mono'), td(c.check, 'mono'), td(DIMENSION_LABELS[c.dimension] || c.dimension),
        td(SEVERITY_LABELS[c.severity] || c.severity, 'dim'), td(describe(c.description), 'desc'));
      return tr;
    }));
  }
  $('#check-search').addEventListener('input', (ev) => { state.checkQuery = ev.target.value; drawChecks(); });

  // ---------- pages, theme, resize
  const DRAW = { sales: drawSales, customers: drawCustomers, operations: drawOperations, quality: drawQuality, project: () => {} };
  function render() {
    drawFilters();
    DRAW[state.page]();
  }
  function showPage(page, push) {
    state.page = PAGES.includes(page) ? page : 'sales';
    $$('.tab').forEach((t) => t.setAttribute('aria-selected', String(t.dataset.page === state.page)));
    PAGES.forEach((p) => { $('#' + p).hidden = p !== state.page; });
    if (push) history.replaceState(null, '', '#' + state.page);
    render();
  }
  $$('.tab').forEach((t) => t.addEventListener('click', () => showPage(t.dataset.page, true)));
  $('.tabs').addEventListener('keydown', (ev) => {
    if (ev.key !== 'ArrowRight' && ev.key !== 'ArrowLeft') return;
    const i = (PAGES.indexOf(state.page) + (ev.key === 'ArrowRight' ? 1 : PAGES.length - 1)) % PAGES.length;
    showPage(PAGES[i], true);
    $('#tab-' + PAGES[i]).focus();
  });
  window.addEventListener('hashchange', () => showPage(location.hash.slice(1), false));

  // Light by default; the button switches to dark and back, and the choice is remembered.
  $('#theme').addEventListener('click', () => {
    const root = document.documentElement;
    root.dataset.theme = root.dataset.theme === 'dark' ? 'light' : 'dark';
    try { localStorage.setItem('theme', root.dataset.theme); } catch (e) { /* storage blocked: the choice lasts for this visit */ }
    render();
  });

  let lastWidth = 0, timer = 0;
  new ResizeObserver(([entry]) => {
    const w = Math.round(entry.contentRect.width);
    if (w === lastWidth) return;
    lastWidth = w;
    clearTimeout(timer);
    timer = setTimeout(render, 120);
  }).observe($('main'));

  $('#fact-checks').textContent = int((D.quality.checks || []).length);
  $('#freshness').textContent = `Données du ${fmtDay(dateOf(0))} au ${fmtDay(dateOf(LAST))} · exportées le ${fmtDay(new Date(D.generated_at))}`;
  applyPreset('all');
  showPage(location.hash.slice(1), false);
})();
