const express = require('express');
const axios = require('axios');

const TOKEN = process.env.HUBSPOT_PRIVATE_APP_TOKEN;
if (!TOKEN) { console.error('HUBSPOT_PRIVATE_APP_TOKEN is required'); process.exit(1); }

const api = axios.create({
  baseURL: 'https://api.hubapi.com',
  headers: { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' },
});
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const app = express();
const PORT = process.env.PORT || 3000;

// ══════════════════════════════════════════
// CONFIGURATION
// ══════════════════════════════════════════
const MONTHLY_COSTS = {
  'Cold Email (Instantly)': 1000,
  'Sales Team (2 reps)':    4000,
  'HubSpot':                 50,
  'Aircall':                100,
};
const PRICING = { basic: 59, pro: 99, team: 249, default: 59 };
const STAGES = [
  { id: 'appointmentscheduled',   label: 'Attempting Contact',      weight: 0.05 },
  { id: 'presentationscheduled',  label: 'Discovery',               weight: 0.15 },
  { id: '2843565802',             label: 'Demo Booked',             weight: 0.35 },
  { id: '2851995329',             label: 'Demo Complete & Closing', weight: 0.60 },
  { id: '2054317800',             label: 'Pause',                   weight: 0.10 },
  { id: '2845034230',             label: 'Nurture',                 weight: 0.10 },
  { id: 'closedlost',             label: 'Closed Lost',             weight: 0.00 },
  { id: 'decisionmakerboughtin',  label: 'Closed as Won',           weight: 1.00 },
];
const DEAL_SOURCES = [
  { value: 'cold_call', label: 'Cold Call' },
  { value: 'cold_email', label: 'Cold Email' },
  { value: 'inbound_signup', label: 'Inbound Signup' },
  { value: 'referral', label: 'Referral' },
];
const CACHE_TTL = 5 * 60 * 1000; // 5 minutes

// ══════════════════════════════════════════
// CACHE
// ══════════════════════════════════════════
let cache = { html: null, data: null, time: 0, loading: false };

// ══════════════════════════════════════════
// HUBSPOT API HELPERS
// ══════════════════════════════════════════
async function withRetry(fn, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try { return await fn(); }
    catch (err) {
      if (err.response?.status === 429 && i < retries - 1) { await sleep((i + 1) * 1500); continue; }
      throw err;
    }
  }
}

async function countContacts(filterGroups) {
  return withRetry(async () => {
    const { data } = await api.post('/crm/v3/objects/contacts/search', { filterGroups, limit: 1 });
    return data.total;
  });
}

async function countByStatus(status) {
  return countContacts([{ filters: [{ propertyName: 'user_status', operator: 'EQ', value: status }] }]);
}

async function countNoStatus() {
  return countContacts([{ filters: [{ propertyName: 'user_status', operator: 'NOT_HAS_PROPERTY' }] }]);
}

async function fetchOwners() {
  try {
    const { data } = await api.get('/crm/v3/owners', { params: { limit: 100 } });
    const map = {};
    for (const o of data.results) map[o.id] = `${o.firstName || ''} ${o.lastName || ''}`.trim() || o.email;
    return map;
  } catch { return {}; }
}

async function fetchAllDeals() {
  const deals = [];
  let after;
  while (true) {
    const params = { limit: 100, properties: 'dealname,dealstage,pipeline,amount,expected_mrr,deal_source,hubspot_owner_id,closedate,createdate,hs_lastmodifieddate' };
    if (after) params.after = after;
    const { data } = await api.get('/crm/v3/objects/deals', { params });
    deals.push(...data.results);
    if (!data.paging?.next?.after) break;
    after = data.paging.next.after;
    await sleep(200);
  }
  return deals;
}

async function fetchPaidCustomers() {
  const results = [];
  let after;
  while (true) {
    const body = {
      filterGroups: [{ filters: [{ propertyName: 'user_status', operator: 'EQ', value: 'paid_customer' }] }],
      properties: ['sammy_subscription_tier', 'createdate', 'firstname', 'lastname'],
      limit: 100,
    };
    if (after) body.after = after;
    const { data } = await withRetry(() => api.post('/crm/v3/objects/contacts/search', body));
    results.push(...data.results);
    if (!data.paging?.next?.after) break;
    after = data.paging.next.after;
    await sleep(300);
  }
  return results;
}

async function fetchEngagements(objectType, daysBack = 30) {
  const since = new Date(Date.now() - daysBack * 86400000).toISOString().split('T')[0];
  const results = [];
  let after;
  try {
    while (true) {
      const body = {
        filterGroups: [{ filters: [{ propertyName: 'hs_createdate', operator: 'GTE', value: since }] }],
        properties: ['hs_createdate', 'hubspot_owner_id'],
        limit: 100,
      };
      if (after) body.after = after;
      const { data } = await withRetry(() => api.post(`/crm/v3/objects/${objectType}/search`, body));
      results.push(...data.results);
      if (!data.paging?.next?.after) break;
      after = data.paging.next.after;
      await sleep(200);
    }
  } catch (err) {
    if (err.response?.status === 403 || err.response?.status === 400) return null;
    throw err;
  }
  return results;
}

// ══════════════════════════════════════════
// DATA FETCHING
// ══════════════════════════════════════════
async function fetchAllData() {
  console.log('[fetch] Starting HubSpot data fetch...');
  const t0 = Date.now();

  const owners = await fetchOwners();
  const deals = await fetchAllDeals();
  await sleep(500);

  const [noStatus, incomplete, activeTrial] = await Promise.all([
    countNoStatus(), countByStatus('incomplete_onboarding'), countByStatus('active_trial'),
  ]);
  await sleep(500);
  const [paid, expired, churned] = await Promise.all([
    countByStatus('paid_customer'), countByStatus('trial_expired'), countByStatus('churned'),
  ]);
  await sleep(500);

  const paidCustomers = await fetchPaidCustomers();
  await sleep(500);

  const [calls, meetings, notes] = await Promise.all([
    fetchEngagements('calls'), fetchEngagements('meetings'), fetchEngagements('notes'),
  ]);
  await sleep(500);

  const actFilters = [{ propertyName: 'user_status', operator: 'EQ', value: 'active_trial' }];
  const [actTotal, actEstimate, actQuoteSent] = await Promise.all([
    countContacts([{ filters: actFilters }]),
    countContacts([{ filters: [...actFilters, { propertyName: 'has_created_estimates', operator: 'EQ', value: 'true' }] }]),
    countContacts([{ filters: [...actFilters, { propertyName: 'estimates_sent', operator: 'GTE', value: '1' }] }]),
  ]);

  console.log(`[fetch] Done in ${((Date.now() - t0) / 1000).toFixed(1)}s — ${deals.length} deals, ${noStatus + incomplete + activeTrial + paid + expired + churned} contacts`);

  return {
    owners, deals,
    funnel: { noStatus, incomplete, activeTrial, paid, expired, churned },
    paidCustomers,
    activity: { calls: calls || [], meetings: meetings || [], notes: notes || [] },
    activation: { total: actTotal, estimateCreated: actEstimate, quoteSent: actQuoteSent, paid },
  };
}

// ══════════════════════════════════════════
// METRIC COMPUTATION
// ══════════════════════════════════════════
function computeMetrics({ owners, deals: allDeals, funnel, paidCustomers, activity, activation }) {
  const now = new Date();
  const totalCosts = Object.values(MONTHLY_COSTS).reduce((s, v) => s + v, 0);
  const stageMap = {};
  for (const s of STAGES) stageMap[s.id] = s;

  // Filter to sales pipeline only (exclude Onboarding & Activation pipeline)
  const deals = allDeals.filter(d => d.properties.pipeline === 'default' || !d.properties.pipeline);

  // ── Today's KPIs ──
  const todayStr = new Date().toLocaleDateString('en-CA', { timeZone: 'Australia/Melbourne' }); // YYYY-MM-DD
  const today = { calls: 0, meetings: 0, notes: 0, byRep: {} };
  const activityTypes = { calls: activity.calls, meetings: activity.meetings, notes: activity.notes };
  for (const [type, records] of Object.entries(activityTypes)) {
    if (!records) continue;
    for (const r of records) {
      const day = (r.properties.hs_createdate || '').split('T')[0];
      if (day === todayStr) {
        today[type]++;
        const oid = r.properties.hubspot_owner_id;
        const name = oid ? (owners[oid] || `Owner ${oid}`) : 'Unassigned';
        if (!today.byRep[name]) today.byRep[name] = { calls: 0, meetings: 0, notes: 0 };
        today.byRep[name][type]++;
      }
    }
  }
  today.total = today.calls + today.meetings + today.notes;

  // Daily average (last 30 days excluding today)
  let totalActivity30d = 0;
  for (const [type, records] of Object.entries(activityTypes)) {
    if (records) totalActivity30d += records.length;
  }
  today.dailyAvg = Math.round(totalActivity30d / 30);

  // ── P&L ──
  let currentMRR = 0;
  const mrrBreakdown = { basic: 0, pro: 0, team: 0, unknown: 0 };
  for (const c of paidCustomers) {
    const tier = c.properties.sammy_subscription_tier;
    const price = PRICING[tier] || PRICING.default;
    currentMRR += price;
    if (PRICING[tier]) mrrBreakdown[tier]++; else mrrBreakdown.unknown++;
  }
  const paidCount = paidCustomers.length;
  const arpu = paidCount > 0 ? currentMRR / paidCount : PRICING.default;
  const monthlyProfit = currentMRR - totalCosts;
  const roi = totalCosts > 0 ? ((monthlyProfit / totalCosts) * 100) : 0;
  const breakEvenCustomers = Math.ceil(totalCosts / arpu);
  const wonDeals = deals.filter(d => d.properties.dealstage === 'decisionmakerboughtin');
  const createDates = deals.map(d => new Date(d.properties.createdate)).filter(d => !isNaN(d));
  const earliest = createDates.length ? Math.min(...createDates) : now.getTime();
  const monthsRunning = Math.max(1, (now.getTime() - earliest) / (30.44 * 86400000));
  const wonPerMonth = wonDeals.length / monthsRunning;
  const cac = wonPerMonth > 0 ? totalCosts / wonPerMonth : null;

  const pnl = { costs: MONTHLY_COSTS, totalCosts, currentMRR, mrrBreakdown, paidCount, arpu: Math.round(arpu), monthlyProfit, roi: Math.round(roi), cac: cac !== null ? Math.round(cac) : null, breakEvenCustomers };

  // ── Pipeline ──
  const pipeline = [];
  let totalPipelineValue = 0, weightedPipelineValue = 0, totalWon = 0, totalLost = 0, wonValue = 0;
  for (const stage of STAGES) {
    const stageDeals = deals.filter(d => d.properties.dealstage === stage.id);
    let value = 0, noOwner = 0;
    for (const d of stageDeals) {
      value += parseFloat(d.properties.amount || d.properties.expected_mrr || '0') || PRICING.default;
      if (!d.properties.hubspot_owner_id) noOwner++;
    }
    const weighted = value * stage.weight;
    pipeline.push({ id: stage.id, label: stage.label, weight: stage.weight, count: stageDeals.length, value: Math.round(value), weightedValue: Math.round(weighted), noOwner });
    if (stage.id === 'decisionmakerboughtin') { totalWon = stageDeals.length; wonValue = value; }
    else if (stage.id === 'closedlost') { totalLost = stageDeals.length; }
    else { totalPipelineValue += value; weightedPipelineValue += weighted; }
  }
  const winRate = (totalWon + totalLost) > 0 ? Math.round((totalWon / (totalWon + totalLost)) * 100) : 0;

  // ── Deal Source ──
  const sourceStats = {};
  for (const s of DEAL_SOURCES) sourceStats[s.value] = { label: s.label, total: 0, won: 0, lost: 0, open: 0, wonMRR: 0, winRate: 0 };
  sourceStats.unknown = { label: 'Unknown', total: 0, won: 0, lost: 0, open: 0, wonMRR: 0, winRate: 0 };
  for (const d of deals) {
    const src = d.properties.deal_source || 'unknown';
    const bucket = sourceStats[src] || sourceStats.unknown;
    bucket.total++;
    const mrr = parseFloat(d.properties.expected_mrr || d.properties.amount || '0') || PRICING.default;
    if (d.properties.dealstage === 'decisionmakerboughtin') { bucket.won++; bucket.wonMRR += mrr; }
    else if (d.properties.dealstage === 'closedlost') { bucket.lost++; }
    else { bucket.open++; }
  }
  for (const s of Object.values(sourceStats)) { s.wonMRR = Math.round(s.wonMRR); s.winRate = (s.won + s.lost) > 0 ? Math.round((s.won / (s.won + s.lost)) * 100) : 0; }

  // ── Rep Performance ──
  const repStats = {};
  for (const d of deals) {
    const oid = d.properties.hubspot_owner_id;
    const name = oid ? (owners[oid] || `Owner ${oid}`) : 'Unassigned';
    if (!repStats[name]) repStats[name] = { name, ownerId: oid, total: 0, won: 0, lost: 0, open: 0, wonMRR: 0, winRate: 0, calls: 0, meetings: 0, notes: 0, wonCycleDays: [] };
    const rep = repStats[name];
    rep.total++;
    const mrr = parseFloat(d.properties.expected_mrr || d.properties.amount || '0') || PRICING.default;
    if (d.properties.dealstage === 'decisionmakerboughtin') {
      rep.won++; rep.wonMRR += mrr;
      const cd = new Date(d.properties.closedate), cr = new Date(d.properties.createdate);
      if (!isNaN(cd) && !isNaN(cr)) rep.wonCycleDays.push(Math.round((cd - cr) / 86400000));
    } else if (d.properties.dealstage === 'closedlost') { rep.lost++; }
    else { rep.open++; }
  }
  for (const [type, records] of Object.entries(activityTypes)) {
    if (!records) continue;
    for (const r of records) {
      const oid = r.properties.hubspot_owner_id;
      const name = oid ? (owners[oid] || `Owner ${oid}`) : 'Unassigned';
      if (!repStats[name]) repStats[name] = { name, ownerId: oid, total: 0, won: 0, lost: 0, open: 0, wonMRR: 0, winRate: 0, calls: 0, meetings: 0, notes: 0, wonCycleDays: [] };
      repStats[name][type]++;
    }
  }
  const reps = Object.values(repStats).map(r => {
    r.wonMRR = Math.round(r.wonMRR);
    r.winRate = (r.won + r.lost) > 0 ? Math.round((r.won / (r.won + r.lost)) * 100) : 0;
    r.avgCycleDays = r.wonCycleDays.length > 0 ? Math.round(r.wonCycleDays.reduce((a, b) => a + b, 0) / r.wonCycleDays.length) : null;
    delete r.wonCycleDays;
    return r;
  }).sort((a, b) => b.wonMRR - a.wonMRR);

  // ── Daily Activity ──
  const dailyMap = {};
  for (let i = 29; i >= 0; i--) { const d = new Date(now.getTime() - i * 86400000); dailyMap[d.toISOString().split('T')[0]] = { calls: 0, meetings: 0, notes: 0 }; }
  for (const [type, records] of Object.entries(activityTypes)) {
    if (!records) continue;
    for (const r of records) { const day = (r.properties.hs_createdate || '').split('T')[0]; if (dailyMap[day]) dailyMap[day][type]++; }
  }
  const daily = Object.entries(dailyMap).map(([date, counts]) => ({ date, ...counts }));
  const actTotals = { calls: 0, meetings: 0, notes: 0 };
  for (const d of daily) { actTotals.calls += d.calls; actTotals.meetings += d.meetings; actTotals.notes += d.notes; }

  // ── Conversion Funnel ──
  const totalContacts = funnel.noStatus + funnel.incomplete + funnel.activeTrial + funnel.paid + funnel.expired + funnel.churned;
  const trialEver = funnel.activeTrial + funnel.paid + funnel.expired + funnel.churned;
  const funnelData = { ...funnel, totalContacts,
    trialToPaidRate: trialEver > 0 ? Math.round((funnel.paid / trialEver) * 100) : 0,
    signupToTrialRate: totalContacts > 0 ? Math.round((trialEver / totalContacts) * 100) : 0,
    overallConversion: totalContacts > 0 ? Math.round((funnel.paid / totalContacts) * 100) : 0,
  };

  // ── Deal Velocity ──
  const wonCycleDays = [];
  for (const d of wonDeals) {
    const cd = new Date(d.properties.closedate), cr = new Date(d.properties.createdate);
    if (!isNaN(cd) && !isNaN(cr)) wonCycleDays.push({ name: d.properties.dealname, days: Math.round((cd - cr) / 86400000), source: d.properties.deal_source || 'unknown', rep: owners[d.properties.hubspot_owner_id] || 'Unassigned' });
  }
  wonCycleDays.sort((a, b) => a.days - b.days);
  const avgCycle = wonCycleDays.length > 0 ? Math.round(wonCycleDays.reduce((s, d) => s + d.days, 0) / wonCycleDays.length) : null;
  const medianCycle = wonCycleDays.length > 0 ? wonCycleDays[Math.floor(wonCycleDays.length / 2)].days : null;

  const stageAges = {};
  const staleDeals = [];
  for (const d of deals) {
    const sid = d.properties.dealstage;
    if (sid === 'decisionmakerboughtin' || sid === 'closedlost') continue;
    const cr = new Date(d.properties.createdate);
    if (isNaN(cr)) continue;
    const age = Math.round((now - cr) / 86400000);
    if (!stageAges[sid]) stageAges[sid] = [];
    stageAges[sid].push(age);
    if (age > 30) staleDeals.push({ name: d.properties.dealname, stage: stageMap[sid]?.label || sid, days: age, rep: owners[d.properties.hubspot_owner_id] || 'Unassigned' });
  }
  const avgAgeByStage = STAGES.filter(s => s.id !== 'decisionmakerboughtin' && s.id !== 'closedlost').map(s => ({
    label: s.label, avgDays: stageAges[s.id] ? Math.round(stageAges[s.id].reduce((a, b) => a + b, 0) / stageAges[s.id].length) : 0, count: (stageAges[s.id] || []).length,
  }));
  staleDeals.sort((a, b) => b.days - a.days);

  return {
    generated: now.toLocaleString('en-AU', { timeZone: 'Australia/Melbourne', dateStyle: 'full', timeStyle: 'short' }),
    today, pnl, pipeline, winRate, totalPipelineValue: Math.round(totalPipelineValue), weightedPipelineValue: Math.round(weightedPipelineValue),
    totalWon, totalLost, wonValue: Math.round(wonValue), totalDeals: deals.length,
    sourceStats: Object.values(sourceStats).filter(s => s.total > 0),
    reps, daily, actTotals, funnel: funnelData, activation,
    velocity: { avgCycle, medianCycle, wonCycleDays, avgAgeByStage, staleDeals, staleDealCount: staleDeals.length },
  };
}

// ══════════════════════════════════════════
// HTML GENERATION
// ══════════════════════════════════════════
function generateHTML(data) {
  const json = JSON.stringify(data);
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta http-equiv="refresh" content="300">
<title>Sammy AI - Sales Dashboard</title>
<script src="https://cdn.tailwindcss.com"><\/script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"><\/script>
<style>
  body { font-family: 'Inter', system-ui, -apple-system, sans-serif; }
  @media print { .no-print { display: none; } canvas { max-height: 300px; } }
</style>
</head>
<body class="bg-gray-50 min-h-screen">

<header class="bg-white border-b border-gray-200 px-6 py-4">
  <div class="max-w-7xl mx-auto flex justify-between items-center">
    <div>
      <h1 class="text-2xl font-bold text-gray-900">Sammy AI Sales Dashboard</h1>
      <p class="text-sm text-gray-500" id="timestamp"></p>
    </div>
    <div class="text-right">
      <p class="text-sm text-gray-500" id="dataInfo"></p>
      <div class="flex gap-3 mt-1 justify-end">
        <a href="/refresh" class="text-xs text-blue-500 hover:text-blue-700 no-print">Refresh now</a>
        <span class="text-xs text-gray-300">|</span>
        <span class="text-xs text-gray-400">Auto-refreshes every 5 min</span>
      </div>
    </div>
  </div>
</header>

<main class="max-w-7xl mx-auto px-6 py-8 space-y-10">

  <!-- SECTION 0: TODAY'S KPIs -->
  <section>
    <h2 class="text-lg font-semibold text-gray-800 mb-4">Today</h2>
    <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4" id="todayCards"></div>
    <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-5" id="todayByRep"></div>
  </section>

  <!-- SECTION 1: P&L SUMMARY -->
  <section>
    <h2 class="text-lg font-semibold text-gray-800 mb-4">Profit & Loss</h2>
    <div class="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4 mb-4" id="pnlCards"></div>
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
      <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-5">
        <h3 class="text-sm font-medium text-gray-500 mb-3">Monthly Cost Breakdown</h3>
        <table class="w-full text-sm" id="costTable"></table>
      </div>
      <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-5">
        <h3 class="text-sm font-medium text-gray-500 mb-3">MRR Breakdown by Tier</h3>
        <div class="flex items-center gap-6">
          <canvas id="mrrChart" class="max-h-48"></canvas>
          <div id="mrrLegend" class="text-sm space-y-2"></div>
        </div>
      </div>
    </div>
    <p class="text-sm text-gray-500 mt-3" id="breakEvenNote"></p>
  </section>

  <!-- SECTION 2: PIPELINE OVERVIEW -->
  <section>
    <h2 class="text-lg font-semibold text-gray-800 mb-4">Pipeline Overview</h2>
    <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
      <div class="lg:col-span-2 bg-white rounded-xl shadow-sm border border-gray-100 p-5">
        <canvas id="pipelineChart" height="260"></canvas>
      </div>
      <div class="space-y-4" id="pipelineKPIs"></div>
    </div>
  </section>

  <!-- SECTION 3: DEAL SOURCE -->
  <section>
    <h2 class="text-lg font-semibold text-gray-800 mb-4">Deal Source Performance</h2>
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
      <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-5">
        <canvas id="sourceChart" height="220"></canvas>
      </div>
      <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-5 overflow-x-auto">
        <table class="w-full text-sm" id="sourceTable"></table>
      </div>
    </div>
  </section>

  <!-- SECTION 4: REP PERFORMANCE -->
  <section>
    <h2 class="text-lg font-semibold text-gray-800 mb-4">Sales Rep Performance</h2>
    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4" id="repCards"></div>
  </section>

  <!-- SECTION 5: DAILY ACTIVITY -->
  <section>
    <h2 class="text-lg font-semibold text-gray-800 mb-4">Daily Activity (Last 30 Days)</h2>
    <div class="grid grid-cols-3 gap-4 mb-4" id="activityTotals"></div>
    <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-5">
      <canvas id="activityChart" height="180"></canvas>
    </div>
  </section>

  <!-- SECTION 6: CONVERSION FUNNEL -->
  <section>
    <h2 class="text-lg font-semibold text-gray-800 mb-4">Conversion Funnel</h2>
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
      <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-5">
        <canvas id="funnelChart" height="200"></canvas>
      </div>
      <div class="space-y-4" id="funnelKPIs"></div>
    </div>
  </section>

  <!-- SECTION 7: DEAL VELOCITY -->
  <section>
    <h2 class="text-lg font-semibold text-gray-800 mb-4">Deal Velocity</h2>
    <div class="grid grid-cols-2 md:grid-cols-3 gap-4 mb-4" id="velocityKPIs"></div>
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
      <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-5">
        <canvas id="velocityChart" height="220"></canvas>
      </div>
      <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-5 overflow-y-auto max-h-96">
        <h3 class="text-sm font-medium text-gray-500 mb-3">Stale Deals (&gt;30 days in stage)</h3>
        <table class="w-full text-sm" id="staleTable"></table>
      </div>
    </div>
  </section>

</main>

<footer class="text-center text-xs text-gray-400 py-6">Sammy AI Sales Dashboard &mdash; live data from HubSpot</footer>

<script>
const D = ${json};
const $ = id => document.getElementById(id);
const fmt = n => n == null ? 'N/A' : '$' + Math.abs(n).toLocaleString();
const pct = n => n == null ? 'N/A' : n + '%';
const BLUE = '#3b82f6', GREEN = '#22c55e', RED = '#ef4444', AMBER = '#f59e0b', PURPLE = '#8b5cf6', CYAN = '#06b6d4', GRAY = '#9ca3af', LGRAY = '#e5e7eb';

function card(label, value, color, sub) {
  return '<div class="bg-white rounded-xl shadow-sm border border-gray-100 p-5">'
    + '<p class="text-xs font-medium text-gray-500 uppercase tracking-wide">' + label + '</p>'
    + '<p class="text-2xl font-bold mt-1" style="color:' + color + '">' + value + '</p>'
    + (sub ? '<p class="text-xs text-gray-400 mt-1">' + sub + '</p>' : '') + '</div>';
}

$('timestamp').textContent = 'Live data as of ' + D.generated;
$('dataInfo').textContent = D.totalDeals + ' deals, ' + D.funnel.totalContacts + ' contacts';

// ═══ TODAY ═══
const T = D.today;
const todayColor = T.total > T.dailyAvg ? GREEN : (T.total > 0 ? AMBER : RED);
$('todayCards').innerHTML = [
  card('Calls Today', T.calls, BLUE, 'avg ' + Math.round(D.actTotals.calls / 30) + '/day'),
  card('Meetings Today', T.meetings, GREEN, 'avg ' + Math.round(D.actTotals.meetings / 30) + '/day'),
  card('Notes Today', T.notes, PURPLE, 'avg ' + Math.round(D.actTotals.notes / 30) + '/day'),
  card('Total Activity', T.total, todayColor, T.total >= T.dailyAvg ? 'On pace (' + T.dailyAvg + '/day avg)' : 'Below avg (' + T.dailyAvg + '/day)'),
].join('');

const repNames = Object.keys(T.byRep);
if (repNames.length > 0) {
  let repHTML = '<h3 class="text-sm font-medium text-gray-500 mb-3">Today by Rep</h3><div class="grid grid-cols-' + Math.min(repNames.length, 4) + ' gap-4">';
  for (const name of repNames.sort()) {
    const r = T.byRep[name];
    const total = r.calls + r.meetings + r.notes;
    repHTML += '<div class="text-center p-3 rounded-lg bg-gray-50"><p class="font-semibold text-sm">' + name + '</p>'
      + '<p class="text-2xl font-bold mt-1" style="color:' + (total > 0 ? BLUE : GRAY) + '">' + total + '</p>'
      + '<p class="text-xs text-gray-400 mt-1">' + r.calls + ' calls, ' + r.meetings + ' mtgs, ' + r.notes + ' notes</p></div>';
  }
  repHTML += '</div>';
  $('todayByRep').innerHTML = repHTML;
} else {
  $('todayByRep').innerHTML = '<p class="text-gray-400 text-sm">No activity logged today yet</p>';
}

// ═══ P&L ═══
const profitColor = D.pnl.monthlyProfit >= 0 ? GREEN : RED;
const roiColor = D.pnl.roi > 0 ? GREEN : (D.pnl.roi > -25 ? AMBER : RED);
const cacColor = D.pnl.cac && D.pnl.cac < D.pnl.arpu * 3 ? GREEN : (D.pnl.cac && D.pnl.cac < D.pnl.arpu * 6 ? AMBER : RED);
$('pnlCards').innerHTML = [
  card('Current MRR', fmt(D.pnl.currentMRR) + '/mo', D.pnl.currentMRR > 0 ? GREEN : GRAY, D.pnl.paidCount + ' paid customers'),
  card('Monthly Costs', fmt(D.pnl.totalCosts) + '/mo', GRAY, Object.keys(D.pnl.costs).length + ' line items'),
  card('Monthly Profit', (D.pnl.monthlyProfit >= 0 ? '+' : '-') + fmt(D.pnl.monthlyProfit), profitColor),
  card('ROI', pct(D.pnl.roi), roiColor, 'return on spend'),
  card('CAC', D.pnl.cac ? fmt(D.pnl.cac) : 'No won deals', cacColor, D.pnl.cac ? 'per customer' : ''),
  card('Break-Even', D.pnl.paidCount >= D.pnl.breakEvenCustomers ? 'Achieved' : D.pnl.breakEvenCustomers + ' customers', D.pnl.paidCount >= D.pnl.breakEvenCustomers ? GREEN : AMBER, 'need ' + D.pnl.breakEvenCustomers + ' at $' + D.pnl.arpu + ' ARPU'),
].join('');
let costRows = '<thead><tr class="border-b"><th class="text-left py-2 text-gray-500">Item</th><th class="text-right py-2 text-gray-500">Amount</th></tr></thead><tbody>';
for (const [name, amount] of Object.entries(D.pnl.costs)) costRows += '<tr class="border-b border-gray-50"><td class="py-2">' + name + '</td><td class="text-right py-2 font-medium">$' + amount.toLocaleString() + '</td></tr>';
costRows += '<tr class="font-bold"><td class="py-2">Total</td><td class="text-right py-2">$' + D.pnl.totalCosts.toLocaleString() + '</td></tr></tbody>';
$('costTable').innerHTML = costRows;
const mrrLabels = [], mrrValues = [], mrrColors = [GREEN, BLUE, PURPLE, GRAY];
if (D.pnl.mrrBreakdown.basic) { mrrLabels.push('Basic ($59)'); mrrValues.push(D.pnl.mrrBreakdown.basic); }
if (D.pnl.mrrBreakdown.pro) { mrrLabels.push('Pro ($99)'); mrrValues.push(D.pnl.mrrBreakdown.pro); }
if (D.pnl.mrrBreakdown.team) { mrrLabels.push('Team ($249)'); mrrValues.push(D.pnl.mrrBreakdown.team); }
if (D.pnl.mrrBreakdown.unknown) { mrrLabels.push('Unknown ($59)'); mrrValues.push(D.pnl.mrrBreakdown.unknown); }
if (mrrValues.length > 0) {
  new Chart($('mrrChart'), { type:'doughnut', data:{ labels:mrrLabels, datasets:[{ data:mrrValues, backgroundColor:mrrColors.slice(0,mrrValues.length) }] }, options:{ plugins:{ legend:{ display:false } }, cutout:'60%' } });
  $('mrrLegend').innerHTML = mrrLabels.map((l,i) => '<div class="flex items-center gap-2"><span class="w-3 h-3 rounded-full" style="background:'+mrrColors[i]+'"></span><span>'+l+': <b>'+mrrValues[i]+'</b></span></div>').join('');
} else { $('mrrChart').parentElement.innerHTML = '<p class="text-gray-400 text-sm">No paid customers yet</p>'; }
$('breakEvenNote').textContent = 'At $' + D.pnl.arpu + ' ARPU and $' + D.pnl.totalCosts.toLocaleString() + '/mo total spend, you need ' + D.pnl.breakEvenCustomers + ' paid customers to break even. Currently at ' + D.pnl.paidCount + '.';

// ═══ PIPELINE ═══
new Chart($('pipelineChart'), { type:'bar', data:{ labels:D.pipeline.map(s=>s.label), datasets:[{ label:'Deals', data:D.pipeline.map(s=>s.count), backgroundColor:BLUE, borderRadius:4 }] }, options:{ indexAxis:'y', responsive:true, plugins:{ legend:{display:false}, title:{display:true,text:'Deals by Stage'} }, scales:{ x:{beginAtZero:true} } } });
$('pipelineKPIs').innerHTML = [
  card('Total Open Pipeline', fmt(D.totalPipelineValue), BLUE, D.totalDeals-D.totalWon-D.totalLost+' open deals'),
  card('Weighted Pipeline', fmt(D.weightedPipelineValue), PURPLE, 'probability-adjusted'),
  card('Win Rate', pct(D.winRate), D.winRate>=30?GREEN:(D.winRate>=15?AMBER:RED), D.totalWon+' won / '+D.totalLost+' lost'),
  card('Closed Won Value', fmt(D.wonValue), GREEN, D.totalWon+' deals'),
].join('');

// ═══ DEAL SOURCE ═══
new Chart($('sourceChart'), { type:'bar', data:{ labels:D.sourceStats.map(s=>s.label), datasets:[ {label:'Won',data:D.sourceStats.map(s=>s.won),backgroundColor:GREEN,borderRadius:4}, {label:'Lost',data:D.sourceStats.map(s=>s.lost),backgroundColor:RED,borderRadius:4}, {label:'Open',data:D.sourceStats.map(s=>s.open),backgroundColor:LGRAY,borderRadius:4} ] }, options:{ responsive:true, plugins:{title:{display:true,text:'Deals by Source'}}, scales:{x:{stacked:false},y:{beginAtZero:true}} } });
let srcRows = '<thead><tr class="border-b"><th class="text-left py-2 text-gray-500">Source</th><th class="text-right py-2 text-gray-500">Total</th><th class="text-right py-2 text-gray-500">Won</th><th class="text-right py-2 text-gray-500">Lost</th><th class="text-right py-2 text-gray-500">Win Rate</th><th class="text-right py-2 text-gray-500">Won MRR</th></tr></thead><tbody>';
for (const s of D.sourceStats) srcRows += '<tr class="border-b border-gray-50"><td class="py-2">'+s.label+'</td><td class="text-right py-2">'+s.total+'</td><td class="text-right py-2 text-green-600 font-medium">'+s.won+'</td><td class="text-right py-2 text-red-500">'+s.lost+'</td><td class="text-right py-2">'+s.winRate+'%</td><td class="text-right py-2 font-medium">$'+s.wonMRR.toLocaleString()+'/mo</td></tr>';
srcRows += '</tbody>'; $('sourceTable').innerHTML = srcRows;

// ═══ REPS ═══
$('repCards').innerHTML = D.reps.map(r => {
  const initials = r.name.split(' ').map(w=>w[0]).join('').toUpperCase();
  const color = r.name==='Unassigned'?GRAY:(D.reps.indexOf(r)%2===0?BLUE:PURPLE);
  return '<div class="bg-white rounded-xl shadow-sm border border-gray-100 p-5">'
    +'<div class="flex items-center gap-3 mb-4"><div class="w-10 h-10 rounded-full flex items-center justify-center text-white text-sm font-bold" style="background:'+color+'">'+initials+'</div><div><p class="font-semibold">'+r.name+'</p><p class="text-xs text-gray-400">'+r.total+' deals total</p></div></div>'
    +'<div class="grid grid-cols-2 gap-3 text-sm">'
    +'<div><p class="text-gray-500">Won</p><p class="font-bold text-green-600">'+r.won+'</p></div>'
    +'<div><p class="text-gray-500">Lost</p><p class="font-bold text-red-500">'+r.lost+'</p></div>'
    +'<div><p class="text-gray-500">Open</p><p class="font-bold">'+r.open+'</p></div>'
    +'<div><p class="text-gray-500">Win Rate</p><p class="font-bold">'+r.winRate+'%</p></div>'
    +'<div><p class="text-gray-500">Won MRR</p><p class="font-bold text-green-600">$'+r.wonMRR.toLocaleString()+'</p></div>'
    +'<div><p class="text-gray-500">Avg Cycle</p><p class="font-bold">'+(r.avgCycleDays!=null?r.avgCycleDays+'d':'N/A')+'</p></div>'
    +'</div><div class="mt-4 pt-3 border-t border-gray-100 text-sm">'
    +'<p class="text-gray-500 text-xs uppercase tracking-wide mb-2">Activity (30d)</p>'
    +'<div class="flex gap-4"><span>&#128222; '+r.calls+'</span><span>&#128197; '+r.meetings+'</span><span>&#128221; '+r.notes+'</span></div>'
    +'</div></div>';
}).join('');

// ═══ ACTIVITY ═══
$('activityTotals').innerHTML = [
  card('Calls (30d)', D.actTotals.calls, BLUE, Math.round(D.actTotals.calls/4.3)+'/week avg'),
  card('Meetings (30d)', D.actTotals.meetings, GREEN, Math.round(D.actTotals.meetings/4.3)+'/week avg'),
  card('Notes (30d)', D.actTotals.notes, PURPLE, Math.round(D.actTotals.notes/4.3)+'/week avg'),
].join('');
const dateLabels = D.daily.map(d => { const dt=new Date(d.date); return dt.toLocaleDateString('en-AU',{month:'short',day:'numeric'}); });
new Chart($('activityChart'), { type:'line', data:{ labels:dateLabels, datasets:[ {label:'Calls',data:D.daily.map(d=>d.calls),borderColor:BLUE,backgroundColor:BLUE+'20',fill:true,tension:0.3}, {label:'Meetings',data:D.daily.map(d=>d.meetings),borderColor:GREEN,backgroundColor:GREEN+'20',fill:true,tension:0.3}, {label:'Notes',data:D.daily.map(d=>d.notes),borderColor:PURPLE,backgroundColor:PURPLE+'20',fill:true,tension:0.3} ] }, options:{ responsive:true, scales:{y:{beginAtZero:true}}, plugins:{legend:{position:'top'}} } });

// ═══ FUNNEL ═══
const F = D.funnel;
new Chart($('funnelChart'), { type:'bar', data:{ labels:['All Contacts','Started Trial','Active Trial','Paid Customer'], datasets:[{ data:[F.totalContacts,F.activeTrial+F.paid+F.expired+F.churned,F.activeTrial,F.paid], backgroundColor:[LGRAY,CYAN,BLUE,GREEN], borderRadius:4 }] }, options:{ indexAxis:'y', responsive:true, plugins:{legend:{display:false},title:{display:true,text:'Conversion Funnel'}}, scales:{x:{beginAtZero:true}} } });
$('funnelKPIs').innerHTML = [
  card('Signup to Trial', pct(F.signupToTrialRate), BLUE, (F.activeTrial+F.paid+F.expired+F.churned)+' of '+F.totalContacts+' contacts'),
  card('Trial to Paid', pct(F.trialToPaidRate), GREEN, F.paid+' converted'),
  card('Overall Conversion', pct(F.overallConversion), F.overallConversion>=5?GREEN:AMBER, 'contacts to paid'),
  card('Trial Expired', F.expired, RED, 'did not convert'),
  card('Churned', F.churned, RED, 'lost after paying'),
  card('Active Trials', F.activeTrial, BLUE, 'in progress'),
].join('');

// ═══ VELOCITY ═══
$('velocityKPIs').innerHTML = [
  card('Avg Won Cycle', D.velocity.avgCycle!=null?D.velocity.avgCycle+' days':'No data', BLUE),
  card('Median Won Cycle', D.velocity.medianCycle!=null?D.velocity.medianCycle+' days':'No data', PURPLE),
  card('Stale Deals', D.velocity.staleDealCount, D.velocity.staleDealCount>5?RED:(D.velocity.staleDealCount>0?AMBER:GREEN), '>30 days in stage'),
].join('');
const velStages = D.velocity.avgAgeByStage.filter(s=>s.count>0);
new Chart($('velocityChart'), { type:'bar', data:{ labels:velStages.map(s=>s.label), datasets:[{ label:'Avg Days in Stage', data:velStages.map(s=>s.avgDays), backgroundColor:velStages.map(s=>s.avgDays>30?RED:(s.avgDays>14?AMBER:BLUE)), borderRadius:4 }] }, options:{ responsive:true, plugins:{legend:{display:false},title:{display:true,text:'Average Days in Stage (Open Deals)'}}, scales:{y:{beginAtZero:true}} } });
if (D.velocity.staleDeals.length > 0) {
  let staleRows = '<thead><tr class="border-b"><th class="text-left py-2 text-gray-500">Deal</th><th class="text-left py-2 text-gray-500">Stage</th><th class="text-right py-2 text-gray-500">Days</th><th class="text-left py-2 text-gray-500">Rep</th></tr></thead><tbody>';
  for (const d of D.velocity.staleDeals) { const color=d.days>60?'text-red-600 font-bold':'text-amber-600'; staleRows+='<tr class="border-b border-gray-50"><td class="py-2 max-w-[180px] truncate">'+d.name+'</td><td class="py-2 text-sm">'+d.stage+'</td><td class="text-right py-2 '+color+'">'+d.days+'</td><td class="py-2 text-sm">'+d.rep+'</td></tr>'; }
  staleRows += '</tbody>'; $('staleTable').innerHTML = staleRows;
} else { $('staleTable').parentElement.innerHTML = '<p class="text-green-600 text-sm font-medium">No stale deals - everything is moving!</p>'; }
<\/script>
</body>
</html>`;
}

function loadingHTML() {
  return `<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Loading...</title>
<meta http-equiv="refresh" content="5">
<script src="https://cdn.tailwindcss.com"><\/script></head>
<body class="bg-gray-50 min-h-screen flex items-center justify-center">
<div class="text-center"><div class="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500 mx-auto"></div>
<p class="mt-4 text-gray-600 text-lg">Loading dashboard...</p>
<p class="mt-2 text-gray-400 text-sm">Fetching live data from HubSpot (10-15 seconds)</p>
</div></body></html>`;
}

// ══════════════════════════════════════════
// CACHE & REFRESH
// ══════════════════════════════════════════
async function refreshCache() {
  if (cache.loading) return;
  cache.loading = true;
  try {
    const raw = await fetchAllData();
    const data = computeMetrics(raw);
    cache.data = data;
    cache.html = generateHTML(data);
    cache.time = Date.now();
    console.log('[cache] Refreshed at ' + new Date().toISOString());
  } catch (err) {
    console.error('[cache] Refresh failed:', err.response?.data?.message || err.message);
  } finally {
    cache.loading = false;
  }
}

// ══════════════════════════════════════════
// ROUTES
// ══════════════════════════════════════════
app.get('/health', (req, res) => res.send('ok'));

app.get('/api/data', (req, res) => {
  if (!cache.data) return res.status(503).json({ status: 'loading' });
  res.json(cache.data);
});

app.get('/refresh', async (req, res) => {
  cache.time = 0; // invalidate
  refreshCache();
  res.redirect('/');
});

app.get('/', async (req, res) => {
  // If cache is stale or empty, trigger refresh
  if (Date.now() - cache.time > CACHE_TTL) {
    refreshCache(); // fire and forget
  }
  if (cache.html) {
    res.type('html').send(cache.html);
  } else {
    res.type('html').send(loadingHTML());
  }
});

// ══════════════════════════════════════════
// START
// ══════════════════════════════════════════
app.listen(PORT, () => {
  console.log(`Sammy Dashboard running on http://localhost:${PORT}`);
  console.log('Fetching initial data...');
  refreshCache();
});
