require('dotenv').config();
const express = require('express');
const axios = require('axios');
const fs = require('fs');
const path = require('path');

const TOKEN = process.env.HUBSPOT_PRIVATE_APP_TOKEN;
if (!TOKEN) { console.error('HUBSPOT_PRIVATE_APP_TOKEN is required'); process.exit(1); }

const api = axios.create({
  baseURL: 'https://api.hubapi.com',
  headers: { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' },
});
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const app = express();
app.use(express.json());
const PORT = process.env.PORT || 3000;

// ══════════════════════════════════════════
// TASK STORAGE (file-based, JSON)
// ══════════════════════════════════════════
const TASKS_DIR = path.join(__dirname, 'data');
const TASKS_FILE = path.join(TASKS_DIR, 'tasks.json');

function ensureTasksFile() {
  if (!fs.existsSync(TASKS_DIR)) fs.mkdirSync(TASKS_DIR, { recursive: true });
  if (!fs.existsSync(TASKS_FILE)) fs.writeFileSync(TASKS_FILE, '{}');
}
function readTasks() { ensureTasksFile(); try { return JSON.parse(fs.readFileSync(TASKS_FILE, 'utf8')); } catch { return {}; } }
function writeTasks(all) { ensureTasksFile(); fs.writeFileSync(TASKS_FILE, JSON.stringify(all, null, 2)); }
function getTodayMelbourne() { return new Date().toLocaleDateString('en-CA', { timeZone: 'Australia/Melbourne' }); }
function getTasksForDate(dateStr) { return readTasks()[dateStr] || []; }
function saveTasksForDate(dateStr, tasks) {
  const all = readTasks(); all[dateStr] = tasks;
  const cutoff = new Date(Date.now() - 30 * 86400000).toISOString().split('T')[0];
  for (const key of Object.keys(all)) { if (key < cutoff) delete all[key]; }
  writeTasks(all);
}
function generateTaskId() { return Date.now().toString(36) + Math.random().toString(36).slice(2, 7); }

// ══════════════════════════════════════════
// CONFIGURATION
// ══════════════════════════════════════════
const HUBSPOT_PORTAL = '244038625';
const CACHE_TTL = 5 * 60 * 1000;

const PLAN_PRICING = { founder_59: 59, monthly_99: 99, annual_950: 79, free: 0, default: 59 };

const STAGES = [
  { id: 'appointmentscheduled', label: 'Attempting Contact', weight: 0.05 },
  { id: 'presentationscheduled', label: 'Discovery', weight: 0.15 },
  { id: '2843565802', label: 'Demo Booked', weight: 0.35 },
  { id: '2851995329', label: 'Demo Complete', weight: 0.60 },
  { id: '2054317800', label: 'Pause', weight: 0.10 },
  { id: '2845034230', label: 'Nurture', weight: 0.10 },
  { id: 'closedlost', label: 'Closed Lost', weight: 0.00 },
  { id: 'decisionmakerboughtin', label: 'Closed Won', weight: 1.00 },
];

const MONTHLY_COSTS = { 'Cold Email': 1000, 'Sales Team': 4000, 'HubSpot': 50, 'Aircall': 100 };

const ACTIVE_REPS = ['Lucas Gibson', 'Krishna Pryor'];
const REP_TARGETS = {
  'Lucas Gibson': { uniqueCalls: 100, callHours: 5 },
  'Krishna Pryor': { uniqueCalls: 100, callHours: 5 },
};
const COMMISSION_PER_CLOSE = 100;

// ── Instantly Config ──
const INSTANTLY_KEY = process.env.INSTANTLY_API_KEY || '';
const instantlyApi = axios.create({
  baseURL: 'https://api.instantly.ai/api/v2',
  headers: { Authorization: `Bearer ${INSTANTLY_KEY}`, 'Content-Type': 'application/json' },
});

// ══════════════════════════════════════════
// CACHE
// ══════════════════════════════════════════
let cache = { data: null, time: 0, loading: false, lastError: null, lastSuccess: 0 };

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
    const params = {
      limit: 100,
      properties: 'dealname,dealstage,pipeline,amount,expected_mrr,deal_source,hubspot_owner_id,closedate,createdate,hs_lastmodifieddate,hs_v2_date_entered_decisionmakerboughtin,hs_v2_date_entered_appointmentscheduled,hs_v2_date_entered_presentationscheduled,hs_v2_date_entered_2843565802,hs_v2_date_entered_2851995329,hs_v2_date_entered_closedlost',
      associations: 'contacts',
    };
    if (after) params.after = after;
    const { data } = await api.get('/crm/v3/objects/deals', { params });
    deals.push(...data.results);
    if (!data.paging?.next?.after) break;
    after = data.paging.next.after;
    await sleep(200);
  }
  return deals;
}

async function fetchContactEmails(contactIds) {
  const emailMap = {};
  const analyticsSourceMap = {};
  for (let i = 0; i < contactIds.length; i += 100) {
    const batch = contactIds.slice(i, i + 100);
    try {
      const { data } = await withRetry(() => api.post('/crm/v3/objects/contacts/batch/read', {
        inputs: batch.map(id => ({ id })),
        properties: ['email', 'hs_analytics_source', 'hs_analytics_source_data_1'],
      }));
      for (const c of data.results) {
        if (c.properties.email) emailMap[c.id] = c.properties.email.toLowerCase();
        if (c.properties.hs_analytics_source) analyticsSourceMap[c.id] = {
          source: c.properties.hs_analytics_source,
          sourceData: c.properties.hs_analytics_source_data_1 || '',
        };
      }
    } catch (err) {
      console.error('[fetch] Contact batch read failed:', err.response?.data?.message || err.message);
    }
    if (i + 100 < contactIds.length) await sleep(300);
  }
  return { emailMap, analyticsSourceMap };
}

async function fetchPaidCustomers() {
  const results = []; let after;
  while (true) {
    const body = {
      filterGroups: [{ filters: [{ propertyName: 'user_status', operator: 'EQ', value: 'paid_customer' }] }],
      properties: ['sammy_pricing_plan', 'sammy_subscription_tier', 'createdate', 'firstname', 'lastname', 'email'],
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

async function fetchActiveTrials() {
  const results = []; let after;
  while (true) {
    const body = {
      filterGroups: [{ filters: [{ propertyName: 'user_status', operator: 'EQ', value: 'active_trial' }] }],
      properties: ['firstname', 'lastname', 'email', 'sammy_trial_end_date', 'sammy_trial_start_date', 'createdate'],
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

async function fetchChurnedCustomers() {
  const results = []; let after;
  while (true) {
    const body = {
      filterGroups: [{ filters: [{ propertyName: 'user_status', operator: 'EQ', value: 'churned' }] }],
      properties: ['sammy_pricing_plan', 'sammy_subscription_tier', 'createdate', 'firstname', 'lastname', 'hs_lastmodifieddate'],
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

async function fetchEngagements(objectType, daysBack = 30, extraProperties = []) {
  const since = new Date(Date.now() - daysBack * 86400000).toISOString().split('T')[0];
  const results = [];
  let after;
  try {
    while (true) {
      const body = {
        filterGroups: [{ filters: [{ propertyName: 'hs_createdate', operator: 'GTE', value: since }] }],
        properties: ['hs_createdate', 'hubspot_owner_id', ...extraProperties],
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
// INSTANTLY API HELPERS
// ══════════════════════════════════════════
async function fetchInstantlyCampaigns() {
  try {
    const { data } = await instantlyApi.get('/campaigns', { params: { limit: 100 } });
    return data.items || [];
  } catch (err) {
    console.error('[instantly] Campaign fetch failed:', err.response?.status, err.message);
    return [];
  }
}

async function fetchInstantlyAccounts() {
  try {
    const { data } = await instantlyApi.get('/accounts', { params: { limit: 100 } });
    return data.items || [];
  } catch (err) {
    console.error('[instantly] Accounts fetch failed:', err.message);
    return [];
  }
}

// ══════════════════════════════════════════
// DATA FETCHING ORCHESTRATOR
// ══════════════════════════════════════════
async function fetchAllData() {
  console.log('[fetch] Starting data fetch...');
  const t0 = Date.now();

  const instantlyPromise = Promise.all([fetchInstantlyCampaigns(), fetchInstantlyAccounts()]);

  const owners = await fetchOwners();
  const deals = await fetchAllDeals();

  const contactIds = new Set();
  for (const d of deals) {
    const assoc = d.associations?.contacts?.results;
    if (assoc) for (const c of assoc) contactIds.add(c.id);
  }
  const { emailMap: contactEmails, analyticsSourceMap: contactAnalyticsSources } = await fetchContactEmails([...contactIds]);

  const dealEmailMap = {}, dealContactIdMap = {};
  for (const d of deals) {
    const assoc = d.associations?.contacts?.results;
    if (assoc) for (const c of assoc) {
      if (contactEmails[c.id]) { dealEmailMap[d.id] = contactEmails[c.id]; dealContactIdMap[d.id] = c.id; break; }
    }
  }

  await sleep(500);
  const [noStatus, incomplete, activeTrial] = await Promise.all([countNoStatus(), countByStatus('incomplete_onboarding'), countByStatus('active_trial')]);
  await sleep(500);
  const [paid, expired, churned] = await Promise.all([countByStatus('paid_customer'), countByStatus('trial_expired'), countByStatus('churned')]);
  await sleep(500);

  const paidCustomers = await fetchPaidCustomers();
  const churnedCustomers = await fetchChurnedCustomers();
  const activeTrials = await fetchActiveTrials();
  await sleep(500);

  const [calls, meetings, notes] = await Promise.all([
    fetchEngagements('calls', 30, ['hs_call_duration', 'hs_call_to_number']),
    fetchEngagements('meetings'), fetchEngagements('notes'),
  ]);

  const [instantlyCampaigns, instantlyAccounts] = await instantlyPromise;

  console.log(`[fetch] Done in ${((Date.now() - t0) / 1000).toFixed(1)}s — ${deals.length} deals, ${paid} paid, ${instantlyCampaigns.length} campaigns`);

  return {
    owners, deals, dealEmailMap, dealContactIdMap, contactAnalyticsSources,
    funnel: { noStatus, incomplete, activeTrial, paid, expired, churned },
    paidCustomers, churnedCustomers, activeTrials,
    activity: { calls: calls || [], meetings: meetings || [], notes: notes || [] },
    instantly: { campaigns: instantlyCampaigns, accounts: instantlyAccounts },
  };
}

// ══════════════════════════════════════════
// METRICS COMPUTATION
// ══════════════════════════════════════════

function computeMRR(paidCustomers) {
  let total = 0;
  const breakdown = { founder_59: 0, monthly_99: 0, annual_950: 0, free: 0, unknown: 0 };
  for (const c of paidCustomers) {
    const plan = c.properties.sammy_pricing_plan || 'unknown';
    const rate = PLAN_PRICING[plan] || PLAN_PRICING.default;
    total += rate;
    if (breakdown[plan] !== undefined) breakdown[plan]++;
    else breakdown.unknown++;
  }
  return { total, breakdown, count: paidCustomers.length };
}

function computeChurn(churnedCustomers) {
  const thirtyDaysAgo = new Date(Date.now() - 30 * 86400000);
  let churnMRR = 0, churnCount = 0;
  for (const c of churnedCustomers) {
    const modDate = new Date(c.properties.hs_lastmodifieddate);
    if (isNaN(modDate) || modDate < thirtyDaysAgo) continue;
    const plan = c.properties.sammy_pricing_plan || 'unknown';
    churnMRR += PLAN_PRICING[plan] || PLAN_PRICING.default;
    churnCount++;
  }
  return { churnMRR, churnCount, totalChurned: churnedCustomers.length };
}

function computePipeline(deals, owners) {
  const pipeline = {};
  for (const s of STAGES) {
    pipeline[s.id] = { label: s.label, weight: s.weight, count: 0, value: 0, deals: [] };
  }
  for (const d of deals) {
    const stage = d.properties.dealstage;
    if (!pipeline[stage]) continue;
    const val = parseFloat(d.properties.amount) || parseFloat(d.properties.expected_mrr) || 0;
    const ownerName = owners[d.properties.hubspot_owner_id] || 'Unassigned';
    const daysSinceMod = Math.round((Date.now() - new Date(d.properties.hs_lastmodifieddate)) / 86400000);
    pipeline[stage].count++;
    pipeline[stage].value += val;
    pipeline[stage].deals.push({
      id: d.id,
      name: d.properties.dealname,
      value: val,
      owner: ownerName,
      daysSinceMod,
      stage: d.properties.dealstage,
      stageLabel: pipeline[stage].label,
      createdate: d.properties.createdate,
      closedate: d.properties.closedate,
    });
  }
  return pipeline;
}

function computeWeightedForecast(pipeline) {
  let weighted = 0;
  for (const stageId of Object.keys(pipeline)) {
    const s = pipeline[stageId];
    weighted += s.value * s.weight;
  }
  return weighted;
}

function computeRepStats(deals, owners, activity) {
  const reps = {};
  for (const name of ACTIVE_REPS) {
    reps[name] = {
      totalDeals: 0, wonDeals: 0, lostDeals: 0, openDeals: 0,
      wonValue: 0, pipelineValue: 0, deals: [],
    };
  }

  // Build reverse owner map: name -> id(s)
  const ownerNameToIds = {};
  for (const [id, name] of Object.entries(owners)) {
    if (!ownerNameToIds[name]) ownerNameToIds[name] = [];
    ownerNameToIds[name].push(id);
  }

  for (const d of deals) {
    const ownerId = d.properties.hubspot_owner_id;
    const ownerName = owners[ownerId];
    if (!ownerName || !reps[ownerName]) continue;

    const rep = reps[ownerName];
    const stage = d.properties.dealstage;
    const val = parseFloat(d.properties.amount) || parseFloat(d.properties.expected_mrr) || 0;

    rep.totalDeals++;
    if (stage === 'decisionmakerboughtin') {
      rep.wonDeals++;
      rep.wonValue += val;
    } else if (stage === 'closedlost') {
      rep.lostDeals++;
    } else {
      rep.openDeals++;
      rep.pipelineValue += val;
    }
    rep.deals.push({
      id: d.id, name: d.properties.dealname, stage, value: val,
      lastModified: d.properties.hs_lastmodifieddate,
    });
  }

  // Add call stats from activity
  if (activity.calls) {
    for (const name of ACTIVE_REPS) {
      const ids = ownerNameToIds[name] || [];
      const repCalls = activity.calls.filter(c => ids.includes(c.properties.hubspot_owner_id));
      const uniqueNumbers = new Set();
      let totalDuration = 0;
      for (const c of repCalls) {
        if (c.properties.hs_call_to_number) uniqueNumbers.add(c.properties.hs_call_to_number);
        totalDuration += parseInt(c.properties.hs_call_duration) || 0;
      }
      reps[name].totalCalls30d = repCalls.length;
      reps[name].uniqueNumbers30d = uniqueNumbers.size;
      reps[name].talkTime30d = totalDuration;
    }
  }

  return reps;
}

function computeDayMetrics(dateStr, activity, deals, owners) {
  const result = {};
  const ownerNameToIds = {};
  for (const [id, name] of Object.entries(owners)) {
    if (!ownerNameToIds[name]) ownerNameToIds[name] = [];
    ownerNameToIds[name].push(id);
  }

  for (const repName of ACTIVE_REPS) {
    const ids = ownerNameToIds[repName] || [];
    const dayCalls = (activity.calls || []).filter(c => {
      if (!ids.includes(c.properties.hubspot_owner_id)) return false;
      const cd = c.properties.hs_createdate;
      return cd && cd.startsWith(dateStr);
    });

    const uniqueNumbers = new Set();
    let talkSec = 0;
    for (const c of dayCalls) {
      if (c.properties.hs_call_to_number) uniqueNumbers.add(c.properties.hs_call_to_number);
      talkSec += parseInt(c.properties.hs_call_duration) || 0;
    }

    // Week demos (Mon-Sun containing dateStr)
    const d = new Date(dateStr + 'T00:00:00');
    const dayOfWeek = d.getDay() === 0 ? 6 : d.getDay() - 1;
    const weekStart = new Date(d); weekStart.setDate(d.getDate() - dayOfWeek);
    const weekEnd = new Date(weekStart); weekEnd.setDate(weekStart.getDate() + 6);
    const weekStartStr = weekStart.toISOString().split('T')[0];
    const weekEndStr = weekEnd.toISOString().split('T')[0];

    const weekDemos = deals.filter(deal => {
      if (!ids.includes(deal.properties.hubspot_owner_id)) return false;
      const entered = deal.properties.hs_v2_date_entered_2843565802;
      if (!entered) return false;
      const enteredDate = entered.split('T')[0];
      return enteredDate >= weekStartStr && enteredDate <= weekEndStr;
    }).length;

    // Month closes
    const monthStr = dateStr.slice(0, 7);
    const monthCloses = deals.filter(deal => {
      if (!ids.includes(deal.properties.hubspot_owner_id)) return false;
      if (deal.properties.dealstage !== 'decisionmakerboughtin') return false;
      const wonDate = deal.properties.hs_v2_date_entered_decisionmakerboughtin;
      return wonDate && wonDate.startsWith(monthStr);
    });
    const monthCloseCount = monthCloses.length;
    const monthCloseValue = monthCloses.reduce((s, deal) =>
      s + (parseFloat(deal.properties.amount) || parseFloat(deal.properties.expected_mrr) || 0), 0);

    // YTD closes for commission
    const yearStr = dateStr.slice(0, 4);
    const ytdCloses = deals.filter(deal => {
      if (!ids.includes(deal.properties.hubspot_owner_id)) return false;
      if (deal.properties.dealstage !== 'decisionmakerboughtin') return false;
      const wonDate = deal.properties.hs_v2_date_entered_decisionmakerboughtin;
      return wonDate && wonDate.startsWith(yearStr);
    });

    // Week closes for commission
    const weekCloses = deals.filter(deal => {
      if (!ids.includes(deal.properties.hubspot_owner_id)) return false;
      if (deal.properties.dealstage !== 'decisionmakerboughtin') return false;
      const wonDate = deal.properties.hs_v2_date_entered_decisionmakerboughtin;
      if (!wonDate) return false;
      const wd = wonDate.split('T')[0];
      return wd >= weekStartStr && wd <= weekEndStr;
    });

    const target = REP_TARGETS[repName] || {};
    result[repName] = {
      dials: dayCalls.length,
      uniqueDials: uniqueNumbers.size,
      talkTimeSec: talkSec,
      talkTimeMin: Math.round(talkSec / 60),
      talkTimeHrs: (talkSec / 3600).toFixed(1),
      dialTarget: target.uniqueCalls || 100,
      talkTarget: target.callHours || 5,
      dialPct: Math.min(100, Math.round((uniqueNumbers.size / (target.uniqueCalls || 100)) * 100)),
      talkPct: Math.min(100, Math.round(((talkSec / 3600) / (target.callHours || 5)) * 100)),
      weekDemos,
      monthCloses: monthCloseCount,
      monthCloseValue,
      commissionMonth: monthCloseCount * COMMISSION_PER_CLOSE,
      commissionWeek: weekCloses.length * COMMISSION_PER_CLOSE,
      commissionYTD: ytdCloses.length * COMMISSION_PER_CLOSE,
    };
  }
  return result;
}

function computeChannelROI(deals, dealContactIdMap, contactAnalyticsSources, owners) {
  const channels = {
    cold_call: { label: 'Cold Call', deals: 0, won: 0, revenue: 0, cost: MONTHLY_COSTS['Cold Email'] || 0 },
    cold_email: { label: 'Cold Email', deals: 0, won: 0, revenue: 0, cost: MONTHLY_COSTS['Cold Email'] || 0 },
    inbound: { label: 'Inbound', deals: 0, won: 0, revenue: 0, cost: 0 },
    unknown: { label: 'Unknown', deals: 0, won: 0, revenue: 0, cost: 0 },
  };

  for (const d of deals) {
    const contactId = dealContactIdMap[d.id];
    let channel = 'unknown';

    // First try deal_source property
    const dealSource = d.properties.deal_source;
    if (dealSource === 'cold_call') channel = 'cold_call';
    else if (dealSource === 'cold_email') channel = 'cold_email';
    else if (dealSource === 'inbound_signup' || dealSource === 'referral') channel = 'inbound';
    else if (contactId && contactAnalyticsSources[contactId]) {
      const src = contactAnalyticsSources[contactId].source;
      if (src === 'OFFLINE_SOURCES') channel = 'cold_call';
      else if (src === 'EMAIL_MARKETING') channel = 'cold_email';
      else if (src === 'ORGANIC_SEARCH' || src === 'DIRECT_TRAFFIC') channel = 'inbound';
    }

    if (!channels[channel]) channel = 'unknown';
    channels[channel].deals++;
    const val = parseFloat(d.properties.amount) || parseFloat(d.properties.expected_mrr) || 0;
    if (d.properties.dealstage === 'decisionmakerboughtin') {
      channels[channel].won++;
      channels[channel].revenue += val;
    }
  }

  // Compute ROI
  for (const ch of Object.values(channels)) {
    ch.roi = ch.cost > 0 ? ((ch.revenue - ch.cost) / ch.cost * 100).toFixed(0) : 'N/A';
    ch.winRate = ch.deals > 0 ? ((ch.won / ch.deals) * 100).toFixed(0) : '0';
  }

  return channels;
}

function computePnL(mrr, churnData) {
  const totalCosts = Object.values(MONTHLY_COSTS).reduce((s, v) => s + v, 0);
  const netMRR = mrr.total - churnData.churnMRR;
  return {
    revenue: mrr.total,
    costs: totalCosts,
    costBreakdown: MONTHLY_COSTS,
    profit: mrr.total - totalCosts,
    margin: mrr.total > 0 ? ((mrr.total - totalCosts) / mrr.total * 100).toFixed(0) : '0',
    netMRR,
    churnMRR: churnData.churnMRR,
  };
}

function computeInstantlyMetrics(instantly) {
  const campaigns = instantly.campaigns || [];
  const accounts = instantly.accounts || [];
  const activeCampaigns = campaigns.filter(c => c.status === 'active' || c.status === 1).length;
  return {
    totalCampaigns: campaigns.length,
    activeCampaigns,
    pausedCampaigns: campaigns.length - activeCampaigns,
    sendingAccounts: accounts.length,
    campaigns: campaigns.map(c => ({
      id: c.id,
      name: c.name,
      status: c.status,
    })),
  };
}

function computeMetrics(raw) {
  const { owners, deals, dealEmailMap, dealContactIdMap, contactAnalyticsSources,
    funnel, paidCustomers, churnedCustomers, activeTrials, activity, instantly } = raw;

  const mrr = computeMRR(paidCustomers);
  const churn = computeChurn(churnedCustomers);
  const pipeline = computePipeline(deals, owners);
  const forecast = computeWeightedForecast(pipeline);
  const repStats = computeRepStats(deals, owners, activity);
  const today = getTodayMelbourne();
  const dayMetrics = computeDayMetrics(today, activity, deals, owners);
  const channels = computeChannelROI(deals, dealContactIdMap, contactAnalyticsSources, owners);
  const pnl = computePnL(mrr, churn);
  const instantlyMetrics = computeInstantlyMetrics(instantly);

  // Win rate
  const wonCount = deals.filter(d => d.properties.dealstage === 'decisionmakerboughtin').length;
  const lostCount = deals.filter(d => d.properties.dealstage === 'closedlost').length;
  const closedTotal = wonCount + lostCount;
  const winRate = closedTotal > 0 ? ((wonCount / closedTotal) * 100).toFixed(0) : '0';

  // LTV:CAC
  const avgMRRPerCustomer = mrr.count > 0 ? mrr.total / mrr.count : 0;
  const avgLifetimeMonths = 12; // assumed
  const ltv = avgMRRPerCustomer * avgLifetimeMonths;
  const totalCosts = Object.values(MONTHLY_COSTS).reduce((s, v) => s + v, 0);
  const dealsCreatedLast30 = deals.filter(d => {
    const cd = new Date(d.properties.createdate);
    return (Date.now() - cd) / 86400000 <= 30;
  }).length;
  const cac = dealsCreatedLast30 > 0 ? totalCosts / dealsCreatedLast30 : totalCosts;
  const ltvCac = cac > 0 ? (ltv / cac).toFixed(1) : 'N/A';

  // Strategic insights
  const insights = [];
  if (mrr.total > 0) {
    insights.push(`MRR is $${mrr.total.toLocaleString()} across ${mrr.count} customers.`);
  }
  if (churn.churnCount > 0) {
    insights.push(`${churn.churnCount} customer(s) churned in the last 30 days ($${churn.churnMRR} MRR lost).`);
  }
  if (parseInt(winRate) > 30) {
    insights.push(`Win rate at ${winRate}% is healthy.`);
  } else if (parseInt(winRate) > 0) {
    insights.push(`Win rate at ${winRate}% — review loss reasons to improve.`);
  }
  const staleDeals = deals.filter(d => {
    if (['closedlost', 'decisionmakerboughtin'].includes(d.properties.dealstage)) return false;
    return (Date.now() - new Date(d.properties.hs_lastmodifieddate)) / 86400000 > 14;
  });
  if (staleDeals.length > 0) {
    insights.push(`${staleDeals.length} deal(s) stale for 14+ days — need attention.`);
  }
  if (pnl.profit > 0) {
    insights.push(`Monthly P&L is positive: $${pnl.profit.toLocaleString()} profit (${pnl.margin}% margin).`);
  } else {
    insights.push(`Monthly P&L is negative: -$${Math.abs(pnl.profit).toLocaleString()}. Revenue needs to exceed $${totalCosts.toLocaleString()}/mo.`);
  }

  return {
    mrr, churn, pipeline, forecast, repStats, dayMetrics, channels, pnl,
    instantly: instantlyMetrics,
    funnel, winRate, ltvCac, ltv: Math.round(ltv), cac: Math.round(cac),
    insights, owners,
    activeTrials: activeTrials || [],
    deals,
    today,
    lastRefreshed: new Date().toISOString(),
  };
}

// ══════════════════════════════════════════
// SEED DAILY TASKS (SOP Priority Queue)
// ══════════════════════════════════════════
function seedDailyTasks(dateStr, data, repName) {
  const existing = getTasksForDate(dateStr);
  if (existing.length > 0) return existing;

  const tasks = [];
  if (!data) return tasks;

  // 1. Expiring trials
  if (data.activeTrials) {
    const now = new Date();
    const trials = data.activeTrials
      .filter(c => {
        const end = c.properties.sammy_trial_end_date;
        if (!end) return false;
        const endDate = new Date(end);
        const daysLeft = Math.ceil((endDate - now) / 86400000);
        return daysLeft <= 3 && daysLeft >= -2;
      })
      .sort((a, b) => new Date(a.properties.sammy_trial_end_date) - new Date(b.properties.sammy_trial_end_date));

    for (const c of trials.slice(0, 10)) {
      const name = [c.properties.firstname, c.properties.lastname].filter(Boolean).join(' ') || c.properties.email || 'Unknown';
      const end = new Date(c.properties.sammy_trial_end_date);
      const daysLeft = Math.ceil((end - now) / 86400000);
      const urgency = daysLeft <= 0 ? 'EXPIRED' : daysLeft === 1 ? 'Tomorrow' : `${daysLeft}d left`;
      tasks.push({
        id: generateTaskId(), text: `Call ${name} — trial ${urgency}`,
        done: false, source: 'auto', category: 'expiring_trial',
        severity: daysLeft <= 0 ? 'critical' : 'warning',
        createdAt: new Date().toISOString(),
      });
    }
  }

  // 2. Warm leads (Demo Booked + Demo Complete deals)
  if (data.deals) {
    const warmStages = ['2843565802', '2851995329'];
    const warm = data.deals
      .filter(d => warmStages.includes(d.properties.dealstage))
      .sort((a, b) => new Date(a.properties.hs_lastmodifieddate) - new Date(b.properties.hs_lastmodifieddate));
    for (const d of warm.slice(0, 5)) {
      const stage = STAGES.find(s => s.id === d.properties.dealstage);
      tasks.push({
        id: generateTaskId(), text: `${d.properties.dealname} — ${stage?.label || 'Pipeline'}`,
        done: false, source: 'auto', category: 'warm_lead', severity: 'warning',
        createdAt: new Date().toISOString(),
      });
    }
  }

  // 3. Stale pipeline (7+ days)
  if (data.deals) {
    const stale = data.deals
      .filter(d => {
        if (['closedlost', 'decisionmakerboughtin'].includes(d.properties.dealstage)) return false;
        const mod = new Date(d.properties.hs_lastmodifieddate);
        return (Date.now() - mod) / 86400000 > 7;
      })
      .sort((a, b) => new Date(a.properties.hs_lastmodifieddate) - new Date(b.properties.hs_lastmodifieddate))
      .slice(0, 5);
    for (const d of stale) {
      const days = Math.round((Date.now() - new Date(d.properties.hs_lastmodifieddate)) / 86400000);
      tasks.push({
        id: generateTaskId(), text: `Follow up: ${d.properties.dealname} — ${days}d stale`,
        done: false, source: 'auto', category: 'stale_deal', severity: days > 14 ? 'critical' : 'info',
        createdAt: new Date().toISOString(),
      });
    }
  }

  // 4. Daily targets
  for (const rn of ACTIVE_REPS) {
    const t = REP_TARGETS[rn];
    if (!t) continue;
    tasks.push({
      id: generateTaskId(), text: `${rn.split(' ')[0]}: Hit ${t.uniqueCalls} unique dials`,
      done: false, source: 'auto', category: 'daily_target', severity: 'info',
      createdAt: new Date().toISOString(),
    });
  }

  if (tasks.length > 0) saveTasksForDate(dateStr, tasks);
  return tasks;
}

// ══════════════════════════════════════════
// HTML GENERATION
// ══════════════════════════════════════════
function generateHTML(data, { view = 'rep', rep = '', date = '' } = {}) {
  const today = date || data.today || getTodayMelbourne();
  const selectedRep = rep || ACTIVE_REPS[0];
  const dm = data.dayMetrics || {};
  const repDay = dm[selectedRep] || {};
  const repStat = (data.repStats || {})[selectedRep] || {};
  const tasks = seedDailyTasks(today, data, selectedRep);
  const tasksDone = tasks.filter(t => t.done).length;
  const tasksPct = tasks.length > 0 ? Math.round((tasksDone / tasks.length) * 100) : 0;

  // Build pipeline deals for selected rep
  const repPipelineDeals = {};
  for (const s of STAGES) {
    const stageData = (data.pipeline || {})[s.id];
    if (!stageData) continue;
    const filtered = stageData.deals.filter(d => !rep || d.owner === selectedRep || rep === '');
    repPipelineDeals[s.id] = { ...stageData, deals: filtered, count: filtered.length };
  }

  const safeData = {
    mrr: data.mrr, churn: data.churn, pipeline: data.pipeline, forecast: data.forecast,
    repStats: data.repStats, dayMetrics: data.dayMetrics, channels: data.channels,
    pnl: data.pnl, instantly: data.instantly, funnel: data.funnel,
    winRate: data.winRate, ltvCac: data.ltvCac, ltv: data.ltv, cac: data.cac,
    insights: data.insights, today: data.today, lastRefreshed: data.lastRefreshed,
    activeReps: ACTIVE_REPS, stages: STAGES, commissionPerClose: COMMISSION_PER_CLOSE,
  };

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Sammy Dashboard</title>
<script src="https://cdn.tailwindcss.com"></script>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
  .view-panel { display: none; }
  .view-panel.active { display: block; }
  .task-done { text-decoration: line-through; opacity: 0.5; }
  .card { background: white; border-radius: 12px; box-shadow: 0 1px 3px rgba(0,0,0,0.08), 0 1px 2px rgba(0,0,0,0.06); border: 1px solid #e5e7eb; }
  .kpi-card { background: white; border-radius: 12px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); border: 1px solid #e5e7eb; }
  .badge { display: inline-flex; align-items: center; padding: 2px 8px; border-radius: 9999px; font-size: 11px; font-weight: 600; }
  .badge-critical { background: #fef2f2; color: #dc2626; }
  .badge-warning { background: #fffbeb; color: #d97706; }
  .badge-info { background: #eff6ff; color: #3b82f6; }
  .badge-success { background: #f0fdf4; color: #16a34a; }
  .progress-bar { height: 8px; border-radius: 4px; background: #f3f4f6; overflow: hidden; }
  .progress-fill { height: 100%; border-radius: 4px; transition: width 0.5s ease; }
  .stage-bar { height: 32px; border-radius: 6px; display: flex; align-items: center; padding: 0 12px; font-size: 13px; font-weight: 500; min-width: 40px; transition: width 0.5s ease; }
  .health-dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
  @media (max-width: 768px) {
    .kpi-grid { grid-template-columns: repeat(2, 1fr) !important; }
    .desktop-only { display: none; }
  }
</style>
</head>
<body class="bg-gray-50 min-h-screen">

<!-- Header -->
<header class="bg-white border-b border-gray-200 sticky top-0 z-50">
  <div class="max-w-7xl mx-auto px-4 py-3 flex items-center justify-between flex-wrap gap-3">
    <div class="flex items-center gap-4">
      <h1 class="text-xl font-bold text-gray-900">Sammy</h1>
      <div class="flex bg-gray-100 rounded-lg p-0.5">
        <button onclick="switchView('rep')" id="btn-rep" class="px-4 py-1.5 text-sm font-medium rounded-md transition-all ${view === 'rep' ? 'bg-white shadow text-gray-900' : 'text-gray-500 hover:text-gray-700'}">Rep</button>
        <button onclick="switchView('revenue')" id="btn-revenue" class="px-4 py-1.5 text-sm font-medium rounded-md transition-all ${view === 'revenue' ? 'bg-white shadow text-gray-900' : 'text-gray-500 hover:text-gray-700'}">Revenue</button>
      </div>
    </div>
    <div class="flex items-center gap-3 flex-wrap">
      <select id="rep-select" onchange="changeRep(this.value)" class="text-sm border border-gray-200 rounded-lg px-3 py-1.5 bg-white">
        ${ACTIVE_REPS.map(r => `<option value="${r}" ${r === selectedRep ? 'selected' : ''}>${r}</option>`).join('')}
      </select>
      <input type="date" id="date-picker" value="${today}" onchange="changeDate(this.value)" class="text-sm border border-gray-200 rounded-lg px-3 py-1.5 bg-white" />
      <a href="/refresh" class="text-sm text-blue-600 hover:text-blue-800 font-medium">Refresh</a>
      <span class="text-xs text-gray-400">${new Date(data.lastRefreshed).toLocaleTimeString('en-AU', { timeZone: 'Australia/Melbourne', hour: '2-digit', minute: '2-digit' })} AEST</span>
    </div>
  </div>
</header>

<main class="max-w-7xl mx-auto px-4 py-6">

<!-- ═══════════════════════════════ REP VIEW ═══════════════════════════════ -->
<div id="view-rep" class="view-panel ${view === 'rep' ? 'active' : ''}">

  <!-- Today's Queue -->
  <div class="card p-6 mb-6">
    <div class="flex items-center justify-between mb-4">
      <div>
        <h2 class="text-lg font-semibold text-gray-900">Today's Queue</h2>
        <p class="text-sm text-gray-500">${tasksDone}/${tasks.length} complete</p>
      </div>
      <div class="text-right">
        <div class="text-2xl font-bold ${tasksPct === 100 ? 'text-green-600' : tasksPct > 50 ? 'text-blue-600' : 'text-gray-900'}">${tasksPct}%</div>
      </div>
    </div>
    <div class="progress-bar mb-4">
      <div class="progress-fill ${tasksPct === 100 ? 'bg-green-500' : tasksPct > 50 ? 'bg-blue-500' : 'bg-gray-400'}" style="width:${tasksPct}%"></div>
    </div>
    <div id="task-list" class="space-y-2 mb-4">
      ${tasks.map(t => {
        const catBadge = t.category === 'expiring_trial' ? '<span class="badge badge-critical">Trial</span>'
          : t.category === 'warm_lead' ? '<span class="badge badge-warning">Pipeline</span>'
          : t.category === 'stale_deal' ? '<span class="badge badge-info">Follow-up</span>'
          : t.category === 'daily_target' ? '<span class="badge badge-success">Target</span>'
          : '<span class="badge badge-info">Task</span>';
        return `<div class="flex items-center gap-3 py-2 px-3 rounded-lg hover:bg-gray-50 group ${t.done ? 'task-done' : ''}">
          <input type="checkbox" ${t.done ? 'checked' : ''} onchange="toggleTask('${t.id}')" class="w-4 h-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500" />
          <span class="flex-1 text-sm text-gray-700">${escapeHtml(t.text)}</span>
          ${catBadge}
          <button onclick="deleteTask('${t.id}')" class="opacity-0 group-hover:opacity-100 text-gray-400 hover:text-red-500 text-sm transition-opacity">&times;</button>
        </div>`;
      }).join('')}
    </div>
    <div class="flex gap-2">
      <input type="text" id="new-task-input" placeholder="Add a task..." class="flex-1 text-sm border border-gray-200 rounded-lg px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent" onkeydown="if(event.key==='Enter')addTask()" />
      <button onclick="addTask()" class="px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 transition-colors">Add</button>
    </div>
  </div>

  <!-- My Numbers -->
  <h2 class="text-lg font-semibold text-gray-900 mb-3">My Numbers</h2>
  <div class="grid grid-cols-2 md:grid-cols-5 gap-4 mb-6 kpi-grid" style="grid-template-columns: repeat(5, 1fr);">
    <!-- Dials -->
    <div class="kpi-card">
      <div class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">Unique Dials</div>
      <div class="text-2xl font-bold text-gray-900">${repDay.uniqueDials || 0}</div>
      <div class="text-xs text-gray-500 mb-2">of ${repDay.dialTarget || 100} target</div>
      <div class="progress-bar">
        <div class="progress-fill ${(repDay.dialPct || 0) >= 100 ? 'bg-green-500' : (repDay.dialPct || 0) >= 50 ? 'bg-blue-500' : 'bg-amber-500'}" style="width:${repDay.dialPct || 0}%"></div>
      </div>
    </div>
    <!-- Talk Time -->
    <div class="kpi-card">
      <div class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">Talk Time</div>
      <div class="text-2xl font-bold text-gray-900">${repDay.talkTimeHrs || '0.0'}h</div>
      <div class="text-xs text-gray-500 mb-2">of ${repDay.talkTarget || 5}h target</div>
      <div class="progress-bar">
        <div class="progress-fill ${(repDay.talkPct || 0) >= 100 ? 'bg-green-500' : (repDay.talkPct || 0) >= 50 ? 'bg-blue-500' : 'bg-amber-500'}" style="width:${repDay.talkPct || 0}%"></div>
      </div>
    </div>
    <!-- Demos -->
    <div class="kpi-card">
      <div class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">Demos This Week</div>
      <div class="text-2xl font-bold text-gray-900">${repDay.weekDemos || 0}</div>
      <div class="text-xs text-gray-500">booked</div>
    </div>
    <!-- Closes -->
    <div class="kpi-card">
      <div class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">Closes This Month</div>
      <div class="text-2xl font-bold text-green-600">${repDay.monthCloses || 0}</div>
      <div class="text-xs text-gray-500">$${(repDay.monthCloseValue || 0).toLocaleString()} value</div>
    </div>
    <!-- Commission -->
    <div class="kpi-card">
      <div class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">Commission YTD</div>
      <div class="text-2xl font-bold text-purple-600">$${(repDay.commissionYTD || 0).toLocaleString()}</div>
      <div class="text-xs text-gray-500">$${(repDay.commissionMonth || 0).toLocaleString()} this month</div>
    </div>
  </div>

  <!-- My Pipeline -->
  <h2 class="text-lg font-semibold text-gray-900 mb-3">My Pipeline</h2>
  <div class="card p-6 mb-6">
    ${STAGES.filter(s => !['closedlost', 'decisionmakerboughtin'].includes(s.id)).map(s => {
      const stageData = repPipelineDeals[s.id] || { deals: [], count: 0 };
      return `<div class="mb-4 last:mb-0">
        <div class="flex items-center justify-between mb-2">
          <span class="text-sm font-medium text-gray-700">${s.label}</span>
          <span class="text-sm text-gray-500">${stageData.count} deal${stageData.count !== 1 ? 's' : ''}</span>
        </div>
        ${stageData.deals.length > 0 ? `<div class="space-y-1.5">
          ${stageData.deals.map(d => {
            const health = d.daysSinceMod <= 3 ? 'bg-green-500' : d.daysSinceMod <= 7 ? 'bg-amber-500' : 'bg-red-500';
            return `<div class="flex items-center gap-3 py-1.5 px-3 bg-gray-50 rounded-lg text-sm">
              <span class="health-dot ${health}"></span>
              <span class="flex-1 text-gray-700">${escapeHtml(d.name)}</span>
              <span class="text-gray-400 text-xs">${d.daysSinceMod}d ago</span>
              ${d.value > 0 ? `<span class="text-gray-600 font-medium">$${d.value}</span>` : ''}
            </div>`;
          }).join('')}
        </div>` : '<div class="text-sm text-gray-400 italic">No deals in this stage</div>'}
      </div>`;
    }).join('<hr class="my-3 border-gray-100">')}
  </div>

  <!-- Rep 30-Day Summary -->
  <h2 class="text-lg font-semibold text-gray-900 mb-3">30-Day Summary</h2>
  <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
    <div class="kpi-card">
      <div class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">Total Deals</div>
      <div class="text-2xl font-bold text-gray-900">${repStat.totalDeals || 0}</div>
    </div>
    <div class="kpi-card">
      <div class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">Won</div>
      <div class="text-2xl font-bold text-green-600">${repStat.wonDeals || 0}</div>
      <div class="text-xs text-gray-500">$${(repStat.wonValue || 0).toLocaleString()}</div>
    </div>
    <div class="kpi-card">
      <div class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">Open Pipeline</div>
      <div class="text-2xl font-bold text-blue-600">${repStat.openDeals || 0}</div>
      <div class="text-xs text-gray-500">$${(repStat.pipelineValue || 0).toLocaleString()}</div>
    </div>
    <div class="kpi-card">
      <div class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">Calls (30d)</div>
      <div class="text-2xl font-bold text-gray-900">${repStat.totalCalls30d || 0}</div>
      <div class="text-xs text-gray-500">${repStat.uniqueNumbers30d || 0} unique</div>
    </div>
  </div>
</div>

<!-- ═══════════════════════════════ REVENUE VIEW ═══════════════════════════════ -->
<div id="view-revenue" class="view-panel ${view === 'revenue' ? 'active' : ''}">

  <!-- Scoreboard -->
  <h2 class="text-lg font-semibold text-gray-900 mb-3">Scoreboard</h2>
  <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6 kpi-grid" style="grid-template-columns: repeat(4, 1fr);">
    <!-- MRR -->
    <div class="kpi-card">
      <div class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">MRR</div>
      <div class="text-3xl font-bold text-gray-900">$${(data.mrr?.total || 0).toLocaleString()}</div>
      <div class="text-xs text-gray-500 mt-1">
        ${data.mrr?.breakdown ? Object.entries(data.mrr.breakdown).filter(([, v]) => v > 0).map(([k, v]) => {
          const price = PLAN_PRICING[k] || PLAN_PRICING.default;
          return `${v} @ $${price}`;
        }).join(' / ') : ''}
      </div>
    </div>
    <!-- Net MRR -->
    <div class="kpi-card">
      <div class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">Net MRR (30d)</div>
      <div class="text-3xl font-bold ${(data.pnl?.netMRR || 0) >= 0 ? 'text-green-600' : 'text-red-600'}">$${(data.pnl?.netMRR || 0).toLocaleString()}</div>
      <div class="text-xs text-gray-500 mt-1">-$${(data.churn?.churnMRR || 0).toLocaleString()} churn</div>
    </div>
    <!-- Customers -->
    <div class="kpi-card">
      <div class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">Customers</div>
      <div class="text-3xl font-bold text-gray-900">${data.funnel?.paid || 0}</div>
      <div class="text-xs text-gray-500 mt-1">${data.funnel?.activeTrial || 0} trials / ${data.churn?.churnCount || 0} churned (30d)</div>
    </div>
    <!-- P&L -->
    <div class="kpi-card">
      <div class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">Monthly P&L</div>
      <div class="text-3xl font-bold ${(data.pnl?.profit || 0) >= 0 ? 'text-green-600' : 'text-red-600'}">
        ${(data.pnl?.profit || 0) >= 0 ? '' : '-'}$${Math.abs(data.pnl?.profit || 0).toLocaleString()}
      </div>
      <div class="text-xs text-gray-500 mt-1">${data.pnl?.margin || 0}% margin</div>
    </div>
  </div>

  <!-- Second row: Win Rate, LTV:CAC, Funnel, Forecast -->
  <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6 kpi-grid" style="grid-template-columns: repeat(4, 1fr);">
    <div class="kpi-card">
      <div class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">Win Rate</div>
      <div class="text-3xl font-bold text-gray-900">${data.winRate || 0}%</div>
    </div>
    <div class="kpi-card">
      <div class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">LTV:CAC</div>
      <div class="text-3xl font-bold ${parseFloat(data.ltvCac) >= 3 ? 'text-green-600' : 'text-amber-600'}">${data.ltvCac || 'N/A'}x</div>
      <div class="text-xs text-gray-500 mt-1">LTV $${(data.ltv || 0).toLocaleString()} / CAC $${(data.cac || 0).toLocaleString()}</div>
    </div>
    <div class="kpi-card">
      <div class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">Funnel</div>
      <div class="text-sm text-gray-700 mt-1 space-y-0.5">
        <div class="flex justify-between"><span>No Status</span><span class="font-medium">${data.funnel?.noStatus || 0}</span></div>
        <div class="flex justify-between"><span>Incomplete</span><span class="font-medium">${data.funnel?.incomplete || 0}</span></div>
        <div class="flex justify-between"><span>Active Trial</span><span class="font-medium">${data.funnel?.activeTrial || 0}</span></div>
        <div class="flex justify-between"><span>Paid</span><span class="font-medium text-green-600">${data.funnel?.paid || 0}</span></div>
        <div class="flex justify-between"><span>Expired</span><span class="font-medium">${data.funnel?.expired || 0}</span></div>
        <div class="flex justify-between"><span>Churned</span><span class="font-medium text-red-600">${data.funnel?.churned || 0}</span></div>
      </div>
    </div>
    <div class="kpi-card">
      <div class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">Weighted Forecast</div>
      <div class="text-3xl font-bold text-blue-600">$${Math.round(data.forecast || 0).toLocaleString()}</div>
      <div class="text-xs text-gray-500 mt-1">pipeline-weighted</div>
    </div>
  </div>

  <!-- Channel Performance -->
  <h2 class="text-lg font-semibold text-gray-900 mb-3">Channel Performance</h2>
  <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
    ${['cold_call', 'cold_email', 'inbound'].map(ch => {
      const c = (data.channels || {})[ch] || {};
      return `<div class="card p-5">
        <h3 class="font-semibold text-gray-900 mb-3">${c.label || ch}</h3>
        <div class="space-y-2 text-sm">
          <div class="flex justify-between"><span class="text-gray-500">Deals</span><span class="font-medium">${c.deals || 0}</span></div>
          <div class="flex justify-between"><span class="text-gray-500">Won</span><span class="font-medium text-green-600">${c.won || 0}</span></div>
          <div class="flex justify-between"><span class="text-gray-500">Revenue</span><span class="font-medium">$${(c.revenue || 0).toLocaleString()}</span></div>
          <div class="flex justify-between"><span class="text-gray-500">Win Rate</span><span class="font-medium">${c.winRate || 0}%</span></div>
          ${ch !== 'inbound' ? `<div class="flex justify-between"><span class="text-gray-500">ROI</span><span class="font-medium ${parseInt(c.roi) > 0 ? 'text-green-600' : 'text-red-600'}">${c.roi || 'N/A'}%</span></div>` : ''}
        </div>
      </div>`;
    }).join('')}
  </div>

  <!-- Instantly / Cold Email -->
  <h2 class="text-lg font-semibold text-gray-900 mb-3">Cold Email (Instantly)</h2>
  <div class="card p-5 mb-6">
    <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
      <div>
        <div class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">Campaigns</div>
        <div class="text-2xl font-bold text-gray-900">${data.instantly?.totalCampaigns || 0}</div>
        <div class="text-xs text-gray-500">${data.instantly?.activeCampaigns || 0} active</div>
      </div>
      <div>
        <div class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">Sending Accounts</div>
        <div class="text-2xl font-bold text-gray-900">${data.instantly?.sendingAccounts || 0}</div>
      </div>
      <div>
        <div class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">Paused</div>
        <div class="text-2xl font-bold text-gray-900">${data.instantly?.pausedCampaigns || 0}</div>
      </div>
      <div>
        <div class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-1">Monthly Cost</div>
        <div class="text-2xl font-bold text-gray-900">$${MONTHLY_COSTS['Cold Email'].toLocaleString()}</div>
      </div>
    </div>
    ${(data.instantly?.campaigns || []).length > 0 ? `
    <div class="mt-4 border-t border-gray-100 pt-4">
      <div class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-2">Campaign List</div>
      <div class="space-y-1">
        ${(data.instantly.campaigns || []).slice(0, 10).map(c => `
          <div class="flex items-center gap-2 text-sm">
            <span class="health-dot ${c.status === 'active' || c.status === 1 ? 'bg-green-500' : 'bg-gray-400'}"></span>
            <span class="text-gray-700">${escapeHtml(c.name || 'Unnamed')}</span>
            <span class="text-gray-400 text-xs ml-auto">${c.status || 'unknown'}</span>
          </div>
        `).join('')}
      </div>
    </div>` : ''}
  </div>

  <!-- Team Performance -->
  <h2 class="text-lg font-semibold text-gray-900 mb-3">Team Performance</h2>
  <div class="card overflow-hidden mb-6">
    <table class="w-full text-sm">
      <thead class="bg-gray-50 border-b border-gray-200">
        <tr>
          <th class="text-left py-3 px-4 font-medium text-gray-500">Rep</th>
          <th class="text-right py-3 px-4 font-medium text-gray-500">Deals</th>
          <th class="text-right py-3 px-4 font-medium text-gray-500">Won</th>
          <th class="text-right py-3 px-4 font-medium text-gray-500">Won Value</th>
          <th class="text-right py-3 px-4 font-medium text-gray-500">Open</th>
          <th class="text-right py-3 px-4 font-medium text-gray-500">Pipeline $</th>
          <th class="text-right py-3 px-4 font-medium text-gray-500 desktop-only">Calls (30d)</th>
          <th class="text-right py-3 px-4 font-medium text-gray-500 desktop-only">Talk Time</th>
        </tr>
      </thead>
      <tbody>
        ${ACTIVE_REPS.map(name => {
          const rs = (data.repStats || {})[name] || {};
          return `<tr class="border-b border-gray-100 hover:bg-gray-50">
            <td class="py-3 px-4 font-medium text-gray-900">${name}</td>
            <td class="py-3 px-4 text-right text-gray-700">${rs.totalDeals || 0}</td>
            <td class="py-3 px-4 text-right text-green-600 font-medium">${rs.wonDeals || 0}</td>
            <td class="py-3 px-4 text-right text-gray-700">$${(rs.wonValue || 0).toLocaleString()}</td>
            <td class="py-3 px-4 text-right text-gray-700">${rs.openDeals || 0}</td>
            <td class="py-3 px-4 text-right text-gray-700">$${(rs.pipelineValue || 0).toLocaleString()}</td>
            <td class="py-3 px-4 text-right text-gray-700 desktop-only">${rs.totalCalls30d || 0}</td>
            <td class="py-3 px-4 text-right text-gray-700 desktop-only">${rs.talkTime30d ? (rs.talkTime30d / 3600).toFixed(1) + 'h' : '0h'}</td>
          </tr>`;
        }).join('')}
      </tbody>
    </table>
  </div>

  <!-- Pipeline Health -->
  <h2 class="text-lg font-semibold text-gray-900 mb-3">Pipeline Health</h2>
  <div class="card p-6 mb-6">
    <!-- Funnel bars -->
    <div class="space-y-2 mb-6">
      ${(() => {
        const maxCount = Math.max(1, ...STAGES.map(s => (data.pipeline?.[s.id]?.count || 0)));
        const colors = {
          appointmentscheduled: '#93c5fd', presentationscheduled: '#60a5fa',
          '2843565802': '#3b82f6', '2851995329': '#2563eb',
          '2054317800': '#f59e0b', '2845034230': '#d97706',
          closedlost: '#ef4444', decisionmakerboughtin: '#22c55e',
        };
        return STAGES.map(s => {
          const pd = data.pipeline?.[s.id] || { count: 0, value: 0 };
          const pct = Math.max(5, (pd.count / maxCount) * 100);
          return `<div class="flex items-center gap-3">
            <div class="w-36 text-sm text-gray-600 text-right flex-shrink-0">${s.label}</div>
            <div class="stage-bar text-white" style="width:${pct}%; background:${colors[s.id] || '#6b7280'}">${pd.count}</div>
            <div class="text-sm text-gray-500 flex-shrink-0">$${(pd.value || 0).toLocaleString()}</div>
          </div>`;
        }).join('');
      })()}
    </div>

    <!-- Stale deals list -->
    ${(() => {
      const staleDeals = [];
      for (const s of STAGES) {
        if (['closedlost', 'decisionmakerboughtin'].includes(s.id)) continue;
        const sd = data.pipeline?.[s.id];
        if (!sd) continue;
        for (const d of sd.deals) {
          if (d.daysSinceMod > 7) staleDeals.push({ ...d, stageLabel: s.label });
        }
      }
      staleDeals.sort((a, b) => b.daysSinceMod - a.daysSinceMod);
      if (staleDeals.length === 0) return '';
      return `<div class="border-t border-gray-100 pt-4">
        <h3 class="text-sm font-medium text-gray-700 mb-2">Stale Deals (7+ days)</h3>
        <div class="space-y-1.5">
          ${staleDeals.slice(0, 10).map(d => `
            <div class="flex items-center gap-3 text-sm py-1.5 px-3 bg-red-50 rounded-lg">
              <span class="health-dot bg-red-500"></span>
              <span class="flex-1 text-gray-700">${escapeHtml(d.name)}</span>
              <span class="text-gray-500">${d.stageLabel}</span>
              <span class="text-gray-400">${d.owner}</span>
              <span class="font-medium text-red-600">${d.daysSinceMod}d</span>
            </div>
          `).join('')}
        </div>
      </div>`;
    })()}
  </div>

  <!-- Cost Breakdown -->
  <h2 class="text-lg font-semibold text-gray-900 mb-3">Cost Breakdown</h2>
  <div class="card p-5 mb-6">
    <div class="space-y-2">
      ${Object.entries(MONTHLY_COSTS).map(([name, cost]) => {
        const totalCosts = Object.values(MONTHLY_COSTS).reduce((s, v) => s + v, 0);
        const pct = Math.round((cost / totalCosts) * 100);
        return `<div class="flex items-center gap-3">
          <span class="w-28 text-sm text-gray-600">${name}</span>
          <div class="flex-1 progress-bar">
            <div class="progress-fill bg-blue-500" style="width:${pct}%"></div>
          </div>
          <span class="text-sm font-medium text-gray-900 w-16 text-right">$${cost.toLocaleString()}</span>
        </div>`;
      }).join('')}
      <div class="flex items-center gap-3 border-t border-gray-100 pt-2 mt-2">
        <span class="w-28 text-sm font-semibold text-gray-900">Total</span>
        <div class="flex-1"></div>
        <span class="text-sm font-bold text-gray-900 w-16 text-right">$${Object.values(MONTHLY_COSTS).reduce((s, v) => s + v, 0).toLocaleString()}</span>
      </div>
    </div>
  </div>

  <!-- Strategic Insights -->
  <h2 class="text-lg font-semibold text-gray-900 mb-3">Strategic Insights</h2>
  <div class="card p-5 mb-6">
    <div class="space-y-3">
      ${(data.insights || []).map(insight => `
        <div class="flex gap-3 text-sm">
          <span class="text-blue-500 mt-0.5 flex-shrink-0">&#9679;</span>
          <span class="text-gray-700">${escapeHtml(insight)}</span>
        </div>
      `).join('')}
    </div>
  </div>
</div>

</main>

<footer class="max-w-7xl mx-auto px-4 py-6 text-center text-xs text-gray-400">
  Sammy Dashboard &middot; Data refreshes every 5 minutes &middot; <a href="https://app.hubspot.com/contacts/${HUBSPOT_PORTAL}" target="_blank" class="text-blue-500 hover:underline">HubSpot</a>
</footer>

<script>
const D = ${JSON.stringify(safeData)};
const TODAY = '${today}';
const SELECTED_REP = '${selectedRep}';

function switchView(v) {
  document.querySelectorAll('.view-panel').forEach(el => el.classList.remove('active'));
  document.getElementById('view-' + v).classList.add('active');
  document.getElementById('btn-rep').className = 'px-4 py-1.5 text-sm font-medium rounded-md transition-all ' +
    (v === 'rep' ? 'bg-white shadow text-gray-900' : 'text-gray-500 hover:text-gray-700');
  document.getElementById('btn-revenue').className = 'px-4 py-1.5 text-sm font-medium rounded-md transition-all ' +
    (v === 'revenue' ? 'bg-white shadow text-gray-900' : 'text-gray-500 hover:text-gray-700');
  const url = new URL(window.location);
  url.searchParams.set('view', v);
  window.history.replaceState({}, '', url);
}

function changeRep(name) {
  const url = new URL(window.location);
  url.searchParams.set('rep', name);
  window.location = url;
}

function changeDate(d) {
  const url = new URL(window.location);
  url.searchParams.set('date', d);
  window.location = url;
}

async function toggleTask(id) {
  try {
    const res = await fetch('/api/tasks/' + id + '/toggle', { method: 'PATCH' });
    if (res.ok) window.location.reload();
  } catch (e) { console.error('Toggle failed:', e); }
}

async function addTask() {
  const input = document.getElementById('new-task-input');
  const text = input.value.trim();
  if (!text) return;
  try {
    const res = await fetch('/api/tasks', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text, date: TODAY }),
    });
    if (res.ok) window.location.reload();
  } catch (e) { console.error('Add failed:', e); }
}

async function deleteTask(id) {
  if (!confirm('Delete this task?')) return;
  try {
    const res = await fetch('/api/tasks/' + id + '?date=' + TODAY, { method: 'DELETE' });
    if (res.ok) window.location.reload();
  } catch (e) { console.error('Delete failed:', e); }
}
</script>
</body>
</html>`;
}

function escapeHtml(str) {
  if (!str) return '';
  return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function loadingHTML() {
  return `<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Sammy Dashboard</title>
<script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-50 min-h-screen flex items-center justify-center">
<div class="text-center">
  <div class="animate-spin w-12 h-12 border-4 border-blue-500 border-t-transparent rounded-full mx-auto mb-4"></div>
  <h1 class="text-xl font-semibold text-gray-900 mb-2">Loading Dashboard</h1>
  <p class="text-gray-500">Fetching data from HubSpot + Instantly...</p>
  <p class="text-gray-400 text-sm mt-2">This usually takes 15-30 seconds</p>
  <script>setTimeout(() => window.location.reload(), 10000);</script>
</div>
</body></html>`;
}

// ══════════════════════════════════════════
// CACHE REFRESH
// ══════════════════════════════════════════
async function refreshCache() {
  if (cache.loading) return;
  cache.loading = true;
  try {
    const raw = await fetchAllData();
    const metrics = computeMetrics(raw);
    cache.data = metrics;
    cache.time = Date.now();
    cache.lastSuccess = Date.now();
    cache.lastError = null;
    console.log('[cache] Refreshed successfully');
  } catch (err) {
    cache.lastError = err.message;
    console.error('[cache] Refresh failed:', err.message);
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

app.get('/api/tasks/:date', (req, res) => {
  const dateStr = req.params.date;
  const tasks = getTasksForDate(dateStr);
  res.json(tasks);
});

app.post('/api/tasks', (req, res) => {
  const { text, date } = req.body;
  if (!text) return res.status(400).json({ error: 'text required' });
  const dateStr = date || getTodayMelbourne();
  const tasks = getTasksForDate(dateStr);
  const task = {
    id: generateTaskId(),
    text,
    done: false,
    source: 'manual',
    category: 'manual',
    severity: 'info',
    createdAt: new Date().toISOString(),
  };
  tasks.push(task);
  saveTasksForDate(dateStr, tasks);
  res.json(task);
});

app.patch('/api/tasks/:id/toggle', (req, res) => {
  const dateStr = req.query.date || getTodayMelbourne();
  const tasks = getTasksForDate(dateStr);
  const task = tasks.find(t => t.id === req.params.id);
  if (!task) return res.status(404).json({ error: 'not found' });
  task.done = !task.done;
  saveTasksForDate(dateStr, tasks);
  res.json(task);
});

app.delete('/api/tasks/:id', (req, res) => {
  const dateStr = req.query.date || getTodayMelbourne();
  const tasks = getTasksForDate(dateStr);
  const idx = tasks.findIndex(t => t.id === req.params.id);
  if (idx < 0) return res.status(404).json({ error: 'not found' });
  tasks.splice(idx, 1);
  saveTasksForDate(dateStr, tasks);
  res.json({ ok: true });
});

app.get('/refresh', async (req, res) => {
  cache.time = 0;
  refreshCache();
  res.redirect('/');
});

app.get('/', async (req, res) => {
  if (Date.now() - cache.time > CACHE_TTL) refreshCache();
  if (cache.data) {
    const view = req.query.view || 'rep';
    const rep = req.query.rep || '';
    const date = req.query.date || '';
    res.type('html').send(generateHTML(cache.data, { view, rep, date }));
  } else {
    res.type('html').send(loadingHTML());
  }
});

app.listen(PORT, () => {
  console.log(`Dashboard on :${PORT}`);
  refreshCache();
});
