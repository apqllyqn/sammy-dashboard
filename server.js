require('dotenv').config();
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
const HUBSPOT_PORTAL = '244038625';
const CACHE_TTL = 5 * 60 * 1000; // 5 minutes

// ── EmailBison Config ──
const EB_BASE = 'https://spellcast.hirecharm.com';
const EB_TOKEN = process.env.EB_TOKEN || '25|Ln8AxgPNAYCfTn729my8B6S1zRkjg4Z9lPb9AXUr16075017';
const EB_WORKSPACE_ID = 8;
const EB_CAMPAIGN_PREFIX = 'Sammy';
const ebApi = axios.create({
  baseURL: EB_BASE,
  headers: { Authorization: `Bearer ${EB_TOKEN}`, 'Content-Type': 'application/json', Accept: 'application/json' },
});

const EB_MONTHLY_SPEND = 1000;

const REP_TARGETS = {
  'Lucas Gibson': { uniqueCalls: 100, callHours: 5, dailyRevenue: 297 },
  'Krishna Pryor': { uniqueCalls: 100, callHours: 5, dailyRevenue: 297 },
};

// ══════════════════════════════════════════
// CACHE
// ══════════════════════════════════════════
let cache = { data: null, time: 0, loading: false };

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
    const params = { limit: 100, properties: 'dealname,dealstage,pipeline,amount,expected_mrr,deal_source,hubspot_owner_id,closedate,createdate,hs_lastmodifieddate,hs_v2_date_entered_decisionmakerboughtin,hs_v2_date_entered_appointmentscheduled,hs_v2_date_entered_presentationscheduled,hs_v2_date_entered_2843565802,hs_v2_date_entered_2851995329,hs_v2_date_entered_closedlost' };
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
// EMAILBISON API HELPERS
// ══════════════════════════════════════════
async function switchEBWorkspace() {
  await ebApi.post('/api/workspaces/switch-workspace', { team_id: EB_WORKSPACE_ID });
}

async function fetchEBCampaigns() {
  const campaigns = [];
  let page = 1;
  while (true) {
    const { data } = await ebApi.get('/api/campaigns', { params: { page, per_page: 50 } });
    const items = data.data || [];
    campaigns.push(...items);
    if (!data.next_page_url || items.length < 50) break;
    page++;
    await sleep(300);
  }
  return campaigns.filter(c => c.name && c.name.startsWith(EB_CAMPAIGN_PREFIX));
}

async function fetchEBLeads(campaignId) {
  const leads = [];
  let page = 1;
  while (true) {
    const { data } = await ebApi.get(`/api/campaigns/${campaignId}/leads`, { params: { page, per_page: 100 } });
    const items = data.data || [];
    leads.push(...items);
    if (!data.next_page_url || items.length < 100) break;
    page++;
    await sleep(300);
  }
  return leads;
}

async function fetchAllEBData() {
  try {
    console.log('[eb] Fetching EmailBison data...');
    await switchEBWorkspace();
    const campaigns = await fetchEBCampaigns();
    console.log(`[eb] Found ${campaigns.length} Sammy campaigns`);
    const campaignsWithLeads = [];
    for (const c of campaigns) {
      const leads = await fetchEBLeads(c.id);
      campaignsWithLeads.push({ ...c, leads });
    }
    return campaignsWithLeads;
  } catch (err) {
    console.error('[eb] EmailBison fetch failed:', err.response?.data?.message || err.message);
    return null;
  }
}

// ══════════════════════════════════════════
// DATA FETCHING
// ══════════════════════════════════════════
async function fetchAllData() {
  console.log('[fetch] Starting HubSpot + EmailBison data fetch...');
  const t0 = Date.now();

  // Launch EB fetch in parallel — doesn't block HubSpot
  const ebPromise = fetchAllEBData();

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
    fetchEngagements('calls', 30, ['hs_call_duration', 'hs_call_to_number']),
    fetchEngagements('meetings'),
    fetchEngagements('notes'),
  ]);
  await sleep(500);

  const actFilters = [{ propertyName: 'user_status', operator: 'EQ', value: 'active_trial' }];
  const [actTotal, actEstimate, actQuoteSent] = await Promise.all([
    countContacts([{ filters: actFilters }]),
    countContacts([{ filters: [...actFilters, { propertyName: 'has_created_estimates', operator: 'EQ', value: 'true' }] }]),
    countContacts([{ filters: [...actFilters, { propertyName: 'estimates_sent', operator: 'GTE', value: '1' }] }]),
  ]);

  const ebData = await ebPromise;
  console.log(`[fetch] Done in ${((Date.now() - t0) / 1000).toFixed(1)}s — ${deals.length} deals, ${noStatus + incomplete + activeTrial + paid + expired + churned} contacts, EB: ${ebData ? ebData.length + ' campaigns' : 'unavailable'}`);

  return {
    owners, deals,
    funnel: { noStatus, incomplete, activeTrial, paid, expired, churned },
    paidCustomers,
    activity: { calls: calls || [], meetings: meetings || [], notes: notes || [] },
    activation: { total: actTotal, estimateCreated: actEstimate, quoteSent: actQuoteSent, paid },
    ebData,
  };
}

// ══════════════════════════════════════════
// METRIC COMPUTATION
// ══════════════════════════════════════════
function computeDayMetrics(dateStr, activity, deals, owners) {
  const day = { calls: 0, meetings: 0, notes: 0, byRep: {} };
  const activityTypes = { calls: activity.calls, meetings: activity.meetings, notes: activity.notes };
  for (const [type, records] of Object.entries(activityTypes)) {
    if (!records) continue;
    for (const r of records) {
      const melbDate = new Date(r.properties.hs_createdate).toLocaleDateString('en-CA', { timeZone: 'Australia/Melbourne' });
      if (melbDate === dateStr) {
        day[type]++;
        const oid = r.properties.hubspot_owner_id;
        const name = oid ? (owners[oid] || `Owner ${oid}`) : 'Unassigned';
        if (!day.byRep[name]) day.byRep[name] = { calls: 0, meetings: 0, notes: 0 };
        day.byRep[name][type]++;
      }
    }
  }
  day.total = day.calls + day.meetings + day.notes;

  const kpis = {};
  for (const [repName, targets] of Object.entries(REP_TARGETS)) {
    const uniqueNumbers = new Set();
    let callDurationMs = 0;
    if (activity.calls) {
      for (const r of activity.calls) {
        const melbDate = new Date(r.properties.hs_createdate).toLocaleDateString('en-CA', { timeZone: 'Australia/Melbourne' });
        if (melbDate !== dateStr) continue;
        const oid = r.properties.hubspot_owner_id;
        const name = oid ? (owners[oid] || `Owner ${oid}`) : 'Unassigned';
        if (name !== repName) continue;
        if (r.properties.hs_call_to_number) uniqueNumbers.add(r.properties.hs_call_to_number);
        callDurationMs += parseInt(r.properties.hs_call_duration || '0');
      }
    }
    let dailyRevenue = 0;
    for (const d of deals) {
      if (d.properties.dealstage !== 'decisionmakerboughtin') continue;
      const oid = d.properties.hubspot_owner_id;
      const name = oid ? (owners[oid] || `Owner ${oid}`) : 'Unassigned';
      if (name !== repName) continue;
      // Use hs_v2_date_entered (exact date deal entered Won stage), fallback to closedate
      const wonDate = new Date(d.properties.hs_v2_date_entered_decisionmakerboughtin || d.properties.closedate).toLocaleDateString('en-CA', { timeZone: 'Australia/Melbourne' });
      if (wonDate === dateStr) {
        dailyRevenue += parseFloat(d.properties.expected_mrr || d.properties.amount || '0') || PRICING.default;
      }
    }
    kpis[repName] = {
      uniqueCalls: uniqueNumbers.size,
      callMinutes: Math.round(callDurationMs / 60000),
      callHours: parseFloat((callDurationMs / 3600000).toFixed(1)),
      dailyRevenue: Math.round(dailyRevenue),
      targets,
    };
  }

  return { day, kpis };
}

function computeEBMetrics(ebData, deals, owners) {
  if (!ebData) return null;

  const campaignMetrics = [];
  let totLeads = 0, totSent = 0, totOpens = 0, totReplies = 0, totInterested = 0, totBounced = 0;
  const statusCounts = {};

  for (const c of ebData) {
    const leads = c.leads || [];
    let sent = 0, opens = 0, replies = 0, interested = 0, bounced = 0;
    for (const l of leads) {
      const s = (l.status || '').toLowerCase();
      statusCounts[s] = (statusCounts[s] || 0) + 1;
      if (s !== 'unverified' && s !== 'bounced') sent++;
      if (l.opened_count > 0 || s === 'opened' || s === 'replied' || s === 'interested') opens++;
      if (s === 'replied' || s === 'interested') replies++;
      if (s === 'interested') interested++;
      if (s === 'bounced') bounced++;
    }
    campaignMetrics.push({
      name: c.name, status: c.status || 'unknown', leads: leads.length,
      sent, opens, replies, interested, bounced,
      openRate: sent > 0 ? Math.round((opens / sent) * 100) : 0,
      replyRate: sent > 0 ? Math.round((replies / sent) * 100) : 0,
    });
    totLeads += leads.length; totSent += sent; totOpens += opens; totReplies += replies; totInterested += interested; totBounced += bounced;
  }

  const totals = {
    leads: totLeads, sent: totSent, opens: totOpens, replies: totReplies, interested: totInterested, bounced: totBounced,
    openRate: totSent > 0 ? Math.round((totOpens / totSent) * 100) : 0,
    replyRate: totSent > 0 ? Math.round((totReplies / totSent) * 100) : 0,
  };

  // Attribution: deals with deal_source === 'cold_email'
  const attribution = { total: 0, pipeline: 0, pipelineValue: 0, won: 0, wonMRR: 0, byRep: {} };
  for (const d of deals) {
    if (d.properties.deal_source !== 'cold_email') continue;
    attribution.total++;
    const mrr = parseFloat(d.properties.expected_mrr || d.properties.amount || '0') || PRICING.default;
    const repName = d.properties.hubspot_owner_id ? (owners[d.properties.hubspot_owner_id] || 'Unassigned') : 'Unassigned';
    if (!attribution.byRep[repName]) attribution.byRep[repName] = { total: 0, won: 0, wonMRR: 0, pipeline: 0, pipelineValue: 0 };
    const repAttr = attribution.byRep[repName];
    repAttr.total++;
    if (d.properties.dealstage === 'decisionmakerboughtin') {
      attribution.won++; attribution.wonMRR += mrr;
      repAttr.won++; repAttr.wonMRR += mrr;
    } else if (d.properties.dealstage !== 'closedlost') {
      attribution.pipeline++; attribution.pipelineValue += mrr;
      repAttr.pipeline++; repAttr.pipelineValue += mrr;
    }
  }
  attribution.wonMRR = Math.round(attribution.wonMRR);
  attribution.pipelineValue = Math.round(attribution.pipelineValue);
  for (const r of Object.values(attribution.byRep)) { r.wonMRR = Math.round(r.wonMRR); r.pipelineValue = Math.round(r.pipelineValue); }

  // EB Funnel stages
  const funnel = [
    { label: 'Leads Pushed', value: totLeads },
    { label: 'Sent', value: totSent },
    { label: 'Opened', value: totOpens },
    { label: 'Replied', value: totReplies },
    { label: 'Interested', value: totInterested },
    { label: 'Deals Created', value: attribution.total },
    { label: 'Deals Won', value: attribution.won },
  ];

  // Cost metrics
  const costs = {
    costPerLead: totLeads > 0 ? Math.round((EB_MONTHLY_SPEND / totLeads) * 100) / 100 : null,
    costPerReply: totReplies > 0 ? Math.round((EB_MONTHLY_SPEND / totReplies) * 100) / 100 : null,
    costPerDeal: attribution.total > 0 ? Math.round(EB_MONTHLY_SPEND / attribution.total) : null,
    monthlySpend: EB_MONTHLY_SPEND,
  };

  // Per-rep cold email deals list (for rep view)
  const repCEDeals = {};
  for (const d of deals) {
    if (d.properties.deal_source !== 'cold_email') continue;
    const repName = d.properties.hubspot_owner_id ? (owners[d.properties.hubspot_owner_id] || 'Unassigned') : 'Unassigned';
    if (!repCEDeals[repName]) repCEDeals[repName] = [];
    const stageInfo = STAGES.find(s => s.id === d.properties.dealstage);
    repCEDeals[repName].push({
      id: d.id, name: d.properties.dealname,
      stage: stageInfo?.label || d.properties.dealstage,
      value: Math.round(parseFloat(d.properties.expected_mrr || d.properties.amount || '0') || PRICING.default),
      age: Math.round((Date.now() - new Date(d.properties.createdate)) / 86400000),
      won: d.properties.dealstage === 'decisionmakerboughtin',
      lost: d.properties.dealstage === 'closedlost',
    });
  }

  return { campaignMetrics, totals, attribution, statusCounts, funnel, costs, repCEDeals };
}

function computeStageConversion(deals) {
  const linearStages = ['appointmentscheduled', 'presentationscheduled', '2843565802', '2851995329', 'decisionmakerboughtin'];
  const stageProps = {
    'appointmentscheduled': 'hs_v2_date_entered_appointmentscheduled',
    'presentationscheduled': 'hs_v2_date_entered_presentationscheduled',
    '2843565802': 'hs_v2_date_entered_2843565802',
    '2851995329': 'hs_v2_date_entered_2851995329',
    'decisionmakerboughtin': 'hs_v2_date_entered_decisionmakerboughtin',
  };
  const entered = {};
  for (const sid of linearStages) entered[sid] = 0;
  for (const d of deals) {
    for (const sid of linearStages) {
      if (d.properties[stageProps[sid]]) entered[sid]++;
    }
  }
  const conversions = [];
  for (let i = 0; i < linearStages.length - 1; i++) {
    conversions.push({
      from: STAGES.find(s => s.id === linearStages[i])?.label || linearStages[i],
      to: STAGES.find(s => s.id === linearStages[i + 1])?.label || linearStages[i + 1],
      fromCount: entered[linearStages[i]], toCount: entered[linearStages[i + 1]],
      rate: entered[linearStages[i]] > 0 ? Math.round((entered[linearStages[i + 1]] / entered[linearStages[i]]) * 100) : 0,
    });
  }
  return conversions;
}

function computeSourceCycle(deals) {
  const bySrc = {};
  for (const d of deals) {
    if (d.properties.dealstage !== 'decisionmakerboughtin') continue;
    const src = d.properties.deal_source || 'unknown';
    if (!bySrc[src]) bySrc[src] = [];
    const cd = new Date(d.properties.hs_v2_date_entered_decisionmakerboughtin || d.properties.closedate), cr = new Date(d.properties.createdate);
    if (!isNaN(cd) && !isNaN(cr)) bySrc[src].push(Math.round((cd - cr) / 86400000));
  }
  const result = {};
  for (const [src, days] of Object.entries(bySrc)) {
    result[src] = { avgDays: Math.round(days.reduce((a, b) => a + b, 0) / days.length), count: days.length, label: DEAL_SOURCES.find(s => s.value === src)?.label || src };
  }
  return result;
}

function computeDealHealth(deals, owners) {
  const now = Date.now();
  const health = { healthy: 0, needsAttention: 0, stale: 0, critical: 0, deals: [] };
  for (const d of deals) {
    const stage = d.properties.dealstage;
    if (stage === 'decisionmakerboughtin' || stage === 'closedlost') continue;
    const lastMod = new Date(d.properties.hs_lastmodifieddate);
    const created = new Date(d.properties.createdate);
    const daysSinceUpdate = !isNaN(lastMod) ? Math.round((now - lastMod) / 86400000) : null;
    const age = !isNaN(created) ? Math.round((now - created) / 86400000) : 0;
    let status = 'healthy';
    if (daysSinceUpdate === null || daysSinceUpdate > 30) { status = 'critical'; health.critical++; }
    else if (daysSinceUpdate > 14) { status = 'stale'; health.stale++; }
    else if (daysSinceUpdate > 7) { status = 'needsAttention'; health.needsAttention++; }
    else { health.healthy++; }
    health.deals.push({
      id: d.id, name: d.properties.dealname, stage: STAGES.find(s => s.id === stage)?.label || stage,
      rep: d.properties.hubspot_owner_id ? (owners[d.properties.hubspot_owner_id] || 'Unassigned') : 'Unassigned',
      age, daysSinceUpdate, health: status,
      value: Math.round(parseFloat(d.properties.expected_mrr || d.properties.amount || '0') || PRICING.default),
    });
  }
  health.deals.sort((a, b) => (b.daysSinceUpdate || 999) - (a.daysSinceUpdate || 999));
  return health;
}

function computeWeightedForecast(deals) {
  const now = new Date();
  const thisMonth = now.toISOString().slice(0, 7);
  const nextMonth = new Date(now.getFullYear(), now.getMonth() + 1, 1).toISOString().slice(0, 7);
  const forecast = { thisMonth: { value: 0, weighted: 0, deals: 0 }, nextMonth: { value: 0, weighted: 0, deals: 0 }, later: { value: 0, weighted: 0, deals: 0 } };
  const stageMap = {};
  for (const s of STAGES) stageMap[s.id] = s;
  for (const d of deals) {
    const stage = d.properties.dealstage;
    if (stage === 'decisionmakerboughtin' || stage === 'closedlost') continue;
    const weight = stageMap[stage]?.weight || 0.1;
    const value = parseFloat(d.properties.expected_mrr || d.properties.amount || '0') || PRICING.default;
    const closeDate = d.properties.closedate ? d.properties.closedate.slice(0, 7) : '';
    let bucket = 'later';
    if (closeDate && closeDate <= thisMonth) bucket = 'thisMonth';
    else if (closeDate && closeDate <= nextMonth) bucket = 'nextMonth';
    forecast[bucket].value += value;
    forecast[bucket].weighted += value * weight;
    forecast[bucket].deals++;
  }
  for (const b of Object.values(forecast)) { b.value = Math.round(b.value); b.weighted = Math.round(b.weighted); }
  return forecast;
}

function computeRepChannelAttribution(deals, owners) {
  const result = {};
  for (const d of deals) {
    const rep = d.properties.hubspot_owner_id ? (owners[d.properties.hubspot_owner_id] || 'Unassigned') : 'Unassigned';
    const src = d.properties.deal_source || 'unknown';
    if (!result[rep]) result[rep] = {};
    if (!result[rep][src]) result[rep][src] = { label: DEAL_SOURCES.find(s => s.value === src)?.label || src, total: 0, won: 0, lost: 0, open: 0, wonMRR: 0 };
    const bucket = result[rep][src];
    bucket.total++;
    const mrr = parseFloat(d.properties.expected_mrr || d.properties.amount || '0') || PRICING.default;
    if (d.properties.dealstage === 'decisionmakerboughtin') { bucket.won++; bucket.wonMRR += Math.round(mrr); }
    else if (d.properties.dealstage === 'closedlost') { bucket.lost++; }
    else { bucket.open++; }
  }
  return result;
}

function computeTouchVelocity(deals, activity, owners) {
  const repEng = {};
  for (const [type, records] of Object.entries({ calls: activity.calls, meetings: activity.meetings, notes: activity.notes })) {
    if (!records) continue;
    for (const r of records) {
      const name = r.properties.hubspot_owner_id ? (owners[r.properties.hubspot_owner_id] || 'Unassigned') : 'Unassigned';
      if (!repEng[name]) repEng[name] = { calls: 0, meetings: 0, notes: 0, total: 0 };
      repEng[name][type]++;
      repEng[name].total++;
    }
  }
  const repDealsByStage = {};
  for (const d of deals) {
    const stage = d.properties.dealstage;
    if (stage === 'decisionmakerboughtin' || stage === 'closedlost') continue;
    const rep = d.properties.hubspot_owner_id ? (owners[d.properties.hubspot_owner_id] || 'Unassigned') : 'Unassigned';
    if (!repDealsByStage[rep]) repDealsByStage[rep] = {};
    if (!repDealsByStage[rep][stage]) repDealsByStage[rep][stage] = 0;
    repDealsByStage[rep][stage]++;
  }
  const byStage = {};
  for (const [rep, stages] of Object.entries(repDealsByStage)) {
    const totalDeals = Object.values(stages).reduce((s, v) => s + v, 0);
    const repTotal = repEng[rep]?.total || 0;
    const touchesPerDeal = totalDeals > 0 ? repTotal / totalDeals : 0;
    for (const [stageId, count] of Object.entries(stages)) {
      if (!byStage[stageId]) byStage[stageId] = { totalTouches: 0, dealCount: 0 };
      byStage[stageId].totalTouches += touchesPerDeal * count;
      byStage[stageId].dealCount += count;
    }
  }
  const stageVelocity = STAGES.filter(s => s.id !== 'decisionmakerboughtin' && s.id !== 'closedlost').map(s => ({
    id: s.id, label: s.label,
    avgTouches: byStage[s.id] ? Math.round((byStage[s.id].totalTouches / byStage[s.id].dealCount) * 10) / 10 : 0,
    dealCount: byStage[s.id]?.dealCount || 0,
  }));
  return { byStage: stageVelocity, repEngagements: repEng };
}

function computeMetrics({ owners, deals: allDeals, funnel, paidCustomers, activity, activation, ebData }) {
  const now = new Date();
  const totalCosts = Object.values(MONTHLY_COSTS).reduce((s, v) => s + v, 0);
  const stageMap = {};
  for (const s of STAGES) stageMap[s.id] = s;

  // Filter to sales pipeline only (exclude Onboarding & Activation pipeline)
  const deals = allDeals.filter(d => d.properties.pipeline === 'default' || !d.properties.pipeline);

  // ── Today's KPIs ──
  const todayStr = new Date().toLocaleDateString('en-CA', { timeZone: 'Australia/Melbourne' }); // YYYY-MM-DD
  const activityTypes = { calls: activity.calls, meetings: activity.meetings, notes: activity.notes };
  const { day: today, kpis: todayKPIs } = computeDayMetrics(todayStr, activity, deals, owners);

  // Daily average (last 30 days excluding today)
  let totalActivity30d = 0;
  for (const [type, records] of Object.entries(activityTypes)) {
    if (records) totalActivity30d += records.length;
  }
  today.dailyAvg = Math.round(totalActivity30d / 30);

  // ── Per-rep daily activity (for per-rep averages) ──
  const dailyByRep = {};
  for (const [type, records] of Object.entries(activityTypes)) {
    if (!records) continue;
    for (const r of records) {
      const day = (r.properties.hs_createdate || '').split('T')[0];
      const oid = r.properties.hubspot_owner_id;
      const name = oid ? (owners[oid] || `Owner ${oid}`) : 'Unassigned';
      if (!dailyByRep[name]) dailyByRep[name] = { calls: 0, meetings: 0, notes: 0 };
      dailyByRep[name][type]++;
    }
  }
  // Convert totals to daily averages
  const dailyAvgByRep = {};
  for (const [name, totals] of Object.entries(dailyByRep)) {
    dailyAvgByRep[name] = {
      calls: Math.round(totals.calls / 30),
      meetings: Math.round(totals.meetings / 30),
      notes: Math.round(totals.notes / 30),
      total: Math.round((totals.calls + totals.meetings + totals.notes) / 30),
    };
  }

  // ── Rep slug map ──
  const repSlugMap = {};
  for (const [oid, name] of Object.entries(owners)) {
    const slug = name.split(' ')[0].toLowerCase();
    repSlugMap[slug] = name;
  }

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
  for (const s of DEAL_SOURCES) sourceStats[s.value] = { label: s.label, total: 0, won: 0, lost: 0, open: 0, wonMRR: 0, winRate: 0, cycleDays: [] };
  sourceStats.unknown = { label: 'Unknown', total: 0, won: 0, lost: 0, open: 0, wonMRR: 0, winRate: 0, cycleDays: [] };
  for (const d of deals) {
    const src = d.properties.deal_source || 'unknown';
    const bucket = sourceStats[src] || sourceStats.unknown;
    bucket.total++;
    const mrr = parseFloat(d.properties.expected_mrr || d.properties.amount || '0') || PRICING.default;
    if (d.properties.dealstage === 'decisionmakerboughtin') {
      bucket.won++; bucket.wonMRR += mrr;
      const cd = new Date(d.properties.closedate), cr = new Date(d.properties.createdate);
      if (!isNaN(cd) && !isNaN(cr)) bucket.cycleDays.push(Math.round((cd - cr) / 86400000));
    }
    else if (d.properties.dealstage === 'closedlost') { bucket.lost++; }
    else { bucket.open++; }
  }
  for (const s of Object.values(sourceStats)) {
    s.wonMRR = Math.round(s.wonMRR);
    s.winRate = (s.won + s.lost) > 0 ? Math.round((s.won / (s.won + s.lost)) * 100) : 0;
    s.avgCycleDays = s.cycleDays.length > 0 ? Math.round(s.cycleDays.reduce((a, b) => a + b, 0) / s.cycleDays.length) : null;
    delete s.cycleDays;
  }

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
      const cd = new Date(d.properties.hs_v2_date_entered_decisionmakerboughtin || d.properties.closedate), cr = new Date(d.properties.createdate);
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
    const totalActivity = r.calls + r.meetings + r.notes;
    r.efficiency = totalActivity > 0 ? Math.round((r.wonMRR / totalActivity) * 10) / 10 : 0;
    delete r.wonCycleDays;
    return r;
  }).sort((a, b) => b.wonMRR - a.wonMRR);

  // ── Daily Activity ──
  const dailyMap = {};
  for (let i = 29; i >= 0; i--) { const d = new Date(now.getTime() - i * 86400000); const melbKey = d.toLocaleDateString('en-CA', { timeZone: 'Australia/Melbourne' }); dailyMap[melbKey] = { calls: 0, meetings: 0, notes: 0 }; }
  for (const [type, records] of Object.entries(activityTypes)) {
    if (!records) continue;
    for (const r of records) { const melbDay = new Date(r.properties.hs_createdate).toLocaleDateString('en-CA', { timeZone: 'Australia/Melbourne' }); if (dailyMap[melbDay]) dailyMap[melbDay][type]++; }
  }
  const daily = Object.entries(dailyMap).map(([date, counts]) => ({ date, ...counts }));
  const actTotals = { calls: 0, meetings: 0, notes: 0 };
  for (const d of daily) { actTotals.calls += d.calls; actTotals.meetings += d.meetings; actTotals.notes += d.notes; }

  // ── Historical Per-Day Data ──
  const availableDates = Object.keys(dailyMap); // chronological (oldest first)
  const historicalByDay = {};
  for (const dateStr of availableDates) {
    const { day, kpis } = computeDayMetrics(dateStr, activity, deals, owners);
    historicalByDay[dateStr] = { ...day, kpis };
  }

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
    const cd = new Date(d.properties.hs_v2_date_entered_decisionmakerboughtin || d.properties.closedate), cr = new Date(d.properties.createdate);
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
    if (age > 30) staleDeals.push({ id: d.id, name: d.properties.dealname, stage: stageMap[sid]?.label || sid, days: age, rep: owners[d.properties.hubspot_owner_id] || 'Unassigned' });
  }
  const avgAgeByStage = STAGES.filter(s => s.id !== 'decisionmakerboughtin' && s.id !== 'closedlost').map(s => ({
    label: s.label, avgDays: stageAges[s.id] ? Math.round(stageAges[s.id].reduce((a, b) => a + b, 0) / stageAges[s.id].length) : 0, count: (stageAges[s.id] || []).length,
  }));
  staleDeals.sort((a, b) => b.days - a.days);

  // ── EmailBison Metrics ──
  const eb = computeEBMetrics(ebData, deals, owners);

  // ── New Computations ──
  const stageConversion = computeStageConversion(deals);
  const sourceCycle = computeSourceCycle(deals);
  const dealHealth = computeDealHealth(deals, owners);
  const weightedForecast = computeWeightedForecast(deals);
  const repChannelAttribution = computeRepChannelAttribution(deals, owners);
  const touchVelocity = computeTouchVelocity(deals, activity, owners);

  // ── Weekly Rollups (7-day ending at each date) ──
  const weeklyRollup = {};
  for (const repName of Object.keys(REP_TARGETS)) {
    const days = [];
    for (let i = 0; i < Math.min(7, availableDates.length); i++) {
      const dStr = availableDates[availableDates.length - 1 - i];
      const hd = historicalByDay[dStr];
      if (!hd) continue;
      const rd = hd.byRep[repName] || { calls: 0, meetings: 0, notes: 0 };
      const kd = hd.kpis?.[repName];
      days.push({ date: dStr, calls: rd.calls, meetings: rd.meetings, notes: rd.notes, uniqueCalls: kd?.uniqueCalls || 0, callHours: kd?.callHours || 0, dailyRevenue: kd?.dailyRevenue || 0 });
    }
    days.reverse();
    const totDials = days.reduce((s, d) => s + d.uniqueCalls, 0);
    const totHours = parseFloat(days.reduce((s, d) => s + d.callHours, 0).toFixed(1));
    const totMeetings = days.reduce((s, d) => s + d.meetings, 0);
    const totNotes = days.reduce((s, d) => s + d.notes, 0);
    const totRevenue = days.reduce((s, d) => s + d.dailyRevenue, 0);
    const best = days.reduce((b, d) => d.uniqueCalls > (b?.uniqueCalls || 0) ? d : b, null);
    weeklyRollup[repName] = {
      days, totDials, totHours, totMeetings, totNotes, totRevenue,
      avgDials: days.length > 0 ? Math.round(totDials / days.length) : 0,
      avgHours: days.length > 0 ? parseFloat((totHours / days.length).toFixed(1)) : 0,
      bestDay: best ? { date: best.date, dials: best.uniqueCalls, revenue: best.dailyRevenue } : null,
    };
  }

  // ── Channel ROI ──
  const coldEmailStats = sourceStats.cold_email || { won: 0, wonMRR: 0, total: 0 };
  const coldCallStats = sourceStats.cold_call || { won: 0, wonMRR: 0, total: 0 };
  const channelROI = {
    coldEmail: { spend: MONTHLY_COSTS['Cold Email (Instantly)'] || 0, won: coldEmailStats.won, wonMRR: coldEmailStats.wonMRR, cac: coldEmailStats.won > 0 ? Math.round((MONTHLY_COSTS['Cold Email (Instantly)'] || 0) / coldEmailStats.won) : null },
    coldCall: { spend: MONTHLY_COSTS['Sales Team (2 reps)'] + (MONTHLY_COSTS['Aircall'] || 0), won: coldCallStats.won, wonMRR: coldCallStats.wonMRR, cac: coldCallStats.won > 0 ? Math.round((MONTHLY_COSTS['Sales Team (2 reps)'] + (MONTHLY_COSTS['Aircall'] || 0)) / coldCallStats.won) : null },
    total: { spend: totalCosts, wonMRR: Math.round(Object.values(sourceStats).reduce((s, v) => s + (v.wonMRR || 0), 0)) },
  };
  channelROI.coldEmail.roi = channelROI.coldEmail.spend > 0 ? Math.round((channelROI.coldEmail.wonMRR / channelROI.coldEmail.spend) * 100) : 0;
  channelROI.coldCall.roi = channelROI.coldCall.spend > 0 ? Math.round((channelROI.coldCall.wonMRR / channelROI.coldCall.spend) * 100) : 0;
  channelROI.total.roi = channelROI.total.spend > 0 ? Math.round((channelROI.total.wonMRR / channelROI.total.spend) * 100) : 0;

  return {
    generated: now.toLocaleString('en-AU', { timeZone: 'Australia/Melbourne', dateStyle: 'full', timeStyle: 'short' }),
    today, pnl, pipeline, winRate, totalPipelineValue: Math.round(totalPipelineValue), weightedPipelineValue: Math.round(weightedPipelineValue),
    totalWon, totalLost, wonValue: Math.round(wonValue), totalDeals: deals.length,
    sourceStats: Object.values(sourceStats).filter(s => s.total > 0),
    reps, daily, actTotals, funnel: funnelData, activation,
    velocity: { avgCycle, medianCycle, wonCycleDays, avgAgeByStage, staleDeals, staleDealCount: staleDeals.length },
    dailyAvgByRep, repSlugMap, portalId: HUBSPOT_PORTAL, todayKPIs,
    historicalByDay, availableDates, todayStr, eb, weeklyRollup, channelROI,
    stageConversion, sourceCycle, dealHealth, weightedForecast, repChannelAttribution, touchVelocity,
  };
}

// ══════════════════════════════════════════
// HTML GENERATION
// ══════════════════════════════════════════
function generateHTML(data, { tab = 'today', rep = '', date = '' } = {}) {
  const json = JSON.stringify(data);
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Sammy AI - RevOps Dashboard</title>
<script src="https://cdn.tailwindcss.com"><\/script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"><\/script>
<style>
  body { font-family: 'Inter', system-ui, -apple-system, sans-serif; }
  .progress-bar { transition: width 0.5s ease; }
  .tab-btn { transition: all 0.15s ease; }
  .tab-btn.active { background: #3b82f6; color: white; }
  .tab-btn:not(.active) { background: #f9fafb; color: #4b5563; }
  .tab-btn:not(.active):hover { background: #f3f4f6; }
  .health-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; }
  @media print { .no-print { display: none; } canvas { max-height: 300px; } }
</style>
</head>
<body class="bg-gray-50 min-h-screen">

<!-- STICKY HEADER -->
<header class="sticky top-0 z-50 bg-white border-b border-gray-200 px-4 py-2 shadow-sm">
  <div class="max-w-6xl mx-auto">
    <div class="flex flex-wrap justify-between items-center gap-2 mb-2">
      <div class="min-w-0">
        <h1 class="text-lg font-bold text-gray-900">Sammy AI <span class="text-xs font-normal text-gray-400">RevOps</span></h1>
        <p class="text-xs text-gray-400" id="timestamp"></p>
      </div>
      <div class="flex items-center gap-3 no-print">
        <select id="repSelect" class="text-sm border border-gray-200 rounded-lg px-3 py-1.5 bg-white focus:ring-2 focus:ring-blue-500" onchange="setRep(this.value)">
          <option value="">All Reps</option>
        </select>
        <div id="dateNav" class="flex items-center gap-1 bg-white border border-gray-200 rounded-lg px-1">
          <button id="btnDatePrev" class="p-1.5 text-gray-500 hover:text-blue-500 disabled:opacity-30 disabled:cursor-not-allowed" onclick="navigateDate(-1)">&lsaquo;</button>
          <span id="dateDisplay" class="text-sm font-medium text-gray-700 px-2 min-w-[110px] text-center"></span>
          <button id="btnDateNext" class="p-1.5 text-gray-500 hover:text-blue-500 disabled:opacity-30 disabled:cursor-not-allowed" onclick="navigateDate(1)">&rsaquo;</button>
        </div>
        <a href="/refresh" class="text-xs text-gray-400 hover:text-blue-500" title="Force refresh">&#x21bb;</a>
      </div>
    </div>
    <!-- TAB BAR -->
    <nav class="flex gap-1 overflow-x-auto no-print pb-1">
      <button id="btnToday" class="tab-btn px-3 py-1.5 text-sm font-medium rounded-lg whitespace-nowrap" onclick="switchTab('today')">Today</button>
      <button id="btnPipeline" class="tab-btn px-3 py-1.5 text-sm font-medium rounded-lg whitespace-nowrap" onclick="switchTab('pipeline')">Pipeline</button>
      <button id="btnChannels" class="tab-btn px-3 py-1.5 text-sm font-medium rounded-lg whitespace-nowrap" onclick="switchTab('channels')">Channels</button>
      <button id="btnReps" class="tab-btn px-3 py-1.5 text-sm font-medium rounded-lg whitespace-nowrap" onclick="switchTab('reps')">Reps</button>
      <button id="btnRevenue" class="tab-btn px-3 py-1.5 text-sm font-medium rounded-lg whitespace-nowrap" onclick="switchTab('revenue')">Revenue</button>
    </nav>
  </div>
</header>

<main class="max-w-6xl mx-auto px-4 py-6">

  <!-- TAB 1: TODAY -->
  <div id="tabToday" class="space-y-6">
    <div id="scorecardSection"></div>
    <div id="drilldownSection" class="hidden"></div>
    <div id="myDaySection"></div>
    <div id="weeklySection"></div>
    <div id="todayCESection"></div>
  </div>

  <!-- TAB 2: PIPELINE -->
  <div id="tabPipeline" class="space-y-6 hidden">
    <div class="grid grid-cols-2 md:grid-cols-4 gap-3" id="pipelineKPIs"></div>
    <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
      <div class="lg:col-span-2 bg-white rounded-xl border border-gray-100 p-4">
        <canvas id="pipelineChart" height="260"></canvas>
      </div>
      <div class="bg-white rounded-xl border border-gray-100 p-4">
        <h3 class="text-sm font-medium text-gray-500 mb-3">Stage Conversion</h3>
        <div id="stageConversionSection"></div>
      </div>
    </div>
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
      <div class="bg-white rounded-xl border border-gray-100 p-4">
        <h3 class="text-sm font-medium text-gray-500 mb-3">Touch Velocity by Stage</h3>
        <canvas id="touchVelocityChart" height="200"></canvas>
      </div>
      <div class="bg-white rounded-xl border border-gray-100 p-4">
        <h3 class="text-sm font-medium text-gray-500 mb-3">Deal Health</h3>
        <div id="dealHealthSection"></div>
      </div>
    </div>
    <div class="bg-white rounded-xl border border-gray-100 p-4">
      <h3 class="text-sm font-medium text-gray-500 mb-3">Deals Needing Attention</h3>
      <div id="dealsAttentionSection"></div>
    </div>
  </div>

  <!-- TAB 3: CHANNELS -->
  <div id="tabChannels" class="space-y-6 hidden">
    <div id="sourceAttributionSection"></div>
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
      <div class="bg-white rounded-xl border border-gray-100 p-4">
        <h3 class="text-sm font-medium text-gray-500 mb-3">Deals by Source</h3>
        <canvas id="sourceChart" height="220"></canvas>
      </div>
      <div class="bg-white rounded-xl border border-gray-100 p-4">
        <h3 class="text-sm font-medium text-gray-500 mb-3">Avg Deal Cycle by Source</h3>
        <canvas id="sourceCycleChart" height="220"></canvas>
      </div>
    </div>
    <div id="ebFunnelSection"></div>
    <div id="ebCampaignSection"></div>
    <div id="channelROISection"></div>
  </div>

  <!-- TAB 4: REPS -->
  <div id="tabReps" class="space-y-6 hidden">
    <div id="repComparisonSection"></div>
    <div class="grid grid-cols-3 gap-3" id="actTotals"></div>
    <div class="bg-white rounded-xl border border-gray-100 p-4">
      <canvas id="activityChart" height="180"></canvas>
    </div>
    <div id="repChannelSection"></div>
    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4" id="repCards"></div>
  </div>

  <!-- TAB 5: REVENUE -->
  <div id="tabRevenue" class="space-y-6 hidden">
    <div class="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3" id="pnlCards"></div>
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
      <div class="bg-white rounded-xl border border-gray-100 p-4">
        <h3 class="text-sm font-medium text-gray-500 mb-3">Cost Breakdown</h3>
        <table class="w-full text-sm" id="costTable"></table>
      </div>
      <div class="bg-white rounded-xl border border-gray-100 p-4">
        <h3 class="text-sm font-medium text-gray-500 mb-3">MRR by Tier</h3>
        <div class="flex items-center gap-6">
          <canvas id="mrrChart" class="max-h-40"></canvas>
          <div id="mrrLegend" class="text-sm space-y-2"></div>
        </div>
      </div>
    </div>
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
      <div class="bg-white rounded-xl border border-gray-100 p-4">
        <h3 class="text-sm font-medium text-gray-500 mb-3">Conversion Funnel</h3>
        <canvas id="funnelChart" height="200"></canvas>
      </div>
      <div class="grid grid-cols-2 md:grid-cols-3 gap-3" id="funnelKPIs"></div>
    </div>
    <div id="forecastSection"></div>
    <section>
      <h2 class="text-base font-semibold text-gray-800 mb-4">Deal Velocity</h2>
      <div class="grid grid-cols-2 md:grid-cols-3 gap-3 mb-4" id="velocityKPIs"></div>
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div class="bg-white rounded-xl border border-gray-100 p-4">
          <canvas id="velocityChart" height="220"></canvas>
        </div>
        <div class="bg-white rounded-xl border border-gray-100 p-4 overflow-y-auto max-h-96">
          <h3 class="text-sm font-medium text-gray-500 mb-3">Stale Deals (&gt;30 days)</h3>
          <table class="w-full text-sm" id="staleTable"></table>
        </div>
      </div>
    </section>
  </div>

</main>

<footer class="text-center text-xs text-gray-400 py-6 no-print">Auto-refreshes every 5 min</footer>

<script>
const D = ${json};
const TAB_INIT = '${tab}';
const REP_INIT = '${rep}';
const DATE_INIT = '${date}';
const TODAY_STR = D.todayStr;
const PORTAL = D.portalId;
const $ = id => document.getElementById(id);
const fmt = n => n == null ? 'N/A' : '$' + Math.abs(n).toLocaleString();
const pct = n => n == null ? 'N/A' : n + '%';
const BLUE = '#3b82f6', GREEN = '#22c55e', RED = '#ef4444', AMBER = '#f59e0b', PURPLE = '#8b5cf6', CYAN = '#06b6d4', GRAY = '#9ca3af', LGRAY = '#e5e7eb';

function card(label, value, color, sub) {
  return '<div class="bg-white rounded-xl border border-gray-100 p-4">'
    + '<p class="text-xs font-medium text-gray-500 uppercase tracking-wide">' + label + '</p>'
    + '<p class="text-xl font-bold mt-1" style="color:' + color + '">' + value + '</p>'
    + (sub ? '<p class="text-xs text-gray-400 mt-1">' + sub + '</p>' : '') + '</div>';
}

// ═══ STATE ═══
const TABS = ['today','pipeline','channels','reps','revenue'];
let activeTab = TABS.includes(TAB_INIT) ? TAB_INIT : 'today';
let selectedRepName = null;
let selectedDate = (DATE_INIT && D.availableDates.includes(DATE_INIT)) ? DATE_INIT : TODAY_STR;
const renderedTabs = new Set();
let drillDownRep = null;
const charts = {};

function formatDateDisplay(dateStr) {
  const d = new Date(dateStr + 'T12:00:00');
  const label = d.toLocaleDateString('en-AU', { weekday: 'short', day: 'numeric', month: 'short' });
  return dateStr === TODAY_STR ? label + ' (Today)' : label;
}

function updateDateUI() {
  $('dateDisplay').textContent = formatDateDisplay(selectedDate);
  const idx = D.availableDates.indexOf(selectedDate);
  $('btnDatePrev').disabled = idx <= 0;
  $('btnDateNext').disabled = idx >= D.availableDates.length - 1;
  // Show date nav only on Today tab
  $('dateNav').style.display = activeTab === 'today' ? 'flex' : 'none';
}

function navigateDate(dir) {
  const idx = D.availableDates.indexOf(selectedDate);
  const newIdx = idx + dir;
  if (newIdx < 0 || newIdx >= D.availableDates.length) return;
  setDate(D.availableDates[newIdx]);
}

function setDate(dateStr) {
  selectedDate = dateStr;
  updateDateUI();
  renderToday();
  const url = new URL(window.location);
  if (dateStr === TODAY_STR) url.searchParams.delete('date');
  else url.searchParams.set('date', dateStr);
  history.replaceState(null, '', url);
}

// Find rep from slug
if (REP_INIT) {
  const slug = REP_INIT.toLowerCase();
  selectedRepName = D.repSlugMap[slug] || D.reps.find(r => r.name.toLowerCase().includes(slug))?.name || null;
}

// Populate rep dropdown
const sel = $('repSelect');
const salesReps = D.reps.filter(r => r.name !== 'Unassigned' && !r.name.startsWith('Owner '));
for (const r of salesReps) {
  const opt = document.createElement('option');
  opt.value = r.name;
  opt.textContent = r.name;
  if (r.name === selectedRepName) opt.selected = true;
  sel.appendChild(opt);
}

$('timestamp').textContent = 'Live \\u2014 ' + D.generated;

// ═══ TAB SWITCHING ═══
function switchTab(tab) {
  activeTab = tab;
  for (const t of TABS) {
    const el = $('tab' + t.charAt(0).toUpperCase() + t.slice(1));
    if (el) el.classList.toggle('hidden', t !== tab);
    const btn = $('btn' + t.charAt(0).toUpperCase() + t.slice(1));
    if (btn) { btn.classList.toggle('active', t === tab); btn.classList.toggle('bg-gray-50', t !== tab); }
  }
  updateDateUI();
  if (!renderedTabs.has(tab)) { renderTab(tab); renderedTabs.add(tab); }
  const url = new URL(window.location);
  url.searchParams.set('tab', tab);
  history.replaceState(null, '', url);
}

function renderTab(tab) {
  switch(tab) {
    case 'today': renderToday(); break;
    case 'pipeline': renderPipeline(); break;
    case 'channels': renderChannels(); break;
    case 'reps': renderRepsTab(); break;
    case 'revenue': renderRevenue(); break;
  }
}

function setRep(name) {
  selectedRepName = name || null;
  if (activeTab === 'today') renderToday();
  const url = new URL(window.location);
  if (name) url.searchParams.set('rep', name.split(' ')[0].toLowerCase());
  else url.searchParams.delete('rep');
  history.replaceState(null, '', url);
}

// ═══ TAB 1: TODAY ═══
function renderToday() {
  const rep = selectedRepName ? D.reps.find(r => r.name === selectedRepName) : null;
  const dayData = D.historicalByDay[selectedDate] || D.today;
  const todayData = selectedRepName ? (dayData.byRep[selectedRepName] || { calls: 0, meetings: 0, notes: 0 }) : dayData;
  const avgData = selectedRepName ? (D.dailyAvgByRep[selectedRepName] || { calls: 0, meetings: 0, notes: 0, total: 0 }) : { calls: Math.round(D.actTotals.calls/30), meetings: Math.round(D.actTotals.meetings/30), notes: Math.round(D.actTotals.notes/30), total: D.today.dailyAvg };
  const todayTotal = todayData.calls + todayData.meetings + todayData.notes;
  const avgTotal = avgData.total || (avgData.calls + avgData.meetings + avgData.notes);
  const isToday = selectedDate === TODAY_STR;
  const dateLabel = formatDateDisplay(selectedDate);

  // ── SCORECARD TABLE ──
  renderScorecard();

  // ── MY DAY ──
  const kpi = selectedRepName ? (dayData.kpis?.[selectedRepName] || null) : null;

  if (kpi) {
    function kpiBar(label, current, target, prefix, suffix, color) {
      const pct = target > 0 ? Math.min(Math.round((current / target) * 100), 100) : 0;
      const barCol = pct >= 100 ? GREEN : (pct >= 50 ? AMBER : RED);
      return '<div class="mb-4">'
        + '<div class="flex justify-between items-baseline mb-1">'
        + '<span class="text-sm font-medium text-gray-700">' + label + '</span>'
        + '<span class="text-sm font-bold" style="color:' + barCol + '">' + prefix + current + suffix + ' / ' + prefix + target + suffix + '</span></div>'
        + '<div class="w-full bg-gray-100 rounded-full h-3 overflow-hidden">'
        + '<div class="h-3 rounded-full transition-all duration-500" style="width:' + pct + '%;background:' + barCol + '"></div></div></div>';
    }

    const overallPct = Math.round(((kpi.uniqueCalls / kpi.targets.uniqueCalls) + (kpi.callHours / kpi.targets.callHours) + (kpi.dailyRevenue / kpi.targets.dailyRevenue)) / 3 * 100);
    const overallColor = overallPct >= 100 ? GREEN : (overallPct >= 50 ? AMBER : RED);
    const overallText = overallPct >= 100 ? 'Targets hit!' : (overallPct >= 50 ? 'Getting there' : (overallPct === 0 ? 'Not started' : 'Behind'));

    $('myDaySection').innerHTML = '<div class="bg-white rounded-xl border border-gray-100 p-5 shadow-sm">'
      + '<div class="flex justify-between items-start mb-5">'
      + '<div><h2 class="text-base font-semibold text-gray-800">' + selectedRepName.split(' ')[0] + "'s " + (isToday ? "Day" : dateLabel) + '</h2>'
      + '<p class="text-xs text-gray-400 mt-0.5">Daily KPI targets</p></div>'
      + '<span class="text-sm font-medium px-2.5 py-1 rounded-full" style="background:' + overallColor + '18;color:' + overallColor + '">' + overallText + '</span></div>'
      + kpiBar('Unique Dials', kpi.uniqueCalls, kpi.targets.uniqueCalls, '', '', BLUE)
      + kpiBar('Call Time', kpi.callHours, kpi.targets.callHours, '', 'h', PURPLE)
      + kpiBar('Revenue', kpi.dailyRevenue, kpi.targets.dailyRevenue, '$', '', GREEN)
      + '<div class="mt-4 pt-3 border-t border-gray-100">'
      + '<p class="text-sm text-gray-600 font-medium">' + kpi.uniqueCalls + ' dials | ' + kpi.callHours + 'h talk | ' + todayData.meetings + ' meetings | ' + todayData.notes + ' notes | $' + kpi.dailyRevenue + ' closed</p>'
      + '</div></div>';
  } else {
    const pctDone = avgTotal > 0 ? Math.min(Math.round((todayTotal / avgTotal) * 100), 100) : (todayTotal > 0 ? 100 : 0);
    const barColor = pctDone >= 100 ? GREEN : (pctDone >= 50 ? AMBER : RED);
    const statusText = pctDone >= 100 ? 'On pace' : (pctDone >= 50 ? 'Getting there' : (todayTotal === 0 ? 'Not started' : 'Behind'));

    $('myDaySection').innerHTML = '<div class="bg-white rounded-xl border border-gray-100 p-5 shadow-sm">'
      + '<div class="flex justify-between items-start mb-4">'
      + '<div><h2 class="text-base font-semibold text-gray-800">' + (selectedRepName ? selectedRepName.split(' ')[0] + "'s " + (isToday ? "Day" : dateLabel) : (isToday ? "Team Today" : "Team " + dateLabel)) + '</h2>'
      + '<p class="text-xs text-gray-400 mt-0.5">Activity vs 30-day average</p></div>'
      + '<span class="text-sm font-medium px-2.5 py-1 rounded-full" style="background:' + barColor + '18;color:' + barColor + '">' + statusText + '</span></div>'
      + '<div class="w-full bg-gray-100 rounded-full h-4 mb-4 overflow-hidden"><div class="progress-bar h-4 rounded-full" style="width:' + pctDone + '%;background:' + barColor + '"></div></div>'
      + '<p class="text-sm text-gray-500 mb-5">' + todayTotal + ' of ' + avgTotal + ' daily avg (' + pctDone + '%)</p>'
      + '<div class="grid grid-cols-3 gap-4 text-center">'
      + '<div><p class="text-3xl font-bold" style="color:' + BLUE + '">' + todayData.calls + '</p><p class="text-xs text-gray-500 mt-1">Calls</p><p class="text-xs text-gray-400">avg ' + avgData.calls + '/day</p></div>'
      + '<div><p class="text-3xl font-bold" style="color:' + GREEN + '">' + todayData.meetings + '</p><p class="text-xs text-gray-500 mt-1">Meetings</p><p class="text-xs text-gray-400">avg ' + avgData.meetings + '/day</p></div>'
      + '<div><p class="text-3xl font-bold" style="color:' + PURPLE + '">' + todayData.notes + '</p><p class="text-xs text-gray-500 mt-1">Notes</p><p class="text-xs text-gray-400">avg ' + avgData.notes + '/day</p></div>'
      + '</div></div>';
  }

  // ── WEEKLY SUMMARY ──
  if (selectedRepName && D.weeklyRollup[selectedRepName]) {
    const wr = D.weeklyRollup[selectedRepName];
    const bestLabel = wr.bestDay ? new Date(wr.bestDay.date + 'T12:00:00').toLocaleDateString('en-AU', { weekday: 'short' }) + ' \\u2014 ' + wr.bestDay.dials + ' dials, $' + wr.bestDay.revenue : 'N/A';
    $('weeklySection').innerHTML = '<div class="bg-white rounded-xl border border-gray-100 p-4">'
      + '<h2 class="text-base font-semibold text-gray-800 mb-3">This Week (7 days)</h2>'
      + '<div class="grid grid-cols-2 md:grid-cols-3 gap-3">'
      + card('Dials', wr.totDials, BLUE, 'avg ' + wr.avgDials + '/day')
      + card('Talk Time', wr.totHours + 'h', PURPLE, 'avg ' + wr.avgHours + 'h/day')
      + card('Meetings', wr.totMeetings, GREEN, wr.days.length + ' days')
      + card('Notes', wr.totNotes, CYAN, wr.days.length + ' days')
      + card('Revenue', fmt(wr.totRevenue), GREEN, wr.days.length + ' days')
      + card('Best Day', bestLabel, AMBER, '')
      + '</div></div>';
  } else if (!selectedRepName) {
    let teamDials = 0, teamHours = 0, teamMeetings = 0, teamRevenue = 0;
    for (const rn of Object.keys(D.weeklyRollup)) { const wr = D.weeklyRollup[rn]; teamDials += wr.totDials; teamHours += wr.totHours; teamMeetings += wr.totMeetings; teamRevenue += wr.totRevenue; }
    $('weeklySection').innerHTML = '<div class="bg-white rounded-xl border border-gray-100 p-4">'
      + '<h2 class="text-base font-semibold text-gray-800 mb-3">Team This Week (7 days)</h2>'
      + '<div class="grid grid-cols-2 md:grid-cols-4 gap-3">'
      + card('Team Dials', teamDials, BLUE, Object.keys(D.weeklyRollup).length + ' reps')
      + card('Talk Time', teamHours.toFixed(1) + 'h', PURPLE, '')
      + card('Meetings', teamMeetings, GREEN, '')
      + card('Revenue', fmt(teamRevenue), GREEN, '')
      + '</div></div>';
  } else { $('weeklySection').innerHTML = ''; }

  // ── COLD EMAIL SUMMARY ──
  if (D.eb && D.eb.attribution.total > 0) {
    const a = D.eb.attribution;
    if (selectedRepName) {
      const repDeals = D.eb.repCEDeals?.[selectedRepName] || [];
      const repAttr = a.byRep[selectedRepName];
      if (repDeals.length > 0 || (repAttr && repAttr.total > 0)) {
        let ceHTML = '<div class="bg-white rounded-xl border border-gray-100 p-4">'
          + '<h2 class="text-base font-semibold text-gray-800 mb-3">Cold Email Pipeline</h2>'
          + '<div class="grid grid-cols-3 gap-3 mb-3">'
          + card('CE Deals', repAttr?.total || 0, BLUE, 'from cold email')
          + card('Won', repAttr?.won || 0, GREEN, fmt(repAttr?.wonMRR || 0) + ' MRR')
          + card('In Pipeline', repAttr?.pipeline || 0, PURPLE, fmt(repAttr?.pipelineValue || 0))
          + '</div>';
        if (repDeals.length > 0) {
          ceHTML += '<div class="space-y-2">';
          for (const d of repDeals.filter(d => !d.lost).slice(0, 10)) {
            const sc = d.won ? 'bg-green-50 text-green-600' : 'bg-blue-50 text-blue-600';
            ceHTML += '<a href="https://app.hubspot.com/contacts/' + PORTAL + '/deal/' + d.id + '" target="_blank" class="flex items-center justify-between p-2 rounded-lg hover:bg-gray-50 border border-gray-50">'
              + '<p class="text-sm font-medium text-gray-800 truncate flex-1">' + d.name + '</p>'
              + '<span class="text-xs px-2 py-0.5 rounded-full ' + sc + ' mx-2">' + d.stage + '</span>'
              + '<span class="text-sm font-medium text-gray-700">$' + d.value + '</span></a>';
          }
          ceHTML += '</div>';
        }
        $('todayCESection').innerHTML = ceHTML + '</div>';
      } else { $('todayCESection').innerHTML = ''; }
    } else {
      $('todayCESection').innerHTML = '<div class="bg-white rounded-xl border border-gray-100 p-4">'
        + '<h2 class="text-base font-semibold text-gray-800 mb-3">Cold Email Pipeline</h2>'
        + '<div class="grid grid-cols-2 md:grid-cols-4 gap-3">'
        + card('Total CE Deals', a.total, BLUE, 'from cold email')
        + card('In Pipeline', a.pipeline, PURPLE, fmt(a.pipelineValue) + ' value')
        + card('Won', a.won, GREEN, fmt(a.wonMRR) + ' MRR')
        + card('Reply Rate', pct(D.eb.totals.replyRate), D.eb.totals.replyRate >= 3 ? GREEN : AMBER, D.eb.totals.replies + ' replies')
        + '</div></div>';
    }
  } else { $('todayCESection').innerHTML = ''; }
} // end renderToday

// ═══ SCORECARD & DRILL-DOWN ═══
function renderScorecard() {
  const dayData = D.historicalByDay[selectedDate] || D.today;
  const repsWithTargets = Object.keys(D.weeklyRollup).filter(n => n !== 'Unassigned');
  let behindCount = 0;

  let html = '<div class="flex justify-between items-center mb-4">'
    + '<h2 class="text-base font-semibold text-gray-800">Daily Scorecard — ' + formatDateDisplay(selectedDate) + '</h2>'
    + '<span id="behindBadge" class="text-xs font-medium px-2 py-1 rounded-full"></span></div>';

  html += '<div class="bg-white rounded-xl border border-gray-100 overflow-x-auto">'
    + '<table class="w-full text-sm"><thead><tr class="border-b text-xs text-gray-500 uppercase tracking-wide">'
    + '<th class="text-left py-3 px-4">Rep</th>'
    + '<th class="text-right py-3 px-2">Dials</th>'
    + '<th class="text-right py-3 px-2">Unique</th>'
    + '<th class="text-right py-3 px-2">Hours</th>'
    + '<th class="text-right py-3 px-2">Mtgs</th>'
    + '<th class="text-right py-3 px-2">Notes</th>'
    + '<th class="text-right py-3 px-2">Revenue</th>'
    + '<th class="text-right py-3 px-4">Score</th>'
    + '</tr></thead><tbody>';

  for (const repName of repsWithTargets) {
    const rd = dayData.byRep[repName] || { calls: 0, meetings: 0, notes: 0 };
    const kd = dayData.kpis?.[repName] || { uniqueCalls: 0, callHours: 0, dailyRevenue: 0, targets: { uniqueCalls: 100, callHours: 5, dailyRevenue: 297 } };
    const targets = kd.targets;
    const dialPct = targets.uniqueCalls > 0 ? (kd.uniqueCalls / targets.uniqueCalls) * 100 : 0;
    const hoursPct = targets.callHours > 0 ? (kd.callHours / targets.callHours) * 100 : 0;
    const revPct = targets.dailyRevenue > 0 ? (kd.dailyRevenue / targets.dailyRevenue) * 100 : 0;
    const score = Math.round((dialPct + hoursPct + revPct) / 3);
    const isBehind = score < 50;
    if (isBehind) behindCount++;

    function cellColor(pct) { return pct >= 100 ? 'text-green-600 font-bold' : (pct >= 50 ? 'text-amber-600' : 'text-red-600 font-bold'); }
    const scoreColor = score >= 100 ? 'bg-green-50 text-green-700' : (score >= 50 ? 'bg-amber-50 text-amber-700' : 'bg-red-50 text-red-700');
    const borderClass = isBehind ? 'border-l-4 border-l-red-400' : '';
    const isActive = drillDownRep === repName;

    html += '<tr class="border-b border-gray-50 hover:bg-gray-50 cursor-pointer ' + borderClass + (isActive ? ' bg-blue-50' : '') + '" onclick="toggleDrillDown(\\'' + repName.replace("'", "\\\\'") + '\\')">'
      + '<td class="py-3 px-4 font-medium">' + repName + '</td>'
      + '<td class="text-right py-3 px-2">' + rd.calls + '</td>'
      + '<td class="text-right py-3 px-2 ' + cellColor(dialPct) + '">' + kd.uniqueCalls + '</td>'
      + '<td class="text-right py-3 px-2 ' + cellColor(hoursPct) + '">' + kd.callHours + 'h</td>'
      + '<td class="text-right py-3 px-2">' + rd.meetings + '</td>'
      + '<td class="text-right py-3 px-2">' + rd.notes + '</td>'
      + '<td class="text-right py-3 px-2 ' + cellColor(revPct) + '">$' + kd.dailyRevenue + '</td>'
      + '<td class="text-right py-3 px-4"><span class="text-xs font-bold px-2 py-1 rounded-full ' + scoreColor + '">' + score + '%</span></td>'
      + '</tr>';
  }
  html += '</tbody></table></div>';

  $('scorecardSection').innerHTML = html;
  const badge = $('behindBadge');
  if (behindCount > 0) { badge.className = 'text-xs font-medium px-2 py-1 rounded-full bg-red-50 text-red-600'; badge.textContent = behindCount + ' rep' + (behindCount > 1 ? 's' : '') + ' behind'; }
  else { badge.className = 'text-xs font-medium px-2 py-1 rounded-full bg-green-50 text-green-600'; badge.textContent = 'All on track'; }
}

function toggleDrillDown(repName) {
  if (drillDownRep === repName) {
    drillDownRep = null;
    $('drilldownSection').classList.add('hidden');
    $('drilldownSection').innerHTML = '';
    renderScorecard();
    return;
  }
  drillDownRep = repName;
  renderScorecard();

  const wr = D.weeklyRollup[repName];
  const rep = D.reps.find(r => r.name === repName);
  const a = D.eb?.attribution?.byRep?.[repName];
  const ceDeals = D.eb?.repCEDeals?.[repName] || [];

  let html = '<div class="bg-white rounded-xl border border-blue-200 p-5 shadow-sm">'
    + '<div class="flex justify-between items-start mb-4">'
    + '<h2 class="text-base font-semibold text-gray-800">' + repName + ' — 7-Day Drill-Down</h2>'
    + '<button onclick="toggleDrillDown(\\'' + repName.replace("'", "\\\\'") + '\\')" class="text-xs text-gray-400 hover:text-gray-600">Close</button></div>';

  // 7-day activity table
  if (wr && wr.days.length > 0) {
    html += '<div class="overflow-x-auto mb-4"><table class="w-full text-xs">'
      + '<thead><tr class="border-b text-gray-500 uppercase">'
      + '<th class="text-left py-2 px-2">Day</th><th class="text-right py-2 px-2">Dials</th><th class="text-right py-2 px-2">Hours</th><th class="text-right py-2 px-2">Mtgs</th><th class="text-right py-2 px-2">Revenue</th>'
      + '</tr></thead><tbody>';
    for (const d of wr.days) {
      const dayLabel = new Date(d.date + 'T12:00:00').toLocaleDateString('en-AU', { weekday: 'short', day: 'numeric', month: 'short' });
      html += '<tr class="border-b border-gray-50">'
        + '<td class="py-1.5 px-2">' + dayLabel + '</td>'
        + '<td class="text-right py-1.5 px-2 font-medium">' + d.uniqueCalls + '</td>'
        + '<td class="text-right py-1.5 px-2">' + d.callHours + 'h</td>'
        + '<td class="text-right py-1.5 px-2">' + d.meetings + '</td>'
        + '<td class="text-right py-1.5 px-2 font-medium">$' + d.dailyRevenue + '</td>'
        + '</tr>';
    }
    html += '<tr class="font-bold border-t"><td class="py-1.5 px-2">Total</td>'
      + '<td class="text-right py-1.5 px-2">' + wr.totDials + '</td>'
      + '<td class="text-right py-1.5 px-2">' + wr.totHours + 'h</td>'
      + '<td class="text-right py-1.5 px-2">' + wr.totMeetings + '</td>'
      + '<td class="text-right py-1.5 px-2">$' + wr.totRevenue + '</td></tr>'
      + '</tbody></table></div>';
  }

  // Pipeline + EB attribution
  html += '<div class="grid grid-cols-2 md:grid-cols-4 gap-3">';
  if (rep) {
    html += card('Open Deals', rep.open, BLUE, rep.total + ' total')
      + card('Won MRR', fmt(rep.wonMRR), GREEN, rep.won + ' deals')
      + card('Win Rate', pct(rep.winRate), rep.winRate >= 30 ? GREEN : AMBER, '');
  }
  if (a && a.total > 0) {
    html += card('CE Deals', a.total, CYAN, fmt(a.wonMRR) + ' won MRR');
  }
  html += '</div>';

  // CE deal list
  if (ceDeals.length > 0) {
    html += '<div class="mt-3"><p class="text-xs font-medium text-gray-500 uppercase mb-2">Cold Email Deals</p><div class="space-y-1">';
    for (const d of ceDeals.filter(dd => !dd.lost).slice(0, 8)) {
      const sc = d.won ? 'text-green-600' : 'text-blue-600';
      html += '<div class="flex justify-between text-xs py-1"><span class="truncate flex-1">' + d.name + '</span><span class="' + sc + ' font-medium ml-2">$' + d.value + '</span></div>';
    }
    html += '</div></div>';
  }

  html += '</div>';
  $('drilldownSection').innerHTML = html;
  $('drilldownSection').classList.remove('hidden');
}

// ═══ TAB 2: PIPELINE ═══
function renderPipeline() {
  $('pipelineKPIs').innerHTML = [
    card('Open Pipeline', fmt(D.totalPipelineValue), BLUE, (D.totalDeals-D.totalWon-D.totalLost)+' open'),
    card('Weighted', fmt(D.weightedPipelineValue), PURPLE, 'probability-adjusted'),
    card('Win Rate', pct(D.winRate), D.winRate>=30?GREEN:AMBER, D.totalWon+' won / '+D.totalLost+' lost'),
    card('Won Value', fmt(D.wonValue), GREEN, D.totalWon+' deals'),
  ].join('');

  charts.pipeline = new Chart($('pipelineChart'), { type:'bar', data:{ labels:D.pipeline.map(s=>s.label), datasets:[{ label:'Deals', data:D.pipeline.map(s=>s.count), backgroundColor:BLUE, borderRadius:4 }] }, options:{ indexAxis:'y', responsive:true, plugins:{ legend:{display:false}, title:{display:true,text:'Deals by Stage'} }, scales:{ x:{beginAtZero:true} } } });

  // Stage Conversion
  if (D.stageConversion && D.stageConversion.length > 0) {
    let convHTML = '';
    for (const c of D.stageConversion) {
      const color = c.rate >= 50 ? GREEN : (c.rate >= 25 ? AMBER : RED);
      convHTML += '<div class="flex items-center justify-between py-2 border-b border-gray-50">'
        + '<div class="text-xs"><span class="text-gray-600">' + c.from + '</span> <span class="text-gray-400">\\u2192</span> <span class="text-gray-600">' + c.to + '</span></div>'
        + '<div class="flex items-center gap-2"><span class="text-xs text-gray-400">' + c.fromCount + '\\u2192' + c.toCount + '</span>'
        + '<span class="text-sm font-bold" style="color:' + color + '">' + c.rate + '%</span></div></div>';
    }
    $('stageConversionSection').innerHTML = convHTML;
  }

  // Touch Velocity
  if (D.touchVelocity && D.touchVelocity.byStage.length > 0) {
    const tv = D.touchVelocity.byStage.filter(s => s.dealCount > 0);
    charts.touchVelocity = new Chart($('touchVelocityChart'), { type:'bar', data:{ labels:tv.map(s=>s.label), datasets:[{ label:'Avg Touches/Deal', data:tv.map(s=>s.avgTouches), backgroundColor:tv.map(s=>s.avgTouches>20?GREEN:(s.avgTouches>5?BLUE:AMBER)), borderRadius:4 }] }, options:{ responsive:true, plugins:{legend:{display:false},title:{display:true,text:'Avg Activities per Deal (30d)'}}, scales:{y:{beginAtZero:true}} } });
  }

  // Deal Health
  if (D.dealHealth) {
    const dh = D.dealHealth;
    let dhHTML = '<div class="grid grid-cols-4 gap-2 mb-3 text-center">'
      + '<div><p class="text-lg font-bold" style="color:' + GREEN + '">' + dh.healthy + '</p><p class="text-xs text-gray-500">Healthy</p></div>'
      + '<div><p class="text-lg font-bold" style="color:' + AMBER + '">' + dh.needsAttention + '</p><p class="text-xs text-gray-500">Attention</p></div>'
      + '<div><p class="text-lg font-bold" style="color:' + RED + '">' + dh.stale + '</p><p class="text-xs text-gray-500">Stale</p></div>'
      + '<div><p class="text-lg font-bold" style="color:' + RED + '">' + dh.critical + '</p><p class="text-xs text-gray-500">Critical</p></div>'
      + '</div>';
    const badDeals = dh.deals.filter(d => d.health !== 'healthy').slice(0, 10);
    if (badDeals.length > 0) {
      dhHTML += '<div class="space-y-1">';
      for (const d of badDeals) {
        const hc = d.health === 'critical' ? RED : (d.health === 'stale' ? RED : AMBER);
        dhHTML += '<div class="flex items-center justify-between text-xs py-1 border-b border-gray-50">'
          + '<span class="health-dot mr-2" style="background:' + hc + '"></span>'
          + '<a href="https://app.hubspot.com/contacts/' + PORTAL + '/deal/' + d.id + '" target="_blank" class="truncate flex-1 hover:text-blue-600">' + d.name + '</a>'
          + '<span class="text-gray-400 mx-2">' + d.stage + '</span>'
          + '<span class="font-medium" style="color:' + hc + '">' + (d.daysSinceUpdate != null ? d.daysSinceUpdate + 'd idle' : 'unknown') + '</span></div>';
      }
      dhHTML += '</div>';
    }
    $('dealHealthSection').innerHTML = dhHTML;
  }

  // Deals needing attention
  const stale = D.dealHealth ? D.dealHealth.deals.filter(d => d.health === 'stale' || d.health === 'critical') : D.velocity.staleDeals;
  const shown = stale.slice(0, 15);
  if (shown.length > 0) {
    let daHTML = '<div class="space-y-2">';
    for (const d of shown) {
      const dc = (d.daysSinceUpdate || d.days || 0) > 30 ? 'text-red-600 font-bold' : 'text-amber-600';
      daHTML += '<a href="https://app.hubspot.com/contacts/' + PORTAL + '/deal/' + d.id + '" target="_blank" class="flex items-center justify-between p-3 rounded-lg hover:bg-gray-50 border border-gray-50">'
        + '<div class="min-w-0 flex-1"><p class="text-sm font-medium text-gray-800 truncate">' + d.name + '</p><p class="text-xs text-gray-400">' + (d.rep || '') + '</p></div>'
        + '<span class="text-xs px-2 py-0.5 rounded-full bg-gray-100 text-gray-600 mx-2">' + d.stage + '</span>'
        + '<span class="text-sm ' + dc + '">' + (d.daysSinceUpdate != null ? d.daysSinceUpdate + 'd idle' : d.days + 'd old') + '</span></a>';
    }
    daHTML += '</div>';
    if (stale.length > 15) daHTML += '<p class="text-xs text-gray-400 mt-2">+ ' + (stale.length - 15) + ' more</p>';
    $('dealsAttentionSection').innerHTML = daHTML;
  } else {
    $('dealsAttentionSection').innerHTML = '<p class="text-green-600 text-sm font-medium py-4 text-center">All deals healthy!</p>';
  }
}

// ═══ TAB 3: CHANNELS ═══
function renderChannels() {
  // Source Attribution Table
  let satHTML = '<div class="bg-white rounded-xl border border-gray-100 p-4"><h2 class="text-base font-semibold text-gray-800 mb-3">Source Attribution</h2>'
    + '<div class="overflow-x-auto"><table class="w-full text-sm">'
    + '<thead><tr class="border-b text-xs text-gray-500 uppercase tracking-wide">'
    + '<th class="text-left py-2">Source</th><th class="text-right py-2">Total</th><th class="text-right py-2">Won</th><th class="text-right py-2">Lost</th><th class="text-right py-2">Open</th><th class="text-right py-2">Win%</th><th class="text-right py-2">MRR</th><th class="text-right py-2">Avg Cycle</th>'
    + '</tr></thead><tbody>';
  for (const s of D.sourceStats) {
    satHTML += '<tr class="border-b border-gray-50"><td class="py-2">' + s.label + '</td>'
      + '<td class="text-right py-2">' + s.total + '</td>'
      + '<td class="text-right py-2 text-green-600 font-medium">' + s.won + '</td>'
      + '<td class="text-right py-2 text-red-500">' + s.lost + '</td>'
      + '<td class="text-right py-2">' + s.open + '</td>'
      + '<td class="text-right py-2">' + s.winRate + '%</td>'
      + '<td class="text-right py-2 font-medium">$' + s.wonMRR.toLocaleString() + '</td>'
      + '<td class="text-right py-2">' + (s.avgCycleDays != null ? s.avgCycleDays + 'd' : '-') + '</td></tr>';
  }
  satHTML += '</tbody></table></div></div>';
  $('sourceAttributionSection').innerHTML = satHTML;

  // Source charts
  charts.source = new Chart($('sourceChart'), { type:'bar', data:{ labels:D.sourceStats.map(s=>s.label), datasets:[ {label:'Won',data:D.sourceStats.map(s=>s.won),backgroundColor:GREEN,borderRadius:4}, {label:'Lost',data:D.sourceStats.map(s=>s.lost),backgroundColor:RED,borderRadius:4}, {label:'Open',data:D.sourceStats.map(s=>s.open),backgroundColor:LGRAY,borderRadius:4} ] }, options:{ responsive:true, plugins:{title:{display:true,text:'Deals by Source'}}, scales:{y:{beginAtZero:true}} } });

  // Source cycle chart
  if (D.sourceCycle) {
    const srcs = Object.values(D.sourceCycle).filter(s => s.count > 0);
    if (srcs.length > 0) {
      charts.sourceCycle = new Chart($('sourceCycleChart'), { type:'bar', data:{ labels:srcs.map(s=>s.label), datasets:[{ label:'Avg Days to Close', data:srcs.map(s=>s.avgDays), backgroundColor:srcs.map(s=>s.avgDays>30?AMBER:BLUE), borderRadius:4 }] }, options:{ responsive:true, plugins:{legend:{display:false},title:{display:true,text:'Avg Days to Close by Source'}}, scales:{y:{beginAtZero:true}} } });
    }
  }

  // EB Funnel + Cost Metrics
  if (D.eb) {
    const eb = D.eb, t = eb.totals, a = eb.attribution, co = eb.costs;
    let ebHTML = '<div class="bg-white rounded-xl border border-gray-100 p-4"><h2 class="text-base font-semibold text-gray-800 mb-3">EmailBison Funnel</h2>'
      + '<div class="grid grid-cols-1 lg:grid-cols-2 gap-4">'
      + '<div><canvas id="ebFunnelChart" height="200"></canvas></div>'
      + '<div class="grid grid-cols-2 gap-3">'
      + card('Leads', t.leads.toLocaleString(), BLUE, eb.campaignMetrics.length + ' campaigns')
      + card('Reply Rate', pct(t.replyRate), t.replyRate >= 3 ? GREEN : AMBER, t.replies + ' replies')
      + card('Cost/Lead', co.costPerLead != null ? '$' + co.costPerLead.toFixed(2) : 'N/A', BLUE, '$' + co.monthlySpend + '/mo')
      + card('Cost/Reply', co.costPerReply != null ? '$' + co.costPerReply.toFixed(0) : 'N/A', co.costPerReply && co.costPerReply < 50 ? GREEN : AMBER, '')
      + card('Cost/Deal', co.costPerDeal != null ? '$' + co.costPerDeal : 'N/A', co.costPerDeal && co.costPerDeal < D.pnl.arpu * 3 ? GREEN : RED, a.total + ' deals')
      + card('Won MRR', fmt(a.wonMRR), GREEN, a.won + ' won')
      + '</div></div></div>';
    $('ebFunnelSection').innerHTML = ebHTML;

    if (eb.funnel && eb.funnel.length > 0) {
      const fc = [BLUE, CYAN, AMBER, PURPLE, GREEN, BLUE, GREEN];
      charts.ebFunnel = new Chart($('ebFunnelChart'), { type:'bar', data:{ labels:eb.funnel.map(f=>f.label), datasets:[{data:eb.funnel.map(f=>f.value),backgroundColor:fc.slice(0,eb.funnel.length),borderRadius:4}] }, options:{indexAxis:'y',responsive:true,plugins:{legend:{display:false}},scales:{x:{beginAtZero:true}}} });
    }

    // Campaign cards
    let ccHTML = '<h3 class="text-sm font-medium text-gray-500 mb-3">Campaign Performance</h3><div class="grid grid-cols-1 md:grid-cols-2 gap-3">';
    for (const c of eb.campaignMetrics) {
      const sb = c.status==='active'?'bg-green-50 text-green-600':(c.status==='paused'?'bg-amber-50 text-amber-600':'bg-gray-100 text-gray-600');
      ccHTML += '<div class="bg-white rounded-xl border border-gray-100 p-4">'
        + '<div class="flex justify-between items-start mb-2"><p class="text-sm font-semibold text-gray-800 truncate flex-1">' + c.name + '</p><span class="text-xs px-2 py-0.5 rounded-full ' + sb + ' ml-2">' + c.status + '</span></div>'
        + '<div class="grid grid-cols-3 gap-2 text-xs">'
        + '<div><p class="text-gray-500">Leads</p><p class="font-bold">' + c.leads + '</p></div>'
        + '<div><p class="text-gray-500">Sent</p><p class="font-bold">' + c.sent + '</p></div>'
        + '<div><p class="text-gray-500">Opens</p><p class="font-bold">' + c.opens + ' (' + c.openRate + '%)</p></div>'
        + '<div><p class="text-gray-500">Replies</p><p class="font-bold text-blue-600">' + c.replies + '</p></div>'
        + '<div><p class="text-gray-500">Interested</p><p class="font-bold text-green-600">' + c.interested + '</p></div>'
        + '<div><p class="text-gray-500">Reply%</p><p class="font-bold">' + c.replyRate + '%</p></div>'
        + '</div></div>';
    }
    ccHTML += '</div>';
    $('ebCampaignSection').innerHTML = ccHTML;
  } else {
    $('ebFunnelSection').innerHTML = '<div class="bg-amber-50 border border-amber-200 rounded-xl p-4 text-sm text-amber-700">EmailBison data unavailable</div>';
    $('ebCampaignSection').innerHTML = '';
  }

  // Channel ROI
  if (D.channelROI) {
    const cr = D.channelROI;
    $('channelROISection').innerHTML = '<h2 class="text-base font-semibold text-gray-800 mb-3">Spend ROI by Channel</h2>'
      + '<div class="grid grid-cols-1 md:grid-cols-3 gap-4">'
      + '<div class="bg-white rounded-xl border border-gray-100 p-4">'
      + '<h3 class="text-sm font-medium text-gray-500 mb-2">Cold Email</h3>'
      + '<p class="text-2xl font-bold" style="color:' + (cr.coldEmail.roi > 10 ? GREEN : AMBER) + '">' + cr.coldEmail.roi + '% ROI</p>'
      + '<p class="text-xs text-gray-500 mt-1">' + fmt(cr.coldEmail.spend) + ' spend \\u2192 ' + fmt(cr.coldEmail.wonMRR) + ' MRR</p>'
      + '<p class="text-xs text-gray-500">' + cr.coldEmail.won + ' won | CAC: ' + (cr.coldEmail.cac ? fmt(cr.coldEmail.cac) : 'N/A') + '</p></div>'
      + '<div class="bg-white rounded-xl border border-gray-100 p-4">'
      + '<h3 class="text-sm font-medium text-gray-500 mb-2">Cold Call / Sales</h3>'
      + '<p class="text-2xl font-bold" style="color:' + (cr.coldCall.roi > 10 ? GREEN : AMBER) + '">' + cr.coldCall.roi + '% ROI</p>'
      + '<p class="text-xs text-gray-500 mt-1">' + fmt(cr.coldCall.spend) + ' spend \\u2192 ' + fmt(cr.coldCall.wonMRR) + ' MRR</p>'
      + '<p class="text-xs text-gray-500">' + cr.coldCall.won + ' won | CAC: ' + (cr.coldCall.cac ? fmt(cr.coldCall.cac) : 'N/A') + '</p></div>'
      + '<div class="bg-white rounded-xl border border-gray-100 p-4">'
      + '<h3 class="text-sm font-medium text-gray-500 mb-2">Total</h3>'
      + '<p class="text-2xl font-bold" style="color:' + (cr.total.roi > 10 ? GREEN : AMBER) + '">' + cr.total.roi + '% ROI</p>'
      + '<p class="text-xs text-gray-500 mt-1">' + fmt(cr.total.spend) + ' spend \\u2192 ' + fmt(cr.total.wonMRR) + ' MRR</p></div></div>';
  }
}

// ═══ TAB 4: REPS ═══
function renderRepsTab() {
  // Rep Comparison Table
  const reps = D.reps.filter(r => r.name !== 'Unassigned' && !r.name.startsWith('Owner '));
  let rcHTML = '<div class="bg-white rounded-xl border border-gray-100 p-4"><h2 class="text-base font-semibold text-gray-800 mb-3">Rep Comparison</h2>'
    + '<div class="overflow-x-auto"><table class="w-full text-sm">'
    + '<thead><tr class="border-b text-xs text-gray-500 uppercase tracking-wide">'
    + '<th class="text-left py-2">Rep</th><th class="text-right py-2">Deals</th><th class="text-right py-2">Won</th><th class="text-right py-2">Lost</th><th class="text-right py-2">Win%</th><th class="text-right py-2">MRR</th><th class="text-right py-2">Cycle</th><th class="text-right py-2">Calls</th><th class="text-right py-2">Mtgs</th><th class="text-right py-2">Notes</th><th class="text-right py-2">$/Activity</th>'
    + '</tr></thead><tbody>';
  for (const r of reps) {
    rcHTML += '<tr class="border-b border-gray-50">'
      + '<td class="py-2 font-medium">' + r.name + '</td>'
      + '<td class="text-right py-2">' + r.total + '</td>'
      + '<td class="text-right py-2 text-green-600 font-medium">' + r.won + '</td>'
      + '<td class="text-right py-2 text-red-500">' + r.lost + '</td>'
      + '<td class="text-right py-2">' + r.winRate + '%</td>'
      + '<td class="text-right py-2 font-bold text-green-600">$' + r.wonMRR.toLocaleString() + '</td>'
      + '<td class="text-right py-2">' + (r.avgCycleDays != null ? r.avgCycleDays + 'd' : '-') + '</td>'
      + '<td class="text-right py-2">' + r.calls + '</td>'
      + '<td class="text-right py-2">' + r.meetings + '</td>'
      + '<td class="text-right py-2">' + r.notes + '</td>'
      + '<td class="text-right py-2 font-medium">$' + (r.efficiency || 0).toFixed(1) + '</td></tr>';
  }
  rcHTML += '</tbody></table></div></div>';
  $('repComparisonSection').innerHTML = rcHTML;

  // Activity totals + chart
  $('actTotals').innerHTML = [
    card('Calls (30d)', D.actTotals.calls, BLUE, Math.round(D.actTotals.calls/4.3)+'/wk'),
    card('Meetings (30d)', D.actTotals.meetings, GREEN, Math.round(D.actTotals.meetings/4.3)+'/wk'),
    card('Notes (30d)', D.actTotals.notes, PURPLE, Math.round(D.actTotals.notes/4.3)+'/wk'),
  ].join('');
  const dateLabels = D.daily.map(d => new Date(d.date).toLocaleDateString('en-AU',{month:'short',day:'numeric'}));
  charts.activity = new Chart($('activityChart'), { type:'line', data:{ labels:dateLabels, datasets:[ {label:'Calls',data:D.daily.map(d=>d.calls),borderColor:BLUE,backgroundColor:BLUE+'20',fill:true,tension:0.3}, {label:'Meetings',data:D.daily.map(d=>d.meetings),borderColor:GREEN,backgroundColor:GREEN+'20',fill:true,tension:0.3}, {label:'Notes',data:D.daily.map(d=>d.notes),borderColor:PURPLE,backgroundColor:PURPLE+'20',fill:true,tension:0.3} ] }, options:{ responsive:true, scales:{y:{beginAtZero:true}}, plugins:{legend:{position:'top'}} } });

  // Per-rep channel attribution
  if (D.repChannelAttribution) {
    let raHTML = '<div class="bg-white rounded-xl border border-gray-100 p-4"><h3 class="text-sm font-medium text-gray-500 mb-3">Per-Rep Channel Attribution</h3><div class="overflow-x-auto"><table class="w-full text-xs">'
      + '<thead><tr class="border-b text-gray-500 uppercase"><th class="text-left py-2">Rep</th><th class="text-left py-2">Channel</th><th class="text-right py-2">Deals</th><th class="text-right py-2">Won</th><th class="text-right py-2">MRR</th></tr></thead><tbody>';
    for (const [repName, channels] of Object.entries(D.repChannelAttribution)) {
      if (repName === 'Unassigned') continue;
      for (const [src, data] of Object.entries(channels)) {
        raHTML += '<tr class="border-b border-gray-50"><td class="py-1.5">' + repName + '</td><td class="py-1.5">' + data.label + '</td>'
          + '<td class="text-right py-1.5">' + data.total + '</td>'
          + '<td class="text-right py-1.5 text-green-600">' + data.won + '</td>'
          + '<td class="text-right py-1.5 font-medium">$' + data.wonMRR.toLocaleString() + '</td></tr>';
      }
    }
    raHTML += '</tbody></table></div></div>';
    $('repChannelSection').innerHTML = raHTML;
  }

  // Rep cards
  $('repCards').innerHTML = reps.map(r => {
    const initials = r.name.split(' ').map(w=>w[0]).join('').toUpperCase();
    const color = reps.indexOf(r) % 3 === 0 ? BLUE : (reps.indexOf(r) % 3 === 1 ? PURPLE : GREEN);
    return '<div class="bg-white rounded-xl border border-gray-100 p-4">'
      +'<div class="flex items-center gap-3 mb-3"><div class="w-9 h-9 rounded-full flex items-center justify-center text-white text-xs font-bold" style="background:'+color+'">'+initials+'</div><div><p class="font-semibold text-sm">'+r.name+'</p><p class="text-xs text-gray-400">'+r.total+' deals</p></div></div>'
      +'<div class="grid grid-cols-3 gap-2 text-xs">'
      +'<div><p class="text-gray-500">Won</p><p class="font-bold text-green-600">'+r.won+'</p></div>'
      +'<div><p class="text-gray-500">Lost</p><p class="font-bold text-red-500">'+r.lost+'</p></div>'
      +'<div><p class="text-gray-500">Win%</p><p class="font-bold">'+r.winRate+'%</p></div>'
      +'<div><p class="text-gray-500">MRR</p><p class="font-bold text-green-600">$'+r.wonMRR.toLocaleString()+'</p></div>'
      +'<div><p class="text-gray-500">Cycle</p><p class="font-bold">'+(r.avgCycleDays!=null?r.avgCycleDays+'d':'N/A')+'</p></div>'
      +'<div><p class="text-gray-500">Open</p><p class="font-bold">'+r.open+'</p></div></div></div>';
  }).join('');
}

// ═══ TAB 5: REVENUE ═══
function renderRevenue() {
  // P&L
  const profitColor = D.pnl.monthlyProfit >= 0 ? GREEN : RED;
  const roiColor = D.pnl.roi > 0 ? GREEN : (D.pnl.roi > -25 ? AMBER : RED);
  const cacColor = D.pnl.cac && D.pnl.cac < D.pnl.arpu * 3 ? GREEN : (D.pnl.cac && D.pnl.cac < D.pnl.arpu * 6 ? AMBER : RED);
  $('pnlCards').innerHTML = [
    card('Current MRR', fmt(D.pnl.currentMRR) + '/mo', D.pnl.currentMRR > 0 ? GREEN : GRAY, D.pnl.paidCount + ' paid'),
    card('Monthly Costs', fmt(D.pnl.totalCosts) + '/mo', GRAY, Object.keys(D.pnl.costs).length + ' items'),
    card('Profit', (D.pnl.monthlyProfit >= 0 ? '+' : '-') + fmt(D.pnl.monthlyProfit), profitColor),
    card('ROI', pct(D.pnl.roi), roiColor, 'return on spend'),
    card('CAC', D.pnl.cac ? fmt(D.pnl.cac) : 'N/A', cacColor, D.pnl.cac ? 'per customer' : ''),
    card('Break-Even', D.pnl.paidCount >= D.pnl.breakEvenCustomers ? 'Achieved' : D.pnl.breakEvenCustomers + ' needed', D.pnl.paidCount >= D.pnl.breakEvenCustomers ? GREEN : AMBER, D.pnl.paidCount + '/' + D.pnl.breakEvenCustomers + ' at $' + D.pnl.arpu + ' ARPU'),
  ].join('');

  let costRows = '<tbody>';
  for (const [name, amount] of Object.entries(D.pnl.costs)) costRows += '<tr class="border-b border-gray-50"><td class="py-1.5">' + name + '</td><td class="text-right py-1.5 font-medium">$' + amount.toLocaleString() + '</td></tr>';
  costRows += '<tr class="font-bold"><td class="py-1.5">Total</td><td class="text-right py-1.5">$' + D.pnl.totalCosts.toLocaleString() + '</td></tr></tbody>';
  $('costTable').innerHTML = costRows;

  const mrrLabels = [], mrrValues = [], mrrColors = [GREEN, BLUE, PURPLE, GRAY];
  if (D.pnl.mrrBreakdown.basic) { mrrLabels.push('Basic ($59)'); mrrValues.push(D.pnl.mrrBreakdown.basic); }
  if (D.pnl.mrrBreakdown.pro) { mrrLabels.push('Pro ($99)'); mrrValues.push(D.pnl.mrrBreakdown.pro); }
  if (D.pnl.mrrBreakdown.team) { mrrLabels.push('Team ($249)'); mrrValues.push(D.pnl.mrrBreakdown.team); }
  if (D.pnl.mrrBreakdown.unknown) { mrrLabels.push('Unknown ($59)'); mrrValues.push(D.pnl.mrrBreakdown.unknown); }
  if (mrrValues.length > 0) {
    charts.mrr = new Chart($('mrrChart'), { type:'doughnut', data:{ labels:mrrLabels, datasets:[{ data:mrrValues, backgroundColor:mrrColors.slice(0,mrrValues.length) }] }, options:{ plugins:{ legend:{ display:false } }, cutout:'60%' } });
    $('mrrLegend').innerHTML = mrrLabels.map((l,i) => '<div class="flex items-center gap-2"><span class="w-3 h-3 rounded-full" style="background:'+mrrColors[i]+'"></span><span>'+l+': <b>'+mrrValues[i]+'</b></span></div>').join('');
  }

  // Funnel
  const F = D.funnel;
  charts.funnel = new Chart($('funnelChart'), { type:'bar', data:{ labels:['All Contacts','Started Trial','Active Trial','Paid'], datasets:[{ data:[F.totalContacts,F.activeTrial+F.paid+F.expired+F.churned,F.activeTrial,F.paid], backgroundColor:[LGRAY,CYAN,BLUE,GREEN], borderRadius:4 }] }, options:{ indexAxis:'y', responsive:true, plugins:{legend:{display:false},title:{display:true,text:'Conversion Funnel'}}, scales:{x:{beginAtZero:true}} } });
  $('funnelKPIs').innerHTML = [
    card('Signup\\u2192Trial', pct(F.signupToTrialRate), BLUE, (F.activeTrial+F.paid+F.expired+F.churned)+' of '+F.totalContacts),
    card('Trial\\u2192Paid', pct(F.trialToPaidRate), GREEN, F.paid+' converted'),
    card('Overall', pct(F.overallConversion), F.overallConversion>=5?GREEN:AMBER, 'contacts to paid'),
    card('Expired', F.expired, RED, 'did not convert'),
    card('Churned', F.churned, RED, 'lost after paying'),
    card('Active Trials', F.activeTrial, BLUE, 'in progress'),
  ].join('');

  // Weighted Forecast
  if (D.weightedForecast) {
    const wf = D.weightedForecast;
    $('forecastSection').innerHTML = '<h2 class="text-base font-semibold text-gray-800 mb-3">Weighted Forecast</h2>'
      + '<div class="grid grid-cols-1 md:grid-cols-3 gap-4">'
      + '<div class="bg-white rounded-xl border border-gray-100 p-4">'
      + '<h3 class="text-sm font-medium text-gray-500 mb-1">This Month</h3>'
      + '<p class="text-2xl font-bold" style="color:' + GREEN + '">' + fmt(wf.thisMonth.weighted) + '</p>'
      + '<p class="text-xs text-gray-400">' + wf.thisMonth.deals + ' deals | ' + fmt(wf.thisMonth.value) + ' unweighted</p></div>'
      + '<div class="bg-white rounded-xl border border-gray-100 p-4">'
      + '<h3 class="text-sm font-medium text-gray-500 mb-1">Next Month</h3>'
      + '<p class="text-2xl font-bold" style="color:' + BLUE + '">' + fmt(wf.nextMonth.weighted) + '</p>'
      + '<p class="text-xs text-gray-400">' + wf.nextMonth.deals + ' deals | ' + fmt(wf.nextMonth.value) + ' unweighted</p></div>'
      + '<div class="bg-white rounded-xl border border-gray-100 p-4">'
      + '<h3 class="text-sm font-medium text-gray-500 mb-1">Later</h3>'
      + '<p class="text-2xl font-bold" style="color:' + PURPLE + '">' + fmt(wf.later.weighted) + '</p>'
      + '<p class="text-xs text-gray-400">' + wf.later.deals + ' deals | ' + fmt(wf.later.value) + ' unweighted</p></div></div>';
  }

  // Velocity
  $('velocityKPIs').innerHTML = [
    card('Avg Cycle', D.velocity.avgCycle!=null?D.velocity.avgCycle+' days':'N/A', BLUE),
    card('Median Cycle', D.velocity.medianCycle!=null?D.velocity.medianCycle+' days':'N/A', PURPLE),
    card('Stale Deals', D.velocity.staleDealCount, D.velocity.staleDealCount>5?RED:(D.velocity.staleDealCount>0?AMBER:GREEN), '>30d in stage'),
  ].join('');
  const velStages = D.velocity.avgAgeByStage.filter(s=>s.count>0);
  charts.velocity = new Chart($('velocityChart'), { type:'bar', data:{ labels:velStages.map(s=>s.label), datasets:[{ label:'Avg Days', data:velStages.map(s=>s.avgDays), backgroundColor:velStages.map(s=>s.avgDays>30?RED:(s.avgDays>14?AMBER:BLUE)), borderRadius:4 }] }, options:{ responsive:true, plugins:{legend:{display:false},title:{display:true,text:'Avg Days in Stage'}}, scales:{y:{beginAtZero:true}} } });
  if (D.velocity.staleDeals.length > 0) {
    let staleRows = '<thead><tr class="border-b text-xs text-gray-500 uppercase"><th class="text-left py-2">Deal</th><th class="text-left py-2">Stage</th><th class="text-right py-2">Days</th><th class="text-left py-2">Rep</th></tr></thead><tbody>';
    for (const d of D.velocity.staleDeals) { const color=d.days>60?'text-red-600 font-bold':'text-amber-600'; staleRows+='<tr class="border-b border-gray-50"><td class="py-1.5 max-w-[160px] truncate"><a href="https://app.hubspot.com/contacts/'+PORTAL+'/deal/'+d.id+'" target="_blank" class="hover:text-blue-600">'+d.name+'</a></td><td class="py-1.5 text-xs">'+d.stage+'</td><td class="text-right py-1.5 '+color+'">'+d.days+'</td><td class="py-1.5 text-xs">'+d.rep+'</td></tr>'; }
    $('staleTable').innerHTML = staleRows + '</tbody>';
  } else { $('staleTable').parentElement.innerHTML = '<p class="text-green-600 text-sm font-medium">No stale deals!</p>'; }
}

// ═══ INIT ═══
updateDateUI();
switchTab(activeTab);

// Auto-refresh preserving query params
setTimeout(() => window.location.reload(), 300000);
<\/script>
</body>
</html>`;
}

function loadingHTML() {
  return `<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Loading...</title>
<meta http-equiv="refresh" content="5">
<script src="https://cdn.tailwindcss.com"><\/script></head>
<body class="bg-gray-50 min-h-screen flex items-center justify-center">
<div class="text-center"><div class="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500 mx-auto"></div>
<p class="mt-4 text-gray-600 text-lg">Loading dashboard...</p>
<p class="mt-2 text-gray-400 text-sm">Pulling live data from HubSpot</p>
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
  cache.time = 0;
  refreshCache();
  res.redirect('/');
});

app.get('/', async (req, res) => {
  if (Date.now() - cache.time > CACHE_TTL) {
    refreshCache();
  }
  if (cache.data) {
    const tab = req.query.tab || 'today';
    const rep = req.query.rep || '';
    const date = req.query.date || '';
    res.type('html').send(generateHTML(cache.data, { tab, rep, date }));
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
