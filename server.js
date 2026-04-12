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

function readTasks() {
  ensureTasksFile();
  try {
    return JSON.parse(fs.readFileSync(TASKS_FILE, 'utf8'));
  } catch { return {}; }
}

function writeTasks(allTasks) {
  ensureTasksFile();
  fs.writeFileSync(TASKS_FILE, JSON.stringify(allTasks, null, 2));
}

function getTodayMelbourne() {
  return new Date().toLocaleDateString('en-CA', { timeZone: 'Australia/Melbourne' });
}

function getTasksForDate(dateStr) {
  const all = readTasks();
  return all[dateStr] || [];
}

function saveTasksForDate(dateStr, tasks) {
  const all = readTasks();
  all[dateStr] = tasks;
  const cutoff = new Date(Date.now() - 30 * 86400000).toISOString().split('T')[0];
  for (const key of Object.keys(all)) {
    if (key < cutoff) delete all[key];
  }
  writeTasks(all);
}

function generateTaskId() {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 7);
}

function seedDailyTasks(dateStr) {
  const existing = getTasksForDate(dateStr);
  if (existing.length > 0) return existing;
  const tasks = [];
  const data = cache.data;
  if (!data) return tasks;
  if (data.priorities) {
    for (const p of data.priorities.slice(0, 5)) {
      tasks.push({
        id: generateTaskId(),
        text: p.message,
        done: false,
        source: 'auto',
        severity: p.severity,
        createdAt: new Date().toISOString(),
      });
    }
  }
  for (const repName of ACTIVE_REPS) {
    const target = REP_TARGETS[repName];
    if (!target) continue;
    tasks.push({
      id: generateTaskId(),
      text: repName.split(' ')[0] + ': Hit ' + target.uniqueCalls + ' unique dials',
      done: false,
      source: 'auto',
      severity: 'info',
      createdAt: new Date().toISOString(),
    });
  }
  if (tasks.length > 0) saveTasksForDate(dateStr, tasks);
  return tasks;
}

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

// Active reps — Jared Korinko is inactive, excluded from daily scorecard/targets
const ACTIVE_REPS = ['Lucas Gibson', 'Krishna Pryor'];

// Commission Configuration
const COMMISSION_CONFIG = {
  perClose: 100, // $100 flat per close
};

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
    const params = { limit: 100, properties: 'dealname,dealstage,pipeline,amount,expected_mrr,deal_source,hubspot_owner_id,closedate,createdate,hs_lastmodifieddate,hs_v2_date_entered_decisionmakerboughtin,hs_v2_date_entered_appointmentscheduled,hs_v2_date_entered_presentationscheduled,hs_v2_date_entered_2843565802,hs_v2_date_entered_2851995329,hs_v2_date_entered_closedlost', associations: 'contacts' };
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

async function fetchChurnedCustomers() {
  const results = [];
  let after;
  while (true) {
    const body = {
      filterGroups: [{ filters: [{ propertyName: 'user_status', operator: 'EQ', value: 'churned' }] }],
      properties: ['sammy_subscription_tier', 'createdate', 'firstname', 'lastname', 'hs_lastmodifieddate'],
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

  const ebPromise = fetchAllEBData();

  const owners = await fetchOwners();
  const deals = await fetchAllDeals();

  // Collect contact IDs from deal associations and fetch their emails + analytics source
  const contactIds = new Set();
  for (const d of deals) {
    const assoc = d.associations?.contacts?.results;
    if (assoc) for (const c of assoc) contactIds.add(c.id);
  }
  console.log(`[fetch] ${deals.length} deals, ${contactIds.size} associated contacts — fetching emails + analytics source...`);
  const { emailMap: contactEmails, analyticsSourceMap: contactAnalyticsSources } = await fetchContactEmails([...contactIds]);
  console.log(`[fetch] Got ${Object.keys(contactEmails).length} contact emails, ${Object.keys(contactAnalyticsSources).length} analytics sources`);

  // Build deal → email map and deal → contact ID map
  const dealEmailMap = {};
  const dealContactIdMap = {};
  for (const d of deals) {
    const assoc = d.associations?.contacts?.results;
    if (assoc) {
      for (const c of assoc) {
        if (contactEmails[c.id]) { dealEmailMap[d.id] = contactEmails[c.id]; dealContactIdMap[d.id] = c.id; break; }
      }
    }
  }
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
  const churnedCustomers = await fetchChurnedCustomers();
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
  await sleep(500);

  const [engOnFire, engHot, engWarm, engCold] = await Promise.all([
    countContacts([{ filters: [{ propertyName: 'engagement_tier', operator: 'EQ', value: 'on_fire' }] }]),
    countContacts([{ filters: [{ propertyName: 'engagement_tier', operator: 'EQ', value: 'hot' }] }]),
    countContacts([{ filters: [{ propertyName: 'engagement_tier', operator: 'EQ', value: 'warm' }] }]),
    countContacts([{ filters: [{ propertyName: 'engagement_tier', operator: 'EQ', value: 'cold' }] }]),
  ]);

  const ebData = await ebPromise;
  console.log(`[fetch] Done in ${((Date.now() - t0) / 1000).toFixed(1)}s — ${deals.length} deals, ${noStatus + incomplete + activeTrial + paid + expired + churned} contacts, EB: ${ebData ? ebData.length + ' campaigns' : 'unavailable'}`);

  return {
    owners, deals, dealEmailMap, dealContactIdMap, contactAnalyticsSources,
    funnel: { noStatus, incomplete, activeTrial, paid, expired, churned },
    paidCustomers, churnedCustomers,
    activity: { calls: calls || [], meetings: meetings || [], notes: notes || [] },
    activation: { total: actTotal, estimateCreated: actEstimate, quoteSent: actQuoteSent, paid },
    engagementTiers: { onFire: engOnFire, hot: engHot, warm: engWarm, cold: engCold },
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
      // Fixed mapping: count all non-draft leads as "sent" (they were delivered to the sending queue)
      // EB statuses: draft, queued, sending, sent, delivered, opened, clicked, replied, bounced, unsubscribed, interested, unverified
      const sentStatuses = ['sent', 'delivered', 'opened', 'clicked', 'replied', 'interested', 'queued', 'sending'];
      if (sentStatuses.includes(s)) sent++;
      if (l.opened_count > 0 || ['opened', 'clicked', 'replied', 'interested'].includes(s)) opens++;
      if (['replied', 'interested'].includes(s)) replies++;
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
  const activeCampaigns = campaignMetrics.filter(c => c.status === 'active').length || 1;
  const costs = {
    costPerLead: totLeads > 0 ? Math.round((EB_MONTHLY_SPEND / totLeads) * 100) / 100 : null,
    costPerReply: totReplies > 0 ? Math.round((EB_MONTHLY_SPEND / totReplies) * 100) / 100 : null,
    costPerDeal: attribution.total > 0 ? Math.round(EB_MONTHLY_SPEND / attribution.total) : null,
    monthlySpend: EB_MONTHLY_SPEND,
  };

  // Per-campaign ROI
  const spendPerCampaign = EB_MONTHLY_SPEND / activeCampaigns;
  for (const cm of campaignMetrics) {
    cm.allocatedSpend = Math.round(spendPerCampaign);
    cm.roi = cm.status === 'active' ? -100 : -100; // Default; updated below if deals attributed
  }

  // Per-rep cold email deals list
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

  // EB sync health: flag if all leads have same status
  const uniqueStatuses = Object.keys(statusCounts);
  const syncHealthy = uniqueStatuses.length > 1 || totLeads === 0;

  return { campaignMetrics, totals, attribution, statusCounts, funnel, costs, repCEDeals, syncHealthy, debugStatuses: statusCounts };
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

// ── NEW: Deal Health Scoring (composite formula from design.md) ──
function computeDealHealthScores(deals, owners, activity) {
  const now = Date.now();
  const stageMap = {};
  for (const s of STAGES) stageMap[s.id] = s;

  // Compute average touches per stage for engagement scoring
  const stageTouchAvg = {};
  const stageTouchCounts = {};
  for (const s of STAGES) { stageTouchAvg[s.id] = 0; stageTouchCounts[s.id] = 0; }
  // Simplified: use deal count per stage as proxy
  for (const d of deals) {
    const sid = d.properties.dealstage;
    if (sid === 'decisionmakerboughtin' || sid === 'closedlost') continue;
    stageTouchCounts[sid] = (stageTouchCounts[sid] || 0) + 1;
  }

  const scoredDeals = [];
  for (const d of deals) {
    const stage = d.properties.dealstage;
    if (stage === 'decisionmakerboughtin' || stage === 'closedlost') continue;

    const lastMod = new Date(d.properties.hs_lastmodifieddate);
    const created = new Date(d.properties.createdate);
    const daysSinceUpdate = !isNaN(lastMod) ? Math.round((now - lastMod) / 86400000) : 999;
    const age = !isNaN(created) ? Math.round((now - created) / 86400000) : 0;

    // Stage freshness (40% weight)
    let stageFreshness;
    if (daysSinceUpdate < 7) stageFreshness = 100;
    else if (daysSinceUpdate < 14) stageFreshness = 75;
    else if (daysSinceUpdate < 30) stageFreshness = 40;
    else stageFreshness = 10;

    // Touch recency (35% weight) — using lastModified as proxy for last touch
    let touchRecency;
    if (daysSinceUpdate <= 2) touchRecency = 100;
    else if (daysSinceUpdate <= 5) touchRecency = 75;
    else if (daysSinceUpdate <= 14) touchRecency = 40;
    else touchRecency = 10;

    // Engagement level (25% weight) — simplified scoring
    let engagementLevel = 30; // default: below avg
    if (daysSinceUpdate <= 3 && age > 0) engagementLevel = 100;
    else if (daysSinceUpdate <= 7) engagementLevel = 60;
    else if (daysSinceUpdate > 30) engagementLevel = 0;

    const score = Math.round(stageFreshness * 0.4 + touchRecency * 0.35 + engagementLevel * 0.25);
    let category;
    if (score >= 80) category = 'healthy';
    else if (score >= 50) category = 'monitor';
    else if (score >= 20) category = 'attention';
    else category = 'critical';

    scoredDeals.push({
      id: d.id, name: d.properties.dealname,
      stage: stageMap[stage]?.label || stage, stageId: stage,
      rep: d.properties.hubspot_owner_id ? (owners[d.properties.hubspot_owner_id] || 'Unassigned') : 'Unassigned',
      age, daysSinceUpdate, score, category,
      value: Math.round(parseFloat(d.properties.expected_mrr || d.properties.amount || '0') || PRICING.default),
    });
  }

  scoredDeals.sort((a, b) => a.score - b.score);

  const counts = { healthy: 0, monitor: 0, attention: 0, critical: 0 };
  for (const d of scoredDeals) counts[d.category]++;

  return { deals: scoredDeals, counts };
}

// ── NEW: Bottleneck Detection ──
function computeBottlenecks(deals) {
  const stageMap = {};
  for (const s of STAGES) stageMap[s.id] = s;
  const stageDays = {};
  for (const d of deals) {
    const sid = d.properties.dealstage;
    if (sid === 'decisionmakerboughtin' || sid === 'closedlost') continue;
    const age = Math.round((Date.now() - new Date(d.properties.createdate)) / 86400000);
    if (!stageDays[sid]) stageDays[sid] = [];
    stageDays[sid].push(age);
  }
  const stageMetrics = [];
  let allMedians = [];
  for (const s of STAGES.filter(s => s.id !== 'decisionmakerboughtin' && s.id !== 'closedlost')) {
    const days = (stageDays[s.id] || []).sort((a, b) => a - b);
    const median = days.length > 0 ? days[Math.floor(days.length / 2)] : 0;
    const avg = days.length > 0 ? Math.round(days.reduce((a, b) => a + b, 0) / days.length) : 0;
    if (median > 0) allMedians.push(median);
    stageMetrics.push({ id: s.id, label: s.label, median, avg, count: days.length });
  }
  const overallMedian = allMedians.length > 0 ? allMedians.sort((a, b) => a - b)[Math.floor(allMedians.length / 2)] : 1;
  for (const sm of stageMetrics) {
    sm.stallFactor = overallMedian > 0 ? parseFloat((sm.median / overallMedian).toFixed(1)) : 0;
    sm.isBottleneck = sm.stallFactor > 2 && sm.count >= 3;
  }
  return { stageMetrics, overallMedian };
}

// ── NEW: Source Attribution with Fallback Chain ──
function computeSourceAttribution(deals, dealContactIdMap, contactAnalyticsSources, owners) {
  const HS_SOURCE_MAP = {
    'PAID_SEARCH': 'inbound_signup', 'ORGANIC_SEARCH': 'inbound_signup',
    'DIRECT_TRAFFIC': 'inbound_signup', 'SOCIAL_MEDIA': 'inbound_signup',
    'EMAIL_MARKETING': 'cold_email', 'REFERRALS': 'referral',
    'PAID_SOCIAL': 'inbound_signup', 'OFFLINE': 'cold_call',
    'OTHER_CAMPAIGNS': 'cold_email',
  };

  const enhanced = [];
  let unknownBefore = 0, unknownAfter = 0;
  for (const d of deals) {
    const raw = d.properties.deal_source || '';
    let source = raw;
    let method = 'deal_property';

    if (!raw || raw === 'unknown') {
      unknownBefore++;
      // Fallback 1: contact analytics source
      const contactId = dealContactIdMap[d.id];
      const analytics = contactId ? contactAnalyticsSources[contactId] : null;
      if (analytics && HS_SOURCE_MAP[analytics.source]) {
        source = HS_SOURCE_MAP[analytics.source];
        method = 'contact_analytics';
      } else {
        source = 'unknown';
        method = 'unknown';
        unknownAfter++;
      }
    }
    enhanced.push({ dealId: d.id, source, method, original: raw });
  }

  return { enhanced, unknownBefore, unknownAfter, improved: unknownBefore - unknownAfter };
}

// ── NEW: Data Quality Scorecard ──
function computeDataQuality(deals, paidCustomers, engagementTiers, eb) {
  const totalDeals = deals.length;
  let withSource = 0, withOwner = 0;
  for (const d of deals) {
    if (d.properties.deal_source && d.properties.deal_source !== 'unknown') withSource++;
    if (d.properties.hubspot_owner_id) withOwner++;
  }
  const totalPaid = paidCustomers.length;
  let withTier = 0;
  for (const c of paidCustomers) {
    if (c.properties.sammy_subscription_tier) withTier++;
  }
  const totalEng = engagementTiers.onFire + engagementTiers.hot + engagementTiers.warm + engagementTiers.cold;
  const ebSyncOk = eb ? eb.syncHealthy : true;

  const sourcePct = totalDeals > 0 ? Math.round((withSource / totalDeals) * 100) : 100;
  const ownerPct = totalDeals > 0 ? Math.round((withOwner / totalDeals) * 100) : 100;
  const tierPct = totalPaid > 0 ? Math.round((withTier / totalPaid) * 100) : 100;
  const overallScore = Math.round((sourcePct + ownerPct + tierPct) / 3);

  return { sourcePct, ownerPct, tierPct, overallScore, ebSyncOk, totalEng };
}

// ── NEW: Unit Economics ──
function computeUnitEconomics(pnl, wonDeals) {
  const arpu = pnl.arpu || PRICING.default;
  const ltv = arpu * 12; // conservative 12-month proxy
  const cac = pnl.cac;
  const ltvCacRatio = cac && cac > 0 ? parseFloat((ltv / cac).toFixed(1)) : null;
  const monthsToPayback = cac && arpu > 0 ? parseFloat((cac / arpu).toFixed(1)) : null;
  const breakEven = pnl.breakEvenCustomers;
  const currentCustomers = pnl.paidCount;
  const gap = Math.max(0, breakEven - currentCustomers);
  return { arpu, ltv, cac, ltvCacRatio, monthsToPayback, breakEven, currentCustomers, gap };
}

// ── NEW: MRR Waterfall ──
function computeMRRWaterfall(deals, churnedCustomers, paidCustomers) {
  const now = new Date();
  const thirtyDaysAgo = new Date(now.getTime() - 30 * 86400000);

  // New MRR: deals that entered "Closed Won" in last 30 days
  let newMRR = 0, newDeals = 0;
  for (const d of deals) {
    if (d.properties.dealstage !== 'decisionmakerboughtin') continue;
    const wonDate = new Date(d.properties.hs_v2_date_entered_decisionmakerboughtin || d.properties.closedate);
    if (!isNaN(wonDate) && wonDate >= thirtyDaysAgo) {
      newMRR += parseFloat(d.properties.expected_mrr || d.properties.amount || '0') || PRICING.default;
      newDeals++;
    }
  }

  // Churn MRR: only contacts who churned in last 30 days
  let churnMRR = 0;
  let churnCount = 0;
  const totalChurnCount = churnedCustomers.length;
  for (const c of churnedCustomers) {
    const modDate = new Date(c.properties.hs_lastmodifieddate);
    if (isNaN(modDate) || modDate < thirtyDaysAgo) continue;
    const tier = c.properties.sammy_subscription_tier;
    churnMRR += PRICING[tier] || PRICING.default;
    churnCount++;
  }

  const netMRR = Math.round(newMRR) - Math.round(churnMRR);
  return { newMRR: Math.round(newMRR), newDeals, churnMRR: Math.round(churnMRR), churnCount, totalChurnCount, netMRR };
}

// ── NEW: Week-over-Week Comparison ──
function computeWoWComparison(historicalByDay, availableDates, deals, owners) {
  const now = new Date();
  // Split last 14 days into this week (0-6) and last week (7-13)
  const thisWeekDates = availableDates.slice(-7);
  const lastWeekDates = availableDates.slice(-14, -7);

  function sumPeriod(dates) {
    let calls = 0, meetings = 0, notes = 0, uniqueDials = 0, revenue = 0;
    for (const d of dates) {
      const hd = historicalByDay[d];
      if (!hd) continue;
      calls += hd.calls || 0;
      meetings += hd.meetings || 0;
      notes += hd.notes || 0;
      for (const repName of ACTIVE_REPS) {
        const kd = hd.kpis?.[repName];
        if (kd) { uniqueDials += kd.uniqueCalls || 0; revenue += kd.dailyRevenue || 0; }
      }
    }
    return { calls, meetings, notes, uniqueDials, revenue, days: dates.length };
  }

  const thisWeek = sumPeriod(thisWeekDates);
  const lastWeek = sumPeriod(lastWeekDates);

  function delta(curr, prev) {
    if (prev === 0) return curr > 0 ? 100 : 0;
    return Math.round(((curr - prev) / prev) * 100);
  }

  // Count deals created/won in each period
  let dealsCreatedThisWeek = 0, dealsWonThisWeek = 0;
  let dealsCreatedLastWeek = 0, dealsWonLastWeek = 0;
  const twStart = thisWeekDates[0], twEnd = thisWeekDates[thisWeekDates.length - 1];
  const lwStart = lastWeekDates[0], lwEnd = lastWeekDates.length > 0 ? lastWeekDates[lastWeekDates.length - 1] : '';

  for (const d of deals) {
    const cd = new Date(d.properties.createdate).toLocaleDateString('en-CA', { timeZone: 'Australia/Melbourne' });
    if (cd >= twStart && cd <= twEnd) dealsCreatedThisWeek++;
    if (cd >= lwStart && cd <= lwEnd) dealsCreatedLastWeek++;
    if (d.properties.dealstage === 'decisionmakerboughtin') {
      const wd = new Date(d.properties.hs_v2_date_entered_decisionmakerboughtin || d.properties.closedate).toLocaleDateString('en-CA', { timeZone: 'Australia/Melbourne' });
      if (wd >= twStart && wd <= twEnd) dealsWonThisWeek++;
      if (wd >= lwStart && wd <= lwEnd) dealsWonLastWeek++;
    }
  }

  thisWeek.dealsCreated = dealsCreatedThisWeek;
  thisWeek.dealsWon = dealsWonThisWeek;
  lastWeek.dealsCreated = dealsCreatedLastWeek;
  lastWeek.dealsWon = dealsWonLastWeek;

  const deltas = {
    calls: delta(thisWeek.calls, lastWeek.calls),
    meetings: delta(thisWeek.meetings, lastWeek.meetings),
    uniqueDials: delta(thisWeek.uniqueDials, lastWeek.uniqueDials),
    revenue: delta(thisWeek.revenue, lastWeek.revenue),
    dealsCreated: delta(thisWeek.dealsCreated, lastWeek.dealsCreated),
    dealsWon: delta(thisWeek.dealsWon, lastWeek.dealsWon),
  };

  return { thisWeek, lastWeek, deltas };
}

// ── NEW: Alerts Engine ──
function computeAlerts(metrics) {
  const alerts = [];
  const { pnl, dealHealthScores, eb, dataQuality, woW } = metrics;

  // Negative ROI
  if (pnl.roi < 0) {
    alerts.push({ severity: 'critical', message: `Negative ROI (${pnl.roi}%) — need ${pnl.breakEvenCustomers - pnl.paidCount} more customers to break even`, icon: '!' });
  }

  // Stale pipeline
  if (dealHealthScores) {
    const totalOpen = dealHealthScores.deals.length;
    const staleCount = dealHealthScores.counts.attention + dealHealthScores.counts.critical;
    const stalePct = totalOpen > 0 ? Math.round((staleCount / totalOpen) * 100) : 0;
    if (stalePct > 50) {
      alerts.push({ severity: 'critical', message: `${stalePct}% of pipeline needs attention — ${staleCount} deals stale or critical`, icon: '!' });
    }
  }

  // Rep underperformance (use today's data)
  if (metrics.todayKPIs) {
    for (const repName of ACTIVE_REPS) {
      const kpi = metrics.todayKPIs[repName];
      if (!kpi) continue;
      const t = kpi.targets;
      const score = Math.round(((kpi.uniqueCalls / t.uniqueCalls) + (kpi.callHours / t.callHours) + (kpi.dailyRevenue / t.dailyRevenue)) / 3 * 100);
      if (score < 50 && score > 0) {
        alerts.push({ severity: 'warning', message: `${repName.split(' ')[0]} at ${score}% of daily targets`, icon: '!' });
      }
    }
  }

  // EB sync issue
  if (eb && !eb.syncHealthy) {
    alerts.push({ severity: 'warning', message: 'EmailBison data may be stale — check lead status distribution', icon: '!' });
  }

  // Data quality
  if (dataQuality && dataQuality.sourcePct < 50) {
    alerts.push({ severity: 'info', message: `${100 - dataQuality.sourcePct}% of deals have unknown source — channel ROI unreliable`, icon: 'i' });
  }

  // Sort by severity
  const severityOrder = { critical: 0, warning: 1, info: 2 };
  alerts.sort((a, b) => severityOrder[a.severity] - severityOrder[b.severity]);
  return alerts.slice(0, 5);
}

// ── Existing compute helpers ──
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

// ══════════════════════════════════════════
// COMMISSIONS
// ══════════════════════════════════════════
function computeCommissions(deals, owners, contacts, paidCustomers) {
  const now = new Date();
  const thisMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const yearStart = new Date(now.getFullYear(), 0, 1);
  const dayOfWeek = now.getDay();
  const thisWeekStart = new Date(now);
  thisWeekStart.setDate(now.getDate() - (dayOfWeek === 0 ? 6 : dayOfWeek - 1));
  thisWeekStart.setHours(0, 0, 0, 0);

  const wonDeals = deals.filter(d => d.properties.dealstage === 'decisionmakerboughtin');
  const perRep = {};
  for (const repName of ACTIVE_REPS) {
    perRep[repName] = { closesThisWeek: 0, commissionThisWeek: 0, closesThisMonth: 0, commissionThisMonth: 0, closesYTD: 0, commissionYTD: 0, dealBreakdown: [], monthlyTrend: {} };
  }

  for (const d of wonDeals) {
    const oid = d.properties.hubspot_owner_id;
    const repName = oid ? (owners[oid] || 'Unassigned') : 'Unassigned';
    if (!ACTIVE_REPS.includes(repName)) continue;
    const closeDate = new Date(d.properties.hs_v2_date_entered_decisionmakerboughtin || d.properties.closedate);
    if (isNaN(closeDate)) continue;
    const mrr = parseFloat(d.properties.expected_mrr || d.properties.amount || '0') || PRICING.default;
    const commission = COMMISSION_CONFIG.perClose;
    const monthKey = closeDate.toISOString().slice(0, 7);
    const rep = perRep[repName];
    if (!rep) continue;
    if (closeDate >= yearStart) { rep.closesYTD++; rep.commissionYTD += commission; }
    if (closeDate >= thisMonthStart) { rep.closesThisMonth++; rep.commissionThisMonth += commission; }
    if (closeDate >= thisWeekStart) { rep.closesThisWeek++; rep.commissionThisWeek += commission; }
    if (!rep.monthlyTrend[monthKey]) rep.monthlyTrend[monthKey] = { closes: 0, commission: 0 };
    rep.monthlyTrend[monthKey].closes++;
    rep.monthlyTrend[monthKey].commission += commission;
    if (closeDate >= thisMonthStart) {
      rep.dealBreakdown.push({ id: d.id, name: d.properties.dealname, mrr, closeDate: closeDate.toISOString().split('T')[0], commission });
    }
  }

  // Build customer roster from paid customers (like paid-customers-who-closed.html)
  const customerRoster = [];
  const rosterByRep = {};
  for (const repName of ACTIVE_REPS) rosterByRep[repName] = { count: 0, mrr: 0 };
  rosterByRep['Unattributed'] = { count: 0, mrr: 0 };

  for (const d of wonDeals) {
    const oid = d.properties.hubspot_owner_id;
    const closer = oid ? (owners[oid] || 'Unattributed') : 'Unattributed';
    const mrr = parseFloat(d.properties.expected_mrr || d.properties.amount || '0') || PRICING.default;
    const closeDate = new Date(d.properties.hs_v2_date_entered_decisionmakerboughtin || d.properties.closedate);
    const plan = mrr >= 99 ? 'Monthly $99' : (mrr >= 59 ? 'Founder $59' : '$' + mrr);
    customerRoster.push({
      name: d.properties.dealname || 'Unknown',
      company: d.properties.dealname || '',
      plan, mrr: Math.round(mrr), closer,
      closeDate: !isNaN(closeDate) ? closeDate.toISOString().split('T')[0] : 'Unknown',
    });
    const bucket = ACTIVE_REPS.includes(closer) ? closer : 'Unattributed';
    if (!rosterByRep[bucket]) rosterByRep[bucket] = { count: 0, mrr: 0 };
    rosterByRep[bucket].count++;
    rosterByRep[bucket].mrr += Math.round(mrr);
  }
  customerRoster.sort((a, b) => b.closeDate.localeCompare(a.closeDate));

  const trendLabels = [];
  for (let i = 5; i >= 0; i--) {
    const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
    trendLabels.push(d.toISOString().slice(0, 7));
  }
  let totalThisMonth = 0, totalYTD = 0;
  for (const rep of Object.values(perRep)) { totalThisMonth += rep.commissionThisMonth; totalYTD += rep.commissionYTD; }

  return { perRep, trendLabels, totalThisMonth, totalYTD, perCloseRate: COMMISSION_CONFIG.perClose, customerRoster, rosterByRep, totalCustomers: customerRoster.length, totalMRR: customerRoster.reduce((s, c) => s + c.mrr, 0) };
}

// ══════════════════════════════════════════
// LEADERBOARD
// ══════════════════════════════════════════
function computeLeaderboard(reps, weeklyRollup, commissions, deals, owners) {
  const now = new Date();
  const dayOfWeek = now.getDay();
  const thisWeekStart = new Date(now);
  thisWeekStart.setDate(now.getDate() - (dayOfWeek === 0 ? 6 : dayOfWeek - 1));
  thisWeekStart.setHours(0, 0, 0, 0);
  const thisMonthStart = new Date(now.getFullYear(), now.getMonth(), 1);

  function computePeriodStats(periodStart) {
    const repData = {};
    for (const repName of ACTIVE_REPS) {
      const wr = weeklyRollup[repName] || {};
      const comm = commissions.perRep[repName] || {};
      let wonMRR = 0, dealsWon = 0, winCount = 0, lossCount = 0;
      for (const d of deals) {
        const oid = d.properties.hubspot_owner_id;
        const name = oid ? (owners[oid] || 'Unassigned') : 'Unassigned';
        if (name !== repName) continue;
        const wonDate = new Date(d.properties.hs_v2_date_entered_decisionmakerboughtin || d.properties.closedate);
        if (d.properties.dealstage === 'decisionmakerboughtin' && !isNaN(wonDate) && wonDate >= periodStart) {
          wonMRR += parseFloat(d.properties.expected_mrr || d.properties.amount || '0') || PRICING.default;
          dealsWon++;
        }
        if (d.properties.dealstage === 'decisionmakerboughtin') winCount++;
        if (d.properties.dealstage === 'closedlost') lossCount++;
      }
      const winRate = (winCount + lossCount) > 0 ? Math.round((winCount / (winCount + lossCount)) * 100) : 0;
      const dials = wr.totDials || 0;
      const hours = wr.totHours || 0;
      const commEarned = periodStart >= thisMonthStart ? (comm.commissionThisWeek || 0) : (comm.commissionThisMonth || 0);
      repData[repName] = { wonMRR: Math.round(wonMRR), dealsWon, dials, hours, winRate, commEarned };
    }
    return repData;
  }

  function scoreReps(repData) {
    const weights = { wonMRR: 0.30, commEarned: 0.20, dealsWon: 0.15, dials: 0.15, hours: 0.10, winRate: 0.10 };
    const categories = Object.keys(weights);
    const maxes = {};
    for (const cat of categories) maxes[cat] = Math.max(...Object.values(repData).map(r => r[cat] || 0), 1);
    const scored = {};
    for (const [repName, data] of Object.entries(repData)) {
      const catScores = {};
      let composite = 0;
      for (const cat of categories) {
        const normalized = maxes[cat] > 0 ? Math.round((data[cat] / maxes[cat]) * 100) : 0;
        catScores[cat] = { raw: data[cat], normalized };
        composite += normalized * weights[cat];
      }
      scored[repName] = { composite: Math.round(composite), categories: catScores, ...data };
    }
    const leaders = {};
    for (const cat of categories) {
      let best = null, bestVal = -1;
      for (const [name, s] of Object.entries(scored)) {
        if (s.categories[cat].raw > bestVal) { bestVal = s.categories[cat].raw; best = name; }
      }
      leaders[cat] = best;
    }
    return { ranked: Object.entries(scored).sort((a, b) => b[1].composite - a[1].composite), leaders };
  }

  return {
    week: scoreReps(computePeriodStats(thisWeekStart)),
    month: scoreReps(computePeriodStats(thisMonthStart)),
    allTime: scoreReps(computePeriodStats(new Date(0))),
  };
}

// ══════════════════════════════════════════
// HEALTH PULSE
// ══════════════════════════════════════════
function computeHealthPulse(winRate, coverageRatio, unitEconomics, woW, dataQuality) {
  const pipelineCov = Math.min(Math.round((coverageRatio / 3) * 100), 100);
  const winRateHealth = Math.min(Math.round((winRate / 25) * 100), 100);
  const cacEfficiency = unitEconomics.ltvCacRatio ? Math.min(Math.round((unitEconomics.ltvCacRatio / 3) * 100), 100) : 0;
  let momentum = 50;
  if (woW && woW.thisWeek && woW.lastWeek) {
    const twActivity = woW.thisWeek.calls + woW.thisWeek.meetings;
    const lwActivity = woW.lastWeek.calls + woW.lastWeek.meetings;
    if (lwActivity > 0) { momentum = Math.min(Math.max(Math.round(50 + ((twActivity - lwActivity) / lwActivity) * 50), 0), 100); }
    else if (twActivity > 0) { momentum = 100; }
  }
  const components = [
    { name: 'Pipeline Coverage', score: pipelineCov, detail: coverageRatio + 'x' },
    { name: 'Win Rate', score: winRateHealth, detail: winRate + '%' },
    { name: 'CAC Efficiency', score: cacEfficiency, detail: unitEconomics.ltvCacRatio ? unitEconomics.ltvCacRatio + ':1 LTV:CAC' : 'N/A' },
    { name: 'Activity Momentum', score: momentum, detail: (woW?.deltas?.calls >= 0 ? '+' : '') + (woW?.deltas?.calls || 0) + '% WoW' },
  ];
  const overall = Math.round(components.reduce((s, c) => s + c.score, 0) / components.length);
  const weakest = components.reduce((a, b) => a.score < b.score ? a : b);
  const strongest = components.reduce((a, b) => a.score > b.score ? a : b);
  let diagnosis = '';
  if (overall >= 70) { diagnosis = 'Business is healthy — ' + strongest.name.toLowerCase() + ' is strong at ' + strongest.detail; }
  else if (overall >= 40) {
    diagnosis = weakest.name + ' needs attention at ' + weakest.detail;
    if (weakest.name === 'Pipeline Coverage') diagnosis += ' — need more deals to hit 3x safety';
    else if (weakest.name === 'Activity Momentum') diagnosis += ' — activity declining week-over-week';
  } else { diagnosis = 'Critical: ' + weakest.name.toLowerCase() + ' at ' + weakest.detail + ' — immediate action needed'; }
  const color = overall >= 70 ? 'green' : (overall >= 40 ? 'amber' : 'red');
  return { overall, color, diagnosis, components };
}

// ══════════════════════════════════════════
// MAIN COMPUTE METRICS
// ══════════════════════════════════════════
function computeMetrics({ owners, deals: allDeals, dealEmailMap, dealContactIdMap, contactAnalyticsSources, funnel, paidCustomers, churnedCustomers, activity, activation, engagementTiers, ebData }) {
  const now = new Date();
  const totalCosts = Object.values(MONTHLY_COSTS).reduce((s, v) => s + v, 0);
  const stageMap = {};
  for (const s of STAGES) stageMap[s.id] = s;

  const deals = allDeals.filter(d => d.properties.pipeline === 'default' || !d.properties.pipeline);

  // ── Today's KPIs ──
  const todayStr = new Date().toLocaleDateString('en-CA', { timeZone: 'Australia/Melbourne' });
  const activityTypes = { calls: activity.calls, meetings: activity.meetings, notes: activity.notes };
  const { day: today, kpis: todayKPIs } = computeDayMetrics(todayStr, activity, deals, owners);

  let totalActivity30d = 0;
  for (const [type, records] of Object.entries(activityTypes)) {
    if (records) totalActivity30d += records.length;
  }
  today.dailyAvg = Math.round(totalActivity30d / 30);

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
  const dailyAvgByRep = {};
  for (const [name, totals] of Object.entries(dailyByRep)) {
    dailyAvgByRep[name] = {
      calls: Math.round(totals.calls / 30), meetings: Math.round(totals.meetings / 30),
      notes: Math.round(totals.notes / 30), total: Math.round((totals.calls + totals.meetings + totals.notes) / 30),
    };
  }

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

  // ── Deal Source (with fallback attribution) ──
  const sourceAttribution = computeSourceAttribution(deals, dealContactIdMap, contactAnalyticsSources, owners);
  const enhancedSourceMap = {};
  for (const ea of sourceAttribution.enhanced) enhancedSourceMap[ea.dealId] = ea.source;

  const sourceStats = {};
  for (const s of DEAL_SOURCES) sourceStats[s.value] = { label: s.label, total: 0, won: 0, lost: 0, open: 0, wonMRR: 0, winRate: 0, cycleDays: [] };
  sourceStats.unknown = { label: 'Unknown', total: 0, won: 0, lost: 0, open: 0, wonMRR: 0, winRate: 0, cycleDays: [] };
  for (const d of deals) {
    const src = enhancedSourceMap[d.id] || d.properties.deal_source || 'unknown';
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

  // ── Rep Performance (filter active reps for display) ──
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
    r.isActive = ACTIVE_REPS.includes(r.name);
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
  const availableDates = Object.keys(dailyMap);
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

  // ── Existing Compute ──
  const stageConversion = computeStageConversion(deals);
  const sourceCycle = computeSourceCycle(deals);
  const dealHealth = computeDealHealth(deals, owners);
  const weightedForecast = computeWeightedForecast(deals);
  const repChannelAttribution = computeRepChannelAttribution(deals, owners);
  const touchVelocity = computeTouchVelocity(deals, activity, owners);

  // ── NEW Compute ──
  const dealHealthScores = computeDealHealthScores(deals, owners, activity);
  const bottlenecks = computeBottlenecks(deals);
  const dataQuality = computeDataQuality(deals, paidCustomers, engagementTiers, eb);
  const unitEconomics = computeUnitEconomics(pnl, wonDeals);
  const mrrWaterfall = computeMRRWaterfall(deals, churnedCustomers, paidCustomers);
  const woW = computeWoWComparison(historicalByDay, availableDates, deals, owners);

  // ── Weekly Rollups ──
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

  // Channel mix recommendation
  const channels = [
    { name: 'Cold Email', ...channelROI.coldEmail },
    { name: 'Cold Call / Sales', ...channelROI.coldCall },
  ].filter(c => c.won > 0).sort((a, b) => (a.cac || 9999) - (b.cac || 9999));
  const bestChannel = channels[0] || null;

  // ── Pipeline Coverage ──
  const monthlyTarget = totalCosts; // break-even as minimum target
  const coverageRatio = monthlyTarget > 0 ? parseFloat((weightedPipelineValue / monthlyTarget).toFixed(1)) : 0;
  const pipelineCoverage = { weighted: Math.round(weightedPipelineValue), target: Math.round(monthlyTarget), ratio: coverageRatio, gap: Math.max(0, Math.round(monthlyTarget * 3 - weightedPipelineValue)) };

  // ── Alerts ──
  const partialMetrics = { pnl, dealHealthScores, eb, dataQuality, todayKPIs, woW };
  const alerts = computeAlerts(partialMetrics);

  // ── Executive Summary ──
  const healthScore = Math.round(
    (Math.min(winRate, 100) * 0.25) +
    (Math.min(coverageRatio * 33, 100) * 0.25) +
    ((unitEconomics.ltvCacRatio ? Math.min(unitEconomics.ltvCacRatio * 33, 100) : 0) * 0.25) +
    (dataQuality.overallScore * 0.25)
  );

  // MRR sparkline data (daily MRR approximation — use won deal dates)
  const mrrSparkline = [];
  let runningMRR = currentMRR;
  for (let i = 29; i >= 0; i--) {
    mrrSparkline.push(runningMRR); // simplified: constant for now since we can't derive historical MRR without snapshots
  }

  const executiveSummary = {
    mrr: currentMRR,
    mrrDelta7d: mrrWaterfall.netMRR,
    customers: paidCount,
    pipeline: Math.round(weightedPipelineValue),
    winRate,
    cac: pnl.cac,
    healthScore,
    alerts,
    mrrSparkline,
  };

  // ── Commissions, Leaderboard, Health Pulse ──
  const commissions = computeCommissions(deals, owners, contactAnalyticsSources, paidCustomers);
  const leaderboard = computeLeaderboard(reps, weeklyRollup, commissions, deals, owners);
  const healthPulse = computeHealthPulse(winRate, coverageRatio, unitEconomics, woW, dataQuality);

  // ── AI Priorities ──
  const priorities = [];
  // Reps behind on daily targets
  if (todayKPIs) {
    for (const repName of ACTIVE_REPS) {
      const kpi = todayKPIs[repName];
      if (!kpi) continue;
      const t = kpi.targets;
      const score = Math.round(((kpi.uniqueCalls / t.uniqueCalls) + (kpi.callHours / t.callHours) + (kpi.dailyRevenue / t.dailyRevenue)) / 3 * 100);
      if (score < 50 && score > 0) priorities.push({ severity: 'warning', message: repName.split(' ')[0] + ' at ' + score + '% of daily targets — check in' });
    }
  }
  // Deals in Demo Complete >7 days
  for (const d of deals) {
    if (d.properties.dealstage === '2851995329') {
      const entered = new Date(d.properties.hs_v2_date_entered_2851995329 || d.properties.hs_lastmodifieddate);
      const days = Math.round((Date.now() - entered) / 86400000);
      if (days > 7) {
        const rep = d.properties.hubspot_owner_id ? (owners[d.properties.hubspot_owner_id] || '') : '';
        priorities.push({ severity: 'critical', message: d.properties.dealname + ' — Demo Complete for ' + days + 'd, push to close' + (rep ? ' (' + rep.split(' ')[0] + ')' : '') });
      }
    }
  }
  // Stale deals needing follow-up
  if (dealHealthScores) {
    const critical = dealHealthScores.deals.filter(d => d.category === 'critical').slice(0, 3);
    for (const d of critical) {
      priorities.push({ severity: 'warning', message: d.name + ' — no activity in ' + d.daysSinceUpdate + 'd (' + d.rep + ')' });
    }
  }
  // Churn vs new
  if (mrrWaterfall.churnMRR > mrrWaterfall.newMRR && mrrWaterfall.churnCount > 0) {
    priorities.push({ severity: 'critical', message: 'Churn ($' + mrrWaterfall.churnMRR + ') outpacing new MRR ($' + mrrWaterfall.newMRR + ') in 30d — ' + mrrWaterfall.churnCount + ' lost' });
  }
  // Negative ROI
  if (pnl.roi < 0) {
    priorities.push({ severity: 'info', message: 'ROI at ' + pnl.roi + '% — need ' + (pnl.breakEvenCustomers - paidCount) + ' more customers to break even' });
  }
  priorities.sort((a, b) => ({ critical: 0, warning: 1, info: 2 }[a.severity] - { critical: 0, warning: 1, info: 2 }[b.severity]));

  // ── Cache metadata ──
  const cacheAge = cache.time > 0 ? Math.round((Date.now() - cache.time) / 60000) : 0;
  const dataFreshness = { cacheAgeMinutes: cacheAge, lastError: cache.lastError, isStale: cacheAge > 10 };

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
    engagementTiers,
    executiveSummary, dealHealthScores, bottlenecks, dataQuality, unitEconomics, mrrWaterfall, woW,
    sourceAttribution, pipelineCoverage, bestChannel, dataFreshness, activeReps: ACTIVE_REPS,
    // Merged from new version
    commissions, leaderboard, healthPulse, priorities,
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
<title>Sammy AI - RevOps</title>
<script src="https://cdn.tailwindcss.com"><\/script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"><\/script>
<style>
  :root {
    --bg: #f5f5f7; --surface: #ffffff; --surface-hover: #fafafa;
    --text-primary: #1d1d1f; --text-secondary: #6e6e73; --text-tertiary: #aeaeb2;
    --border: rgba(0,0,0,0.06); --border-strong: rgba(0,0,0,0.1);
    --blue: #007aff; --green: #34c759; --red: #ff3b30; --orange: #ff9500;
    --purple: #af52de; --teal: #5ac8fa; --indigo: #5856d6;
    --blue-bg: rgba(0,122,255,0.08); --green-bg: rgba(52,199,89,0.08);
    --red-bg: rgba(255,59,48,0.08); --orange-bg: rgba(255,149,0,0.08);
    --radius: 16px; --radius-sm: 10px; --radius-xs: 6px;
    --shadow: 0 1px 3px rgba(0,0,0,0.04), 0 1px 2px rgba(0,0,0,0.02);
    --shadow-md: 0 4px 12px rgba(0,0,0,0.06), 0 1px 3px rgba(0,0,0,0.04);
    --shadow-lg: 0 8px 30px rgba(0,0,0,0.08), 0 2px 8px rgba(0,0,0,0.04);
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Display', 'SF Pro Text', 'Helvetica Neue', system-ui, sans-serif; background: var(--bg); color: var(--text-primary); -webkit-font-smoothing: antialiased; line-height: 1.47; }
  .container { max-width: 1200px; margin: 0 auto; padding: 0 24px; }

  /* ── Header ── */
  .header { position: sticky; top: 0; z-index: 100; background: rgba(255,255,255,0.72); backdrop-filter: saturate(180%) blur(20px); -webkit-backdrop-filter: saturate(180%) blur(20px); border-bottom: 0.5px solid var(--border-strong); }
  .header-inner { padding: 12px 0 0; }
  .header-top { display: flex; justify-content: space-between; align-items: center; gap: 12px; flex-wrap: wrap; margin-bottom: 12px; }
  .brand { font-size: 20px; font-weight: 700; letter-spacing: -0.02em; }
  .brand span { font-weight: 400; color: var(--text-tertiary); font-size: 12px; margin-left: 6px; }
  .timestamp { font-size: 11px; color: var(--text-tertiary); margin-top: 1px; }
  .controls { display: flex; align-items: center; gap: 10px; }
  .controls select { font-size: 13px; padding: 6px 12px; border: 0.5px solid var(--border-strong); border-radius: var(--radius-xs); background: var(--surface); color: var(--text-primary); outline: none; appearance: none; cursor: pointer; }
  .controls select:focus { box-shadow: 0 0 0 3px var(--blue-bg); }
  .date-nav { display: flex; align-items: center; gap: 2px; background: var(--surface); border: 0.5px solid var(--border-strong); border-radius: var(--radius-xs); padding: 0 2px; }
  .date-nav button { padding: 6px 8px; background: none; border: none; cursor: pointer; color: var(--text-secondary); font-size: 16px; border-radius: 4px; transition: background 0.15s; }
  .date-nav button:hover:not(:disabled) { background: var(--bg); }
  .date-nav button:disabled { opacity: 0.25; cursor: not-allowed; }
  .date-nav span { font-size: 13px; font-weight: 500; min-width: 110px; text-align: center; color: var(--text-primary); }
  .refresh-btn { color: var(--text-tertiary); font-size: 16px; text-decoration: none; padding: 6px; border-radius: 50%; transition: all 0.2s; }
  .refresh-btn:hover { background: var(--bg); color: var(--text-secondary); }

  /* ── KPI Strip ── */
  .kpi-strip { display: grid; grid-template-columns: repeat(6, 1fr); gap: 1px; background: var(--border); border-radius: var(--radius-sm); overflow: hidden; margin-bottom: 12px; box-shadow: var(--shadow); }
  .kpi-item { background: var(--surface); padding: 10px 14px; text-align: center; }
  .kpi-label { font-size: 10px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.06em; color: var(--text-tertiary); margin-bottom: 2px; }
  .kpi-value { font-size: 17px; font-weight: 700; letter-spacing: -0.02em; }
  .kpi-sub { font-size: 10px; color: var(--text-tertiary); margin-top: 1px; }

  /* ── Alerts ── */
  .alert { display: flex; align-items: center; gap: 8px; padding: 8px 14px; border-radius: var(--radius-xs); font-size: 12px; font-weight: 500; margin-bottom: 4px; }
  .alert-critical { background: var(--red-bg); color: var(--red); }
  .alert-warning { background: var(--orange-bg); color: var(--orange); }
  .alert-info { background: var(--blue-bg); color: var(--blue); }
  .alert-icon { width: 18px; height: 18px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 10px; font-weight: 800; flex-shrink: 0; }
  .alert-critical .alert-icon { background: var(--red); color: white; }
  .alert-warning .alert-icon { background: var(--orange); color: white; }
  .alert-info .alert-icon { background: var(--blue); color: white; }

  /* ── Tabs ── */
  .tab-bar { display: flex; gap: 4px; padding-bottom: 12px; overflow-x: auto; }
  .tab-btn { padding: 6px 16px; font-size: 13px; font-weight: 500; border: none; border-radius: 980px; cursor: pointer; transition: all 0.2s; background: transparent; color: var(--text-secondary); white-space: nowrap; }
  .tab-btn:hover { background: rgba(0,0,0,0.04); }
  .tab-btn.active { background: var(--text-primary); color: white; }

  /* ── Cards ── */
  .card { background: var(--surface); border-radius: var(--radius); padding: 20px; box-shadow: var(--shadow); transition: box-shadow 0.2s; }
  .card:hover { box-shadow: var(--shadow-md); }
  .card-sm { padding: 16px; border-radius: var(--radius-sm); }
  .card-label { font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-tertiary); }
  .card-value { font-size: 28px; font-weight: 700; letter-spacing: -0.03em; margin-top: 4px; line-height: 1.1; }
  .card-sub { font-size: 12px; color: var(--text-tertiary); margin-top: 4px; }
  .metric-card { background: var(--surface); border-radius: var(--radius-sm); padding: 14px 16px; box-shadow: var(--shadow); }
  .metric-label { font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-tertiary); }
  .metric-value { font-size: 22px; font-weight: 700; letter-spacing: -0.02em; margin-top: 2px; }
  .metric-sub { font-size: 11px; color: var(--text-tertiary); margin-top: 2px; }

  /* ── Section headers ── */
  .section-title { font-size: 20px; font-weight: 700; letter-spacing: -0.02em; color: var(--text-primary); margin-bottom: 16px; }
  .section-subtitle { font-size: 13px; font-weight: 600; color: var(--text-secondary); margin-bottom: 12px; text-transform: uppercase; letter-spacing: 0.04em; }

  /* ── Tables ── */
  .table { width: 100%; border-collapse: collapse; font-size: 13px; }
  .table th { font-size: 10px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.06em; color: var(--text-tertiary); padding: 12px 14px; text-align: left; border-bottom: 1px solid var(--border-strong); position: sticky; top: 0; background: var(--surface); z-index: 2; }
  .table th.right { text-align: right; }
  .table td { padding: 12px 14px; border-bottom: 0.5px solid var(--border); color: var(--text-primary); }
  .table td.right { text-align: right; }
  .table td.muted { color: var(--text-tertiary); }
  .table tr { transition: background 0.15s; }
  .table tbody tr:nth-child(even) { background: rgba(0,0,0,0.015); }
  .table tr:hover { background: rgba(0,122,255,0.04); }
  .table tr.clickable { cursor: pointer; }
  .num { font-family: 'SF Mono', 'Fira Code', ui-monospace, monospace; font-weight: 600; }

  /* ── Badges ── */
  .badge { display: inline-flex; align-items: center; padding: 2px 8px; border-radius: 980px; font-size: 11px; font-weight: 600; }
  .badge-green { background: var(--green-bg); color: var(--green); }
  .badge-red { background: var(--red-bg); color: var(--red); }
  .badge-orange { background: var(--orange-bg); color: var(--orange); }
  .badge-blue { background: var(--blue-bg); color: var(--blue); }
  .badge-gray { background: rgba(0,0,0,0.05); color: var(--text-secondary); }

  /* ── Progress bars ── */
  .progress { height: 6px; background: rgba(0,0,0,0.05); border-radius: 3px; overflow: hidden; }
  .progress-fill { height: 100%; border-radius: 3px; transition: width 0.6s cubic-bezier(0.16, 1, 0.3, 1); }

  /* ── Score indicators ── */
  .score-healthy { background: var(--green-bg); color: #1b8a36; }
  .score-monitor { background: rgba(255,204,0,0.12); color: #946800; }
  .score-attention { background: var(--orange-bg); color: #c25e00; }
  .score-critical { background: var(--red-bg); color: #d12215; }

  /* ── WoW ── */
  .wow-up { color: var(--green); }
  .wow-down { color: var(--red); }

  /* ── Deal link cards ── */
  .deal-row { display: flex; align-items: center; justify-content: space-between; padding: 10px 14px; border-radius: var(--radius-sm); transition: background 0.15s; text-decoration: none; color: inherit; }
  .deal-row:hover { background: var(--bg); }

  /* ── Grid layouts ── */
  .grid-2 { display: grid; grid-template-columns: repeat(2, 1fr); gap: 12px; }
  .grid-3 { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; }
  .grid-4 { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }
  .grid-5 { display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px; }
  .grid-6 { display: grid; grid-template-columns: repeat(6, 1fr); gap: 12px; }
  .grid-2-1 { display: grid; grid-template-columns: 2fr 1fr; gap: 16px; }
  .grid-1-1 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
  .gap-sm { gap: 8px; }
  .gap-md { gap: 16px; }
  .gap-lg { gap: 24px; }

  /* ── Utility ── */
  .hidden { display: none !important; }
  .space-y > * + * { margin-top: 20px; }
  .space-y-sm > * + * { margin-top: 12px; }
  .mt-sm { margin-top: 8px; }
  .mt-md { margin-top: 16px; }
  .text-green { color: var(--green); }
  .text-red { color: var(--red); }
  .text-blue { color: var(--blue); }
  .text-orange { color: var(--orange); }
  .text-purple { color: var(--purple); }
  .text-teal { color: var(--teal); }
  .text-muted { color: var(--text-tertiary); }
  .font-bold { font-weight: 700; }
  .font-medium { font-weight: 500; }
  .text-sm { font-size: 13px; }
  .text-xs { font-size: 11px; }
  .text-right { text-align: right; }
  .truncate { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .flex { display: flex; }
  .flex-between { display: flex; justify-content: space-between; align-items: center; }
  .flex-center { display: flex; align-items: center; }
  .flex-1 { flex: 1; min-width: 0; }
  .overflow-auto { overflow-x: auto; }

  /* ── Charts ── */
  canvas { border-radius: var(--radius-sm); }

  /* ── Health Pulse ── */
  .pulse-hero { display: flex; align-items: center; gap: 16px; padding: 14px 20px; border-radius: var(--radius); margin-bottom: 12px; box-shadow: var(--shadow); }
  .pulse-hero.pulse-green { background: linear-gradient(135deg, rgba(52,199,89,0.08), rgba(52,199,89,0.02)); border: 1px solid rgba(52,199,89,0.15); }
  .pulse-hero.pulse-amber { background: linear-gradient(135deg, rgba(255,149,0,0.08), rgba(255,149,0,0.02)); border: 1px solid rgba(255,149,0,0.15); }
  .pulse-hero.pulse-red { background: linear-gradient(135deg, rgba(255,59,48,0.08), rgba(255,59,48,0.02)); border: 1px solid rgba(255,59,48,0.15); }
  .pulse-dot { width: 14px; height: 14px; border-radius: 50%; flex-shrink: 0; animation: pulse-glow 2s ease-in-out infinite; }
  .pulse-green .pulse-dot { background: var(--green); }
  .pulse-amber .pulse-dot { background: var(--orange); }
  .pulse-red .pulse-dot { background: var(--red); }
  @keyframes pulse-glow { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
  .pulse-score { font-size: 28px; font-weight: 800; letter-spacing: -0.03em; line-height: 1; }
  .pulse-diagnosis { font-size: 13px; font-weight: 500; color: var(--text-secondary); flex: 1; }

  /* ── Leaderboard ── */
  .podium { background: var(--surface); border-radius: var(--radius); padding: 24px; box-shadow: var(--shadow-md); text-align: center; border: 2px solid var(--blue); }
  .podium-rank { font-size: 48px; font-weight: 900; color: var(--blue); letter-spacing: -0.04em; line-height: 1; }
  .podium-name { font-size: 20px; font-weight: 700; margin-top: 8px; letter-spacing: -0.02em; }
  .podium-score { font-size: 14px; color: var(--text-secondary); margin-top: 4px; }
  .period-toggle { display: flex; gap: 2px; background: var(--bg); border-radius: var(--radius-xs); padding: 2px; }
  .period-btn { padding: 6px 14px; font-size: 12px; font-weight: 600; border: none; border-radius: 4px; cursor: pointer; background: transparent; color: var(--text-secondary); transition: all 0.15s; }
  .period-btn.active { background: var(--surface); color: var(--text-primary); box-shadow: var(--shadow); }

  /* ── Commission Cards ── */
  .commission-card { background: var(--surface); border-radius: var(--radius); padding: 24px; box-shadow: var(--shadow); }
  .commission-amount { font-size: 36px; font-weight: 800; letter-spacing: -0.04em; line-height: 1; }

  /* ── Priority Alerts ── */
  .priority-item { display: flex; align-items: center; gap: 12px; padding: 12px 16px; border-radius: var(--radius-sm); font-size: 14px; font-weight: 500; margin-bottom: 6px; line-height: 1.4; }
  .priority-critical { background: var(--red-bg); color: var(--red); border-left: 3px solid var(--red); }
  .priority-warning { background: var(--orange-bg); color: var(--orange); border-left: 3px solid var(--orange); }
  .priority-info { background: var(--blue-bg); color: var(--blue); border-left: 3px solid var(--blue); }
  .priority-icon { width: 24px; height: 24px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 12px; font-weight: 800; flex-shrink: 0; color: white; }
  .priority-critical .priority-icon { background: var(--red); }
  .priority-warning .priority-icon { background: var(--orange); }
  .priority-info .priority-icon { background: var(--blue); }

  /* ── Score Pills ── */
  .score-pill { display: inline-flex; align-items: center; gap: 4px; padding: 3px 10px; border-radius: 980px; font-size: 12px; font-weight: 700; min-width: 50px; justify-content: center; }
  .score-green { background: var(--green-bg); color: var(--green); }
  .score-amber { background: var(--orange-bg); color: var(--orange); }
  .score-red { background: var(--red-bg); color: var(--red); }

  /* ── Plan Badges ── */
  .plan-badge { display: inline-flex; padding: 2px 8px; border-radius: 980px; font-size: 11px; font-weight: 600; }
  .plan-founder { background: var(--green-bg); color: var(--green); }
  .plan-monthly { background: var(--blue-bg); color: var(--blue); }
  .plan-other { background: rgba(0,0,0,0.05); color: var(--text-secondary); }

  /* ── Closer Dots ── */
  .closer-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; }

  /* ── Responsive ── */
  @media (max-width: 768px) {
    .kpi-strip { grid-template-columns: repeat(3, 1fr); }
    .grid-4, .grid-5, .grid-6 { grid-template-columns: repeat(2, 1fr); }
    .grid-2-1, .grid-1-1 { grid-template-columns: 1fr; }
    .container { padding: 0 16px; }
  }
  @media (max-width: 480px) {
    .grid-3, .grid-2 { grid-template-columns: 1fr; }
    .kpi-strip { grid-template-columns: repeat(2, 1fr); }
  }
  @media print { .no-print { display: none; } canvas { max-height: 300px; } }

  /* ── Loading shimmer ── */
  .health-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; }
</style>
</head>
<body>

<!-- HEADER -->
<header class="header">
  <div class="container">
    <div class="header-inner">
      <div class="header-top">
        <div>
          <div class="brand">Sammy<span>RevOps</span></div>
          <div class="timestamp" id="timestamp"></div>
        </div>
        <div class="controls no-print">
          <select id="repSelect" onchange="setRep(this.value)">
            <option value="">All Reps</option>
          </select>
          <div class="date-nav" id="dateNav">
            <button id="btnDatePrev" onclick="navigateDate(-1)">&lsaquo;</button>
            <span id="dateDisplay"></span>
            <button id="btnDateNext" onclick="navigateDate(1)">&rsaquo;</button>
          </div>
          <a href="/refresh" class="refresh-btn" title="Refresh">&#x21bb;</a>
        </div>
      </div>

      <div id="healthPulseHero"></div>
      <div class="kpi-strip" id="execSummary"></div>
      <div id="alertsBanner"></div>

      <nav class="tab-bar no-print">
        <button id="btnToday" class="tab-btn" onclick="switchTab('today')">Command Center</button>
        <button id="btnPipeline" class="tab-btn" onclick="switchTab('pipeline')">Pipeline</button>
        <button id="btnChannels" class="tab-btn" onclick="switchTab('channels')">Channels</button>
        <button id="btnRevops" class="tab-btn" onclick="switchTab('revops')">RevOps</button>
        <button id="btnLeaderboard" class="tab-btn" onclick="switchTab('leaderboard')">Leaderboard</button>
      </nav>
    </div>
  </div>
</header>

<main class="container" style="padding-top:24px;padding-bottom:48px;">

  <!-- TODAY / COMMAND CENTER -->
  <div id="tabToday" class="space-y">
    <div id="prioritiesSection"></div>
    <div id="taskChecklistSection"></div>
    <div id="scorecardSection"></div>
    <div id="drilldownSection" class="hidden"></div>
    <div id="myDaySection"></div>
    <div id="weeklySection"></div>
    <div id="commissionsSection"></div>
    <div id="todayCESection"></div>
    <div id="repComparisonSection"></div>
  </div>

  <!-- PIPELINE -->
  <div id="tabPipeline" class="space-y hidden">
    <div class="grid-5" id="pipelineKPIs"></div>
    <div class="grid-2-1 gap-md">
      <div class="card"><canvas id="pipelineChart" height="260"></canvas></div>
      <div class="card"><p class="section-subtitle">Stage Conversion</p><div id="stageConversionSection"></div></div>
    </div>
    <div class="grid-1-1 gap-md">
      <div class="card"><p class="section-subtitle">Touch Velocity</p><canvas id="touchVelocityChart" height="200"></canvas></div>
      <div class="card"><p class="section-subtitle">Deal Health Scores</p><div id="dealHealthScoresSection"></div></div>
    </div>
    <div class="card"><p class="section-subtitle">Stale Deal Triage</p><div id="staleDealTriageSection"></div></div>
  </div>

  <!-- CHANNELS -->
  <div id="tabChannels" class="space-y hidden">
    <div id="dataQualityBanner"></div>
    <div id="sourceAttributionSection"></div>
    <div class="grid-1-1 gap-md">
      <div class="card"><p class="section-subtitle">Deals by Source</p><canvas id="sourceChart" height="220"></canvas></div>
      <div class="card"><p class="section-subtitle">Avg Cycle by Source</p><canvas id="sourceCycleChart" height="220"></canvas></div>
    </div>
    <div id="ebFunnelSection"></div>
    <div id="ebCampaignSection"></div>
    <div id="channelROISection"></div>
    <div id="channelMixSection"></div>
  </div>

  <!-- REVOPS -->
  <div id="tabRevops" class="space-y hidden">
    <div class="grid-4" id="unitEconCards"></div>
    <div class="grid-1-1 gap-md">
      <div class="card"><p class="section-subtitle">MRR Movement (30d)</p><canvas id="mrrWaterfallChart" height="200"></canvas></div>
      <div class="card"><p class="section-subtitle">Week-over-Week</p><div id="wowSection"></div></div>
    </div>
    <div class="grid-6" id="pnlCards"></div>
    <div class="grid-1-1 gap-md">
      <div class="card"><p class="section-subtitle">Cost Breakdown</p><table class="table" id="costTable"></table></div>
      <div class="card"><p class="section-subtitle">MRR by Tier</p><div class="flex-center" style="gap:24px"><canvas id="mrrChart" style="max-height:160px"></canvas><div id="mrrLegend" class="space-y-sm"></div></div></div>
    </div>
    <div class="grid-1-1 gap-md">
      <div class="card"><p class="section-subtitle">Conversion Funnel</p><canvas id="funnelChart" height="200"></canvas></div>
      <div class="card"><p class="section-subtitle">Engagement Distribution</p><div class="flex-center" style="gap:24px"><canvas id="engScoreChart" style="max-height:160px"></canvas><div id="engScoreLegend" class="space-y-sm"></div></div></div>
    </div>
    <div class="grid-3" id="funnelKPIs"></div>
    <div id="forecastSection"></div>
    <div>
      <p class="section-title">Deal Velocity</p>
      <div class="grid-3 mt-sm" id="velocityKPIs"></div>
      <div class="grid-1-1 gap-md mt-md">
        <div class="card"><canvas id="velocityChart" height="220"></canvas></div>
        <div class="card" style="max-height:400px;overflow-y:auto"><p class="section-subtitle">Stale Deals (&gt;30d)</p><table class="table" id="staleTable"></table></div>
      </div>
    </div>
  </div>

  <!-- LEADERBOARD -->
  <div id="tabLeaderboard" class="space-y hidden">
    <div class="flex-between" style="margin-bottom:16px">
      <p class="section-title" style="margin-bottom:0">Leaderboard</p>
      <div class="period-toggle" id="periodToggle">
        <button class="period-btn active" onclick="setPeriod('week')">Week</button>
        <button class="period-btn" onclick="setPeriod('month')">Month</button>
        <button class="period-btn" onclick="setPeriod('allTime')">All-Time</button>
      </div>
    </div>
    <div id="podiumSection"></div>
    <div id="categoryTable"></div>
  </div>

</main>

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
const BLUE = '#007aff', GREEN = '#34c759', RED = '#ff3b30', AMBER = '#ff9500', PURPLE = '#af52de', CYAN = '#5ac8fa', GRAY = '#8e8e93', LGRAY = '#e5e5ea';

function card(label, value, color, sub) {
  return '<div class="metric-card">'
    + '<p class="metric-label">' + label + '</p>'
    + '<p class="metric-value" style="color:' + color + '">' + value + '</p>'
    + (sub ? '<p class="metric-sub">' + sub + '</p>' : '') + '</div>';
}

function miniCard(label, value, color, sub) {
  return '<div class="kpi-item">'
    + '<p class="kpi-label">' + label + '</p>'
    + '<p class="kpi-value" style="color:' + color + '">' + value + '</p>'
    + (sub ? '<p class="kpi-sub">' + sub + '</p>' : '') + '</div>';
}

// ═══ HEALTH PULSE ═══
if (D.healthPulse) {
  const hp = D.healthPulse;
  const colorMap = { green: GREEN, amber: AMBER, red: RED };
  const scoreColor = colorMap[hp.color] || GRAY;
  $('healthPulseHero').innerHTML = '<div class="pulse-hero pulse-' + hp.color + '">'
    + '<div class="pulse-dot"></div>'
    + '<div class="pulse-score" style="color:' + scoreColor + '">' + hp.overall + '</div>'
    + '<div class="pulse-diagnosis">' + hp.diagnosis + '</div>'
    + (D.dataFreshness && D.dataFreshness.isStale ? '<span class="badge badge-orange" style="font-size:10px">Data ' + D.dataFreshness.cacheAgeMinutes + 'm old</span>' : '')
    + '</div>';
}

// ═══ STATE ═══
const TABS = ['today','pipeline','channels','revops','leaderboard'];
let activeTab = TABS.includes(TAB_INIT) ? TAB_INIT : 'today';
let selectedRepName = null;
let selectedDate = (DATE_INIT && D.availableDates.includes(DATE_INIT)) ? DATE_INIT : TODAY_STR;
const renderedTabs = new Set();
let drillDownRep = null;
let lbPeriod = 'week';
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

// Populate rep dropdown — only active reps
const sel = $('repSelect');
const salesReps = D.reps.filter(r => D.activeReps.includes(r.name));
for (const r of salesReps) {
  const opt = document.createElement('option');
  opt.value = r.name;
  opt.textContent = r.name;
  if (r.name === selectedRepName) opt.selected = true;
  sel.appendChild(opt);
}

$('timestamp').textContent = 'Live \\u2014 ' + D.generated;

// ═══ EXECUTIVE SUMMARY (persistent) ═══
function renderExecSummary() {
  const es = D.executiveSummary;
  const mrrArrow = es.mrrDelta7d >= 0 ? '\\u2191' : '\\u2193';
  const mrrDeltaColor = es.mrrDelta7d >= 0 ? GREEN : RED;
  const hsColor = es.healthScore >= 80 ? GREEN : (es.healthScore >= 50 ? AMBER : RED);
  const cacColor = es.cac && es.cac < 300 ? GREEN : (es.cac && es.cac < 600 ? AMBER : RED);

  $('execSummary').innerHTML = [
    miniCard('MRR', fmt(es.mrr), GREEN, '<span style="color:' + mrrDeltaColor + '">' + mrrArrow + ' $' + Math.abs(es.mrrDelta7d) + ' (30d)</span>'),
    miniCard('Customers', es.customers, BLUE, ''),
    miniCard('Pipeline', fmt(es.pipeline), PURPLE, 'weighted'),
    miniCard('Win Rate', pct(es.winRate), es.winRate >= 30 ? GREEN : AMBER, ''),
    miniCard('CAC', es.cac ? fmt(es.cac) : 'N/A', cacColor, ''),
    miniCard('Health', es.healthScore + '/100', hsColor, ''),
  ].join('');

  // Alerts banner
  if (es.alerts && es.alerts.length > 0) {
    let alertHTML = '';
    for (const a of es.alerts.slice(0, 3)) {
      const cls = 'alert alert-' + a.severity;
      alertHTML += '<div class="' + cls + '">'
        + '<span class="alert-icon">' + (a.severity === 'critical' ? '!' : a.severity === 'warning' ? '!' : 'i') + '</span>'
        + '<span>' + a.message + '</span></div>';
    }
    $('alertsBanner').innerHTML = alertHTML;
  } else {
    $('alertsBanner').innerHTML = '';
  }

  if (D.dataFreshness && D.dataFreshness.isStale) {
    $('alertsBanner').innerHTML += '<div class="alert alert-warning"><span class="alert-icon">!</span>Data is ' + D.dataFreshness.cacheAgeMinutes + ' minutes old</div>';
  }
}
renderExecSummary();

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
    case 'revops': renderRevOps(); break;
    case 'leaderboard': renderLeaderboard(); break;
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

  renderScorecard();
  renderTaskChecklist();

  const kpi = selectedRepName ? (dayData.kpis?.[selectedRepName] || null) : null;

  if (kpi) {
    function kpiBar(label, current, target, prefix, suffix) {
      const pct = target > 0 ? Math.min(Math.round((current / target) * 100), 100) : 0;
      const barCol = pct >= 100 ? GREEN : (pct >= 50 ? AMBER : RED);
      return '<div style="margin-bottom:16px">'
        + '<div class="flex-between" style="margin-bottom:6px">'
        + '<span class="text-sm font-medium">' + label + '</span>'
        + '<span class="text-sm font-bold" style="color:' + barCol + '">' + prefix + current + suffix + ' / ' + prefix + target + suffix + '</span></div>'
        + '<div class="progress"><div class="progress-fill" style="width:' + pct + '%;background:' + barCol + '"></div></div></div>';
    }
    const overallPct = Math.round(((kpi.uniqueCalls / kpi.targets.uniqueCalls) + (kpi.callHours / kpi.targets.callHours) + (kpi.dailyRevenue / kpi.targets.dailyRevenue)) / 3 * 100);
    const overallColor = overallPct >= 100 ? GREEN : (overallPct >= 50 ? AMBER : RED);
    const overallText = overallPct >= 100 ? 'Targets hit' : (overallPct >= 50 ? 'Getting there' : (overallPct === 0 ? 'Not started' : 'Behind'));

    $('myDaySection').innerHTML = '<div class="card">'
      + '<div class="flex-between" style="margin-bottom:20px">'
      + '<div><p class="section-title" style="margin-bottom:2px">' + selectedRepName.split(' ')[0] + "'s " + (isToday ? "Day" : dateLabel) + '</p>'
      + '<p class="text-xs text-muted">Daily KPI targets</p></div>'
      + '<span class="badge" style="background:' + overallColor + '14;color:' + overallColor + '">' + overallText + '</span></div>'
      + kpiBar('Unique Dials', kpi.uniqueCalls, kpi.targets.uniqueCalls, '', '')
      + kpiBar('Call Time', kpi.callHours, kpi.targets.callHours, '', 'h')
      + kpiBar('Revenue', kpi.dailyRevenue, kpi.targets.dailyRevenue, '$', '')
      + '<div style="margin-top:16px;padding-top:12px;border-top:0.5px solid var(--border)">'
      + '<p class="text-sm text-muted font-medium">' + kpi.uniqueCalls + ' dials  ·  ' + kpi.callHours + 'h talk  ·  ' + todayData.meetings + ' meetings  ·  ' + todayData.notes + ' notes  ·  $' + kpi.dailyRevenue + ' closed</p>'
      + '</div></div>';
  } else {
    const pctDone = avgTotal > 0 ? Math.min(Math.round((todayTotal / avgTotal) * 100), 100) : (todayTotal > 0 ? 100 : 0);
    const barColor = pctDone >= 100 ? GREEN : (pctDone >= 50 ? AMBER : RED);
    const statusText = pctDone >= 100 ? 'On pace' : (pctDone >= 50 ? 'Getting there' : (todayTotal === 0 ? 'Not started' : 'Behind'));

    $('myDaySection').innerHTML = '<div class="card">'
      + '<div class="flex-between" style="margin-bottom:16px">'
      + '<div><p class="section-title" style="margin-bottom:2px">' + (selectedRepName ? selectedRepName.split(' ')[0] + "'s " + (isToday ? "Day" : dateLabel) : (isToday ? "Team Today" : "Team " + dateLabel)) + '</p>'
      + '<p class="text-xs text-muted">Activity vs 30-day average</p></div>'
      + '<span class="badge" style="background:' + barColor + '14;color:' + barColor + '">' + statusText + '</span></div>'
      + '<div class="progress" style="margin-bottom:16px"><div class="progress-fill" style="width:' + pctDone + '%;background:' + barColor + '"></div></div>'
      + '<p class="text-sm text-muted" style="margin-bottom:20px">' + todayTotal + ' of ' + avgTotal + ' daily avg (' + pctDone + '%)</p>'
      + '<div class="grid-3" style="text-align:center">'
      + '<div><p style="font-size:32px;font-weight:700;letter-spacing:-0.03em;color:' + BLUE + '">' + todayData.calls + '</p><p class="text-xs text-muted" style="margin-top:4px">Calls</p><p class="text-xs text-muted">avg ' + avgData.calls + '/day</p></div>'
      + '<div><p style="font-size:32px;font-weight:700;letter-spacing:-0.03em;color:' + GREEN + '">' + todayData.meetings + '</p><p class="text-xs text-muted" style="margin-top:4px">Meetings</p><p class="text-xs text-muted">avg ' + avgData.meetings + '/day</p></div>'
      + '<div><p style="font-size:32px;font-weight:700;letter-spacing:-0.03em;color:' + PURPLE + '">' + todayData.notes + '</p><p class="text-xs text-muted" style="margin-top:4px">Notes</p><p class="text-xs text-muted">avg ' + avgData.notes + '/day</p></div>'
      + '</div></div>';
  }

  // WEEKLY SUMMARY
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

  // COLD EMAIL SUMMARY
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

  // REP COMPARISON (merged from old Reps tab) — only show in team view
  if (!selectedRepName) {
    const activeReps = D.reps.filter(r => D.activeReps.includes(r.name));
    if (activeReps.length > 0) {
      let rcHTML = '<div class="card" style="padding:0;overflow:hidden"><div style="padding:20px 20px 12px"><p class="section-subtitle">Rep Comparison</p></div>'
        + '<div class="overflow-auto"><table class="table">'
        + '<thead><tr>'
        + '<th>Rep</th><th class="right">Deals</th><th class="right">Won</th><th class="right">Lost</th><th class="right">Win%</th><th class="right">MRR</th><th class="right">Cycle</th><th class="right">$/Activity</th>'
        + '</tr></thead><tbody>';
      for (const r of activeReps) {
        rcHTML += '<tr>'
          + '<td class="font-medium">' + r.name + '</td>'
          + '<td class="right">' + r.total + '</td>'
          + '<td class="right text-green font-bold">' + r.won + '</td>'
          + '<td class="right text-red">' + r.lost + '</td>'
          + '<td class="right">' + r.winRate + '%</td>'
          + '<td class="right font-bold text-green">$' + r.wonMRR.toLocaleString() + '</td>'
          + '<td class="right muted">' + (r.avgCycleDays != null ? r.avgCycleDays + 'd' : '-') + '</td>'
          + '<td class="right font-medium">$' + (r.efficiency || 0).toFixed(1) + '</td></tr>';
      }
      rcHTML += '</tbody></table></div></div>';
      $('repComparisonSection').innerHTML = rcHTML;
    }
  } else { $('repComparisonSection').innerHTML = ''; }
}

// ═══ SCORECARD & DRILL-DOWN ═══
function renderScorecard() {
  const dayData = D.historicalByDay[selectedDate] || D.today;
  const repsWithTargets = D.activeReps.filter(n => D.weeklyRollup[n]);
  let behindCount = 0;

  let html = '<div class="flex-between" style="margin-bottom:16px">'
    + '<p class="section-title" style="margin-bottom:0">Daily Scorecard \\u2014 ' + formatDateDisplay(selectedDate) + '</p>'
    + '<span id="behindBadge" class="badge"></span></div>';

  html += '<div class="card" style="padding:0;overflow:hidden"><div class="overflow-auto">'
    + '<table class="table"><thead><tr>'
    + '<th>Rep</th>'
    + '<th class="right">Dials</th>'
    + '<th class="right">Unique</th>'
    + '<th class="right">Hours</th>'
    + '<th class="right">Mtgs</th>'
    + '<th class="right">Notes</th>'
    + '<th class="right">Revenue</th>'
    + '<th class="right">Score</th>'
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

    function cellColor(pct) { return pct >= 100 ? 'text-green font-bold' : (pct >= 50 ? 'text-orange' : 'text-red font-bold'); }
    const scoreClass = score >= 100 ? 'badge-green' : (score >= 50 ? 'badge-orange' : 'badge-red');
    const borderStyle = isBehind ? 'border-left:3px solid var(--red)' : '';
    const isActive = drillDownRep === repName;
    const bgStyle = isActive ? 'background:var(--blue-bg)' : '';

    var scorePillClass = score >= 100 ? 'score-green' : (score >= 50 ? 'score-amber' : 'score-red');
    html += '<tr class="clickable" style="' + borderStyle + ';' + bgStyle + '" onclick="toggleDrillDown(' + JSON.stringify(repName).replace(/"/g, '&quot;') + ')">'
      + '<td style="font-weight:700;white-space:nowrap">' + repName + '</td>'
      + '<td class="right num">' + rd.calls + '</td>'
      + '<td class="right num ' + cellColor(dialPct) + '">' + kd.uniqueCalls + '</td>'
      + '<td class="right num ' + cellColor(hoursPct) + '">' + kd.callHours + 'h</td>'
      + '<td class="right num">' + rd.meetings + '</td>'
      + '<td class="right num">' + rd.notes + '</td>'
      + '<td class="right num ' + cellColor(revPct) + '">$' + kd.dailyRevenue + '</td>'
      + '<td class="right"><span class="score-pill ' + scorePillClass + '">' + score + '%</span></td>'
      + '</tr>';
  }
  html += '</tbody></table></div></div>';

  $('scorecardSection').innerHTML = html;
  const badge = $('behindBadge');
  if (behindCount > 0) { badge.className = 'badge badge-red'; badge.textContent = behindCount + ' rep' + (behindCount > 1 ? 's' : '') + ' behind'; }
  else { badge.className = 'badge badge-green'; badge.textContent = 'All on track'; }
}

function toggleDrillDown(repName) {
  if (drillDownRep === repName) {
    drillDownRep = null; $('drilldownSection').classList.add('hidden'); $('drilldownSection').innerHTML = ''; renderScorecard(); return;
  }
  drillDownRep = repName; renderScorecard();
  const wr = D.weeklyRollup[repName]; const rep = D.reps.find(r => r.name === repName);
  const a = D.eb?.attribution?.byRep?.[repName]; const ceDeals = D.eb?.repCEDeals?.[repName] || [];

  let html = '<div class="card" style="border:1.5px solid var(--blue);box-shadow:var(--shadow-lg)">'
    + '<div class="flex-between" style="margin-bottom:16px">'
    + '<p class="section-title" style="margin-bottom:0">' + repName + ' \\u2014 7-Day Drill-Down</p>'
    + '<button onclick="toggleDrillDown(' + JSON.stringify(repName).replace(/"/g, '&quot;') + ')" class="text-xs text-muted" style="background:none;border:none;cursor:pointer">Close</button></div>';
  if (wr && wr.days.length > 0) {
    html += '<div class="overflow-auto" style="margin-bottom:16px"><table class="table">'
      + '<thead><tr><th>Day</th><th class="right">Dials</th><th class="right">Hours</th><th class="right">Mtgs</th><th class="right">Revenue</th></tr></thead><tbody>';
    for (const d of wr.days) {
      const dayLabel = new Date(d.date + 'T12:00:00').toLocaleDateString('en-AU', { weekday: 'short', day: 'numeric', month: 'short' });
      html += '<tr><td>' + dayLabel + '</td><td class="right font-bold">' + d.uniqueCalls + '</td><td class="right">' + d.callHours + 'h</td><td class="right">' + d.meetings + '</td><td class="right font-bold">$' + d.dailyRevenue + '</td></tr>';
    }
    html += '<tr style="font-weight:700;border-top:1px solid var(--border-strong)"><td>Total</td><td class="right">' + wr.totDials + '</td><td class="right">' + wr.totHours + 'h</td><td class="right">' + wr.totMeetings + '</td><td class="right">$' + wr.totRevenue + '</td></tr></tbody></table></div>';
  }
  html += '<div class="grid-4">';
  if (rep) { html += card('Open Deals', rep.open, BLUE, rep.total + ' total') + card('Won MRR', fmt(rep.wonMRR), GREEN, rep.won + ' deals') + card('Win Rate', pct(rep.winRate), rep.winRate >= 30 ? GREEN : AMBER, ''); }
  if (a && a.total > 0) { html += card('CE Deals', a.total, CYAN, fmt(a.wonMRR) + ' won MRR'); }
  html += '</div>';
  if (ceDeals.length > 0) {
    html += '<div class="mt-3"><p class="text-xs font-medium text-gray-500 uppercase mb-2">Cold Email Deals</p><div class="space-y-1">';
    for (const d of ceDeals.filter(dd => !dd.lost).slice(0, 8)) { const sc = d.won ? 'text-green-600' : 'text-blue-600'; html += '<div class="flex justify-between text-xs py-1"><span class="truncate flex-1">' + d.name + '</span><span class="' + sc + ' font-medium ml-2">$' + d.value + '</span></div>'; }
    html += '</div></div>';
  }
  html += '</div>'; $('drilldownSection').innerHTML = html; $('drilldownSection').classList.remove('hidden');
}

// ═══ TAB 2: PIPELINE ═══
function renderPipeline() {
  const pc = D.pipelineCoverage;
  const coverageColor = pc.ratio >= 3 ? GREEN : (pc.ratio >= 1 ? AMBER : RED);
  $('pipelineKPIs').innerHTML = [
    card('Open Pipeline', fmt(D.totalPipelineValue), BLUE, (D.totalDeals-D.totalWon-D.totalLost)+' open'),
    card('Weighted', fmt(D.weightedPipelineValue), PURPLE, 'probability-adjusted'),
    card('Win Rate', pct(D.winRate), D.winRate>=30?GREEN:AMBER, D.totalWon+' won / '+D.totalLost+' lost'),
    card('Won Value', fmt(D.wonValue), GREEN, D.totalWon+' deals'),
    card('Coverage', pc.ratio + 'x', coverageColor, pc.ratio >= 3 ? 'Healthy' : 'Gap: ' + fmt(pc.gap)),
  ].join('');

  charts.pipeline = new Chart($('pipelineChart'), { type:'bar', data:{ labels:D.pipeline.map(s=>s.label), datasets:[{ label:'Deals', data:D.pipeline.map(s=>s.count), backgroundColor:D.bottlenecks.stageMetrics.map(sm => sm.isBottleneck ? RED : BLUE).concat([LGRAY, GREEN]), borderRadius:4 }] }, options:{ indexAxis:'y', responsive:true, plugins:{ legend:{display:false}, title:{display:true,text:'Deals by Stage (red = bottleneck)'} }, scales:{ x:{beginAtZero:true} } } });

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

  // Deal Health Scores (NEW - composite scoring)
  if (D.dealHealthScores) {
    const dhs = D.dealHealthScores;
    let dhHTML = '<div class="grid grid-cols-4 gap-2 mb-3 text-center">'
      + '<div><p class="text-lg font-bold" style="color:' + GREEN + '">' + dhs.counts.healthy + '</p><p class="text-xs text-gray-500">Healthy</p></div>'
      + '<div><p class="text-lg font-bold" style="color:' + AMBER + '">' + dhs.counts.monitor + '</p><p class="text-xs text-gray-500">Monitor</p></div>'
      + '<div><p class="text-lg font-bold" style="color:#ea580c">' + dhs.counts.attention + '</p><p class="text-xs text-gray-500">Attention</p></div>'
      + '<div><p class="text-lg font-bold" style="color:' + RED + '">' + dhs.counts.critical + '</p><p class="text-xs text-gray-500">Critical</p></div></div>';
    const worstDeals = dhs.deals.filter(d => d.category !== 'healthy').slice(0, 10);
    if (worstDeals.length > 0) {
      dhHTML += '<div class="space-y-1">';
      for (const d of worstDeals) {
        const cls = 'score-' + d.category;
        dhHTML += '<div class="flex items-center justify-between text-xs py-1 border-b border-gray-50">'
          + '<span class="' + cls + ' text-[10px] font-bold px-1.5 py-0.5 rounded mr-2">' + d.score + '</span>'
          + '<a href="https://app.hubspot.com/contacts/' + PORTAL + '/deal/' + d.id + '" target="_blank" class="truncate flex-1 hover:text-blue-600">' + d.name + '</a>'
          + '<span class="text-gray-400 mx-2">' + d.stage + '</span>'
          + '<span class="text-gray-500">' + d.daysSinceUpdate + 'd idle</span></div>';
      }
      dhHTML += '</div>';
    }
    $('dealHealthScoresSection').innerHTML = dhHTML;
  }

  // Stale Deal Triage (NEW - grouped by severity)
  if (D.dealHealthScores) {
    const allDeals = D.dealHealthScores.deals;
    const tiers = [
      { label: '60+ days', min: 60, color: RED, deals: allDeals.filter(d => d.daysSinceUpdate >= 60) },
      { label: '30-59 days', min: 30, color: '#ea580c', deals: allDeals.filter(d => d.daysSinceUpdate >= 30 && d.daysSinceUpdate < 60) },
      { label: '14-29 days', min: 14, color: AMBER, deals: allDeals.filter(d => d.daysSinceUpdate >= 14 && d.daysSinceUpdate < 30) },
      { label: '7-13 days', min: 7, color: BLUE, deals: allDeals.filter(d => d.daysSinceUpdate >= 7 && d.daysSinceUpdate < 14) },
    ];
    let triageHTML = '<div class="grid grid-cols-4 gap-2 mb-4">';
    for (const t of tiers) {
      triageHTML += '<div class="text-center p-2 rounded-lg" style="background:' + t.color + '10"><p class="text-lg font-bold" style="color:' + t.color + '">' + t.deals.length + '</p><p class="text-xs text-gray-500">' + t.label + '</p></div>';
    }
    triageHTML += '</div>';
    const showDeals = tiers.flatMap(t => t.deals).slice(0, 15);
    if (showDeals.length > 0) {
      triageHTML += '<div class="space-y-2">';
      for (const d of showDeals) {
        const dc = d.daysSinceUpdate >= 60 ? 'text-red-600 font-bold' : (d.daysSinceUpdate >= 30 ? 'text-orange-600' : 'text-amber-600');
        triageHTML += '<a href="https://app.hubspot.com/contacts/' + PORTAL + '/deal/' + d.id + '" target="_blank" class="flex items-center justify-between p-3 rounded-lg hover:bg-gray-50 border border-gray-50">'
          + '<div class="min-w-0 flex-1"><p class="text-sm font-medium text-gray-800 truncate">' + d.name + '</p><p class="text-xs text-gray-400">' + d.rep + '</p></div>'
          + '<span class="text-xs px-2 py-0.5 rounded-full bg-gray-100 text-gray-600 mx-2">' + d.stage + '</span>'
          + '<span class="text-sm ' + dc + '">' + d.daysSinceUpdate + 'd idle</span></a>';
      }
      triageHTML += '</div>';
    } else {
      triageHTML += '<p class="text-green-600 text-sm font-medium py-4 text-center">All deals are active!</p>';
    }
    $('staleDealTriageSection').innerHTML = triageHTML;
  }
}

// ═══ TAB 3: CHANNELS ═══
function renderChannels() {
  // Data Quality Banner
  if (D.dataQuality) {
    const dq = D.dataQuality;
    const bannerColor = dq.sourcePct >= 80 ? 'bg-green-50 border-green-200 text-green-700' : (dq.sourcePct >= 50 ? 'bg-amber-50 border-amber-200 text-amber-700' : 'bg-red-50 border-red-200 text-red-700');
    let dqHTML = '<div class="' + bannerColor + ' border rounded-xl p-4 mb-2">'
      + '<h3 class="text-sm font-semibold mb-2">Data Quality</h3>'
      + '<div class="grid grid-cols-4 gap-3 text-xs">'
      + '<div>Source Known: <b>' + dq.sourcePct + '%</b></div>'
      + '<div>Owner Assigned: <b>' + dq.ownerPct + '%</b></div>'
      + '<div>Tier Set: <b>' + dq.tierPct + '%</b></div>'
      + '<div>EB Sync: <b>' + (dq.ebSyncOk ? 'OK' : 'Issue') + '</b></div></div>';
    if (D.sourceAttribution && D.sourceAttribution.improved > 0) {
      dqHTML += '<p class="text-xs mt-2">Attribution fallback recovered ' + D.sourceAttribution.improved + ' deals from unknown</p>';
    }
    dqHTML += '</div>';
    $('dataQualityBanner').innerHTML = dqHTML;
  }

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

  charts.source = new Chart($('sourceChart'), { type:'bar', data:{ labels:D.sourceStats.map(s=>s.label), datasets:[ {label:'Won',data:D.sourceStats.map(s=>s.won),backgroundColor:GREEN,borderRadius:4}, {label:'Lost',data:D.sourceStats.map(s=>s.lost),backgroundColor:RED,borderRadius:4}, {label:'Open',data:D.sourceStats.map(s=>s.open),backgroundColor:LGRAY,borderRadius:4} ] }, options:{ responsive:true, plugins:{title:{display:true,text:'Deals by Source'}}, scales:{y:{beginAtZero:true}} } });

  if (D.sourceCycle) {
    const srcs = Object.values(D.sourceCycle).filter(s => s.count > 0);
    if (srcs.length > 0) {
      charts.sourceCycle = new Chart($('sourceCycleChart'), { type:'bar', data:{ labels:srcs.map(s=>s.label), datasets:[{ label:'Avg Days to Close', data:srcs.map(s=>s.avgDays), backgroundColor:srcs.map(s=>s.avgDays>30?AMBER:BLUE), borderRadius:4 }] }, options:{ responsive:true, plugins:{legend:{display:false},title:{display:true,text:'Avg Days to Close by Source'}}, scales:{y:{beginAtZero:true}} } });
    }
  }

  // EB Funnel
  if (D.eb) {
    const eb = D.eb, t = eb.totals, a = eb.attribution, co = eb.costs;
    let ebHTML = '<div class="bg-white rounded-xl border border-gray-100 p-4"><h2 class="text-base font-semibold text-gray-800 mb-3">EmailBison Funnel</h2>';
    if (!eb.syncHealthy) { ebHTML += '<div class="alert-warning border rounded-lg px-3 py-1.5 text-xs mb-3">All leads show same status \\u2014 EB sync may be stale. Raw statuses: ' + Object.entries(eb.debugStatuses).map(function(e){return e[0]+'='+e[1]}).join(', ') + '</div>'; }
    ebHTML += '<div class="grid grid-cols-1 lg:grid-cols-2 gap-4">'
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

    // Campaign cards with ROI
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
        + '<div><p class="text-gray-500">Spend</p><p class="font-bold">$' + c.allocatedSpend + '</p></div></div></div>';
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

  // Channel Mix Recommendation (NEW)
  if (D.bestChannel) {
    $('channelMixSection').innerHTML = '<div class="bg-green-50 border border-green-200 rounded-xl p-4">'
      + '<h3 class="text-sm font-semibold text-green-800 mb-1">Best Channel: ' + D.bestChannel.name + '</h3>'
      + '<p class="text-xs text-green-700">Lowest CAC at ' + (D.bestChannel.cac ? fmt(D.bestChannel.cac) : 'N/A') + ' | ' + D.bestChannel.won + ' deals won | ' + fmt(D.bestChannel.wonMRR) + ' MRR</p></div>';
  } else { $('channelMixSection').innerHTML = ''; }
}

// ═══ TAB 4: REVOPS (was Revenue) ═══
function renderRevOps() {
  // Unit Economics cards
  const ue = D.unitEconomics;
  const ltvColor = ue.ltvCacRatio && ue.ltvCacRatio >= 3 ? GREEN : (ue.ltvCacRatio && ue.ltvCacRatio >= 1 ? AMBER : RED);
  const paybackColor = ue.monthsToPayback && ue.monthsToPayback <= 6 ? GREEN : (ue.monthsToPayback && ue.monthsToPayback <= 12 ? AMBER : RED);
  $('unitEconCards').innerHTML = [
    card('LTV (12mo)', fmt(ue.ltv), BLUE, 'ARPU ' + fmt(ue.arpu) + ' x 12'),
    card('LTV:CAC', ue.ltvCacRatio ? ue.ltvCacRatio + ':1' : 'N/A', ltvColor, ue.ltvCacRatio >= 3 ? 'Healthy' : 'Below 3:1 target'),
    card('Payback', ue.monthsToPayback ? ue.monthsToPayback + ' months' : 'N/A', paybackColor, 'CAC / ARPU'),
    card('Break-Even', ue.currentCustomers + '/' + ue.breakEven, ue.gap === 0 ? GREEN : AMBER, ue.gap > 0 ? ue.gap + ' more needed' : 'Achieved!'),
  ].join('');

  // MRR Waterfall
  const mw = D.mrrWaterfall;
  charts.mrrWaterfall = new Chart($('mrrWaterfallChart'), {
    type: 'bar',
    data: {
      labels: ['New MRR', 'Churn', 'Net'],
      datasets: [{
        data: [mw.newMRR, -mw.churnMRR, mw.netMRR],
        backgroundColor: [GREEN, RED, mw.netMRR >= 0 ? BLUE : RED],
        borderRadius: 4,
      }]
    },
    options: { responsive: true, plugins: { legend: { display: false }, title: { display: true, text: 'MRR Movement (' + mw.newDeals + ' new, ' + mw.churnCount + ' churned)' } }, scales: { y: { beginAtZero: true } } }
  });

  // Week-over-Week
  if (D.woW) {
    const w = D.woW;
    function wowRow(label, tw, lw, d) {
      const arrow = d >= 0 ? '\\u2191' : '\\u2193';
      const cls = d >= 0 ? 'wow-up' : 'wow-down';
      return '<tr class="border-b border-gray-50"><td class="py-2 text-sm">' + label + '</td>'
        + '<td class="text-right py-2 font-medium">' + tw + '</td>'
        + '<td class="text-right py-2 text-gray-400">' + lw + '</td>'
        + '<td class="text-right py-2 ' + cls + ' font-bold">' + arrow + ' ' + Math.abs(d) + '%</td></tr>';
    }
    $('wowSection').innerHTML = '<table class="w-full text-sm">'
      + '<thead><tr class="border-b text-xs text-gray-500 uppercase"><th class="text-left py-2">Metric</th><th class="text-right py-2">This Week</th><th class="text-right py-2">Last Week</th><th class="text-right py-2">Delta</th></tr></thead><tbody>'
      + wowRow('Calls', w.thisWeek.calls, w.lastWeek.calls, w.deltas.calls)
      + wowRow('Unique Dials', w.thisWeek.uniqueDials, w.lastWeek.uniqueDials, w.deltas.uniqueDials)
      + wowRow('Meetings', w.thisWeek.meetings, w.lastWeek.meetings, w.deltas.meetings)
      + wowRow('Revenue', '$' + w.thisWeek.revenue, '$' + w.lastWeek.revenue, w.deltas.revenue)
      + wowRow('Deals Created', w.thisWeek.dealsCreated, w.lastWeek.dealsCreated, w.deltas.dealsCreated)
      + wowRow('Deals Won', w.thisWeek.dealsWon, w.lastWeek.dealsWon, w.deltas.dealsWon)
      + '</tbody></table>';
  }

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

  // Engagement Score Distribution
  const ET = D.engagementTiers;
  if (ET && (ET.onFire + ET.hot + ET.warm + ET.cold) > 0) {
    const engLabels = ['On Fire', 'Hot', 'Warm', 'Cold'];
    const engValues = [ET.onFire, ET.hot, ET.warm, ET.cold];
    const engColors = [RED, AMBER, BLUE, GRAY];
    charts.engScore = new Chart($('engScoreChart'), { type:'doughnut', data:{ labels:engLabels, datasets:[{ data:engValues, backgroundColor:engColors }] }, options:{ plugins:{ legend:{ display:false } }, cutout:'60%' } });
    $('engScoreLegend').innerHTML = engLabels.map((l,i) => '<div class="flex items-center gap-2"><span class="w-3 h-3 rounded-full" style="background:'+engColors[i]+'"></span><span>'+l+': <b>'+engValues[i]+'</b></span></div>').join('');
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
      + '<div class="bg-white rounded-xl border border-gray-100 p-4"><h3 class="text-sm font-medium text-gray-500 mb-1">This Month</h3>'
      + '<p class="text-2xl font-bold" style="color:' + GREEN + '">' + fmt(wf.thisMonth.weighted) + '</p>'
      + '<p class="text-xs text-gray-400">' + wf.thisMonth.deals + ' deals | ' + fmt(wf.thisMonth.value) + ' unweighted</p></div>'
      + '<div class="bg-white rounded-xl border border-gray-100 p-4"><h3 class="text-sm font-medium text-gray-500 mb-1">Next Month</h3>'
      + '<p class="text-2xl font-bold" style="color:' + BLUE + '">' + fmt(wf.nextMonth.weighted) + '</p>'
      + '<p class="text-xs text-gray-400">' + wf.nextMonth.deals + ' deals | ' + fmt(wf.nextMonth.value) + ' unweighted</p></div>'
      + '<div class="bg-white rounded-xl border border-gray-100 p-4"><h3 class="text-sm font-medium text-gray-500 mb-1">Later</h3>'
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

// ═══ PRIORITIES ═══
function renderPriorities() {
  if (!D.priorities || D.priorities.length === 0) { $('prioritiesSection').innerHTML = ''; return; }
  var items = D.priorities.slice(0, 5);
  var hasMore = D.priorities.length > 5;
  var html = '<div class="card" style="padding:20px;border-left:4px solid var(--orange)">'
    + '<div class="flex-between" style="margin-bottom:12px"><p class="section-title" style="margin-bottom:0;font-size:16px">Action Items</p>'
    + '<span class="badge badge-' + (items[0].severity === 'critical' ? 'red' : 'orange') + '">' + D.priorities.length + ' items</span></div>';
  for (var i = 0; i < items.length; i++) {
    var p = items[i];
    var icon = p.severity === 'critical' ? '!' : (p.severity === 'warning' ? '!' : 'i');
    html += '<div class="priority-item priority-' + p.severity + '">'
      + '<div class="priority-icon">' + icon + '</div>'
      + '<span>' + p.message + '</span></div>';
  }
  if (hasMore) {
    html += '<p class="text-xs text-muted" style="text-align:center;margin-top:8px;cursor:pointer" onclick="this.parentElement.querySelectorAll(\\'.priority-hidden\\').forEach(function(e){e.style.display=\\'flex\\'});this.style.display=\\'none\\'">Show ' + (D.priorities.length - 5) + ' more...</p>';
  }
  html += '</div>';
  $('prioritiesSection').innerHTML = html;
}

// ═══ TASK CHECKLIST ═══
async function loadTasks(dateStr) {
  try {
    const resp = await fetch('/api/tasks/' + dateStr);
    if (!resp.ok) return [];
    const data = await resp.json();
    return data.tasks || [];
  } catch (e) { return []; }
}

async function renderTaskChecklist() {
  const dateStr = selectedDate || TODAY_STR;
  const isToday = dateStr === TODAY_STR;
  const tasks = await loadTasks(dateStr);
  const doneCount = tasks.filter(function(t) { return t.done; }).length;
  const total = tasks.length;
  const pctDone = total > 0 ? Math.round((doneCount / total) * 100) : 0;
  const barColor = pctDone >= 100 ? GREEN : (pctDone >= 50 ? AMBER : RED);

  if (total === 0 && !isToday) {
    $('taskChecklistSection').innerHTML = '';
    return;
  }

  var html = '<div class="card" style="padding:20px;border-left:4px solid ' + BLUE + '">'
    + '<div class="flex-between" style="margin-bottom:12px">'
    + '<div><p class="section-title" style="margin-bottom:2px">Daily Checklist</p>'
    + '<p class="text-xs text-muted">' + doneCount + ' of ' + total + ' complete</p></div>'
    + '<span class="badge" style="background:' + barColor + '14;color:' + barColor + '">' + pctDone + '%</span></div>'
    + '<div class="progress" style="margin-bottom:16px;height:6px"><div class="progress-fill" style="width:' + pctDone + '%;background:' + barColor + '"></div></div>';

  html += '<div id="taskList">';
  for (var i = 0; i < tasks.length; i++) {
    var t = tasks[i];
    var checked = t.done ? 'checked' : '';
    var strikeStyle = t.done ? 'text-decoration:line-through;opacity:0.5;' : '';
    var sevColor = t.severity === 'critical' ? RED : (t.severity === 'warning' ? AMBER : GRAY);
    var sourceTag = t.source === 'auto' ? '<span class="text-xs" style="color:' + BLUE + ';margin-left:6px">auto</span>' : '';
    html += '<label style="display:flex;align-items:flex-start;gap:10px;padding:8px 0;border-bottom:0.5px solid var(--border);cursor:pointer;' + strikeStyle + '">'
      + '<input type="checkbox" ' + checked + ' onchange="toggleTask(\\'' + t.id + '\\',\\'' + dateStr + '\\')" style="margin-top:3px;accent-color:' + sevColor + '">'
      + '<span class="text-sm" style="flex:1">' + t.text + sourceTag + '</span>'
      + '<button onclick="event.preventDefault();deleteTask(\\'' + t.id + '\\',\\'' + dateStr + '\\')" class="text-xs" style="color:' + GRAY + ';background:none;border:none;cursor:pointer;padding:2px 6px" title="Remove">&times;</button>'
      + '</label>';
  }
  html += '</div>';

  if (isToday) {
    html += '<div style="margin-top:12px;display:flex;gap:8px">'
      + '<input id="newTaskInput" type="text" placeholder="Add a task..." style="flex:1;padding:8px 12px;border:1px solid var(--border);border-radius:8px;font-size:13px;outline:none" onkeydown="if(event.key===\\'Enter\\')addTask(\\'' + dateStr + '\\')">'
      + '<button onclick="addTask(\\'' + dateStr + '\\')" style="padding:8px 16px;background:' + BLUE + ';color:#fff;border:none;border-radius:8px;font-size:13px;cursor:pointer;font-weight:500">Add</button>'
      + '</div>';
  }

  html += '</div>';
  $('taskChecklistSection').innerHTML = html;
}

async function toggleTask(id, dateStr) {
  await fetch('/api/tasks/' + id + '/toggle', {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ date: dateStr }),
  });
  renderTaskChecklist();
}

async function deleteTask(id, dateStr) {
  await fetch('/api/tasks/' + id + '?date=' + dateStr, { method: 'DELETE' });
  renderTaskChecklist();
}

async function addTask(dateStr) {
  var input = $('newTaskInput');
  var text = input.value.trim();
  if (!text) return;
  await fetch('/api/tasks', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ date: dateStr, text: text }),
  });
  input.value = '';
  renderTaskChecklist();
}

// ═══ COMMISSIONS ═══
function renderCommissions() {
  if (!D.commissions) { $('commissionsSection').innerHTML = ''; return; }
  const comm = D.commissions;
  let html = '<div class="card"><p class="section-title" style="margin-bottom:16px">Commissions</p>';

  // Per-rep cards
  html += '<div class="grid-2 gap-md" style="margin-bottom:16px">';
  for (const repName of D.activeReps) {
    const rc = comm.perRep[repName];
    if (!rc) continue;
    html += '<div class="commission-card">'
      + '<p class="metric-label">' + repName + '</p>'
      + '<p class="commission-amount text-green">$' + rc.commissionThisMonth.toLocaleString() + '</p>'
      + '<p class="metric-sub">' + rc.closesThisMonth + ' closes this month &middot; $' + comm.perCloseRate + '/close</p>'
      + '<div class="grid-2 mt-sm" style="gap:8px">'
      + '<div class="metric-card card-sm"><p class="metric-label">This Week</p><p class="metric-value" style="color:' + BLUE + '">$' + rc.commissionThisWeek + '</p><p class="metric-sub">' + rc.closesThisWeek + ' closes</p></div>'
      + '<div class="metric-card card-sm"><p class="metric-label">YTD</p><p class="metric-value" style="color:' + PURPLE + '">$' + rc.commissionYTD.toLocaleString() + '</p><p class="metric-sub">' + rc.closesYTD + ' closes</p></div>'
      + '</div></div>';
  }
  html += '</div>';

  // Team total
  html += '<div class="metric-card" style="text-align:center;margin-bottom:16px"><p class="metric-label">Team Total</p>'
    + '<p class="card-value text-green">$' + comm.totalThisMonth.toLocaleString() + '</p>'
    + '<p class="metric-sub">YTD: $' + comm.totalYTD.toLocaleString() + '</p></div>';

  // Customer roster table
  if (comm.customerRoster && comm.customerRoster.length > 0) {
    html += '<p class="section-subtitle" style="margin-top:20px">Customer Roster (' + comm.totalCustomers + ' customers &middot; $' + comm.totalMRR.toLocaleString() + '/mo)</p>';

    // Summary by closer
    html += '<div class="grid-' + (D.activeReps.length + 1) + ' gap-sm" style="margin-bottom:12px">';
    for (const repName of D.activeReps) {
      const rb = comm.rosterByRep[repName] || { count: 0, mrr: 0 };
      html += '<div class="metric-card card-sm" style="text-align:center"><p class="metric-label">' + repName.split(' ')[0] + '</p><p class="metric-value" style="font-size:18px">' + rb.count + '</p><p class="metric-sub">$' + rb.mrr.toLocaleString() + '/mo</p></div>';
    }
    const unattr = comm.rosterByRep['Unattributed'] || { count: 0, mrr: 0 };
    if (unattr.count > 0) {
      html += '<div class="metric-card card-sm" style="text-align:center"><p class="metric-label">Unattributed</p><p class="metric-value text-red" style="font-size:18px">' + unattr.count + '</p><p class="metric-sub">$' + unattr.mrr.toLocaleString() + '/mo</p></div>';
    }
    html += '</div>';

    // Roster table
    var maxVisible = 15;
    var roster = comm.customerRoster;
    html += '<div class="overflow-auto" style="max-height:500px"><table class="table"><thead><tr><th>#</th><th>Customer</th><th>Plan</th><th class="right">MRR</th><th>Closed By</th><th>Date</th></tr></thead><tbody>';
    roster.forEach(function(c, i) {
      var closerDotColor = c.closer === D.activeReps[0] ? BLUE : (c.closer === D.activeReps[1] ? PURPLE : (c.closer === 'Unattributed' ? RED : GRAY));
      var planClass = c.plan.includes('Founder') ? 'plan-founder' : (c.plan.includes('99') ? 'plan-monthly' : 'plan-other');
      var hiddenStyle = i >= maxVisible ? ' style="display:none" class="roster-hidden"' : '';
      html += '<tr' + hiddenStyle + '><td class="muted">' + (i + 1) + '</td><td style="font-weight:600">' + c.name + '</td><td><span class="plan-badge ' + planClass + '">' + c.plan + '</span></td><td class="right num">$' + c.mrr + '</td><td><span class="closer-dot" style="background:' + closerDotColor + '"></span><span style="font-weight:600">' + c.closer + '</span></td><td class="muted">' + c.closeDate + '</td></tr>';
    });
    html += '</tbody></table></div>';
    if (roster.length > maxVisible) {
      html += '<p class="text-xs" style="text-align:center;margin-top:8px;cursor:pointer;color:var(--blue)" onclick="document.querySelectorAll(\\'.roster-hidden\\').forEach(function(e){e.style.display=\\'table-row\\'});this.style.display=\\'none\\'">Show all ' + roster.length + ' customers</p>';
    }
  }

  html += '</div>';
  $('commissionsSection').innerHTML = html;
}

// ═══ LEADERBOARD ═══
function setPeriod(period) {
  lbPeriod = period;
  document.querySelectorAll('.period-btn').forEach(function(b) {
    var map = { 'Week': 'week', 'Month': 'month', 'All-Time': 'allTime' };
    b.classList.toggle('active', map[b.textContent] === period);
  });
  renderLeaderboardContent();
}

function renderLeaderboard() { renderLeaderboardContent(); }

function renderLeaderboardContent() {
  if (!D.leaderboard) { $('podiumSection').innerHTML = '<p class="text-muted">No leaderboard data</p>'; return; }
  const lb = D.leaderboard[lbPeriod];
  if (!lb || !lb.ranked || lb.ranked.length === 0) {
    $('podiumSection').innerHTML = '<div class="card" style="text-align:center;padding:40px"><p class="text-muted">No data for this period</p></div>';
    $('categoryTable').innerHTML = '';
    return;
  }
  const ranked = lb.ranked;
  const leaders = lb.leaders;
  const winner = ranked[0];
  const runnerUp = ranked.length > 1 ? ranked[1] : null;

  var podiumHTML = '<div class="grid-2 gap-md">';
  podiumHTML += '<div class="podium">'
    + '<div class="podium-rank">#1</div>'
    + '<div class="podium-name">' + winner[0] + '</div>'
    + '<div class="podium-score">Composite Score: ' + winner[1].composite + '</div>'
    + '<div class="grid-3 mt-md" style="gap:8px;text-align:center">'
    + '<div><p class="text-xs text-muted">MRR</p><p class="font-bold text-green">$' + winner[1].wonMRR + '</p></div>'
    + '<div><p class="text-xs text-muted">Commission</p><p class="font-bold text-green">$' + winner[1].commEarned + '</p></div>'
    + '<div><p class="text-xs text-muted">Deals Won</p><p class="font-bold">' + winner[1].dealsWon + '</p></div>'
    + '</div></div>';
  if (runnerUp) {
    podiumHTML += '<div class="card" style="text-align:center;border:1px solid var(--border)">'
      + '<div style="font-size:32px;font-weight:800;color:var(--text-tertiary)">#2</div>'
      + '<div class="podium-name">' + runnerUp[0] + '</div>'
      + '<div class="podium-score">Composite Score: ' + runnerUp[1].composite + '</div>'
      + '<div class="grid-3 mt-md" style="gap:8px;text-align:center">'
      + '<div><p class="text-xs text-muted">MRR</p><p class="font-bold text-green">$' + runnerUp[1].wonMRR + '</p></div>'
      + '<div><p class="text-xs text-muted">Commission</p><p class="font-bold text-green">$' + runnerUp[1].commEarned + '</p></div>'
      + '<div><p class="text-xs text-muted">Deals Won</p><p class="font-bold">' + runnerUp[1].dealsWon + '</p></div>'
      + '</div></div>';
  }
  podiumHTML += '</div>';
  $('podiumSection').innerHTML = podiumHTML;

  // Category table
  var catLabels = { wonMRR: 'MRR Closed', commEarned: 'Commission', dealsWon: 'Deals Won', dials: 'Unique Dials', hours: 'Call Hours', winRate: 'Win Rate' };
  var catWeights = { wonMRR: '30%', commEarned: '20%', dealsWon: '15%', dials: '15%', hours: '10%', winRate: '10%' };
  var catFmt = { wonMRR: function(v){return '$'+v.toLocaleString()}, commEarned: function(v){return '$'+v}, dealsWon: function(v){return v}, dials: function(v){return v}, hours: function(v){return v.toFixed?v.toFixed(1)+'h':v+'h'}, winRate: function(v){return v+'%'} };

  var catHTML = '<div class="card" style="padding:0;overflow:hidden"><div class="overflow-auto"><table class="table"><thead><tr>'
    + '<th>Category</th><th class="right">Weight</th>';
  for (var ri = 0; ri < ranked.length; ri++) catHTML += '<th class="right">' + ranked[ri][0].split(' ')[0] + '</th>';
  catHTML += '<th class="right">Leader</th></tr></thead><tbody>';

  var cats = Object.keys(catLabels);
  for (var ci = 0; ci < cats.length; ci++) {
    var cat = cats[ci];
    catHTML += '<tr><td class="font-medium">' + catLabels[cat] + '</td><td class="right muted">' + catWeights[cat] + '</td>';
    for (var ri2 = 0; ri2 < ranked.length; ri2++) {
      var isLeader = leaders[cat] === ranked[ri2][0];
      var raw = ranked[ri2][1].categories[cat] ? ranked[ri2][1].categories[cat].raw : 0;
      var norm = ranked[ri2][1].categories[cat] ? ranked[ri2][1].categories[cat].normalized : 0;
      var fmtFn = catFmt[cat] || function(v){return v};
      var barColor = isLeader ? GREEN : BLUE;
      catHTML += '<td class="right">' + fmtFn(raw) + '<div class="progress" style="height:4px;margin-top:4px;width:80px;display:inline-block;vertical-align:middle;margin-left:8px"><div class="progress-fill" style="width:' + norm + '%;background:' + barColor + '"></div></div></td>';
    }
    catHTML += '<td class="right"><span class="badge badge-blue">' + (leaders[cat] || '').split(' ')[0] + '</span></td></tr>';
  }
  catHTML += '</tbody></table></div></div>';
  $('categoryTable').innerHTML = catHTML;
}

// ═══ INIT ═══
renderPriorities();
renderCommissions();
updateDateUI();
switchTab(activeTab);

setTimeout(() => window.location.reload(), 300000);
<\/script>
</body>
</html>`;
}

function loadingHTML() {
  return `<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Sammy AI</title>
<meta http-equiv="refresh" content="5">
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Display', system-ui, sans-serif; background: #f5f5f7; display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; -webkit-font-smoothing: antialiased; }
  .loader { text-align: center; }
  .spinner { width: 40px; height: 40px; border: 3px solid #e5e5ea; border-top-color: #007aff; border-radius: 50%; animation: spin 0.8s linear infinite; margin: 0 auto 20px; }
  @keyframes spin { to { transform: rotate(360deg); } }
  .title { font-size: 17px; font-weight: 600; color: #1d1d1f; letter-spacing: -0.01em; }
  .sub { font-size: 13px; color: #8e8e93; margin-top: 6px; }
</style></head>
<body><div class="loader"><div class="spinner"></div><p class="title">Loading Sammy</p><p class="sub">Pulling live data from HubSpot</p></div></body></html>`;
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
    cache.lastError = null;
    cache.lastSuccess = Date.now();
    console.log('[cache] Refreshed at ' + new Date().toISOString());
  } catch (err) {
    cache.lastError = err.message || 'Unknown error';
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

app.get('/api/eb-debug', (req, res) => {
  if (!cache.data || !cache.data.eb) return res.status(503).json({ status: 'no eb data' });
  res.json({ statuses: cache.data.eb.debugStatuses, syncHealthy: cache.data.eb.syncHealthy, campaigns: cache.data.eb.campaignMetrics.length });
});

// ── Task Checklist API ──
app.get('/api/tasks/:date', (req, res) => {
  const dateStr = req.params.date;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) return res.status(400).json({ error: 'Invalid date format' });
  let tasks = getTasksForDate(dateStr);
  const today = getTodayMelbourne();
  if (dateStr === today && tasks.length === 0) tasks = seedDailyTasks(dateStr);
  res.json({ date: dateStr, tasks });
});

app.post('/api/tasks', (req, res) => {
  const { date, text } = req.body;
  if (!date || !text) return res.status(400).json({ error: 'date and text required' });
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return res.status(400).json({ error: 'Invalid date format' });
  const tasks = getTasksForDate(date);
  const task = {
    id: generateTaskId(),
    text: text.trim(),
    done: false,
    source: 'manual',
    severity: 'info',
    createdAt: new Date().toISOString(),
  };
  tasks.push(task);
  saveTasksForDate(date, tasks);
  res.json(task);
});

app.patch('/api/tasks/:id/toggle', (req, res) => {
  const { id } = req.params;
  const date = req.body.date || getTodayMelbourne();
  const tasks = getTasksForDate(date);
  const task = tasks.find(t => t.id === id);
  if (!task) return res.status(404).json({ error: 'Task not found' });
  task.done = !task.done;
  task.toggledAt = new Date().toISOString();
  saveTasksForDate(date, tasks);
  res.json(task);
});

app.delete('/api/tasks/:id', (req, res) => {
  const { id } = req.params;
  const date = req.query.date || getTodayMelbourne();
  let tasks = getTasksForDate(date);
  const idx = tasks.findIndex(t => t.id === id);
  if (idx === -1) return res.status(404).json({ error: 'Task not found' });
  tasks.splice(idx, 1);
  saveTasksForDate(date, tasks);
  res.json({ ok: true });
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
