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

// Active reps — Jared Korinko is inactive, excluded from daily scorecard/targets
const ACTIVE_REPS = ['Lucas Gibson', 'Krishna Pryor'];

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

  // Churn MRR: churned contacts × their tier pricing
  let churnMRR = 0;
  const churnCount = churnedCustomers.length;
  for (const c of churnedCustomers) {
    const tier = c.properties.sammy_subscription_tier;
    churnMRR += PRICING[tier] || PRICING.default;
  }

  const netMRR = Math.round(newMRR) - Math.round(churnMRR);
  return { newMRR: Math.round(newMRR), newDeals, churnMRR: Math.round(churnMRR), churnCount, netMRR };
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
    // NEW
    executiveSummary, dealHealthScores, bottlenecks, dataQuality, unitEconomics, mrrWaterfall, woW,
    sourceAttribution, pipelineCoverage, bestChannel, dataFreshness, activeReps: ACTIVE_REPS,
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
  .sparkline { display: inline-block; vertical-align: middle; }
  .alert-critical { background: #fef2f2; border-color: #fecaca; color: #991b1b; }
  .alert-warning { background: #fffbeb; border-color: #fde68a; color: #92400e; }
  .alert-info { background: #eff6ff; border-color: #bfdbfe; color: #1e40af; }
  .score-healthy { background: #dcfce7; color: #166534; }
  .score-monitor { background: #fef9c3; color: #854d0e; }
  .score-attention { background: #fed7aa; color: #9a3412; }
  .score-critical { background: #fecaca; color: #991b1b; }
  .wow-up { color: #16a34a; }
  .wow-down { color: #dc2626; }
  @media print { .no-print { display: none; } canvas { max-height: 300px; } }
  @media (max-width: 640px) { .kpi-strip { grid-template-columns: repeat(3, 1fr) !important; } .kpi-strip > div:nth-child(n+4) { display: none; } }
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

    <!-- EXECUTIVE SUMMARY KPI STRIP -->
    <div id="execSummary" class="grid grid-cols-6 gap-2 mb-2 kpi-strip"></div>
    <!-- ALERTS BANNER -->
    <div id="alertsBanner" class="mb-2"></div>

    <!-- TAB BAR -->
    <nav class="flex gap-1 overflow-x-auto no-print pb-1">
      <button id="btnToday" class="tab-btn px-3 py-1.5 text-sm font-medium rounded-lg whitespace-nowrap" onclick="switchTab('today')">Today</button>
      <button id="btnPipeline" class="tab-btn px-3 py-1.5 text-sm font-medium rounded-lg whitespace-nowrap" onclick="switchTab('pipeline')">Pipeline</button>
      <button id="btnChannels" class="tab-btn px-3 py-1.5 text-sm font-medium rounded-lg whitespace-nowrap" onclick="switchTab('channels')">Channels</button>
      <button id="btnRevops" class="tab-btn px-3 py-1.5 text-sm font-medium rounded-lg whitespace-nowrap" onclick="switchTab('revops')">RevOps</button>
    </nav>
  </div>
</header>

<main class="max-w-6xl mx-auto px-4 py-6">

  <!-- TAB 1: TODAY (includes rep comparison from old Reps tab) -->
  <div id="tabToday" class="space-y-6">
    <div id="scorecardSection"></div>
    <div id="drilldownSection" class="hidden"></div>
    <div id="myDaySection"></div>
    <div id="weeklySection"></div>
    <div id="todayCESection"></div>
    <div id="repComparisonSection"></div>
  </div>

  <!-- TAB 2: PIPELINE -->
  <div id="tabPipeline" class="space-y-6 hidden">
    <div class="grid grid-cols-2 md:grid-cols-5 gap-3" id="pipelineKPIs"></div>
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
        <h3 class="text-sm font-medium text-gray-500 mb-3">Deal Health Scores</h3>
        <div id="dealHealthScoresSection"></div>
      </div>
    </div>
    <div class="bg-white rounded-xl border border-gray-100 p-4">
      <h3 class="text-sm font-medium text-gray-500 mb-3">Stale Deal Triage</h3>
      <div id="staleDealTriageSection"></div>
    </div>
  </div>

  <!-- TAB 3: CHANNELS -->
  <div id="tabChannels" class="space-y-6 hidden">
    <div id="dataQualityBanner"></div>
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
    <div id="channelMixSection"></div>
  </div>

  <!-- TAB 4: REVOPS (was Revenue) -->
  <div id="tabRevops" class="space-y-6 hidden">
    <div class="grid grid-cols-2 md:grid-cols-4 gap-3" id="unitEconCards"></div>
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
      <div class="bg-white rounded-xl border border-gray-100 p-4">
        <h3 class="text-sm font-medium text-gray-500 mb-3">MRR Movement (30d)</h3>
        <canvas id="mrrWaterfallChart" height="200"></canvas>
      </div>
      <div class="bg-white rounded-xl border border-gray-100 p-4">
        <h3 class="text-sm font-medium text-gray-500 mb-3">Week-over-Week</h3>
        <div id="wowSection"></div>
      </div>
    </div>
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
      <div class="bg-white rounded-xl border border-gray-100 p-4">
        <h3 class="text-sm font-medium text-gray-500 mb-3">Engagement Score Distribution</h3>
        <div class="flex items-center gap-6">
          <canvas id="engScoreChart" class="max-h-40"></canvas>
          <div id="engScoreLegend" class="text-sm space-y-2"></div>
        </div>
      </div>
    </div>
    <div class="grid grid-cols-2 md:grid-cols-3 gap-3" id="funnelKPIs"></div>
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

function miniCard(label, value, color, sub) {
  return '<div class="text-center px-2 py-1.5">'
    + '<p class="text-[10px] text-gray-400 uppercase tracking-wider">' + label + '</p>'
    + '<p class="text-sm font-bold" style="color:' + color + '">' + value + '</p>'
    + (sub ? '<p class="text-[10px] text-gray-400">' + sub + '</p>' : '') + '</div>';
}

// ═══ STATE ═══
const TABS = ['today','pipeline','channels','revops'];
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
    let alertHTML = '<div class="space-y-1">';
    for (const a of es.alerts.slice(0, 3)) {
      const cls = a.severity === 'critical' ? 'alert-critical' : (a.severity === 'warning' ? 'alert-warning' : 'alert-info');
      alertHTML += '<div class="' + cls + ' border rounded-lg px-3 py-1.5 text-xs font-medium flex items-center gap-2">'
        + '<span class="font-bold">' + (a.severity === 'critical' ? '!!!' : a.severity === 'warning' ? '!!' : 'i') + '</span>'
        + '<span>' + a.message + '</span></div>';
    }
    alertHTML += '</div>';
    $('alertsBanner').innerHTML = alertHTML;
  } else {
    $('alertsBanner').innerHTML = '';
  }

  // Data freshness warning
  if (D.dataFreshness && D.dataFreshness.isStale) {
    $('alertsBanner').innerHTML += '<div class="alert-warning border rounded-lg px-3 py-1.5 text-xs font-medium mt-1">Data is ' + D.dataFreshness.cacheAgeMinutes + ' minutes old</div>';
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
      let rcHTML = '<div class="bg-white rounded-xl border border-gray-100 p-4"><h2 class="text-base font-semibold text-gray-800 mb-3">Rep Comparison</h2>'
        + '<div class="overflow-x-auto"><table class="w-full text-sm">'
        + '<thead><tr class="border-b text-xs text-gray-500 uppercase tracking-wide">'
        + '<th class="text-left py-2">Rep</th><th class="text-right py-2">Deals</th><th class="text-right py-2">Won</th><th class="text-right py-2">Lost</th><th class="text-right py-2">Win%</th><th class="text-right py-2">MRR</th><th class="text-right py-2">Cycle</th><th class="text-right py-2">$/Activity</th>'
        + '</tr></thead><tbody>';
      for (const r of activeReps) {
        rcHTML += '<tr class="border-b border-gray-50">'
          + '<td class="py-2 font-medium">' + r.name + '</td>'
          + '<td class="text-right py-2">' + r.total + '</td>'
          + '<td class="text-right py-2 text-green-600 font-medium">' + r.won + '</td>'
          + '<td class="text-right py-2 text-red-500">' + r.lost + '</td>'
          + '<td class="text-right py-2">' + r.winRate + '%</td>'
          + '<td class="text-right py-2 font-bold text-green-600">$' + r.wonMRR.toLocaleString() + '</td>'
          + '<td class="text-right py-2">' + (r.avgCycleDays != null ? r.avgCycleDays + 'd' : '-') + '</td>'
          + '<td class="text-right py-2 font-medium">$' + (r.efficiency || 0).toFixed(1) + '</td></tr>';
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

  let html = '<div class="flex justify-between items-center mb-4">'
    + '<h2 class="text-base font-semibold text-gray-800">Daily Scorecard \\u2014 ' + formatDateDisplay(selectedDate) + '</h2>'
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
    drillDownRep = null; $('drilldownSection').classList.add('hidden'); $('drilldownSection').innerHTML = ''; renderScorecard(); return;
  }
  drillDownRep = repName; renderScorecard();
  const wr = D.weeklyRollup[repName]; const rep = D.reps.find(r => r.name === repName);
  const a = D.eb?.attribution?.byRep?.[repName]; const ceDeals = D.eb?.repCEDeals?.[repName] || [];

  let html = '<div class="bg-white rounded-xl border border-blue-200 p-5 shadow-sm">'
    + '<div class="flex justify-between items-start mb-4">'
    + '<h2 class="text-base font-semibold text-gray-800">' + repName + ' \\u2014 7-Day Drill-Down</h2>'
    + '<button onclick="toggleDrillDown(\\'' + repName.replace("'", "\\\\'") + '\\')" class="text-xs text-gray-400 hover:text-gray-600">Close</button></div>';
  if (wr && wr.days.length > 0) {
    html += '<div class="overflow-x-auto mb-4"><table class="w-full text-xs">'
      + '<thead><tr class="border-b text-gray-500 uppercase"><th class="text-left py-2 px-2">Day</th><th class="text-right py-2 px-2">Dials</th><th class="text-right py-2 px-2">Hours</th><th class="text-right py-2 px-2">Mtgs</th><th class="text-right py-2 px-2">Revenue</th></tr></thead><tbody>';
    for (const d of wr.days) {
      const dayLabel = new Date(d.date + 'T12:00:00').toLocaleDateString('en-AU', { weekday: 'short', day: 'numeric', month: 'short' });
      html += '<tr class="border-b border-gray-50"><td class="py-1.5 px-2">' + dayLabel + '</td><td class="text-right py-1.5 px-2 font-medium">' + d.uniqueCalls + '</td><td class="text-right py-1.5 px-2">' + d.callHours + 'h</td><td class="text-right py-1.5 px-2">' + d.meetings + '</td><td class="text-right py-1.5 px-2 font-medium">$' + d.dailyRevenue + '</td></tr>';
    }
    html += '<tr class="font-bold border-t"><td class="py-1.5 px-2">Total</td><td class="text-right py-1.5 px-2">' + wr.totDials + '</td><td class="text-right py-1.5 px-2">' + wr.totHours + 'h</td><td class="text-right py-1.5 px-2">' + wr.totMeetings + '</td><td class="text-right py-1.5 px-2">$' + wr.totRevenue + '</td></tr></tbody></table></div>';
  }
  html += '<div class="grid grid-cols-2 md:grid-cols-4 gap-3">';
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

// ═══ INIT ═══
updateDateUI();
switchTab(activeTab);

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
<p class="mt-2 text-gray-400 text-sm">Pulling live data from HubSpot + EmailBison</p>
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
