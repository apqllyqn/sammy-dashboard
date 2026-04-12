# Sammy Dashboard Rebuild — Design Spec

## Context

The Sammy Dashboard at `https://sammy.hirecharm.com` is a server-rendered Node.js/Express app that aggregates HubSpot CRM data and email campaign data into a sales operations dashboard for Sammy, a $59-99/mo tradie quoting app (Australian market).

**Why this rebuild:** The current dashboard has broken data (rep attribution shows $0, MRR overcounted by $760/mo, deal sources empty), a cluttered UX (6 tabs, redundant sections), and no actionable daily workflow for the sales team. The goal is a focused tool that holds Lucas Gibson and Krishna Pryor accountable with daily pressure and clarity, while giving the owner (Chris) a single source of truth for revenue decisions.

**Current state:**
- 40 paid customers: 19 on founder plan ($59/mo), 21 on monthly plan ($99/mo)
- Actual MRR: $3,200/mo (dashboard incorrectly shows $3,960)
- 2 active sales reps (Lucas Gibson, Krishna Pryor)
- 9 cold email campaigns on Instantly (replacing EmailBison)
- 108 total deals in HubSpot pipeline
- 30 closed-won deals, 8 closed-lost

## Architecture

Single-file Node.js/Express app (`server.js`) serving server-rendered HTML with client-side JavaScript for interactivity. No database. Data cached in memory from HubSpot API + Instantly API, refreshed every 5 minutes. Task persistence via JSON file on disk.

Two views accessible via toggle:
- **Rep View** — Daily cockpit for sales reps (default)
- **Revenue View** — Business intelligence for the owner

## Data Sources

### HubSpot CRM API (`api.hubapi.com`)

**Contacts:**
- Funnel counts by `user_status`: `incomplete_onboarding`, `active_trial`, `paid_customer`, `trial_expired`, `churned`
- Paid customers with `sammy_pricing_plan` (values: `founder_59`, `monthly_99`, `annual_950`, `free`)
- Active trials with `sammy_trial_end_date` for expiry detection
- Churned customers with `hs_lastmodifieddate` for 30-day churn window

**Deals:**
- All deals with: `dealstage`, `amount`, `expected_mrr`, `deal_source`, `hubspot_owner_id`, `closedate`, `createdate`, `hs_lastmodifieddate`, stage entry timestamps
- Deal-to-contact associations for attribution

**Engagements:**
- Calls (with `hs_call_duration`, `hs_call_to_number`, `hs_call_disposition`)
- Meetings
- Notes
- Tasks assigned to reps (for the task queue)

**Owners:**
- Map `hubspot_owner_id` to rep names

### Instantly API (`api.instantly.ai/api/v2`)

**Auth:** Bearer token `NjcxNWQwNDQtY2Y4Yi00YmEyLWEwYTAtNmQ1YjE0ZGNjYWIwOlN3REl6TFFqbWp1cA==`

**Campaigns:**
- `GET /campaigns` — List all campaigns with status, sequences, lead counts
- Campaign IDs for the 9 active Sammy campaigns

**Leads:**
- `GET /leads?campaign_id={id}` — Leads per campaign with send/open/reply status

**Accounts:**
- `GET /accounts` — 10 sending accounts (all winsammy/improvesammy/optimizesammy/sharesammy domains)

**Note:** `open_tracking` and `link_tracking` are currently `false` on campaigns. Open rate data won't be available until tracking is enabled in Instantly. Reply data IS available.

### Removed: EmailBison

EmailBison integration is removed entirely. All 5 mirrored campaigns show 0 activity. Instantly is the active email platform.

## MRR Calculation

Use `sammy_pricing_plan` property on paid customers:

| Plan | Monthly Rate |
|------|-------------|
| `founder_59` | $59 |
| `monthly_99` | $99 |
| `annual_950` | $79 (950/12) |
| `free` | $0 |
| missing/unknown | $59 (default — most unknowns are pre-Feb-2026 founders) |

Pricing changed Feb 1, 2026. Everyone who converted before that date is on $59 founder plan. After is $99 monthly.

Churn MRR: Only contacts whose `hs_lastmodifieddate` falls within the last 30 days (already implemented in prior fix).

## Rep View

### 1. Header Bar

- Sammy logo/name
- View toggle: **Rep** | Revenue
- Rep selector dropdown (Lucas Gibson / Krishna Pryor / Team)
- Date picker (defaults to today, Melbourne timezone)
- Last refreshed timestamp

### 2. Today's Queue (Primary Section)

A prioritized, checkable task list generated daily. This is the core of the rep experience — what do I do right now?

**Priority order:**

1. **HubSpot Tasks** — Tasks assigned to this rep in HubSpot CRM (engagement type = TASK, status = NOT_STARTED or IN_PROGRESS). Displayed with task subject and associated contact/deal name. Source badge: "CRM".

2. **Expiring Trials** — Contacts with `user_status = active_trial` where `sammy_trial_end_date` is within the next 3 days (or already expired within last 2 days). Sorted by expiry date ascending (most urgent first). Displayed as "Call {firstname} {lastname} — trial expires {date}". Source badge: "Trial".

3. **Warm Leads** — Deals in stages `Demo Booked` (2843565802) or `Demo Complete & Closing` (2851995329) assigned to this rep. Sorted by days in stage ascending. Displayed as "{dealname} — {stage} for {days}d". Source badge: "Pipeline".

4. **Stale Pipeline** — Deals assigned to this rep that haven't been modified in 7+ days, excluding Closed Won/Lost. Sorted by days-since-update descending. Displayed as "Follow up: {dealname} — {days}d stale". Source badge: "Follow-up".

5. **Daily Targets** — Standing items that auto-update based on activity:
   - "Hit {target} unique dials" — progress: {current}/{target}
   - "Log {target}h talk time" — progress: {current}/{target}
   These display with a progress indicator and auto-check when the target is met.

**Behavior:**
- Each task has a checkbox. Checking persists for the day (file-based storage).
- Manual tasks can be added via an input field at the bottom.
- Tasks auto-seed at midnight Melbourne time (or on first load of the day).
- Completed tasks move to bottom with strikethrough.
- Task count + completion percentage displayed at top.

### 3. My Pipeline (Secondary Section)

Deals assigned to this rep, grouped by stage. Each deal card shows:
- Deal name (links to HubSpot)
- Days in current stage
- Deal value
- Health indicator (green < 7d, amber 7-14d, red > 14d)

Summary strip: total open deals, total pipeline value, weighted forecast.

### 4. My Numbers (KPI Strip)

Horizontal card row:
- **Dials Today**: {current} / {target} with progress bar
- **Talk Time**: {hours}h / {target}h with progress bar
- **Demos This Week**: count
- **Closes This Month**: count + commission ($100/close)
- **Commission YTD**: dollar amount

## Revenue View

### 1. Header Bar

Same as Rep View but with "Revenue" toggle active. No rep selector (shows all data).

### 2. Scoreboard (Top Metrics Strip)

Horizontal card row with large numbers:
- **MRR**: ${total} with breakdown tooltip ($59 x {n} + $99 x {n})
- **Net MRR (30d)**: +${new} - ${churn} = ${net}
- **Customers**: {paid} paid / {trial} trial / {churned} churned (30d)
- **Monthly P&L**: ${revenue} - ${costs} = ${profit/loss}
- **LTV:CAC Ratio**: {ratio}x

### 3. Channel Performance

Cards per channel:

**Cold Call:**
- Deals sourced, won, revenue
- CAC per customer
- Win rate
- ROI %

**Cold Email (Instantly):**
- Active campaigns count
- Total leads loaded
- Emails sent (30d)
- Replies received
- Meetings booked from email
- Deals sourced from email
- Cost + ROI

**Inbound / Referral:**
- Self-serve signups → trial → paid conversion
- Organic customer count
- Effective CAC ($0)

### 4. Team Performance

Side-by-side rep comparison table:

| Metric | Lucas | Krishna |
|--------|-------|---------|
| Dials (today) | x / target | x / target |
| Talk Time | x h | x h |
| Demos (week) | x | x |
| Closes (month) | x | x |
| Revenue (month) | $x | $x |
| Commission (month) | $x | $x |
| Pipeline Value | $x | $x |

With trend arrows (up/down vs prior week).

### 5. Pipeline Health

- Stage-by-stage funnel: count + value per stage
- Stage conversion rates
- Average days in stage
- Stale deal list (deals > 14d without activity)
- Weighted forecast total

### 6. Strategic Insights

Auto-generated observations based on data:
- "Cold call ROI is {x}% — your best channel. Consider adding a 3rd rep."
- "Cold email has 0 leads loaded — campaigns are set up but idle."
- "{n} deals in Nurture > 30 days — consider closing or re-qualifying."
- "Trial-to-paid rate is {x}% — {n} trials expiring this week."

These are computed from the metrics, not AI-generated. Simple conditional logic.

## Visual Design

- **Font**: System font stack (-apple-system, BlinkMacSystemFont, Segoe UI, etc.)
- **Colors**: 
  - Primary: #007aff (blue)
  - Success: #34c759 (green) 
  - Warning: #ff9500 (amber)
  - Danger: #ff3b30 (red)
  - Text: #1a1a1a primary, #8e8e93 secondary
  - Surface: #ffffff cards on #f5f5f7 background
- **Cards**: White background, 12px border-radius, subtle box-shadow, 20px padding
- **Progress bars**: 8px height, rounded, color-coded by completion %
- **Task items**: Full-width rows with checkbox, text, source badge, priority color left border
- **Responsive**: Single column on mobile, 2-3 column grid on desktop
- **Tailwind CSS via CDN** for utility classes

## Task Persistence

File-based JSON storage at `data/tasks.json`. Structure:

```json
{
  "2026-04-12": [
    {
      "id": "abc123",
      "text": "Call Phil Brooks — trial expires tomorrow",
      "done": false,
      "source": "auto",
      "category": "expiring_trial",
      "severity": "critical",
      "rep": "Lucas Gibson",
      "createdAt": "2026-04-12T00:00:00Z"
    }
  ]
}
```

- Tasks auto-pruned after 30 days
- Auto-seeded on first load of each day from computed priorities
- Manual tasks have `source: "manual"`
- API endpoints: GET/POST/PATCH/DELETE (already implemented)

## Deal Source Attribution

The `deal_source` custom property is empty on most deals. The dashboard infers source from:

1. `deal_source` property (if set) — authoritative
2. Contact's `hs_analytics_source` + `hs_analytics_source_data_1` — inferred
3. Mapping: `OFFLINE_SOURCES` → cold_call, `EMAIL_MARKETING` → cold_email, `ORGANIC_SEARCH`/`DIRECT_TRAFFIC` → inbound_signup

This inference is already in the current code and works. The fix is to use it consistently across all metrics (channel ROI, rep attribution, source stats).

## Files

All changes in `projects/sammy-dashboard-app/`:

| File | Action | Purpose |
|------|--------|---------|
| `server.js` | Rewrite | Complete rebuild of data layer, compute, and HTML |
| `Dockerfile` | Minor update | Ensure data volume persists |
| `.env` | Add | `INSTANTLY_API_KEY` environment variable |
| `.gitignore` | Keep | Already has `data/` |

## Verification

1. **MRR accuracy**: `/api/data` → `pnl.currentMRR` should be ~$3,200 (not $3,960)
2. **Rep attribution**: `/api/data` → `reps[*].wonDeals` should be non-zero for active reps
3. **Task queue**: `/api/tasks/{date}` returns prioritized tasks with expiring trials
4. **Instantly data**: `/api/data` → campaign metrics show real send/reply counts
5. **Visual**: Dashboard loads clean on mobile and desktop, two views toggle correctly
6. **Task checkoff**: Check a task, reload — it persists
