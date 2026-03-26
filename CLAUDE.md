# C3 Sourcing RFQ Dashboard — Agent Instructions

## Project Overview

A Streamlit dashboard for the C3 sourcing team to explore RFQ data from a SQL Server database, with an embedded AI assistant ("Jerzy") powered by Claude.

**Stack:** Python · Streamlit · Plotly · pandas · pymssql · Anthropic SDK · DigiKey API v4

---

## Architecture (WAT Framework)

**Layer 1: Workflows** — Dashboard tabs define what data is shown and how
**Layer 2: Agent** — `tools/chat_agent.py` (Jerzy) handles NL → SQL → answer
**Layer 3: Tools** — `tools/db.py` (SQL queries), `tools/digikey.py` (live part data)

---

## Directory Layout

```
dashboard.py          # Main Streamlit app, 5 tabs
tools/
  db.py               # run_query() — executes SELECT against SQL Server
  chat_agent.py       # Jerzy: Claude-based NL data assistant
  digikey.py          # DigiKey API v4: OAuth2 + MPN lookup + formatting
.env                  # All secrets (never commit this)
requirements.txt      # Python dependencies
```

---

## Environment Variables (`.env`)

DB credentials are **not** stored in `.env` — they come from Azure Key Vault (see below).
Only non-DB secrets live in `.env`:

```
ANTHROPIC_API_KEY=sk-ant-...
DIGIKEY_CLIENT_ID=...
DIGIKEY_CLIENT_SECRET=...
```

---

## Business Flow

```
CustomerRfqs (CRFQ)
  └─ CustomerRfqParts        — one row per MPN the customer needs
       └─ SupplierRfqs (SRFQ) — C3 sourcing sends one per supplier per part line
                                 Supplier responds: ResponsePrice / ResponseQty
            └─ CustomerQuoteParts + CustomerQuotes
                               — Sales picks best SRFQ offer(s), creates quote for customer
                                 CustomerQuoteParts.SupplierRfqId → winning SRFQ
```

**There is no "CRFQ value" field.** Deal value = `SUM(CustomerQuoteParts.SalePrice × SaleQty)`.

## Database Schema (`C3_Web`)

| Table | Key Columns |
|---|---|
| `CustomerRfqs` | `RfqId`, `CustomerId`, `SalesRepId`, `CreatedDate`, `Deleted` (0=active), `PotentialValue` (estimate only — never use for revenue) |
| `CustomerRfqParts` | `RfqPartId`, `CustomerRfqId`, `Mpn`, `MfrId`, `QuoteStatus` (0=Not Quoted, 10=Lost, 20=Quoted, **30=Won**), `QtyRequested`, `SourcingOwnerId`, `SourcingStatus` |
| `SupplierRfqs` | `RfqId`, `SupplierId`, `CustomerRfqPartId`, `SupplierRfqStatus` (10=Sent, 20=InProgress, 50=Responded), `ResponsePrice` (supplier's price to C3 = **cost**), `ResponseQty`, `SalePrice` (C3's price to customer), `SaleQty` |
| `CustomerQuotes` | `CustomerQuoteId`, `CustomerRfqRfqId` |
| `CustomerQuoteParts` | `CustomerQuotePartId`, `CustomerRfqId`, `CustomerRfqPartId`, `SupplierRfqId` (winning offer), `SalePrice` (**revenue**), `SaleQty`, `SupplierPrice` (**cost**), `CustomerMpn`, `SupplierMpn`, `LeadTimeARO` |
| `Customers` | `CustomerId`, `CustomerName`, `Country` |
| `Suppliers` | `SupplierId`, `SupplierName`, `ATD`, `MFR`, `BRK`, `HYB` (all bits) |
| `Users` | `UserId`, `DisplayName` |
| `Mfrs` | `MfrId`, `MfrName`, `MfrStdName` (use `LTRIM/RTRIM`) |

**Where to find financial data:**
| Question | Table & Column |
|---|---|
| Revenue / deal value | `CustomerQuoteParts.SalePrice × SaleQty` |
| Cost / sourcing price | `SupplierRfqs.ResponsePrice` OR `CustomerQuoteParts.SupplierPrice` |
| Profit / margin | `SalePrice - ResponsePrice` per unit; `SUM((SalePrice - ResponsePrice) × SaleQty)` total |
| Margin % | `(SalePrice - ResponsePrice) / SalePrice × 100` |
| Commission | **Does not exist** — no commission column anywhere in `C3_Web` |

**Critical rules for SQL:**
- Always filter `Deleted = 0` on `CustomerRfqs`
- Win/loss is `CustomerRfqParts.QuoteStatus` (30=Won, 10=Lost) — NOT `RfqResult`
- `RfqResult` is always 10 (Pending) and is meaningless
- `SupplierRfqStatus = 40` is never set

---

## Jerzy (AI Chat Agent)

### Tools Available
- `run_sql` — SELECT-only queries against `C3_Web`
- `lookup_digikey` — Live MPN data (pricing, lifecycle, stock, lead time)

### MPN Workflow (Two-Step) — CRITICAL

**Step 1** (automatic, no user prompt needed):
1. **RUN `run_sql` FIRST** — query `CustomerRfqParts` (exact match + `LIKE '%mpn%'` fallback) AND `SupplierRfqs` for sourcing history
2. Report actual results (rows found = show data; no rows = "No history found in C3's database for [MPN]")
3. Describe part from training knowledge: manufacturer, category, lifecycle status, specs, applications
4. Offer DigiKey lookup

**Step 2** (only if user says yes):
- Call `lookup_digikey` and present: lifecycle, stock, lead time, pricing breaks, URL

**NEVER** assume or state "no history" / "we haven't quoted this" before the `run_sql` result is in context. Silence ≠ absence.

### Key Behavior Rules
1. Only SELECT queries — never INSERT/UPDATE/DELETE/DDL
2. Always filter `Deleted = 0` on `CustomerRfqs`
3. TOP 50 cap unless user asks for count/specific number
4. Use `LIKE '%name%'` for partial customer/supplier name matches
5. Answer without SQL when possible (e.g., schema questions)
6. Always give a business-friendly summary after data results

---

## How to Run

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Authenticate with Azure (one-time per machine)
brew install azure-cli   # if not already installed
az login                 # log in with your Microsoft account

# 3. Start the dashboard
streamlit run dashboard.py
```

The sidebar shows `🟢 Key Vault (prod)` when connected to production, or `🟡 .env (dev/fallback)` if Azure auth failed. If you see the fallback, re-run `az login`.

**DB credentials source:** Azure Key Vault `j2-c3-euwe-kv-prod`, secret `DefaultConnection`.
- Dev vault: `j2-c3-euwe-kv-dev`
- Prod vault: `j2-c3-euwe-kv-prod`
- The code uses `DefaultAzureCredential` — works with Managed Identity on Azure, or `az login` locally.

---

## Common Failure Modes & Fixes

| Problem | Cause | Fix |
|---|---|---|
| Jerzy says "no history" before querying | DB query was after knowledge description in prompt | DB query is now step `a)` — always runs first |
| DigiKey auth fails | Expired token or missing credentials | Token auto-refreshes; check `.env` credentials |
| SQL error on Mfr names | Trailing spaces in `MfrStdName` | Use `LTRIM(RTRIM(...))` |
| `QuoteStatus` win/loss wrong | `CustomerRfqParts.QuoteStatus` is stale | Use `LEFT JOIN (SELECT DISTINCT CustomerRfqPartId FROM CustomerQuoteParts) qp` — presence of a CustomerQuoteParts record means "Quoted", not QuoteStatus value |
| Dashboard shows "No database connection" | Azure CLI not authenticated | Run `az login` in terminal, then restart dashboard |
| `🟡 .env (dev/fallback)` shown in sidebar | Azure auth failed | Run `az login`; ensure `azure-identity` and `azure-keyvault-secrets` are installed in venv |
| `429 rate_limit_error` from Anthropic API | Org-level token/min limit exceeded | See Rate Limits section below |

---

## Rate Limits — MUST READ BEFORE PRODUCTION LAUNCH

**Current org limit: 10,000 input tokens/minute** (Anthropic starter tier).

Each Jerzy request uses ~4,500 input tokens (system prompt + history). This means the entire org can handle roughly **1–2 requests/minute** before hitting 429 errors.

**With 100 users this will fail constantly.**

### Mitigations already in code
- Conversation history capped at last 10 messages (`chat_agent.py`) to prevent unbounded growth
- KEY JOIN PATTERNS SQL examples removed from schema to reduce prompt size

### What must be done before launch
1. **Upgrade Anthropic API plan** — go to `console.anthropic.com` → Settings → Billing, or email `sales@anthropic.com` requesting a rate limit increase for ~100 internal users
2. The limit needed is at minimum **500,000 input tokens/minute** (50× current)
3. Anthropic automatically raises limits as account spend history builds — or contact sales to expedite

---

## Development Notes

- Model: `claude-sonnet-4-6` (upgraded from Haiku — schema grew too large for Haiku's 10k token/min limit; Sonnet also produces better SQL)
- Agentic loop: up to 15 iterations to resolve multi-tool queries (raised from 8 to handle complex comparisons)
- Fallback call added: if loop exits with tool_use still pending, executes remaining tools + forces a final text response
- Conversation history capped at 10 messages (5 turns) to limit token usage per request
- DigiKey token cached in module-level dict with 30s expiry buffer
- Streamlit session state key: `ai_messages` (list of `{role, content}` dicts)
- DB credentials: Azure Key Vault (`j2-c3-euwe-kv-prod`) via `DefaultAzureCredential` — uses Managed Identity on Azure, `az login` locally
- VPN required for remote access — SQL Server is on J2 internal network
- Sidebar shows active credential source (`🟢 Key Vault (prod)` or `🟡 .env (dev/fallback)`)

**QuoteStatus reliability note:** `CustomerRfqParts.QuoteStatus` is often stale. For accurate "Quoted" status, always check `CustomerQuoteParts` existence:
```sql
CASE
  WHEN p.QuoteStatus = 30 THEN 'Won'
  WHEN qp.CustomerRfqPartId IS NOT NULL THEN 'Quoted'   -- CustomerQuoteParts record exists
  WHEN p.QuoteStatus = 10 THEN 'Lost'
  ELSE 'Not Quoted'
END
-- Join: LEFT JOIN (SELECT DISTINCT CustomerRfqPartId FROM CustomerQuoteParts) qp ON qp.CustomerRfqPartId = p.RfqPartId
-- NEVER use a plain LEFT JOIN CustomerQuoteParts — it causes row fan-out and inflates all counts
```
