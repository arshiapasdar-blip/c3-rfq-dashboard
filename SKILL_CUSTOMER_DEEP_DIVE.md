# Skill: C3 Customer Deep Dive — Full Reference for Another Agent

> **Purpose:** Give another Claude Code agent everything it needs to connect to the C3 database, understand the schema, and perform deep customer history analysis (parts requested, quotes sent, deals won/lost, pricing, margins, suppliers used) — with zero research needed.

---

## 1. DATABASE CONNECTION

### Tech Stack
- **Driver:** `pymssql`
- **Database:** SQL Server — `C3_Web`
- **TDS version:** 7.3
- **Isolation:** `READ UNCOMMITTED` (dirty reads OK for analytics)

### How to Connect (copy-paste ready)

```python
import pymssql
import pandas as pd

# Option A: Azure Key Vault (production — preferred)
from azure.identity import AzureCliCredential
from azure.keyvault.secrets import SecretClient

VAULT_URL = "https://j2-c3-euwe-kv-prod.vault.azure.net/"
SECRET_NAME = "DefaultConnectionAz"

credential = AzureCliCredential()
client = SecretClient(vault_url=VAULT_URL, credential=credential)
secret = client.get_secret(SECRET_NAME)

# Parse ADO.NET connection string: "Server=tcp:host,1433;Database=C3_Web;User ID=xxx;Password=yyy;"
def parse_conn_str(cs: str) -> dict:
    result = {}
    for part in cs.split(";"):
        part = part.strip()
        if "=" in part:
            k, v = part.split("=", 1)
            result[k.strip().lower()] = v.strip()
    return result

creds = parse_conn_str(secret.value)
server = creds.get("server") or creds.get("data source")
if server.lower().startswith("tcp:"):
    server = server[4:]
if "," in server:
    server = server.split(",")[0]

conn = pymssql.connect(
    server=server,
    user=creds.get("user id") or creds.get("uid"),
    password=creds.get("password") or creds.get("pwd"),
    database=creds.get("database") or creds.get("initial catalog"),
    tds_version="7.3",
    timeout=30,
)

# Option B: .env fallback (dev only)
# DB_SERVER, DB_NAME, DB_USER, DB_PASSWORD in .env

# Run a query:
def run_query(sql: str) -> pd.DataFrame:
    cursor = conn.cursor(as_dict=True)
    cursor.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
    cursor.execute(sql)
    rows = cursor.fetchall()
    return pd.DataFrame(rows) if rows else pd.DataFrame()
```

### Or reuse existing code:
```python
from tools.db import run_query  # already handles connection pooling, Key Vault, fallback
```

### Prerequisites
- `az login --tenant "83fe0a6e-1364-4aac-ad78-3afa9b9bd6bf" --scope "https://vault.azure.net/.default"` must be active
- VPN required (SQL Server is on J2 internal network)
- `pip install pymssql azure-identity azure-keyvault-secrets python-dotenv pandas`

---

## 2. BUSINESS MODEL (READ THIS FIRST)

```
CustomerRfqs (CRFQ)                    — A customer sends an RFQ
  └─ CustomerRfqParts                  — Each part/MPN the customer needs (1 row per line)
       └─ SupplierRfqs (SRFQ)          — C3 asks suppliers for quotes (1 row per supplier per part)
            └─ CustomerQuoteParts       — C3 picks best offer(s), quotes back to customer
               + CustomerQuotes           (CustomerQuoteParts.SupplierRfqId = winning SRFQ)
```

### Financial Data — Where to Find It
| Question | Source |
|---|---|
| **Revenue / deal value** | `CustomerQuoteParts.SalePrice * SaleQty` (SUM for totals) |
| **Cost / buy price** | `SupplierRfqs.ResponsePrice` OR `CustomerQuoteParts.SupplierPrice` |
| **Profit per unit** | `SalePrice - ResponsePrice` (or `SalePrice - SupplierPrice`) |
| **Total margin** | `SUM((SalePrice - SupplierPrice) * SaleQty)` |
| **Margin %** | `(SalePrice - SupplierPrice) / SalePrice * 100` |
| **Commission** | DOES NOT EXIST in database |
| **PotentialValue** | NEVER use — it's a rough manual estimate, not actual revenue |

---

## 3. COMPLETE SCHEMA REFERENCE

### CustomerRfqs
| Column | Type | Notes |
|---|---|---|
| RfqId | int PK | |
| CustomerId | int | → Customers.CustomerId |
| SalesRepId | int | → Users.UserId |
| CreatedDate | datetime2 | |
| DueDate | datetime2 | Customer deadline |
| Deleted | bit | **ALWAYS filter `Deleted = 0`** |
| PotentialValue | decimal | Estimate only — NEVER use for revenue |
| RfqResult | int | ALWAYS 10 (Pending) — meaningless, NEVER use |
| RfqStatus, RfqType | int | |
| InternalInfo, ExternalInfo | nvarchar | Free-text notes |

### CustomerRfqParts
| Column | Type | Notes |
|---|---|---|
| RfqPartId | int PK | |
| CustomerRfqId | int | → CustomerRfqs.RfqId |
| Mpn | nvarchar | Part number as submitted by customer |
| Cpn | nvarchar | Customer's own part number |
| MfrId | int | → Mfrs.MfrId |
| QtyRequested | decimal | Quantity customer asked for |
| SourcingOwnerId | int | → Users.UserId |
| SourcingStatus | int | 0=Not Started, 10=Sent, 20=InProgress, 30=Sourced, 40=Quoted, 50=Won, 60=Closed |
| QuoteStatus | int | **Often stale** — see reliable method below |
| CreatedDate | datetime2 | |
| Lifecycle | nvarchar | e.g. "Active", "NRND", "Obsolete" |
| Description | nvarchar | Part description |
| BestPrice | decimal | Market reference price |

**Reliable Win/Loss Status (use this, not raw QuoteStatus):**
```sql
CASE
  WHEN p.QuoteStatus = 30 THEN 'Won'
  WHEN EXISTS (SELECT 1 FROM CustomerQuoteParts qp WHERE qp.CustomerRfqPartId = p.RfqPartId) THEN 'Quoted'
  WHEN p.QuoteStatus = 10 THEN 'Lost'
  ELSE 'Not Quoted'
END AS Status
```
**WARNING:** Never use `LEFT JOIN CustomerQuoteParts` directly — it causes row fan-out and inflates counts. Use `EXISTS` or a pre-deduplicated subquery: `LEFT JOIN (SELECT DISTINCT CustomerRfqPartId FROM CustomerQuoteParts) qp`.

### SupplierRfqs
| Column | Type | Notes |
|---|---|---|
| RfqId | int PK | |
| SupplierId | int | → Suppliers.SupplierId |
| CustomerRfqPartId | int | → CustomerRfqParts.RfqPartId |
| Mpn | nvarchar | MPN as sent to supplier |
| MpnSupplier | nvarchar | Supplier's own catalogue number |
| SupplierMfrId | int | → Mfrs.MfrId |
| SupplierRfqStatus | int | 10=Sent, 20=InProgress, **50=Responded** (40 never set) |
| ResponsePrice | decimal | **Supplier's price to C3 = COST** |
| ResponseQty | decimal | |
| ResponseDate | datetime2 | |
| SalePrice | decimal | C3's price to customer |
| SaleQty | decimal | |
| LeadTimeARO | int | Lead time after receipt of order |
| CreatedDate | datetime2 | |
| InStock | int | Stock available per supplier |
| DC | nvarchar | Date code |
| IncludeInQuote | bit | Flagged for customer quote |

### CustomerQuotes
| Column | Type | Notes |
|---|---|---|
| CustomerQuoteId | int PK | |
| CustomerRfqRfqId | int | → CustomerRfqs.RfqId |
| CreatedDate | datetime2 | |
| CreatedByUserId | int | → Users.UserId |

### CustomerQuoteParts
| Column | Type | Notes |
|---|---|---|
| CustomerQuotePartId | int PK | |
| CustomerRfqId | int | → CustomerRfqs.RfqId |
| CustomerRfqPartId | int | → CustomerRfqParts.RfqPartId |
| SupplierRfqId | int | → SupplierRfqs.RfqId **(winning offer)** |
| CustomerQuoteId | int | → CustomerQuotes.CustomerQuoteId |
| CustomerMpn | nvarchar | MPN as shown to customer |
| SupplierMpn | nvarchar | MPN from supplier |
| **SalePrice** | decimal | **Revenue — price quoted to customer** |
| **SaleQty** | decimal | **Quantity quoted** |
| **SupplierPrice** | decimal | **Cost — what C3 pays supplier** |
| SupplierQty | decimal | |
| LeadTimeARO | int | |
| DC | nvarchar | Date code |

### Customers
| Column | Type | Notes |
|---|---|---|
| CustomerId | int PK | |
| CustomerName | nvarchar | Use `LIKE '%name%'` for partial matching |
| Country | nvarchar | |
| SalesRepId | int | → Users.UserId (default sales rep) |
| Prio | bit | Priority customer flag |

### Suppliers
| Column | Type | Notes |
|---|---|---|
| SupplierId | int PK | |
| SupplierName | nvarchar | |
| ATD | bit | Authorized distributor |
| MFR | bit | Manufacturer |
| BRK | bit | Broker |
| HYB | bit | Hybrid |

### Users
| Column | Type | Notes |
|---|---|---|
| UserId | int PK | |
| DisplayName | nvarchar | Human name |

### Mfrs
| Column | Type | Notes |
|---|---|---|
| MfrId | int PK | |
| MfrName | nvarchar | Raw name |
| MfrStdName | nvarchar | **ALWAYS use `LTRIM(RTRIM(...))`** — has trailing spaces |

### Other Useful Tables
- **IHSParts** — Market data: `PartNumber` (MPN), `Lifecycle`, `BestPrice`, `AvgLT`, `Description`
- **PriceList** / **PriceListPart** — Customer-specific agreed pricing
- **StockSalesQuote2** / **StockSalesQuotePart2** — Outbound stock sales (separate from RFQ flow)
- **ExcessList** / **ExcessListPart** — Supplier excess inventory
- **ExchangeRates** — Currency conversion (`CurrencyCode`, `Rate`)

---

## 4. READY-MADE SQL QUERIES FOR CUSTOMER DEEP DIVE

### 4a. Find a Customer
```sql
SELECT TOP 20 CustomerId, CustomerName, Country
FROM Customers
WHERE CustomerName LIKE '%PARTIAL_NAME%'
ORDER BY CustomerName
```

### 4b. All RFQs for a Customer
```sql
SELECT r.RfqId, r.CreatedDate, r.DueDate, u.DisplayName AS SalesRep,
       COUNT(p.RfqPartId) AS PartLines
FROM CustomerRfqs r
JOIN Users u ON u.UserId = r.SalesRepId
JOIN CustomerRfqParts p ON p.CustomerRfqId = r.RfqId
WHERE r.CustomerId = @CUSTOMER_ID AND r.Deleted = 0
GROUP BY r.RfqId, r.CreatedDate, r.DueDate, u.DisplayName
ORDER BY r.CreatedDate DESC
```

### 4c. All Parts Requested by a Customer (with status)
```sql
SELECT r.RfqId, r.CreatedDate, p.RfqPartId, p.Mpn, p.QtyRequested,
       LTRIM(RTRIM(COALESCE(m.MfrStdName, m.MfrName))) AS Manufacturer,
       p.Lifecycle, p.Description,
       CASE
         WHEN p.QuoteStatus = 30 THEN 'Won'
         WHEN EXISTS (SELECT 1 FROM CustomerQuoteParts qp WHERE qp.CustomerRfqPartId = p.RfqPartId) THEN 'Quoted'
         WHEN p.QuoteStatus = 10 THEN 'Lost'
         ELSE 'Not Quoted'
       END AS Status,
       so.DisplayName AS SourcingOwner
FROM CustomerRfqs r
JOIN CustomerRfqParts p ON p.CustomerRfqId = r.RfqId
LEFT JOIN Mfrs m ON m.MfrId = p.MfrId
LEFT JOIN Users so ON so.UserId = p.SourcingOwnerId
WHERE r.CustomerId = @CUSTOMER_ID AND r.Deleted = 0
ORDER BY r.CreatedDate DESC, p.Mpn
```

### 4d. What We Quoted Back to the Customer (with margin)
```sql
SELECT r.RfqId, r.CreatedDate,
       qp.CustomerMpn, qp.SupplierMpn,
       s.SupplierName,
       qp.SaleQty, qp.SalePrice, qp.SupplierPrice,
       (qp.SalePrice - qp.SupplierPrice) AS MarginPerUnit,
       CASE WHEN qp.SalePrice > 0
            THEN ROUND((qp.SalePrice - qp.SupplierPrice) / qp.SalePrice * 100, 2)
            ELSE 0 END AS MarginPct,
       (qp.SalePrice * qp.SaleQty) AS LineRevenue,
       ((qp.SalePrice - qp.SupplierPrice) * qp.SaleQty) AS LineProfit,
       qp.LeadTimeARO, qp.DC
FROM CustomerQuoteParts qp
JOIN CustomerRfqs r ON r.RfqId = qp.CustomerRfqId
LEFT JOIN SupplierRfqs sr ON sr.RfqId = qp.SupplierRfqId
LEFT JOIN Suppliers s ON s.SupplierId = sr.SupplierId
WHERE r.CustomerId = @CUSTOMER_ID AND r.Deleted = 0
ORDER BY r.CreatedDate DESC, qp.CustomerMpn
```

### 4e. What Actually Sold (Won Deals Only)
```sql
SELECT r.RfqId, r.CreatedDate,
       p.Mpn, p.QtyRequested,
       LTRIM(RTRIM(COALESCE(m.MfrStdName, m.MfrName))) AS Manufacturer,
       qp.SaleQty, qp.SalePrice, qp.SupplierPrice,
       (qp.SalePrice * qp.SaleQty) AS Revenue,
       ((qp.SalePrice - qp.SupplierPrice) * qp.SaleQty) AS Profit,
       s.SupplierName AS WinningSupplier,
       qp.LeadTimeARO
FROM CustomerRfqParts p
JOIN CustomerRfqs r ON r.RfqId = p.CustomerRfqId
JOIN CustomerQuoteParts qp ON qp.CustomerRfqPartId = p.RfqPartId
LEFT JOIN SupplierRfqs sr ON sr.RfqId = qp.SupplierRfqId
LEFT JOIN Suppliers s ON s.SupplierId = sr.SupplierId
LEFT JOIN Mfrs m ON m.MfrId = p.MfrId
WHERE r.CustomerId = @CUSTOMER_ID AND r.Deleted = 0
  AND p.QuoteStatus = 30
ORDER BY r.CreatedDate DESC
```

### 4f. Supplier Offers Received for a Customer's Parts
```sql
SELECT r.RfqId, p.Mpn, s.SupplierName,
       sr.SupplierRfqStatus,
       CASE sr.SupplierRfqStatus WHEN 10 THEN 'Sent' WHEN 20 THEN 'In Progress' WHEN 50 THEN 'Responded' END AS StatusText,
       sr.ResponsePrice, sr.ResponseQty, sr.ResponseDate,
       sr.SalePrice AS ProposedSalePrice, sr.LeadTimeARO, sr.InStock
FROM SupplierRfqs sr
JOIN CustomerRfqParts p ON p.RfqPartId = sr.CustomerRfqPartId
JOIN CustomerRfqs r ON r.RfqId = p.CustomerRfqId
JOIN Suppliers s ON s.SupplierId = sr.SupplierId
WHERE r.CustomerId = @CUSTOMER_ID AND r.Deleted = 0
ORDER BY r.CreatedDate DESC, p.Mpn, sr.ResponsePrice
```

### 4g. Customer Summary KPIs
```sql
SELECT
  COUNT(DISTINCT r.RfqId) AS TotalRFQs,
  COUNT(DISTINCT p.RfqPartId) AS TotalPartLines,
  SUM(CASE WHEN p.QuoteStatus = 30 THEN 1 ELSE 0 END) AS WonLines,
  SUM(CASE WHEN p.QuoteStatus = 10 THEN 1 ELSE 0 END) AS LostLines,
  SUM(CASE WHEN p.QuoteStatus = 30 THEN qp.SalePrice * qp.SaleQty ELSE 0 END) AS TotalRevenue,
  SUM(CASE WHEN p.QuoteStatus = 30 THEN (qp.SalePrice - qp.SupplierPrice) * qp.SaleQty ELSE 0 END) AS TotalProfit
FROM CustomerRfqs r
JOIN CustomerRfqParts p ON p.CustomerRfqId = r.RfqId
LEFT JOIN CustomerQuoteParts qp ON qp.CustomerRfqPartId = p.RfqPartId AND p.QuoteStatus = 30
WHERE r.CustomerId = @CUSTOMER_ID AND r.Deleted = 0
```

### 4h. Top Manufacturers Requested by Customer
```sql
SELECT TOP 20
  LTRIM(RTRIM(COALESCE(m.MfrStdName, m.MfrName))) AS Manufacturer,
  COUNT(*) AS TimesRequested,
  COUNT(DISTINCT p.Mpn) AS UniqueMPNs,
  SUM(CASE WHEN p.QuoteStatus = 30 THEN 1 ELSE 0 END) AS Won
FROM CustomerRfqParts p
JOIN CustomerRfqs r ON r.RfqId = p.CustomerRfqId
LEFT JOIN Mfrs m ON m.MfrId = p.MfrId
WHERE r.CustomerId = @CUSTOMER_ID AND r.Deleted = 0
GROUP BY LTRIM(RTRIM(COALESCE(m.MfrStdName, m.MfrName)))
ORDER BY TimesRequested DESC
```

---

## 5. CRITICAL RULES (MUST FOLLOW)

1. **ALWAYS** filter `WHERE r.Deleted = 0` on CustomerRfqs
2. **NEVER** use `PotentialValue` for revenue — use `CustomerQuoteParts.SalePrice * SaleQty`
3. **NEVER** use `RfqResult` — it's always 10 and meaningless
4. **ALWAYS** use `LTRIM(RTRIM(...))` on `MfrStdName` — has trailing whitespace
5. **QuoteStatus is often stale** — use the EXISTS pattern for accurate Quoted status; only `QuoteStatus = 30` (Won) is reliable
6. **NEVER** use plain `LEFT JOIN CustomerQuoteParts` — use `EXISTS` or `SELECT DISTINCT CustomerRfqPartId` subquery to avoid row fan-out
7. **SELECT only** — never INSERT/UPDATE/DELETE
8. Use `LIKE '%name%'` for partial customer/supplier name matching
9. Use `TOP 50` cap unless the user asks for a specific count
10. `SupplierRfqStatus = 40` is never set — don't filter on it
11. `ResponsePrice` = cost to C3 (not the sale price)
12. Commission does not exist in the database

---

## 6. DIRTY MPN FILTER

Exclude junk test data from MPN analysis:
```sql
p.Mpn IS NOT NULL
AND LEN(LTRIM(RTRIM(p.Mpn))) > 2
AND LOWER(LTRIM(RTRIM(p.Mpn))) NOT IN (
  'test','test1','test2','test3','test12345',
  '12345','5678','123','abc','t','maxm',
  'n/a','na','tbd','-','.','string'
)
```

---

## 7. JOIN MAP (Quick Reference)

```
Customers.CustomerId ──→ CustomerRfqs.CustomerId
CustomerRfqs.RfqId ──→ CustomerRfqParts.CustomerRfqId
CustomerRfqs.SalesRepId ──→ Users.UserId
CustomerRfqParts.RfqPartId ──→ SupplierRfqs.CustomerRfqPartId
CustomerRfqParts.MfrId ──→ Mfrs.MfrId
CustomerRfqParts.SourcingOwnerId ──→ Users.UserId
SupplierRfqs.SupplierId ──→ Suppliers.SupplierId
SupplierRfqs.SupplierMfrId ──→ Mfrs.MfrId
CustomerQuotes.CustomerRfqRfqId ──→ CustomerRfqs.RfqId
CustomerQuoteParts.CustomerRfqId ──→ CustomerRfqs.RfqId
CustomerQuoteParts.CustomerRfqPartId ──→ CustomerRfqParts.RfqPartId
CustomerQuoteParts.SupplierRfqId ──→ SupplierRfqs.RfqId  (winning offer)
CustomerQuoteParts.CustomerQuoteId ──→ CustomerQuotes.CustomerQuoteId
PriceList.CustomerId ──→ Customers.CustomerId
PriceListPart.PriceListId ──→ PriceList.PriceListId
```
