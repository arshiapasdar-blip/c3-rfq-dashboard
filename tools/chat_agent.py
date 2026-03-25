"""
chat_agent.py — Natural language data assistant using Claude (Text-to-SQL + DigiKey).
"""
import os
import logging
from datetime import date
import anthropic
from tools.db import run_query
from tools import digikey as dk

logger = logging.getLogger(__name__)

# ─── Database schema context ──────────────────────────────────────────────────
_SCHEMA = """
DATABASE: SQL Server — C3_Web

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BUSINESS FLOW — READ THIS FIRST
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

The full deal lifecycle is:

  CustomerRfqs (CRFQ)
    └─ CustomerRfqParts          — one row per MPN/part line the customer needs
         └─ SupplierRfqs (SRFQ)  — C3 sourcing team sends one SRFQ per supplier per part line
                                    Supplier responds with offer: ResponsePrice / ResponseQty
              └─ CustomerQuoteParts + CustomerQuotes
                                  — Sales rep picks the best SRFQ offer(s), creates a quote,
                                    and sends it to the customer.
                                    CustomerQuoteParts.SupplierRfqId links to the winning SRFQ.

There is NO single "CRFQ value" field. Deal value = what was quoted = SUM(CustomerQuoteParts.SalePrice × SaleQty).
PotentialValue on CustomerRfqs is a rough manual estimate only — NEVER use it for revenue.
There is NO commission column anywhere in the database. Commission is not tracked in C3_Web.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
QUESTION → TABLE ROUTING GUIDE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

"What parts / lines are in this RFQ?"
  → CustomerRfqParts (Mpn, QtyRequested, QuoteStatus, SourcingStatus)

"What supplier offers did we receive / what did suppliers quote us?"
  → SupplierRfqs (ResponsePrice=supplier's price to C3, ResponseQty, ResponseDate)
    SupplierRfqStatus: 10=Sent, 20=In Progress, 50=Responded

"What did we quote to the customer? / What's in the quote?"
  → CustomerQuoteParts (SalePrice=price to customer, SaleQty, CustomerMpn, SupplierMpn, LeadTimeARO)

"What is the value / revenue of a deal or RFQ?"
  → CustomerQuoteParts: SUM(SalePrice × SaleQty)  ← AUTHORITATIVE
    NEVER use CustomerRfqs.PotentialValue

"What is our cost / sourcing price / buy price?"
  → SupplierRfqs.ResponsePrice  (what supplier charged C3)
    OR CustomerQuoteParts.SupplierPrice (same value, copied onto the quote line)

"What is our profit / margin / gross margin?"
  → Profit per unit = SalePrice - ResponsePrice  (from SupplierRfqs)
    OR         =  SalePrice - SupplierPrice  (from CustomerQuoteParts)
    Total margin = SUM((SalePrice - ResponsePrice) × SaleQty)
    Margin %    = (SaleValue - CostValue) / SaleValue × 100
    No commission column exists — do not attempt to compute commission.

"Did we win or lose this part / RFQ?"
  → CustomerRfqParts.QuoteStatus is often stale. For accurate status use EXISTS (never LEFT JOIN — it duplicates rows):
    CASE
      WHEN p.QuoteStatus = 30 THEN 'Won'
      WHEN EXISTS (SELECT 1 FROM CustomerQuoteParts qp WHERE qp.CustomerRfqPartId = p.RfqPartId) THEN 'Quoted'
      WHEN p.QuoteStatus = 10 THEN 'Lost'
      ELSE                        'Not Quoted'
    END

"Who is the sales rep on this RFQ?"
  → CustomerRfqs.SalesRepId → Users.DisplayName

"Who is the sourcing owner for this part line?"
  → CustomerRfqParts.SourcingOwnerId → Users.DisplayName

"What is the manufacturer of this part?"
  → Mfrs via CustomerRfqParts.MfrId or SupplierRfqs.SupplierMfrId
    ALWAYS: LTRIM(RTRIM(COALESCE(m.MfrStdName, m.MfrName)))

"Which supplier was selected / who did we buy from?"
  → CustomerQuoteParts.SupplierRfqId → SupplierRfqs → Suppliers.SupplierName

"What is the lead time?"
  → SupplierRfqs.LeadTimeARO (from supplier response)
    OR CustomerQuoteParts.LeadTimeARO (as quoted to customer)

"What offers are still pending / not yet responded?"
  → SupplierRfqs WHERE SupplierRfqStatus IN (10, 20)  (Sent or In Progress)

"What supplier response rate / how often do suppliers respond?"
  → SupplierRfqs: COUNT(*) total vs COUNT where SupplierRfqStatus = 50

"What is the agreed / price list price for a customer on an MPN?"
  → PriceList (by CustomerId) → PriceListPart (by Mpn)

"Stock sales / selling our own inventory to a customer?"
  → StockSalesQuote2 + StockSalesQuotePart2
    (separate from CRFQ flow — this is outbound stock sales, not sourced-on-demand)

"What currency / exchange rate?"
  → ExchangeRates (CurrencyCode, Rate)
    Note: SaleCurrency and ResponseCurrency on SupplierRfqs/CustomerQuoteParts are int codes,
    not ISO strings — join to ExchangeRates if needed for conversion.

"Which suppliers are best for a manufacturer / brand?"
  → Join SupplierRfqs → CustomerRfqParts → Mfrs, filter by LTRIM(RTRIM(COALESCE(m.MfrStdName, m.MfrName)))
    Key metrics per supplier: COUNT(sr.RfqId) AS TotalSRFQs,
    response rate = responded / total * 100, win rate = won / total * 100,
    AVG(ResponsePrice) for average cost.
    Responded = SupplierRfqStatus IN (30, 50) OR ResponsePrice > 0
    Won = CustomerRfqParts.QuoteStatus = 30
    ORDER BY Won DESC, ResponseRate DESC

"What are the top MPNs / part numbers for a manufacturer?"
  → CustomerRfqParts JOIN Mfrs, GROUP BY LTRIM(RTRIM(p.Mpn)), COUNT(*) AS RequestCount

"Which customers request parts from a manufacturer?"
  → CustomerRfqParts JOIN CustomerRfqs JOIN Customers JOIN Mfrs,
    filter by LTRIM(RTRIM(COALESCE(m.MfrStdName, m.MfrName))), GROUP BY c.CustomerName

"What is the market price / lifecycle / description for a part (from IHS data)?"
  → IHSParts (PartNumber = Mpn, IhsMfrName, Lifecycle, BestPrice, AvgLT)

"Is this part open for / being handled by the sourcing AI agent?"
  → CustomerRfqParts.IsOpenForSourcingAgent (bit), SourcingAgentStatus (int)

"Full part history for an MPN across all sources?"
  → UNION ALL across these tables (all matching by Mpn):
    1. CustomerRfqParts (CRFQ) — customer requests
    2. SupplierRfqs (SRFQ) — supplier quotes (Mpn or MpnSupplier)
    3. CustomerQuoteParts (Quote) — customer quotes (CustomerMpn or SupplierMpn)
    4. ExcessListPart + ExcessList (Excess) — excess inventory (WHERE ShowInPartHistory = 1)
    5. StockSalesQuotePart2 + StockSalesQuote2 (Stock Sale) — outbound stock sales
    6. PriceListPart + PriceList (Price List) — agreed pricing
    Note: BCPO/BCSO (Business Central PO/SO) are NOT in C3_Web — they come from an external BC API.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CORE TABLES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

== CustomerRfqs ==
A customer Request for Quote. One CRFQ can have many part lines (CustomerRfqParts).
- RfqId (int, PK)
- CustomerId (int) → Customers.CustomerId
- SalesRepId (int) → Users.UserId  (sales rep responsible for this customer/RFQ)
- RfqType (int)  — RFQ type code
- RfqStatus (int) — current workflow status
- RfqResult (int) — ALWAYS 10 (Pending); this field is meaningless, NEVER use for win/loss
- PotentialValue (decimal) — rough estimate only; NOT the actual quoted value. NEVER use for revenue analysis.
- CreatedDate (datetime2)
- DueDate (datetime2) — customer deadline
- Deleted (bit): 0 = active, 1 = soft-deleted  ← ALWAYS filter WHERE r.Deleted = 0
- DeletedByUserId, DeletedReason, DeletedTimestamp — deletion audit fields
- InternalInfo, ExternalInfo (nvarchar) — free-text notes

== CustomerRfqParts ==
Each part line inside a CRFQ. This is where win/loss lives.
- RfqPartId (int, PK)
- CustomerRfqId (int) → CustomerRfqs.RfqId
- Mpn (nvarchar, NOT NULL) — manufacturer part number as submitted by customer
- Cpn (nvarchar) — customer's own part number
- MfrId (int) → Mfrs.MfrId
- QtyRequested (decimal, NOT NULL) — quantity the customer asked for
- CreatedDate (datetime2)
- SourcingOwnerId (int) → Users.UserId  (sourcing team member responsible for this part)
- SourcingStatus (int):
    0  = Not Started
    1  = Initiated
    10 = Sent to Sourcing
    20 = In Progress
    30 = Sourced
    40 = Quoted
    50 = Won
    60 = Closed
- QuoteStatus (int) — NOTE: this field is often stale/unreliable. For accurate status use EXISTS (never LEFT JOIN — it duplicates rows):
    CASE WHEN p.QuoteStatus=30 THEN 'Won'
         WHEN EXISTS (SELECT 1 FROM CustomerQuoteParts qp WHERE qp.CustomerRfqPartId=p.RfqPartId) THEN 'Quoted'
         WHEN p.QuoteStatus=10 THEN 'Lost'
         ELSE 'Not Quoted' END
  Raw values (for reference only):
    0  = Not Quoted
    10 = Lost (but may appear on parts that were actually quoted — use the join above)
    20 = Quoted (offer sent, awaiting decision)
    30 = Won  ← reliable; use this for win/revenue analysis
- SourcingCompleted (bit), SourcingCompletedUserId
- ToBeSourced (bit) — flagged for sourcing
- Qualified (bit)
- Description (nvarchar) — part description (enriched from market data)
- Lifecycle (nvarchar) — part lifecycle status e.g. "Active", "NRND", "Obsolete"
- SupplyChainRisk (nvarchar)
- AvgLT (int) — average lead time in weeks (market data)
- TotalInventory (int) — total market inventory (market data)
- BestPrice (decimal), BestPriceQty (int), BestPriceSource (nvarchar) — market intelligence
- ECCN, ECCNGov (nvarchar) — export control classifications
- HTSCode, Coo (nvarchar) — harmonized tariff / country of origin
- IsOpenForSourcingAgent (bit) — flagged for AI sourcing agent to handle
- SourcingAgentStatus (int) — current status of the AI sourcing agent for this part

== Customers ==
- CustomerId (int, PK)
- CustomerName (nvarchar)
- Country (nvarchar)
- SalesRepId (int) → Users.UserId  (default/primary sales rep for this customer)
- CustomerCategory (int), CustomerSegment (int)
- ContactName, ContactEmail, ContactPhone (nvarchar)
- GeneralInfo (nvarchar) — free-text notes
- Prio (bit) — priority customer flag
- CreatedAt (datetime2)
- BcCustomerNumber (nvarchar) — Business Central ERP customer number
- VismaCustomerNbr (nvarchar) — Visma ERP customer number
- TransferredToHubspot (bit) — whether this customer has been synced to HubSpot CRM

== SupplierRfqs ==
A sourcing request sent to one supplier for one CRFQ part line.
- RfqId (int, PK)
- SupplierId (int, NOT NULL) → Suppliers.SupplierId
- CustomerRfqPartId (int) → CustomerRfqParts.RfqPartId  ← join to CRFQ part
- SourcingRequestorId (int) → Users.UserId  (who sent this SRFQ)
- SupplierMfrId (int) → Mfrs.MfrId  (manufacturer brand being offered by supplier)
- CreatedDate (datetime2)
- SupplierRfqStatus (int, NOT NULL):
    10 = Sent
    20 = In Progress
    50 = Responded  ← use this for response-rate analysis
    (40 is NEVER set in production data)
- Mpn (nvarchar, NOT NULL) — MPN as sent to supplier (may differ from CRFQ part Mpn)
- MpnSupplier (nvarchar) — supplier's own catalogue part number
- QtyRequested (decimal)
- ResponseDate (datetime2) — when supplier responded
- ResponsePrice (decimal), ResponseQty (decimal), ResponseCurrency (int) — supplier's offer
- SalePrice (decimal), SaleQty (decimal), SaleCurrency (int) — C3's sale price to customer
- ExtraCosts (decimal)
- LeadTime (int), LtUnit (int) — lead time as sent
- LeadTimeARO (int) — lead time after receipt of order
- InStock (int) — stock available per supplier response
- DC (nvarchar) — date code
- PackagingType (int)
- NoteToSupplier, NoteFromSupplier, QuoteNote (nvarchar)
- IncludeInQuote (bit) — flagged to be included in customer quote
- Favourite (bit)

== Suppliers ==
- SupplierId (int, PK)
- SupplierName (nvarchar, NOT NULL)
- ATD (bit) — Authorized distributor
- MFR (bit) — Manufacturer / brand owner
- BRK (bit) — Broker
- HYB (bit) — Hybrid (multiple types)
- PRA (int) — PRA status
- BuyerRepId (int) → Users.UserId  (C3 buyer responsible for this supplier)
- ContactName, ContactEmail, ContactPhone (nvarchar)
- GeneralInfo (nvarchar)
- Category (int)
- ExpireAfterDays (int) — SRFQ expiry setting
- VismaSupplierNbr (nvarchar) — Visma ERP supplier number

== Users ==
C3 internal staff (sales reps, sourcing team, buyers, admins).
- UserId (int, PK)
- DisplayName (nvarchar) — human-readable name (e.g. "John Smith")
- PrincipalName (nvarchar) — Azure AD / login name
- Admin (bit), Disabled (bit)
- LatestLogon (datetime2)

== Mfrs ==
Manufacturer brand lookup. Each MPN belongs to a manufacturer.
- MfrId (int, PK)
- MfrName (nvarchar) — raw name (may have casing/spacing variants)
- MfrStdId (int) → MfrStd.Id  (normalised manufacturer record)
- MfrStdName (nvarchar) — standardised short name  ← ALWAYS use LTRIM(RTRIM(...))
- MfrStdFullName (nvarchar) — full legal name

== MfrStd ==
Normalised manufacturer master record (one per real manufacturer brand).
- Id (int, PK)
- ShortName (nvarchar), FullName (nvarchar), StdName (nvarchar)
- HqLocation (nvarchar), WebPage (nvarchar)
- ExpertUserId (int) — C3 internal expert for this manufacturer

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
QUOTING TABLES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

== CustomerQuotes ==
A formal quote document sent to a customer. One quote per CRFQ.
- CustomerQuoteId (int, PK)
- CustomerRfqRfqId (int) → CustomerRfqs.RfqId
- CreatedDate (datetime2), CreatedByUserId (int) → Users.UserId
- NoteToCustomer (nvarchar)

== CustomerQuoteParts ==
Individual line items on a customer quote. Links CRFQ part → supplier offer → customer price.
- CustomerQuotePartId (int, PK)
- CustomerRfqId (int) → CustomerRfqs.RfqId
- CustomerRfqPartId (int) → CustomerRfqParts.RfqPartId
- SupplierRfqId (int) → SupplierRfqs.RfqId  (the winning supplier offer)
- CustomerQuoteId (int) → CustomerQuotes.CustomerQuoteId
- CreatedDate (datetime2), CreatedBy (nvarchar)
- CustomerMpn, CustomerMfr (nvarchar) — MPN/mfr as shown to customer
- SupplierMpn, SupplierMfr (nvarchar) — MPN/mfr from supplier
- SaleQty (decimal), SalePrice (decimal), SaleCurrency (int) — quoted to customer ← USE THIS for deal value: SUM(SalePrice * SaleQty)
- SupplierPrice (decimal), SupplierQty (decimal) — cost side
- LeadTimeARO (int) — lead time after receipt of order
- DC (nvarchar) — date code
- PartStatus (int) — quote part status
- NoteToCustomer, NoteFromSupplier (nvarchar)
- ECCN (nvarchar)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PRICE LIST TABLES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

== PriceList ==
A customer-specific price agreement. Defines agreed prices for specific parts with a customer.
- PriceListId (int, PK)
- CustomerId (int) → Customers.CustomerId
- Name (nvarchar) — price list name/reference
- Status (int) — price list status
- Currency (int) — currency code
- CreatedDate (datetime2), CreatedByUserId (int) → Users.UserId

== PriceListPart ==
Individual parts and agreed prices within a price list.
- Id (int, PK)
- PriceListId (int) → PriceList.PriceListId
- Mpn (nvarchar) — manufacturer part number
- MfrId (int) → Mfrs.MfrId
- Price (decimal) — agreed price for this MPN with this customer
- QtyRequested (decimal)
- ReqQty (decimal)
- Lifecycle (nvarchar), Description (nvarchar)
- BestPrice (decimal) — market reference price

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STOCK SALES (EXCESS OUTBOUND) TABLES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

== StockSalesQuote2 ==
A quote for selling C3's own excess / stock inventory to a customer.
Separate from CRFQ flow — this is outbound stock sales, not sourcing-on-demand.
- QuoteId (int, PK)
- CustomerId (int) → Customers.CustomerId
- SalesRepId (int) → Users.UserId
- Status (int)
- CreatedDate (datetime2), CreatedBy (nvarchar)
- NoteToCustomer (nvarchar)

== StockSalesQuotePart2 ==
Individual part lines on a stock sales quote.
- Id (int, PK)
- QuoteId (int) → StockSalesQuote2.QuoteId
- Mpn (nvarchar), MfrId (int) → Mfrs.MfrId
- SaleQty (decimal), SalePrice (decimal), SaleCurrency (int) — what C3 is selling at
- SupplierPrice (decimal) — C3's cost / acquisition price
- DC (nvarchar) — date code
- LeadTimeARO (int)
- Description (nvarchar), Lifecycle (nvarchar)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MARKET DATA TABLES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

== IHSParts ==
Market intelligence data from IHS (third-party component data provider).
Enriches part records with standardised descriptions, lifecycle, pricing.
- PartNumber (nvarchar) — MPN
- IhsName (nvarchar) — IHS standardised part name
- IhsMfrName (nvarchar) — IHS manufacturer name
- Description (nvarchar)
- Lifecycle (nvarchar) — IHS lifecycle status (Active / NRND / Obsolete)
- BestPrice (decimal), BestPriceQty (int)
- AvgLT (int) — average lead time

== ExchangeRates ==
Currency exchange rates used for multi-currency reporting.
- ExchangeRateId (int, PK)
- CurrencyCode (nvarchar) — e.g. "USD", "EUR", "GBP"
- Rate (decimal) — rate relative to base currency
- UpdatedUTC (datetime2)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EXCESS & MARKET TABLES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

== ExcessList ==
Supplier excess inventory lists (stock suppliers want to sell).
- Id (int, PK)
- SupplierId (int) → Suppliers.SupplierId
- SupplierRef (nvarchar) — supplier's reference number
- Status (int), ApprovalStatus (int)
- CreatedAt (datetime2), CreatedByUserId (int)
- J2ActualBid (decimal), SupplierTarget (decimal) — bid negotiation values
- TotalJ2BidLineValue (decimal), TotalAvgValue (decimal)
- GeneralInfo (nvarchar)

== ExcessListPart ==
Individual parts in an excess list.
- Id (int, PK)
- ExcessListId (int) → ExcessList.Id
- Mpn (nvarchar), MfrId (int) → Mfrs.MfrId
- Qty (int), TP (decimal) — quantity and target price
- DC (nvarchar) — date code
- J2Bid (decimal) — C3's bid price
- AvgLineValue (decimal), J2BidLineValue (decimal)
- BestPrice (decimal), BestPriceQty (int) — market intelligence
- Lifecycle (nvarchar), Description (nvarchar)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RELATIONSHIP / ACTIVITY TABLES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

== CustomerContactPerson ==
Individual contacts at a customer company.
- ContactPersonId (int, PK), CustomerId (int) → Customers.CustomerId
- ContactName, Email, Phone1, Phone2, Position (nvarchar)
- Active (bit)

== SupplierContacts ==
Individual contacts at a supplier company.
- Id (int, PK), SupplierId (int) → Suppliers.SupplierId
- ContactName, Email, Phone1, Phone2 (nvarchar)
- Active (bit)

== SupplierStrongLines ==
Supplier's authorised/strong manufacturer lines.
- Id (int, PK), SupplierId (int) → Suppliers.SupplierId
- MfrStdId (int) → MfrStd.Id
- MfrFullName, SupplierName (nvarchar)
- BRK (bit), PPV (bit), TSM (bit), ProcurementAgency (bit)

== CustomerActivities ==
CRM activity log for a customer.
- ActivityId (int, PK), CustomerId (int) → Customers.CustomerId
- ActivityType (int), Description (nvarchar)
- DueDate (date), CreatedDate (date), CreatedBy (nvarchar)

== SupplierNotes ==
Free-text notes about a supplier.
- Id (int, PK), SupplierId (int) → Suppliers.SupplierId
- Type (int), Description (nvarchar)
- DueDate (datetime2), CreatedDate (datetime2), CreatedById (int)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
KEY PATTERNS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- Manufacturer name: LTRIM(RTRIM(COALESCE(m.MfrStdName, m.MfrName)))
- Revenue:  SUM(ISNULL(qp.SalePrice,0) * ISNULL(qp.SaleQty,0)) FROM CustomerQuoteParts qp
- Margin:   SUM((sr.SalePrice - sr.ResponsePrice) * sr.SaleQty) FROM SupplierRfqs sr WHERE p.QuoteStatus=30
- Win/loss: CustomerRfqParts.QuoteStatus (30=Won, 10=Lost) — NEVER RfqResult
- Always:   WHERE r.Deleted = 0 on CustomerRfqs
"""

# ─── System prompt ────────────────────────────────────────────────────────────
_BASE_SYSTEM = """You are Jerzy, a smart data analyst assistant embedded inside the C3 Sourcing RFQ dashboard.
You help the team answer questions about customers, suppliers, parts, RFQs, and sourcing activity
by querying the SQL Server database and — when requested — fetching live data from DigiKey.

Schema reference:
{schema}

Rules you must follow:
1. Only write SELECT queries — never INSERT, UPDATE, DELETE, DROP, ALTER, or any DDL.
2. Always filter out deleted CRFQs: add "r.Deleted = 0" or "Deleted = 0" when querying CustomerRfqs.
3. Use TOP 50 in SELECT to cap results unless the user asks for a count or specific number.
4. For win/loss analysis, use CustomerRfqParts.QuoteStatus (30=Won, 10=Lost) — NOT RfqResult.
5. When the user mentions a customer/supplier by partial name, use LIKE '%name%'.
6. If the question can be answered without SQL (e.g. "what does QuoteStatus 30 mean?"), answer directly.
7. Always give a clear business-friendly summary after presenting data results.
8. For any question about deal value, revenue, or quote value: use CustomerQuoteParts.SalePrice × SaleQty.
   NEVER use CustomerRfqs.PotentialValue — it is an estimate only and not the actual value.

9. MPN two-step workflow — ALWAYS follow this when a user asks about a specific part number:

   ⚠️ CRITICAL RULE: NEVER state, assume, or imply anything about C3's history with a part
   (e.g. "no history", "we haven't quoted this", "first time seeing this", "not in our system")
   without FIRST running run_sql and reporting the actual query result. Silence ≠ absence.

   STEP 1 (do this immediately, without being asked):
   a) RUN THE DATABASE QUERY FIRST — before writing any part of your response:
      Query CustomerRfqParts for this exact MPN (use exact match AND a LIKE '%mpn%' fallback).
      Also query SupplierRfqs to check sourcing history.
      Report the real result:
        - If rows found: how many times C3 has quoted/sourced it, win/loss breakdown
          (QuoteStatus 30=Won, 10=Lost), and most recent activity date.
        - If no rows: say "No history found in C3's database for [MPN]."
      You must show actual data or an explicit "no results" — never infer from memory.
   b) Then describe the part from your own knowledge — ALWAYS include ALL of the following:
      - **Manufacturer**: identify it from the MPN prefix/format (e.g. TI, STMicro, NXP, Vishay...)
      - **Component category**: e.g. op-amp, microcontroller, MOSFET, capacitor, connector, voltage regulator...
      - **Lifecycle status** (from your training knowledge): Active / NRND / Obsolete / etc.
        If the part is known to be NRND or Obsolete, flag it clearly with a ⚠️ warning.
      - What it is and what it does
      - Key specs (voltage, current rating, package, frequency, etc.) if known
      - Typical applications and industries
   c) At the end of your Step 1 reply, ALWAYS add this offer (word it naturally):
      "Would you like me to check live DigiKey pricing, lead time, stock availability,
       and confirmed lifecycle status for [MPN]?"

   STEP 2 (only when user says yes, sure, please, go ahead, or similar):
   Call the lookup_digikey tool with the exact MPN, then present:
   - Manufacturer and DigiKey part number
   - **Lifecycle status** — highlight prominently; if it is NRND, Obsolete, Last Time Buy,
     or Not for New Designs, add a clear warning so the sourcing team is aware
   - Stock available at DigiKey (qty)
   - Lead time in weeks
   - Pricing break table (qty / unit price)
   - Link to the DigiKey product page

10. If the user asks DIRECTLY for DigiKey pricing / lead time / lifecycle (without a prior Step 1),
   you may skip the offer and call lookup_digikey immediately.
{date_context}
"""


def _system_prompt(start_date: str = None, end_date: str = None) -> str:
    today = date.today().isoformat()
    date_ctx = (
        f"\nToday's date is {today}. Use this when the user refers to 'today', 'this week', "
        "'this month', 'yesterday', 'last 7 days', or any other relative date expression. "
        "There is no active date filter — query all available historical data by default. "
        "If the user explicitly specifies a time range, translate it to absolute dates using "
        f"today ({today}) as the reference point and apply it in your SQL query."
    )
    return _BASE_SYSTEM.format(schema=_SCHEMA, date_context=date_ctx)


# ─── SQL tool ─────────────────────────────────────────────────────────────────
_RUN_SQL_TOOL = {
    "name": "run_sql",
    "description": (
        "Execute a SELECT SQL query against the C3_Web SQL Server database "
        "and return the results as a formatted table."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "sql": {"type": "string", "description": "A valid T-SQL SELECT statement"}
        },
        "required": ["sql"],
    },
}


def _execute_sql(sql: str) -> str:
    """Validate and run a SELECT query; return results as a plain-text table."""
    stripped = sql.strip()
    upper = stripped.lstrip("- \n\r").upper()
    if not upper.startswith("SELECT"):
        return "Error: only SELECT queries are permitted."
    try:
        df = run_query(stripped)
        if df.empty:
            return "Query returned no results."
        return df.head(50).to_string(index=False, max_colwidth=60)
    except Exception as exc:
        logger.error("chat_agent SQL error: %s", exc)
        return f"Query error: {exc}"


# ─── DigiKey tool ─────────────────────────────────────────────────────────────
_LOOKUP_DIGIKEY_TOOL = {
    "name": "lookup_digikey",
    "description": (
        "Look up a Manufacturer Part Number (MPN) on DigiKey to retrieve "
        "live pricing breaks (qty/unit price), lead time in weeks, stock availability, "
        "lifecycle status (Active / NRND / Obsolete), manufacturer name, and product URL."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "mpn": {
                "type": "string",
                "description": "The exact Manufacturer Part Number to look up, e.g. 'LM358' or 'STM32F103C8T6'"
            }
        },
        "required": ["mpn"],
    },
}


def _execute_digikey(mpn: str) -> str:
    """Call DigiKey API and return a formatted plain-text result."""
    result = dk.lookup_mpn(mpn.strip())
    return dk.format_result(result)


# ─── Tool dispatcher ──────────────────────────────────────────────────────────
def _dispatch_tool(name: str, inputs: dict) -> str:
    if name == "run_sql":
        return _execute_sql(inputs.get("sql", ""))
    if name == "lookup_digikey":
        return _execute_digikey(inputs.get("mpn", ""))
    return f"Unknown tool: {name}"


# ─── Main function ────────────────────────────────────────────────────────────
def ask_data(display_messages: list, start_date: str = None, end_date: str = None) -> str:
    """
    Send the conversation to Claude, execute any tool calls (SQL or DigiKey), return final answer.

    display_messages: list of {"role": "user"|"assistant", "content": "<text>"}
    start_date / end_date are ignored — the chat has no date restrictions.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return (
            "⚠️ ANTHROPIC_API_KEY is not set in your .env file. "
            "Please add it to enable the AI assistant."
        )

    client = anthropic.Anthropic(api_key=api_key)
    system = _system_prompt(start_date, end_date)

    # Convert display history to API message format.
    # Cap at last 10 messages (5 turns) to prevent unbounded token growth.
    recent = display_messages[-10:] if len(display_messages) > 10 else display_messages
    messages = [{"role": m["role"], "content": m["content"]} for m in recent]

    response = None
    # Agentic loop — keep going until Claude stops calling tools
    for _ in range(15):
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=system,
            tools=[_RUN_SQL_TOOL, _LOOKUP_DIGIKEY_TOOL],
            messages=messages,
        )

        if response.stop_reason != "tool_use":
            break

        # Execute all tool calls in this turn
        tool_results = []
        for block in response.content:
            if block.type == "tool_use":
                result = _dispatch_tool(block.name, block.input)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result,
                })

        # Append assistant response + tool results and loop
        messages.append({"role": "assistant", "content": response.content})
        messages.append({"role": "user", "content": tool_results})

    if response is None:
        return "No response generated."

    # If the loop ended while Claude still wanted to use tools, force a text answer
    if response.stop_reason == "tool_use":
        messages.append({"role": "assistant", "content": response.content})
        # Execute remaining tools so the message history is valid
        tool_results = []
        for block in response.content:
            if block.type == "tool_use":
                result = _dispatch_tool(block.name, block.input)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result,
                })
        messages.append({"role": "user", "content": tool_results})
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=system,
            tools=[],
            messages=messages,
        )

    # Extract final text
    for block in response.content:
        if block.type == "text":
            return block.text

    return "No response generated."
