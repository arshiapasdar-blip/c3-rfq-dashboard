import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import pandas as pd
from tools.db import run_query, DIRTY_MPN_SQL

logger = logging.getLogger(__name__)

# ─── Metric → compatible dimensions ──────────────────────────────────────────
METRIC_DIMENSIONS = {
    # ── Customer-side ──────────────────────────────────────────────────────────
    "CRFQ Count":            ["Month", "Year", "Customer", "Sales Rep", "Country", "RFQ Result"],
    "Unique Customers":      ["Month", "Year", "Country", "Sales Rep", "RFQ Result"],
    "Quoted Parts":          ["Month", "Year", "Customer", "MPN", "Sourcing Status"],
    "Total Parts Requested": ["Month", "Year", "Customer", "MPN", "Sales Rep"],
    "Sale Value":            ["Month", "Year", "Customer", "MPN"],
    "Win Rate (%)":          ["Month", "Year", "Customer", "Sales Rep"],
    # ── Supplier-side ──────────────────────────────────────────────────────────
    "SRFQ Count":            ["Month", "Year", "Supplier", "Supplier Type", "Sales Rep", "MPN"],
    "Responded SRFQs":       ["Month", "Year", "Supplier", "MPN"],
    "Won SRFQs":             ["Month", "Year", "Supplier", "Sales Rep"],
    "Unique Suppliers":      ["Month", "Year", "Supplier Type"],
    "Response Rate (%)":     ["Month", "Year", "Supplier"],
    # ── Parts ──────────────────────────────────────────────────────────────────
    "Sourced Parts":         ["Month", "Year", "MPN", "Supplier", "Sales Rep"],
}

# ─── Date bounds (exclude corrupt rows) ───────────────────────────────────────
_DATE_BOUNDS = ""

# ─── Query registry ───────────────────────────────────────────────────────────
# Each entry: (metric, dimension) -> dict with keys:
#   select  - SELECT clause (always aliases columns as Label, Value)
#   from    - FROM + JOINs
#   where   - base WHERE conditions (no date range, that's added dynamically)
#   group   - GROUP BY expression
#   sort    - "asc" or "desc"
#   date_col - which column to filter on for the date range
#   customer_col - column to apply customer filter (or None)
#   supplier_col - column to apply supplier filter (or None)
#   mpn_col      - column to apply MPN filter (or None)

_R = {}

# ── CRFQ Count ────────────────────────────────────────────────────────────────
_R[("CRFQ Count", "Month")] = dict(
    select="FORMAT(r.CreatedDate,'yyyy-MM') AS Label, COUNT(r.RfqId) AS Value",
    frm="CustomerRfqs r",
    where="r.Deleted=0",
    group="FORMAT(r.CreatedDate,'yyyy-MM')",
    sort="asc", date_col="r.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)
_R[("CRFQ Count", "Year")] = dict(
    select="CAST(YEAR(r.CreatedDate) AS VARCHAR) AS Label, COUNT(r.RfqId) AS Value",
    frm="CustomerRfqs r",
    where="r.Deleted=0",
    group="YEAR(r.CreatedDate)",
    sort="asc", date_col="r.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)
_R[("CRFQ Count", "Customer")] = dict(
    select="c.CustomerName AS Label, COUNT(r.RfqId) AS Value",
    frm="CustomerRfqs r JOIN Customers c ON r.CustomerId=c.CustomerId",
    where="r.Deleted=0",
    group="c.CustomerName",
    sort="desc", date_col="r.CreatedDate",
    customer_col="c.CustomerName", supplier_col=None, mpn_col=None,
)
_R[("CRFQ Count", "Sales Rep")] = dict(
    select="COALESCE(u.DisplayName,'Unassigned') AS Label, COUNT(r.RfqId) AS Value",
    frm="CustomerRfqs r LEFT JOIN Users u ON r.SalesRepId=u.UserId",
    where="r.Deleted=0",
    group="u.DisplayName",
    sort="desc", date_col="r.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)
_R[("CRFQ Count", "Country")] = dict(
    select="c.Country AS Label, COUNT(r.RfqId) AS Value",
    frm="CustomerRfqs r JOIN Customers c ON r.CustomerId=c.CustomerId",
    where=f"r.Deleted=0 AND c.Country IS NOT NULL AND LEN(LTRIM(RTRIM(c.Country)))>2 AND LTRIM(RTRIM(c.Country)) NOT IN ('string','I','6','N/A','n/a')",
    group="c.Country",
    sort="desc", date_col="r.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)
_R[("CRFQ Count", "RFQ Result")] = dict(
    select="""CASE p.QuoteStatus
        WHEN 0  THEN 'Not Quoted'
        WHEN 10 THEN 'Lost'
        WHEN 20 THEN 'Quoted'
        WHEN 30 THEN 'Won'
        ELSE 'Other' END AS Label,
        COUNT(p.RfqPartId) AS Value""",
    frm="CustomerRfqs r JOIN CustomerRfqParts p ON p.CustomerRfqId=r.RfqId",
    where="r.Deleted=0",
    group="p.QuoteStatus",
    sort="desc", date_col="r.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)

# ── Unique Customers ──────────────────────────────────────────────────────────
_R[("Unique Customers", "Month")] = dict(
    select="FORMAT(r.CreatedDate,'yyyy-MM') AS Label, COUNT(DISTINCT r.CustomerId) AS Value",
    frm="CustomerRfqs r",
    where="r.Deleted=0",
    group="FORMAT(r.CreatedDate,'yyyy-MM')",
    sort="asc", date_col="r.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)
_R[("Unique Customers", "Year")] = dict(
    select="CAST(YEAR(r.CreatedDate) AS VARCHAR) AS Label, COUNT(DISTINCT r.CustomerId) AS Value",
    frm="CustomerRfqs r",
    where="r.Deleted=0",
    group="YEAR(r.CreatedDate)",
    sort="asc", date_col="r.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)
_R[("Unique Customers", "Country")] = dict(
    select="c.Country AS Label, COUNT(DISTINCT r.CustomerId) AS Value",
    frm="CustomerRfqs r JOIN Customers c ON r.CustomerId=c.CustomerId",
    where=f"r.Deleted=0 AND c.Country IS NOT NULL AND LEN(LTRIM(RTRIM(c.Country)))>2 AND LTRIM(RTRIM(c.Country)) NOT IN ('string','I','6','N/A','n/a')",
    group="c.Country",
    sort="desc", date_col="r.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)
_R[("Unique Customers", "Sales Rep")] = dict(
    select="COALESCE(u.DisplayName,'Unassigned') AS Label, COUNT(DISTINCT r.CustomerId) AS Value",
    frm="CustomerRfqs r LEFT JOIN Users u ON r.SalesRepId=u.UserId",
    where="r.Deleted=0",
    group="u.DisplayName",
    sort="desc", date_col="r.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)
_R[("Unique Customers", "RFQ Result")] = dict(
    select="""CASE p.QuoteStatus
        WHEN 0  THEN 'Not Quoted'
        WHEN 10 THEN 'Lost'
        WHEN 20 THEN 'Quoted'
        WHEN 30 THEN 'Won'
        ELSE 'Other' END AS Label,
        COUNT(DISTINCT r.CustomerId) AS Value""",
    frm="CustomerRfqs r JOIN CustomerRfqParts p ON p.CustomerRfqId=r.RfqId",
    where="r.Deleted=0",
    group="p.QuoteStatus",
    sort="desc", date_col="r.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)

# ── Total Parts Requested ─────────────────────────────────────────────────────
_R[("Total Parts Requested", "Month")] = dict(
    select="FORMAT(r.CreatedDate,'yyyy-MM') AS Label, COUNT(p.RfqPartId) AS Value",
    frm="CustomerRfqParts p JOIN CustomerRfqs r ON p.CustomerRfqId=r.RfqId",
    where="r.Deleted=0",
    group="FORMAT(r.CreatedDate,'yyyy-MM')",
    sort="asc", date_col="r.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)
_R[("Total Parts Requested", "Year")] = dict(
    select="CAST(YEAR(r.CreatedDate) AS VARCHAR) AS Label, COUNT(p.RfqPartId) AS Value",
    frm="CustomerRfqParts p JOIN CustomerRfqs r ON p.CustomerRfqId=r.RfqId",
    where="r.Deleted=0",
    group="YEAR(r.CreatedDate)",
    sort="asc", date_col="r.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)
_R[("Total Parts Requested", "Customer")] = dict(
    select="c.CustomerName AS Label, COUNT(p.RfqPartId) AS Value",
    frm="CustomerRfqParts p JOIN CustomerRfqs r ON p.CustomerRfqId=r.RfqId JOIN Customers c ON r.CustomerId=c.CustomerId",
    where="r.Deleted=0",
    group="c.CustomerName",
    sort="desc", date_col="r.CreatedDate",
    customer_col="c.CustomerName", supplier_col=None, mpn_col=None,
)
_R[("Total Parts Requested", "MPN")] = dict(
    select=f"LTRIM(RTRIM(p.Mpn)) AS Label, COUNT(p.RfqPartId) AS Value",
    frm="CustomerRfqParts p JOIN CustomerRfqs r ON p.CustomerRfqId=r.RfqId",
    where=f"r.Deleted=0 AND p.Mpn IS NOT NULL AND LEN(LTRIM(RTRIM(p.Mpn)))>2 AND LOWER(LTRIM(RTRIM(p.Mpn))) NOT IN ({DIRTY_MPN_SQL})",
    group="LTRIM(RTRIM(p.Mpn))",
    sort="desc", date_col="r.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col="p.Mpn",
)
_R[("Total Parts Requested", "Sales Rep")] = dict(
    select="COALESCE(u.DisplayName,'Unassigned') AS Label, COUNT(p.RfqPartId) AS Value",
    frm="CustomerRfqParts p JOIN CustomerRfqs r ON p.CustomerRfqId=r.RfqId LEFT JOIN Users u ON r.SalesRepId=u.UserId",
    where="r.Deleted=0",
    group="u.DisplayName",
    sort="desc", date_col="r.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)

# ── Quoted Parts (parts with a CustomerQuoteParts record = actually quoted) ───
_QUOTED_FRM = """CustomerRfqParts p
    JOIN CustomerRfqs r ON p.CustomerRfqId=r.RfqId
    JOIN (SELECT DISTINCT CustomerRfqPartId FROM CustomerQuoteParts) qp ON qp.CustomerRfqPartId=p.RfqPartId"""

_R[("Quoted Parts", "Month")] = dict(
    select="FORMAT(r.CreatedDate,'yyyy-MM') AS Label, COUNT(p.RfqPartId) AS Value",
    frm=_QUOTED_FRM,
    where="r.Deleted=0",
    group="FORMAT(r.CreatedDate,'yyyy-MM')",
    sort="asc", date_col="r.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)
_R[("Quoted Parts", "Year")] = dict(
    select="CAST(YEAR(r.CreatedDate) AS VARCHAR) AS Label, COUNT(p.RfqPartId) AS Value",
    frm=_QUOTED_FRM,
    where="r.Deleted=0",
    group="YEAR(r.CreatedDate)",
    sort="asc", date_col="r.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)
_R[("Quoted Parts", "Customer")] = dict(
    select="c.CustomerName AS Label, COUNT(p.RfqPartId) AS Value",
    frm=_QUOTED_FRM + " JOIN Customers c ON r.CustomerId=c.CustomerId",
    where="r.Deleted=0",
    group="c.CustomerName",
    sort="desc", date_col="r.CreatedDate",
    customer_col="c.CustomerName", supplier_col=None, mpn_col=None,
)
_R[("Quoted Parts", "MPN")] = dict(
    select=f"LTRIM(RTRIM(p.Mpn)) AS Label, COUNT(p.RfqPartId) AS Value",
    frm=_QUOTED_FRM,
    where=f"r.Deleted=0 AND p.Mpn IS NOT NULL AND LEN(LTRIM(RTRIM(p.Mpn)))>2 AND LOWER(LTRIM(RTRIM(p.Mpn))) NOT IN ({DIRTY_MPN_SQL})",
    group="LTRIM(RTRIM(p.Mpn))",
    sort="desc", date_col="r.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col="p.Mpn",
)
_R[("Quoted Parts", "Sourcing Status")] = dict(
    select="""CASE p.SourcingStatus
        WHEN 0 THEN 'Not Started' WHEN 1 THEN 'Initiated'
        WHEN 10 THEN 'Sent to Sourcing' WHEN 20 THEN 'In Progress'
        WHEN 30 THEN 'Sourced' WHEN 40 THEN 'Quoted'
        WHEN 50 THEN 'Won' WHEN 60 THEN 'Closed'
        ELSE CONCAT('Status ',CAST(p.SourcingStatus AS VARCHAR)) END AS Label,
        COUNT(p.RfqPartId) AS Value""",
    frm=_QUOTED_FRM,
    where="r.Deleted=0",
    group="p.SourcingStatus",
    sort="desc", date_col="r.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)

# ── Sale Value ────────────────────────────────────────────────────────────────
_R[("Sale Value", "Month")] = dict(
    select="FORMAT(qp.CreatedDate,'yyyy-MM') AS Label, SUM(ISNULL(qp.SaleQty,0)*ISNULL(qp.SalePrice,0)) AS Value",
    frm="CustomerQuoteParts qp JOIN CustomerRfqs r ON qp.CustomerRfqId=r.RfqId",
    where="r.Deleted=0 AND qp.SalePrice>0",
    group="FORMAT(qp.CreatedDate,'yyyy-MM')",
    sort="asc", date_col="qp.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)
_R[("Sale Value", "Year")] = dict(
    select="CAST(YEAR(qp.CreatedDate) AS VARCHAR) AS Label, SUM(ISNULL(qp.SaleQty,0)*ISNULL(qp.SalePrice,0)) AS Value",
    frm="CustomerQuoteParts qp JOIN CustomerRfqs r ON qp.CustomerRfqId=r.RfqId",
    where="r.Deleted=0 AND qp.SalePrice>0",
    group="YEAR(qp.CreatedDate)",
    sort="asc", date_col="qp.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)
_R[("Sale Value", "Customer")] = dict(
    select="c.CustomerName AS Label, SUM(ISNULL(qp.SaleQty,0)*ISNULL(qp.SalePrice,0)) AS Value",
    frm="CustomerQuoteParts qp JOIN CustomerRfqs r ON qp.CustomerRfqId=r.RfqId JOIN Customers c ON r.CustomerId=c.CustomerId",
    where="r.Deleted=0 AND qp.SalePrice>0",
    group="c.CustomerName",
    sort="desc", date_col="qp.CreatedDate",
    customer_col="c.CustomerName", supplier_col=None, mpn_col=None,
)
_R[("Sale Value", "MPN")] = dict(
    select=f"LTRIM(RTRIM(qp.CustomerMpn)) AS Label, SUM(ISNULL(qp.SaleQty,0)*ISNULL(qp.SalePrice,0)) AS Value",
    frm="CustomerQuoteParts qp JOIN CustomerRfqs r ON qp.CustomerRfqId=r.RfqId",
    where=f"r.Deleted=0 AND qp.SalePrice>0 AND qp.CustomerMpn IS NOT NULL AND LEN(LTRIM(RTRIM(qp.CustomerMpn)))>2 AND LOWER(LTRIM(RTRIM(qp.CustomerMpn))) NOT IN ({DIRTY_MPN_SQL})",
    group="LTRIM(RTRIM(qp.CustomerMpn))",
    sort="desc", date_col="qp.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col="qp.CustomerMpn",
)

# ── SRFQ Count ────────────────────────────────────────────────────────────────
_R[("SRFQ Count", "Month")] = dict(
    select="FORMAT(sr.CreatedDate,'yyyy-MM') AS Label, COUNT(sr.RfqId) AS Value",
    frm="SupplierRfqs sr",
    where="",
    group="FORMAT(sr.CreatedDate,'yyyy-MM')",
    sort="asc", date_col="sr.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)
_R[("SRFQ Count", "Year")] = dict(
    select="CAST(YEAR(sr.CreatedDate) AS VARCHAR) AS Label, COUNT(sr.RfqId) AS Value",
    frm="SupplierRfqs sr",
    where="",
    group="YEAR(sr.CreatedDate)",
    sort="asc", date_col="sr.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)
_R[("SRFQ Count", "Supplier")] = dict(
    select="s.SupplierName AS Label, COUNT(sr.RfqId) AS Value",
    frm="SupplierRfqs sr JOIN Suppliers s ON sr.SupplierId=s.SupplierId",
    where="",
    group="s.SupplierName",
    sort="desc", date_col="sr.CreatedDate",
    customer_col=None, supplier_col="s.SupplierName", mpn_col=None,
)

# ── Responded SRFQs (Status=50 OR has a ResponsePrice) ───────────────────────
_RESPONDED_WHERE = "(sr.SupplierRfqStatus=50 OR sr.ResponsePrice IS NOT NULL)"

_R[("Responded SRFQs", "Month")] = dict(
    select="FORMAT(sr.CreatedDate,'yyyy-MM') AS Label, COUNT(sr.RfqId) AS Value",
    frm="SupplierRfqs sr",
    where=_RESPONDED_WHERE,
    group="FORMAT(sr.CreatedDate,'yyyy-MM')",
    sort="asc", date_col="sr.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)
_R[("Responded SRFQs", "Year")] = dict(
    select="CAST(YEAR(sr.CreatedDate) AS VARCHAR) AS Label, COUNT(sr.RfqId) AS Value",
    frm="SupplierRfqs sr",
    where=_RESPONDED_WHERE,
    group="YEAR(sr.CreatedDate)",
    sort="asc", date_col="sr.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)
_R[("Responded SRFQs", "Supplier")] = dict(
    select="s.SupplierName AS Label, COUNT(sr.RfqId) AS Value",
    frm="SupplierRfqs sr JOIN Suppliers s ON sr.SupplierId=s.SupplierId",
    where=_RESPONDED_WHERE,
    group="s.SupplierName",
    sort="desc", date_col="sr.CreatedDate",
    customer_col=None, supplier_col="s.SupplierName", mpn_col=None,
)

# ── Won SRFQs (lines where CustomerRfqParts.QuoteStatus=30) ──────────────────
_R[("Won SRFQs", "Month")] = dict(
    select="FORMAT(sr.CreatedDate,'yyyy-MM') AS Label, COUNT(sr.RfqId) AS Value",
    frm="SupplierRfqs sr JOIN CustomerRfqParts crfqp ON sr.CustomerRfqPartId=crfqp.RfqPartId",
    where="crfqp.QuoteStatus=30",
    group="FORMAT(sr.CreatedDate,'yyyy-MM')",
    sort="asc", date_col="sr.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)
_R[("Won SRFQs", "Year")] = dict(
    select="CAST(YEAR(sr.CreatedDate) AS VARCHAR) AS Label, COUNT(sr.RfqId) AS Value",
    frm="SupplierRfqs sr JOIN CustomerRfqParts crfqp ON sr.CustomerRfqPartId=crfqp.RfqPartId",
    where="crfqp.QuoteStatus=30",
    group="YEAR(sr.CreatedDate)",
    sort="asc", date_col="sr.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)
_R[("Won SRFQs", "Supplier")] = dict(
    select="s.SupplierName AS Label, COUNT(sr.RfqId) AS Value",
    frm="SupplierRfqs sr JOIN Suppliers s ON sr.SupplierId=s.SupplierId JOIN CustomerRfqParts crfqp ON sr.CustomerRfqPartId=crfqp.RfqPartId",
    where="crfqp.QuoteStatus=30",
    group="s.SupplierName",
    sort="desc", date_col="sr.CreatedDate",
    customer_col=None, supplier_col="s.SupplierName", mpn_col=None,
)

# ── Unique Suppliers ──────────────────────────────────────────────────────────
_R[("Unique Suppliers", "Month")] = dict(
    select="FORMAT(sr.CreatedDate,'yyyy-MM') AS Label, COUNT(DISTINCT sr.SupplierId) AS Value",
    frm="SupplierRfqs sr",
    where="",
    group="FORMAT(sr.CreatedDate,'yyyy-MM')",
    sort="asc", date_col="sr.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)
_R[("Unique Suppliers", "Year")] = dict(
    select="CAST(YEAR(sr.CreatedDate) AS VARCHAR) AS Label, COUNT(DISTINCT sr.SupplierId) AS Value",
    frm="SupplierRfqs sr",
    where="",
    group="YEAR(sr.CreatedDate)",
    sort="asc", date_col="sr.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)

# ── SRFQ Count — new dimensions ───────────────────────────────────────────────
_R[("SRFQ Count", "Sales Rep")] = dict(
    select="COALESCE(u.DisplayName,'Unassigned') AS Label, COUNT(sr.RfqId) AS Value",
    frm="""SupplierRfqs sr
           JOIN CustomerRfqParts crfqp ON sr.CustomerRfqPartId = crfqp.RfqPartId
           JOIN CustomerRfqs crfq ON crfqp.CustomerRfqId = crfq.RfqId
           LEFT JOIN Users u ON crfq.SalesRepId = u.UserId""",
    where="crfq.Deleted=0",
    group="u.DisplayName",
    sort="desc", date_col="sr.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)
_R[("SRFQ Count", "MPN")] = dict(
    select=f"LTRIM(RTRIM(crfqp.Mpn)) AS Label, COUNT(sr.RfqId) AS Value",
    frm="""SupplierRfqs sr
           JOIN CustomerRfqParts crfqp ON sr.CustomerRfqPartId = crfqp.RfqPartId""",
    where=f"crfqp.Mpn IS NOT NULL AND LEN(LTRIM(RTRIM(crfqp.Mpn)))>2 AND LOWER(LTRIM(RTRIM(crfqp.Mpn))) NOT IN ({DIRTY_MPN_SQL})",
    group="LTRIM(RTRIM(crfqp.Mpn))",
    sort="desc", date_col="sr.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col="crfqp.Mpn",
)

# ── Responded SRFQs — new dimensions ─────────────────────────────────────────
_R[("Responded SRFQs", "MPN")] = dict(
    select=f"LTRIM(RTRIM(crfqp.Mpn)) AS Label, COUNT(sr.RfqId) AS Value",
    frm="""SupplierRfqs sr
           JOIN CustomerRfqParts crfqp ON sr.CustomerRfqPartId = crfqp.RfqPartId""",
    where=f"(sr.SupplierRfqStatus=50 OR sr.ResponsePrice IS NOT NULL) AND crfqp.Mpn IS NOT NULL AND LEN(LTRIM(RTRIM(crfqp.Mpn)))>2 AND LOWER(LTRIM(RTRIM(crfqp.Mpn))) NOT IN ({DIRTY_MPN_SQL})",
    group="LTRIM(RTRIM(crfqp.Mpn))",
    sort="desc", date_col="sr.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col="crfqp.Mpn",
)

# ── Won SRFQs — new dimensions ────────────────────────────────────────────────
_R[("Won SRFQs", "Sales Rep")] = dict(
    select="COALESCE(u.DisplayName,'Unassigned') AS Label, COUNT(sr.RfqId) AS Value",
    frm="""SupplierRfqs sr
           JOIN CustomerRfqParts crfqp ON sr.CustomerRfqPartId = crfqp.RfqPartId
           JOIN CustomerRfqs crfq ON crfqp.CustomerRfqId = crfq.RfqId
           LEFT JOIN Users u ON crfq.SalesRepId = u.UserId""",
    where="crfqp.QuoteStatus=30 AND crfq.Deleted=0",
    group="u.DisplayName",
    sort="desc", date_col="sr.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)

# ── Sourced Parts (SourcingStatus >= 30) ──────────────────────────────────────
_R[("Sourced Parts", "Month")] = dict(
    select="FORMAT(r.CreatedDate,'yyyy-MM') AS Label, COUNT(p.RfqPartId) AS Value",
    frm="CustomerRfqParts p JOIN CustomerRfqs r ON p.CustomerRfqId=r.RfqId",
    where="r.Deleted=0 AND p.SourcingStatus >= 30",
    group="FORMAT(r.CreatedDate,'yyyy-MM')",
    sort="asc", date_col="r.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)
_R[("Sourced Parts", "Year")] = dict(
    select="CAST(YEAR(r.CreatedDate) AS VARCHAR) AS Label, COUNT(p.RfqPartId) AS Value",
    frm="CustomerRfqParts p JOIN CustomerRfqs r ON p.CustomerRfqId=r.RfqId",
    where="r.Deleted=0 AND p.SourcingStatus >= 30",
    group="YEAR(r.CreatedDate)",
    sort="asc", date_col="r.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)
_R[("Sourced Parts", "MPN")] = dict(
    select=f"LTRIM(RTRIM(p.Mpn)) AS Label, COUNT(p.RfqPartId) AS Value",
    frm="CustomerRfqParts p JOIN CustomerRfqs r ON p.CustomerRfqId=r.RfqId",
    where=f"r.Deleted=0 AND p.SourcingStatus >= 30 AND p.Mpn IS NOT NULL AND LEN(LTRIM(RTRIM(p.Mpn)))>2 AND LOWER(LTRIM(RTRIM(p.Mpn))) NOT IN ({DIRTY_MPN_SQL})",
    group="LTRIM(RTRIM(p.Mpn))",
    sort="desc", date_col="r.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col="p.Mpn",
)
_R[("Sourced Parts", "Supplier")] = dict(
    select="s.SupplierName AS Label, COUNT(DISTINCT p.RfqPartId) AS Value",
    frm="""CustomerRfqParts p
           JOIN CustomerRfqs r ON p.CustomerRfqId=r.RfqId
           JOIN SupplierRfqs sr ON sr.CustomerRfqPartId=p.RfqPartId
           JOIN Suppliers s ON sr.SupplierId=s.SupplierId""",
    where="r.Deleted=0 AND p.SourcingStatus >= 30",
    group="s.SupplierName",
    sort="desc", date_col="r.CreatedDate",
    customer_col=None, supplier_col="s.SupplierName", mpn_col=None,
)
_R[("Sourced Parts", "Sales Rep")] = dict(
    select="COALESCE(u.DisplayName,'Unassigned') AS Label, COUNT(p.RfqPartId) AS Value",
    frm="CustomerRfqParts p JOIN CustomerRfqs r ON p.CustomerRfqId=r.RfqId LEFT JOIN Users u ON r.SalesRepId=u.UserId",
    where="r.Deleted=0 AND p.SourcingStatus >= 30",
    group="u.DisplayName",
    sort="desc", date_col="r.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)

# ── Supplier Type — special: these use a UNION ALL pattern, handled separately
_SUPPLIER_TYPE_METRICS = {"SRFQ Count", "Unique Suppliers"}

# ── Computed metrics (ratio calculations requiring two queries) ────────────────
COMPUTED_METRICS = {"Win Rate (%)", "Response Rate (%)"}

QUERY_REGISTRY = _R


def _build_supplier_type_query(metric: str, start_date: str, end_date: str,
                                filter_supplier: str) -> pd.DataFrame:
    """UNION ALL query for Supplier Type dimension."""
    type_map = {"ATD": "s.ATD", "MFR": "s.MFR", "BRK": "s.BRK", "HYB": "s.HYB"}

    if metric == "SRFQ Count":
        measure = "COUNT(sr.RfqId)"
    else:  # Unique Suppliers
        measure = "COUNT(DISTINCT sr.SupplierId)"

    supplier_filter = ""
    params = {"start": start_date, "end": end_date}
    if filter_supplier.strip():
        supplier_filter = "AND s.SupplierName LIKE %(supplier_filter)s"
        params["supplier_filter"] = f"%{filter_supplier.strip()}%"

    parts = []
    for label, col in type_map.items():
        parts.append(f"""
            SELECT '{label}' AS Label, {measure} AS Value
            FROM SupplierRfqs sr JOIN Suppliers s ON sr.SupplierId=s.SupplierId
            WHERE ISNULL({col},0)=1
              AND sr.CreatedDate >= %(start)s
              AND sr.CreatedDate < DATEADD(day,1,CAST(%(end)s AS DATE))
                           {supplier_filter}
        """)

    sql = " UNION ALL ".join(parts) + " ORDER BY Value DESC"
    df = run_query(sql, params=params)
    if not df.empty:
        df = df[df["Value"] > 0]
    return df


def run_report(
    metric: str,
    dimension: str,
    start_date: str,
    end_date: str,
    top_n: int = 20,
    filter_customer: str = "",
    filter_supplier: str = "",
    filter_mpn: str = "",
    hub_sql: str = "",
) -> pd.DataFrame:
    """
    Builds and runs a dynamic SQL query.
    Returns DataFrame with columns: Label, Value (+ formatted display Value).
    Returns empty DataFrame on any error or unknown combination.
    """
    # Special case: Supplier Type uses UNION ALL
    if dimension == "Supplier Type":
        return _build_supplier_type_query(metric, start_date, end_date, filter_supplier)

    key = (metric, dimension)
    if key not in QUERY_REGISTRY:
        logger.warning("Unknown metric/dimension combo: %s / %s", metric, dimension)
        return pd.DataFrame()

    tmpl = QUERY_REGISTRY[key]

    # Build WHERE clause
    where_parts = [tmpl["where"]]
    params: dict = {"start": start_date, "end": end_date}

    # Date range on the appropriate column
    date_col = tmpl["date_col"]
    where_parts.append(f"{date_col} >= %(start)s")
    where_parts.append(f"{date_col} < DATEADD(day,1,CAST(%(end)s AS DATE))")

    # Optional text filters
    if filter_customer.strip() and tmpl["customer_col"]:
        where_parts.append(f"{tmpl['customer_col']} LIKE %(cust_f)s")
        params["cust_f"] = f"%{filter_customer.strip()}%"

    if filter_supplier.strip() and tmpl["supplier_col"]:
        where_parts.append(f"{tmpl['supplier_col']} LIKE %(supp_f)s")
        params["supp_f"] = f"%{filter_supplier.strip()}%"

    if filter_mpn.strip() and tmpl["mpn_col"]:
        where_parts.append(f"{tmpl['mpn_col']} LIKE %(mpn_f)s")
        params["mpn_f"] = f"%{filter_mpn.strip()}%"

    if hub_sql.strip():
        # hub_sql starts with "AND ..." — strip the AND to fit into where_parts
        where_parts.append(hub_sql.strip().removeprefix("AND ").strip())

    where_clause = " AND ".join(f"({w})" for w in where_parts if w.strip())
    order_clause = "Value DESC" if tmpl["sort"] == "desc" else "Label ASC"
    top_clause = f"TOP {top_n} " if top_n else ""

    sql = f"""
        SELECT {top_clause}{tmpl['select']}
        FROM {tmpl['frm']}
        WHERE {where_clause}
        GROUP BY {tmpl['group']}
        ORDER BY {order_clause}
    """

    df = run_query(sql, params=params)
    if not df.empty and "Value" in df.columns:
        df["Value"] = pd.to_numeric(df["Value"], errors="coerce").fillna(0)
    return df


# ─── Win Rate numerator registry (won part lines, QuoteStatus=30) ─────────────
_WR_NUM = {}
_WR_NUM[("Win Rate (%)", "Month")] = dict(
    select="FORMAT(r.CreatedDate,'yyyy-MM') AS Label, COUNT(p.RfqPartId) AS Value",
    frm="CustomerRfqs r JOIN CustomerRfqParts p ON p.CustomerRfqId=r.RfqId",
    where="r.Deleted=0 AND p.QuoteStatus=30",
    group="FORMAT(r.CreatedDate,'yyyy-MM')", sort="asc", date_col="r.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)
_WR_NUM[("Win Rate (%)", "Year")] = dict(
    select="CAST(YEAR(r.CreatedDate) AS VARCHAR) AS Label, COUNT(p.RfqPartId) AS Value",
    frm="CustomerRfqs r JOIN CustomerRfqParts p ON p.CustomerRfqId=r.RfqId",
    where="r.Deleted=0 AND p.QuoteStatus=30",
    group="YEAR(r.CreatedDate)", sort="asc", date_col="r.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)
_WR_NUM[("Win Rate (%)", "Customer")] = dict(
    select="c.CustomerName AS Label, COUNT(p.RfqPartId) AS Value",
    frm="CustomerRfqs r JOIN Customers c ON r.CustomerId=c.CustomerId JOIN CustomerRfqParts p ON p.CustomerRfqId=r.RfqId",
    where="r.Deleted=0 AND p.QuoteStatus=30",
    group="c.CustomerName", sort="desc", date_col="r.CreatedDate",
    customer_col="c.CustomerName", supplier_col=None, mpn_col=None,
)
_WR_NUM[("Win Rate (%)", "Sales Rep")] = dict(
    select="COALESCE(u.DisplayName,'Unassigned') AS Label, COUNT(p.RfqPartId) AS Value",
    frm="CustomerRfqs r LEFT JOIN Users u ON r.SalesRepId=u.UserId JOIN CustomerRfqParts p ON p.CustomerRfqId=r.RfqId",
    where="r.Deleted=0 AND p.QuoteStatus=30",
    group="u.DisplayName", sort="desc", date_col="r.CreatedDate",
    customer_col=None, supplier_col=None, mpn_col=None,
)


def _run_numerator(tmpl_key: tuple, start_date: str, end_date: str, top_n: int,
                   filter_customer: str, filter_supplier: str, filter_mpn: str,
                   hub_sql: str = "") -> pd.DataFrame:
    """Run a query from the Win Rate numerator registry using the same builder as run_report."""
    if tmpl_key not in _WR_NUM:
        return pd.DataFrame()
    tmpl = _WR_NUM[tmpl_key]
    where_parts = [tmpl["where"]]
    params: dict = {"start": start_date, "end": end_date}
    date_col = tmpl["date_col"]
    where_parts.append(f"{date_col} >= %(start)s")
    where_parts.append(f"{date_col} < DATEADD(day,1,CAST(%(end)s AS DATE))")
    if filter_customer.strip() and tmpl["customer_col"]:
        where_parts.append(f"{tmpl['customer_col']} LIKE %(cust_f)s")
        params["cust_f"] = f"%{filter_customer.strip()}%"
    if hub_sql.strip():
        where_parts.append(hub_sql.strip().removeprefix("AND ").strip())
    where_clause = " AND ".join(f"({w})" for w in where_parts if w.strip())
    order_clause = "Value DESC" if tmpl["sort"] == "desc" else "Label ASC"
    top_clause = f"TOP {top_n} " if top_n else ""
    sql = f"""
        SELECT {top_clause}{tmpl['select']}
        FROM {tmpl['frm']}
        WHERE {where_clause}
        GROUP BY {tmpl['group']}
        ORDER BY {order_clause}
    """
    df = run_query(sql, params=params)
    if not df.empty and "Value" in df.columns:
        df["Value"] = pd.to_numeric(df["Value"], errors="coerce").fillna(0)
    return df


def run_computed_report(
    metric: str,
    dimension: str,
    start_date: str,
    end_date: str,
    top_n: int = 20,
    filter_customer: str = "",
    filter_supplier: str = "",
    filter_mpn: str = "",
    hub_sql: str = "",
) -> pd.DataFrame:
    """
    Computes ratio metrics that require two queries.
    Win Rate (%)     = Won Part Lines / Total Part Lines × 100
    Response Rate (%) = Responded SRFQs / Total SRFQs × 100
    Returns DataFrame with Label, Value (float %).
    """
    if metric == "Win Rate (%)":
        df_num = _run_numerator(
            ("Win Rate (%)", dimension), start_date, end_date, top_n,
            filter_customer, filter_supplier, filter_mpn, hub_sql,
        )
        df_den = run_report(
            "Total Parts Requested", dimension, start_date, end_date, top_n,
            filter_customer, filter_supplier, filter_mpn, hub_sql,
        )
    elif metric == "Response Rate (%)":
        df_num = run_report(
            "Responded SRFQs", dimension, start_date, end_date, top_n,
            filter_customer, filter_supplier, filter_mpn, hub_sql,
        )
        df_den = run_report(
            "SRFQ Count", dimension, start_date, end_date, top_n,
            filter_customer, filter_supplier, filter_mpn, hub_sql,
        )
    else:
        logger.warning("run_computed_report called with unknown metric: %s", metric)
        return pd.DataFrame()

    if df_num.empty or df_den.empty:
        return pd.DataFrame()

    merged = df_den.merge(df_num, on="Label", suffixes=("_den", "_num"))
    merged["Value"] = merged.apply(
        lambda row: round(row["Value_num"] / row["Value_den"] * 100, 1)
        if row["Value_den"] > 0 else 0.0,
        axis=1,
    )
    result = merged[["Label", "Value"]].sort_values("Value", ascending=False)
    if top_n:
        result = result.head(top_n)
    return result.reset_index(drop=True)


def run_any_report(
    metric: str,
    dimension: str,
    start_date: str,
    end_date: str,
    top_n: int = 20,
    filter_customer: str = "",
    filter_supplier: str = "",
    filter_mpn: str = "",
    hub_sql: str = "",
) -> pd.DataFrame:
    """Unified dispatcher — use this in the UI instead of calling run_report directly."""
    if metric in COMPUTED_METRICS:
        return run_computed_report(
            metric, dimension, start_date, end_date, top_n,
            filter_customer, filter_supplier, filter_mpn, hub_sql,
        )
    return run_report(
        metric, dimension, start_date, end_date, top_n,
        filter_customer, filter_supplier, filter_mpn, hub_sql,
    )
