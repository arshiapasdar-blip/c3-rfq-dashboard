import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from tools.db import run_query, DIRTY_MPN_SQL

DATE_BOUNDS_CRFQ = ""
DATE_BOUNDS_SRFQ = ""

SRFQ_STATUS_LABELS = {
    10: "Sent",
    20: "In Progress",
    40: "Won",
    50: "Responded",
}

SOURCING_STATUS_LABELS = {
    0: "Not Started",
    1: "Initiated",
    10: "Sent to Sourcing",
    20: "In Progress",
    30: "Sourced",
    40: "Quoted",
    50: "Won",
    60: "Closed",
}

RFQ_RESULT_LABELS = {
    10: "Pending",
    12: "No Response",
    14: "Lost / No Quote",
    16: "Declined",
    18: "Cancelled",
    40: "Won",
}


# ─── Search helpers ────────────────────────────────────────────────────────────

def search_customers(query: str, limit: int = 30) -> list:
    if not query or not query.strip():
        return []
    sql = f"""
        SELECT TOP {limit} CustomerName
        FROM Customers
        WHERE CustomerName LIKE %(q)s
        ORDER BY CustomerName
    """
    df = run_query(sql, params={"q": f"%{query.strip()}%"})
    if df.empty:
        return []
    return df["CustomerName"].tolist()


def search_suppliers(query: str, limit: int = 30) -> list:
    if not query or not query.strip():
        return []
    sql = f"""
        SELECT TOP {limit} SupplierName
        FROM Suppliers
        WHERE SupplierName LIKE %(q)s
        ORDER BY SupplierName
    """
    df = run_query(sql, params={"q": f"%{query.strip()}%"})
    if df.empty:
        return []
    return df["SupplierName"].tolist()


def search_mpns(query: str, limit: int = 30) -> list:
    if not query or not query.strip():
        return []
    sql = f"""
        SELECT DISTINCT TOP {limit} LTRIM(RTRIM(Mpn)) AS Mpn
        FROM CustomerRfqParts
        WHERE Mpn LIKE %(q)s
          AND Mpn IS NOT NULL
          AND LEN(LTRIM(RTRIM(Mpn))) > 2
          AND LOWER(LTRIM(RTRIM(Mpn))) NOT IN ({DIRTY_MPN_SQL})
        ORDER BY Mpn
    """
    df = run_query(sql, params={"q": f"%{query.strip()}%"})
    if df.empty:
        return []
    return df["Mpn"].tolist()


# ─── Customer profile ──────────────────────────────────────────────────────────

def get_customer_rfq_raw(customer_name: str) -> pd.DataFrame:
    """Diagnostic: returns all RfqResult values + counts for a customer, no date filter."""
    sql = """
        SELECT r.RfqResult, COUNT(*) AS Count
        FROM CustomerRfqs r
        JOIN Customers c ON r.CustomerId = c.CustomerId
        WHERE r.Deleted = 0
          AND c.CustomerName = %(name)s
        GROUP BY r.RfqResult
        ORDER BY Count DESC
    """
    return run_query(sql, params={"name": customer_name})


def get_customer_kpis(customer_name: str, start_date: str, end_date: str) -> dict:
    sql = f"""
        SELECT
            COUNT(DISTINCT r.RfqId)                                                       AS total_rfqs,
            COUNT(DISTINCT p.RfqPartId)                                                    AS total_parts,
            SUM(CASE WHEN p.QuoteStatus = 30 OR qp.CustomerRfqPartId IS NOT NULL
                     THEN 1 ELSE 0 END)                                                    AS quoted_parts,
            SUM(CASE WHEN p.QuoteStatus = 30 THEN 1 ELSE 0 END)                           AS won_parts,
            MAX(c.Country)                                                                  AS country,
            MAX(COALESCE(u.DisplayName, 'Unassigned'))                                     AS sales_rep
        FROM CustomerRfqs r
        JOIN Customers c ON r.CustomerId = c.CustomerId
        LEFT JOIN Users u ON r.SalesRepId = u.UserId
        LEFT JOIN CustomerRfqParts p ON p.CustomerRfqId = r.RfqId
        LEFT JOIN (SELECT DISTINCT CustomerRfqPartId FROM CustomerQuoteParts) qp
               ON qp.CustomerRfqPartId = p.RfqPartId
        WHERE r.Deleted = 0
          AND c.CustomerName = %(name)s
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {DATE_BOUNDS_CRFQ}
    """
    df = run_query(sql, params={"name": customer_name, "start": start_date, "end": end_date})
    if df.empty:
        return {}
    row = df.iloc[0]
    total_rfqs   = int(row.get("total_rfqs") or 0)
    total_parts  = int(row.get("total_parts") or 0)
    quoted_parts = int(row.get("quoted_parts") or 0)
    won_parts    = int(row.get("won_parts") or 0)
    return {
        "total_rfqs":        total_rfqs,
        "total_parts":       total_parts,
        "quoted_parts":      quoted_parts,
        "won_parts":         won_parts,
        "quoted_rate":       round(quoted_parts / total_parts * 100, 1) if total_parts else 0.0,
        "quoted_won_ratio":  round(won_parts / quoted_parts * 100, 1) if quoted_parts else 0.0,
        "won_rate":          round(won_parts / total_parts * 100, 1) if total_parts else 0.0,
        "country":           str(row.get("country") or ""),
        "sales_rep":         str(row.get("sales_rep") or "Unassigned"),
    }


def get_customer_monthly_trend(customer_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            FORMAT(r.CreatedDate, 'yyyy-MM') AS Month,
            COUNT(r.RfqId)                   AS RfqCount
        FROM CustomerRfqs r
        JOIN Customers c ON r.CustomerId = c.CustomerId
        WHERE r.Deleted = 0
          AND c.CustomerName = %(name)s
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {DATE_BOUNDS_CRFQ}
        GROUP BY FORMAT(r.CreatedDate, 'yyyy-MM')
        ORDER BY Month
    """
    return run_query(sql, params={"name": customer_name, "start": start_date, "end": end_date})


def get_customer_rfq_results(customer_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            CASE
                WHEN p.QuoteStatus = 30               THEN 'Won'
                WHEN qp.CustomerRfqPartId IS NOT NULL THEN 'Quoted'
                WHEN p.QuoteStatus = 10               THEN 'Lost'
                ELSE                                       'Not Quoted'
            END AS Label,
            COUNT(*) AS Count
        FROM CustomerRfqs r
        JOIN Customers c        ON r.CustomerId = c.CustomerId
        JOIN CustomerRfqParts p ON p.CustomerRfqId = r.RfqId
        LEFT JOIN (SELECT DISTINCT CustomerRfqPartId FROM CustomerQuoteParts) qp
               ON qp.CustomerRfqPartId = p.RfqPartId
        WHERE r.Deleted = 0
          AND c.CustomerName = %(name)s
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {DATE_BOUNDS_CRFQ}
        GROUP BY
            CASE
                WHEN p.QuoteStatus = 30               THEN 'Won'
                WHEN qp.CustomerRfqPartId IS NOT NULL THEN 'Quoted'
                WHEN p.QuoteStatus = 10               THEN 'Lost'
                ELSE                                       'Not Quoted'
            END
        ORDER BY Count DESC
    """
    return run_query(sql, params={"name": customer_name, "start": start_date, "end": end_date})


def get_customer_top_mpns(customer_name: str, start_date: str, end_date: str, limit: int = 15) -> pd.DataFrame:
    sql = f"""
        SELECT TOP {limit}
            LTRIM(RTRIM(p.Mpn)) AS Mpn,
            COUNT(*)             AS RequestCount
        FROM CustomerRfqParts p
        JOIN CustomerRfqs r ON p.CustomerRfqId = r.RfqId
        JOIN Customers c ON r.CustomerId = c.CustomerId
        WHERE r.Deleted = 0
          AND c.CustomerName = %(name)s
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {DATE_BOUNDS_CRFQ}
          AND p.Mpn IS NOT NULL
          AND LEN(LTRIM(RTRIM(p.Mpn))) > 2
          AND LOWER(LTRIM(RTRIM(p.Mpn))) NOT IN ({DIRTY_MPN_SQL})
        GROUP BY LTRIM(RTRIM(p.Mpn))
        ORDER BY RequestCount DESC
    """
    return run_query(sql, params={"name": customer_name, "start": start_date, "end": end_date})


# ─── Supplier profile ──────────────────────────────────────────────────────────

def get_supplier_kpis(supplier_name: str, start_date: str, end_date: str) -> dict:
    sql = f"""
        SELECT
            COUNT(sr.RfqId)                                                          AS total_srfqs,
            COUNT(DISTINCT r.RfqId)                                                  AS total_crfqs,
            SUM(CASE WHEN sr.SupplierRfqStatus = 50 OR sr.ResponsePrice IS NOT NULL THEN 1 ELSE 0 END) AS responded,
            SUM(CASE WHEN p.QuoteStatus = 30 THEN 1 ELSE 0 END)                     AS won_parts,
            SUM(ISNULL(sr.SalePrice, 0) * ISNULL(sr.SaleQty, 0))                   AS total_sale_value,
            MAX(CAST(ISNULL(s.ATD, 0) AS INT))                                       AS is_atd,
            MAX(CAST(ISNULL(s.MFR, 0) AS INT))                                       AS is_mfr,
            MAX(CAST(ISNULL(s.BRK, 0) AS INT))                                       AS is_brk,
            MAX(CAST(ISNULL(s.HYB, 0) AS INT))                                       AS is_hyb
        FROM SupplierRfqs sr
        JOIN Suppliers s ON sr.SupplierId = s.SupplierId
        LEFT JOIN CustomerRfqParts p ON sr.CustomerRfqPartId = p.RfqPartId
        LEFT JOIN CustomerRfqs r ON p.CustomerRfqId = r.RfqId
        WHERE s.SupplierName = %(name)s
          AND sr.CreatedDate >= %(start)s
          AND sr.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {DATE_BOUNDS_SRFQ}
    """
    df = run_query(sql, params={"name": supplier_name, "start": start_date, "end": end_date})
    if df.empty:
        return {}
    row = df.iloc[0]
    total = int(row.get("total_srfqs") or 0)
    responded = int(row.get("responded") or 0)
    won_parts = int(row.get("won_parts") or 0)
    return {
        "total_srfqs":      total,
        "total_crfqs":      int(row.get("total_crfqs") or 0),
        "responded":        responded,
        "response_rate":    round(responded / total * 100, 1) if total else 0.0,
        "won_parts":        won_parts,
        "win_rate":         round(won_parts / total * 100, 1) if total else 0.0,
        "total_sale_value": float(pd.to_numeric(row.get("total_sale_value") or 0, errors="coerce") or 0),
        "supplier_types":   [t for t, col in [("ATD", "is_atd"), ("MFR", "is_mfr"), ("BRK", "is_brk"), ("HYB", "is_hyb")]
                             if int(row.get(col) or 0)],
    }


def get_supplier_monthly_trend(supplier_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            FORMAT(sr.CreatedDate, 'yyyy-MM')                                         AS Month,
            COUNT(sr.RfqId)                                                            AS SrfqCount,
            SUM(CASE WHEN sr.SupplierRfqStatus = 50 OR sr.ResponsePrice IS NOT NULL THEN 1 ELSE 0 END) AS RespondedCount
        FROM SupplierRfqs sr
        JOIN Suppliers s ON sr.SupplierId = s.SupplierId
        WHERE s.SupplierName = %(name)s
          AND sr.CreatedDate >= %(start)s
          AND sr.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {DATE_BOUNDS_SRFQ}
        GROUP BY FORMAT(sr.CreatedDate, 'yyyy-MM')
        ORDER BY Month
    """
    return run_query(sql, params={"name": supplier_name, "start": start_date, "end": end_date})


def get_supplier_status_breakdown(supplier_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            CASE p.QuoteStatus
                WHEN 0  THEN 'Not Quoted'
                WHEN 10 THEN 'Lost'
                WHEN 20 THEN 'Quoted'
                WHEN 30 THEN 'Won'
                ELSE        'Other'
            END AS Label,
            COUNT(*) AS Count
        FROM SupplierRfqs sr
        JOIN Suppliers s ON sr.SupplierId = s.SupplierId
        LEFT JOIN CustomerRfqParts p ON sr.CustomerRfqPartId = p.RfqPartId
        WHERE s.SupplierName = %(name)s
          AND sr.CreatedDate >= %(start)s
          AND sr.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {DATE_BOUNDS_SRFQ}
        GROUP BY p.QuoteStatus
        ORDER BY Count DESC
    """
    return run_query(sql, params={"name": supplier_name, "start": start_date, "end": end_date})


# ─── MPN profile ───────────────────────────────────────────────────────────────

def get_mpn_kpis(mpn: str, start_date: str, end_date: str) -> dict:
    sql = f"""
        SELECT
            COUNT(*)                                                                    AS request_count,
            SUM(ISNULL(p.QtyRequested, 0))                                             AS total_qty,
            COUNT(DISTINCT r.CustomerId)                                               AS unique_customers,
            SUM(CASE WHEN p.SourcingStatus >= 30 THEN 1 ELSE 0 END)                   AS sourced_count,
            SUM(CASE WHEN p.QuoteStatus > 0 THEN 1 ELSE 0 END)                        AS quote_count
        FROM CustomerRfqParts p
        JOIN CustomerRfqs r ON p.CustomerRfqId = r.RfqId
        WHERE r.Deleted = 0
          AND LTRIM(RTRIM(p.Mpn)) = %(mpn)s
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {DATE_BOUNDS_CRFQ}
    """
    df = run_query(sql, params={"mpn": mpn, "start": start_date, "end": end_date})
    if df.empty:
        return {}
    row = df.iloc[0]
    total = int(row.get("request_count") or 0)
    sourced = int(row.get("sourced_count") or 0)
    return {
        "request_count": total,
        "total_qty": int(row.get("total_qty") or 0),
        "unique_customers": int(row.get("unique_customers") or 0),
        "sourced_count": sourced,
        "sourced_rate": round(sourced / total * 100, 1) if total else 0.0,
        "quote_count": int(row.get("quote_count") or 0),
    }


def get_mpn_monthly_trend(mpn: str, start_date: str, end_date: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            FORMAT(r.CreatedDate, 'yyyy-MM') AS Month,
            COUNT(*)                          AS RequestCount,
            SUM(ISNULL(p.QtyRequested, 0))   AS TotalQty
        FROM CustomerRfqParts p
        JOIN CustomerRfqs r ON p.CustomerRfqId = r.RfqId
        WHERE r.Deleted = 0
          AND LTRIM(RTRIM(p.Mpn)) = %(mpn)s
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {DATE_BOUNDS_CRFQ}
        GROUP BY FORMAT(r.CreatedDate, 'yyyy-MM')
        ORDER BY Month
    """
    return run_query(sql, params={"mpn": mpn, "start": start_date, "end": end_date})


def get_mpn_top_customers(mpn: str, start_date: str, end_date: str, limit: int = 10) -> pd.DataFrame:
    sql = f"""
        SELECT TOP {limit}
            c.CustomerName,
            COUNT(*)                        AS RequestCount,
            SUM(ISNULL(p.QtyRequested, 0)) AS TotalQty
        FROM CustomerRfqParts p
        JOIN CustomerRfqs r ON p.CustomerRfqId = r.RfqId
        JOIN Customers c ON r.CustomerId = c.CustomerId
        WHERE r.Deleted = 0
          AND LTRIM(RTRIM(p.Mpn)) = %(mpn)s
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {DATE_BOUNDS_CRFQ}
        GROUP BY c.CustomerName
        ORDER BY RequestCount DESC
    """
    return run_query(sql, params={"mpn": mpn, "start": start_date, "end": end_date})


def get_mpn_sourcing_breakdown(mpn: str, start_date: str, end_date: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            CASE p.SourcingStatus
                WHEN 0  THEN 'Not Started'
                WHEN 1  THEN 'Initiated'
                WHEN 10 THEN 'Sent to Sourcing'
                WHEN 20 THEN 'In Progress'
                WHEN 30 THEN 'Sourced'
                WHEN 40 THEN 'Quoted'
                WHEN 50 THEN 'Won'
                WHEN 60 THEN 'Closed'
                ELSE 'Other'
            END AS Label,
            COUNT(*) AS Count
        FROM CustomerRfqParts p
        JOIN CustomerRfqs r ON p.CustomerRfqId = r.RfqId
        WHERE r.Deleted = 0
          AND LTRIM(RTRIM(p.Mpn)) = %(mpn)s
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {DATE_BOUNDS_CRFQ}
        GROUP BY p.SourcingStatus
        ORDER BY Count DESC
    """
    return run_query(sql, params={"mpn": mpn, "start": start_date, "end": end_date})


# ─── Customer extended ─────────────────────────────────────────────────────────

def get_customer_top_suppliers(customer_name: str, start_date: str, end_date: str, limit: int = 15) -> pd.DataFrame:
    sql = f"""
        SELECT TOP {limit}
            s.SupplierName                                                          AS Supplier,
            COUNT(sr.RfqId)                                                         AS SrfqCount,
            SUM(CASE WHEN sr.SupplierRfqStatus = 50 OR sr.ResponsePrice IS NOT NULL THEN 1 ELSE 0 END) AS Responded,
            SUM(CASE WHEN p.QuoteStatus = 30 THEN 1 ELSE 0 END)                    AS Won
        FROM CustomerRfqs r
        JOIN Customers c        ON r.CustomerId = c.CustomerId
        JOIN CustomerRfqParts p ON p.CustomerRfqId = r.RfqId
        JOIN SupplierRfqs sr    ON sr.CustomerRfqPartId = p.RfqPartId
        JOIN Suppliers s        ON sr.SupplierId = s.SupplierId
        WHERE r.Deleted = 0
          AND c.CustomerName = %(name)s
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {DATE_BOUNDS_CRFQ}
        GROUP BY s.SupplierName
        ORDER BY SrfqCount DESC
    """
    return run_query(sql, params={"name": customer_name, "start": start_date, "end": end_date})


def get_customer_manufacturers(customer_name: str, start_date: str, end_date: str, limit: int = 10) -> pd.DataFrame:
    sql = f"""
        SELECT TOP {limit}
            LTRIM(RTRIM(COALESCE(m.MfrStdName, m.MfrName)))  AS Manufacturer,
            COUNT(*)                                           AS SrfqCount
        FROM CustomerRfqs r
        JOIN Customers c        ON r.CustomerId = c.CustomerId
        JOIN CustomerRfqParts p ON p.CustomerRfqId = r.RfqId
        JOIN Mfrs m             ON p.MfrId = m.MfrId
        WHERE r.Deleted = 0
          AND c.CustomerName = %(name)s
          AND p.MfrId IS NOT NULL AND p.MfrId > 0
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {DATE_BOUNDS_CRFQ}
        GROUP BY LTRIM(RTRIM(COALESCE(m.MfrStdName, m.MfrName)))
        ORDER BY SrfqCount DESC
    """
    return run_query(sql, params={"name": customer_name, "start": start_date, "end": end_date})


def get_customer_sales_reps(customer_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            COALESCE(u.DisplayName, 'Unassigned')               AS SalesRep,
            COUNT(r.RfqId)                                       AS RfqCount,
            SUM(CASE WHEN p.QuoteStatus = 30 THEN 1 ELSE 0 END) AS Won
        FROM CustomerRfqs r
        JOIN Customers c        ON r.CustomerId = c.CustomerId
        LEFT JOIN Users u       ON r.SalesRepId = u.UserId
        LEFT JOIN CustomerRfqParts p ON p.CustomerRfqId = r.RfqId
        WHERE r.Deleted = 0
          AND c.CustomerName = %(name)s
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {DATE_BOUNDS_CRFQ}
        GROUP BY u.DisplayName
        ORDER BY RfqCount DESC
    """
    return run_query(sql, params={"name": customer_name, "start": start_date, "end": end_date})


# ─── Supplier extended ─────────────────────────────────────────────────────────

def get_supplier_top_mpns(supplier_name: str, start_date: str, end_date: str, limit: int = 15) -> pd.DataFrame:
    sql = f"""
        SELECT TOP {limit}
            LTRIM(RTRIM(p.Mpn))     AS Mpn,
            COUNT(sr.RfqId)         AS SrfqCount,
            SUM(CASE WHEN sr.SupplierRfqStatus = 50 OR sr.ResponsePrice IS NOT NULL THEN 1 ELSE 0 END) AS Responded
        FROM SupplierRfqs sr
        JOIN Suppliers s            ON sr.SupplierId = s.SupplierId
        JOIN CustomerRfqParts p     ON sr.CustomerRfqPartId = p.RfqPartId
        WHERE s.SupplierName = %(name)s
          AND p.Mpn IS NOT NULL
          AND LEN(LTRIM(RTRIM(p.Mpn))) > 2
          AND LOWER(LTRIM(RTRIM(p.Mpn))) NOT IN ({DIRTY_MPN_SQL})
          AND sr.CreatedDate >= %(start)s
          AND sr.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {DATE_BOUNDS_SRFQ}
        GROUP BY LTRIM(RTRIM(p.Mpn))
        ORDER BY SrfqCount DESC
    """
    return run_query(sql, params={"name": supplier_name, "start": start_date, "end": end_date})


def get_supplier_top_customers(supplier_name: str, start_date: str, end_date: str, limit: int = 10) -> pd.DataFrame:
    sql = f"""
        SELECT TOP {limit}
            c.CustomerName          AS Customer,
            COUNT(sr.RfqId)         AS SrfqCount,
            COUNT(DISTINCT r.RfqId) AS CrfqCount
        FROM SupplierRfqs sr
        JOIN Suppliers s            ON sr.SupplierId = s.SupplierId
        JOIN CustomerRfqParts p     ON sr.CustomerRfqPartId = p.RfqPartId
        JOIN CustomerRfqs r         ON p.CustomerRfqId = r.RfqId
        JOIN Customers c            ON r.CustomerId = c.CustomerId
        WHERE s.SupplierName = %(name)s
          AND r.Deleted = 0
          AND sr.CreatedDate >= %(start)s
          AND sr.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {DATE_BOUNDS_SRFQ}
        GROUP BY c.CustomerName
        ORDER BY SrfqCount DESC
    """
    return run_query(sql, params={"name": supplier_name, "start": start_date, "end": end_date})


def get_supplier_manufacturers(supplier_name: str, start_date: str, end_date: str, limit: int = 10) -> pd.DataFrame:
    sql = f"""
        SELECT TOP {limit}
            LTRIM(RTRIM(COALESCE(m.MfrStdName, m.MfrName)))  AS Manufacturer,
            COUNT(*)                                           AS SrfqCount
        FROM SupplierRfqs sr
        JOIN Suppliers s ON sr.SupplierId = s.SupplierId
        JOIN Mfrs m      ON sr.SupplierMfrId = m.MfrId
        WHERE s.SupplierName = %(name)s
          AND sr.SupplierMfrId IS NOT NULL AND sr.SupplierMfrId > 0
          AND sr.CreatedDate >= %(start)s
          AND sr.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {DATE_BOUNDS_SRFQ}
        GROUP BY LTRIM(RTRIM(COALESCE(m.MfrStdName, m.MfrName)))
        ORDER BY SrfqCount DESC
    """
    return run_query(sql, params={"name": supplier_name, "start": start_date, "end": end_date})


def get_supplier_sales_reps(supplier_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            COALESCE(u.DisplayName, 'Unassigned')               AS SourcingRequestor,
            COUNT(sr.RfqId)                                      AS SrfqCount,
            SUM(CASE WHEN p.QuoteStatus = 30 THEN 1 ELSE 0 END) AS Won
        FROM SupplierRfqs sr
        JOIN Suppliers s            ON sr.SupplierId = s.SupplierId
        JOIN CustomerRfqParts p     ON sr.CustomerRfqPartId = p.RfqPartId
        JOIN CustomerRfqs r         ON p.CustomerRfqId = r.RfqId
        LEFT JOIN Users u           ON sr.SourcingRequestorId = u.UserId
        WHERE s.SupplierName = %(name)s
          AND r.Deleted = 0
          AND sr.CreatedDate >= %(start)s
          AND sr.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {DATE_BOUNDS_SRFQ}
        GROUP BY u.DisplayName
        ORDER BY SrfqCount DESC
    """
    return run_query(sql, params={"name": supplier_name, "start": start_date, "end": end_date})


# ─── MPN extended ──────────────────────────────────────────────────────────────

def get_mpn_top_suppliers(mpn: str, start_date: str, end_date: str, limit: int = 10) -> pd.DataFrame:
    sql = f"""
        SELECT TOP {limit}
            s.SupplierName                                                          AS Supplier,
            COUNT(sr.RfqId)                                                         AS SrfqCount,
            SUM(CASE WHEN sr.SupplierRfqStatus = 50 OR sr.ResponsePrice IS NOT NULL THEN 1 ELSE 0 END) AS Responded,
            SUM(CASE WHEN p.QuoteStatus = 30 THEN 1 ELSE 0 END)                    AS Won
        FROM CustomerRfqParts p
        JOIN CustomerRfqs r     ON p.CustomerRfqId = r.RfqId
        JOIN SupplierRfqs sr    ON sr.CustomerRfqPartId = p.RfqPartId
        JOIN Suppliers s        ON sr.SupplierId = s.SupplierId
        WHERE r.Deleted = 0
          AND LTRIM(RTRIM(p.Mpn)) = %(mpn)s
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {DATE_BOUNDS_CRFQ}
        GROUP BY s.SupplierName
        ORDER BY SrfqCount DESC
    """
    return run_query(sql, params={"mpn": mpn, "start": start_date, "end": end_date})


def get_mpn_manufacturers(mpn: str, start_date: str, end_date: str, limit: int = 10) -> pd.DataFrame:
    sql = f"""
        SELECT TOP {limit}
            s.SupplierName      AS Manufacturer,
            COUNT(sr.RfqId)     AS SrfqCount
        FROM CustomerRfqParts p
        JOIN CustomerRfqs r     ON p.CustomerRfqId = r.RfqId
        JOIN SupplierRfqs sr    ON sr.CustomerRfqPartId = p.RfqPartId
        JOIN Suppliers s        ON sr.SupplierId = s.SupplierId
        WHERE r.Deleted = 0
          AND LTRIM(RTRIM(p.Mpn)) = %(mpn)s
          AND s.MFR = 1
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {DATE_BOUNDS_CRFQ}
        GROUP BY s.SupplierName
        ORDER BY SrfqCount DESC
    """
    return run_query(sql, params={"mpn": mpn, "start": start_date, "end": end_date})


def get_mpn_pricing_trend(mpn: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Supplier offer prices, quoted sale prices, and quoted cost over time for an MPN."""
    sql = f"""
        SELECT
            s.CreatedDate        AS Date,
            'Supplier Offer'     AS PriceType,
            s.ResponsePrice      AS Price,
            s.ResponseQty        AS Qty,
            sup.SupplierName     AS Entity
        FROM SupplierRfqs s
        JOIN Suppliers sup ON sup.SupplierId = s.SupplierId
        WHERE (LTRIM(RTRIM(s.Mpn)) = %(mpn)s OR LTRIM(RTRIM(s.MpnSupplier)) = %(mpn)s)
          AND s.ResponsePrice IS NOT NULL AND s.ResponsePrice > 0
          AND s.CreatedDate >= %(start)s
          AND s.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))

        UNION ALL

        SELECT
            qp.CreatedDate       AS Date,
            'Quoted Sale Price'  AS PriceType,
            qp.SalePrice         AS Price,
            qp.SaleQty           AS Qty,
            c.CustomerName       AS Entity
        FROM CustomerQuoteParts qp
        JOIN CustomerRfqs crfq ON qp.CustomerRfqId = crfq.RfqId
        JOIN Customers c       ON crfq.CustomerId = c.CustomerId
        WHERE crfq.Deleted = 0
          AND (LTRIM(RTRIM(qp.CustomerMpn)) = %(mpn)s OR LTRIM(RTRIM(qp.SupplierMpn)) = %(mpn)s)
          AND qp.SalePrice IS NOT NULL AND qp.SalePrice > 0
          AND qp.CreatedDate >= %(start)s
          AND qp.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))

        UNION ALL

        SELECT
            qp.CreatedDate       AS Date,
            'Quoted Cost'        AS PriceType,
            qp.SupplierPrice     AS Price,
            qp.SaleQty           AS Qty,
            c.CustomerName       AS Entity
        FROM CustomerQuoteParts qp
        JOIN CustomerRfqs crfq ON qp.CustomerRfqId = crfq.RfqId
        JOIN Customers c       ON crfq.CustomerId = c.CustomerId
        WHERE crfq.Deleted = 0
          AND (LTRIM(RTRIM(qp.CustomerMpn)) = %(mpn)s OR LTRIM(RTRIM(qp.SupplierMpn)) = %(mpn)s)
          AND qp.SupplierPrice IS NOT NULL AND qp.SupplierPrice > 0
          AND qp.CreatedDate >= %(start)s
          AND qp.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))

        ORDER BY Date
    """
    return run_query(sql, params={"mpn": mpn, "start": start_date, "end": end_date})


def get_mpn_part_history(mpn: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Unified part history across all source types (CRFQ, SRFQ, CustomerQuote, Excess, StockSales, PriceList)."""
    sql = f"""
        SELECT
            'CRFQ'                  AS Source,
            p.CustomerRfqId         AS RefId,
            p.CreatedDate           AS Date,
            LTRIM(RTRIM(p.Mpn))     AS Mpn,
            p.QtyRequested          AS Qty,
            p.ReferencePrice        AS Price,
            CASE p.QuoteStatus
                WHEN 0  THEN 'Not Quoted'
                WHEN 10 THEN 'Lost'
                WHEN 20 THEN 'Quoted'
                WHEN 30 THEN 'Won'
                ELSE 'Other'
            END                     AS Status,
            u.DisplayName           AS Contact,
            c.CustomerName          AS SourceName
        FROM CustomerRfqParts p
        JOIN CustomerRfqs crfq     ON crfq.RfqId = p.CustomerRfqId
        LEFT JOIN Customers c      ON crfq.CustomerId = c.CustomerId
        LEFT JOIN Users u          ON crfq.SalesRepId = u.UserId
        WHERE crfq.Deleted = 0
          AND LTRIM(RTRIM(p.Mpn)) = %(mpn)s
          AND p.CreatedDate >= %(start)s
          AND p.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))

        UNION ALL

        SELECT
            'SRFQ'                         AS Source,
            sr.RfqId                       AS RefId,
            sr.CreatedDate                 AS Date,
            LTRIM(RTRIM(sr.MpnSupplier))   AS Mpn,
            sr.ResponseQty                 AS Qty,
            sr.ResponsePrice               AS Price,
            CASE sr.SupplierRfqStatus
                WHEN 10 THEN 'Pending'
                WHEN 20 THEN 'In Progress'
                WHEN 30 THEN 'Bid Received'
                WHEN 40 THEN 'Won'
                WHEN 50 THEN 'Responded'
                ELSE 'Other'
            END                            AS Status,
            u.DisplayName                  AS Contact,
            sup.SupplierName               AS SourceName
        FROM SupplierRfqs sr
        LEFT JOIN Suppliers sup    ON sup.SupplierId = sr.SupplierId
        LEFT JOIN Users u          ON sr.SourcingRequestorId = u.UserId
        WHERE (LTRIM(RTRIM(sr.Mpn)) = %(mpn)s OR LTRIM(RTRIM(sr.MpnSupplier)) = %(mpn)s)
          AND sr.CreatedDate >= %(start)s
          AND sr.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))

        UNION ALL

        SELECT
            'Quote'                        AS Source,
            qp.CustomerQuotePartId         AS RefId,
            qp.CreatedDate                 AS Date,
            LTRIM(RTRIM(qp.CustomerMpn))   AS Mpn,
            qp.SaleQty                     AS Qty,
            qp.SalePrice                   AS Price,
            CASE qp.PartStatus
                WHEN 10 THEN 'Quoted'
                WHEN 20 THEN 'Quoted'
                WHEN 30 THEN 'Won'
                ELSE 'Other'
            END                            AS Status,
            u.DisplayName                  AS Contact,
            c.CustomerName                 AS SourceName
        FROM CustomerQuoteParts qp
        LEFT JOIN CustomerRfqs crfq ON qp.CustomerRfqId = crfq.RfqId
        LEFT JOIN Customers c       ON crfq.CustomerId = c.CustomerId
        LEFT JOIN Users u           ON crfq.SalesRepId = u.UserId
        WHERE crfq.Deleted = 0
          AND (LTRIM(RTRIM(qp.CustomerMpn)) = %(mpn)s OR LTRIM(RTRIM(qp.SupplierMpn)) = %(mpn)s)
          AND qp.CreatedDate >= %(start)s
          AND qp.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))

        UNION ALL

        SELECT
            'Excess'                       AS Source,
            elp.Id                         AS RefId,
            el.CreatedAt                   AS Date,
            LTRIM(RTRIM(elp.Mpn))          AS Mpn,
            elp.Qty                        AS Qty,
            elp.TP                         AS Price,
            CASE el.Status
                WHEN 0 THEN 'Open'
                WHEN 1 THEN 'Closed'
                ELSE 'Other'
            END                            AS Status,
            u.DisplayName                  AS Contact,
            sup.SupplierName               AS SourceName
        FROM ExcessListPart elp
        JOIN ExcessList el         ON el.Id = elp.ExcessListId
        LEFT JOIN Suppliers sup    ON sup.SupplierId = el.SupplierId
        LEFT JOIN Users u          ON el.ContactId = u.UserId
        WHERE el.ShowInPartHistory = 1
          AND LTRIM(RTRIM(elp.Mpn)) = %(mpn)s
          AND el.CreatedAt >= %(start)s
          AND el.CreatedAt < DATEADD(day, 1, CAST(%(end)s AS DATE))

        UNION ALL

        SELECT
            'StockSale'                    AS Source,
            ssqp.Id                        AS RefId,
            ssq.CreatedAt                  AS Date,
            LTRIM(RTRIM(ssqp.Mpn))         AS Mpn,
            ssqp.QuotedQty                 AS Qty,
            ssqp.Price                     AS Price,
            'Quoted'                       AS Status,
            u.DisplayName                  AS Contact,
            c.CustomerName                 AS SourceName
        FROM StockSalesQuotePart2 ssqp
        LEFT JOIN StockSalesQuote2 ssq ON ssq.Id = ssqp.QuoteId
        LEFT JOIN Users u              ON ssq.SalesRepId = u.UserId
        LEFT JOIN Customers c          ON ssq.CustomerId = c.CustomerId
        WHERE LTRIM(RTRIM(ssqp.Mpn)) = %(mpn)s
          AND ssq.CreatedAt >= %(start)s
          AND ssq.CreatedAt < DATEADD(day, 1, CAST(%(end)s AS DATE))

        UNION ALL

        SELECT
            'PriceList'                    AS Source,
            plp.Id                         AS RefId,
            pl.CreatedDate                 AS Date,
            LTRIM(RTRIM(plp.Mpn))          AS Mpn,
            plp.Qty                        AS Qty,
            plp.Price                      AS Price,
            'Active'                       AS Status,
            u.DisplayName                  AS Contact,
            sup.SupplierName               AS SourceName
        FROM PriceListPart plp
        LEFT JOIN PriceList pl     ON plp.PriceListId = pl.Id
        LEFT JOIN Users u          ON pl.CreatedById = u.UserId
        LEFT JOIN Suppliers sup    ON pl.SourceId = sup.SupplierId
        WHERE LTRIM(RTRIM(plp.Mpn)) = %(mpn)s
          AND pl.CreatedDate >= %(start)s
          AND pl.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))

        ORDER BY Date DESC
    """
    return run_query(sql, params={"mpn": mpn, "start": start_date, "end": end_date})


def get_mpn_sales_reps(mpn: str, start_date: str, end_date: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            COALESCE(u.DisplayName, 'Unassigned')   AS SalesRep,
            COUNT(p.RfqPartId)                       AS PartCount,
            SUM(ISNULL(p.QtyRequested, 0))           AS TotalQty
        FROM CustomerRfqParts p
        JOIN CustomerRfqs r     ON p.CustomerRfqId = r.RfqId
        LEFT JOIN Users u       ON r.SalesRepId = u.UserId
        WHERE r.Deleted = 0
          AND LTRIM(RTRIM(p.Mpn)) = %(mpn)s
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {DATE_BOUNDS_CRFQ}
        GROUP BY u.DisplayName
        ORDER BY PartCount DESC
    """
    return run_query(sql, params={"mpn": mpn, "start": start_date, "end": end_date})


# ─── Manufacturer profile ─────────────────────────────────────────────────────

def search_manufacturers(query: str, limit: int = 30) -> list:
    if not query or not query.strip():
        return []
    sql = f"""
        SELECT TOP {limit}
            LTRIM(RTRIM(COALESCE(m.MfrStdName, m.MfrName))) AS MfrName
        FROM Mfrs m
        WHERE (m.MfrName LIKE %(q)s OR m.MfrStdName LIKE %(q)s)
        GROUP BY LTRIM(RTRIM(COALESCE(m.MfrStdName, m.MfrName)))
        ORDER BY MfrName
    """
    df = run_query(sql, params={"q": f"%{query.strip()}%"})
    if df.empty:
        return []
    return df["MfrName"].tolist()


def get_mfr_kpis(mfr_name: str, start_date: str, end_date: str) -> dict:
    sql = f"""
        SELECT
            COUNT(DISTINCT p.RfqPartId)                                          AS total_parts,
            COUNT(DISTINCT r.RfqId)                                              AS total_rfqs,
            COUNT(DISTINCT LTRIM(RTRIM(p.Mpn)))                                  AS unique_mpns,
            COUNT(DISTINCT r.CustomerId)                                         AS unique_customers,
            SUM(CASE WHEN p.SourcingStatus >= 30 THEN 1 ELSE 0 END)            AS sourced_count,
            SUM(CASE WHEN p.QuoteStatus = 30 THEN 1 ELSE 0 END)               AS won_count,
            SUM(CASE WHEN qp.CustomerRfqPartId IS NOT NULL THEN 1 ELSE 0 END) AS quoted_count
        FROM CustomerRfqParts p
        JOIN CustomerRfqs r ON p.CustomerRfqId = r.RfqId
        JOIN Mfrs m         ON p.MfrId = m.MfrId
        LEFT JOIN (SELECT DISTINCT CustomerRfqPartId FROM CustomerQuoteParts) qp
               ON qp.CustomerRfqPartId = p.RfqPartId
        WHERE r.Deleted = 0
          AND LTRIM(RTRIM(COALESCE(m.MfrStdName, m.MfrName))) = %(mfr)s
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {DATE_BOUNDS_CRFQ}
    """
    df = run_query(sql, params={"mfr": mfr_name, "start": start_date, "end": end_date})
    if df.empty:
        return {}
    row = df.iloc[0]
    total    = int(row.get("total_parts") or 0)
    sourced  = int(row.get("sourced_count") or 0)
    won      = int(row.get("won_count") or 0)
    quoted   = int(row.get("quoted_count") or 0)
    return {
        "total_parts":      total,
        "total_rfqs":       int(row.get("total_rfqs") or 0),
        "unique_mpns":      int(row.get("unique_mpns") or 0),
        "unique_customers": int(row.get("unique_customers") or 0),
        "sourced_count":    sourced,
        "sourced_rate":     round(sourced / total * 100, 1) if total else 0.0,
        "won_count":        won,
        "win_rate":         round(won / total * 100, 1) if total else 0.0,
        "quoted_count":     quoted,
        "quote_rate":       round(quoted / total * 100, 1) if total else 0.0,
    }


def get_mfr_best_suppliers(mfr_name: str, start_date: str, end_date: str, limit: int = 15) -> pd.DataFrame:
    """Suppliers ranked by success with this manufacturer — the core 'who to buy from' insight."""
    sql = f"""
        SELECT TOP {limit}
            s.SupplierName                                                                    AS Supplier,
            CASE WHEN s.MFR = 1 THEN 'MFR' WHEN s.ATD = 1 THEN 'ATD'
                 WHEN s.BRK = 1 THEN 'BRK' WHEN s.HYB = 1 THEN 'HYB' ELSE '' END           AS Type,
            COUNT(sr.RfqId)                                                                   AS TotalSRFQs,
            SUM(CASE WHEN sr.SupplierRfqStatus IN (30, 50) OR sr.ResponsePrice > 0
                     THEN 1 ELSE 0 END)                                                       AS Responded,
            CAST(ROUND(
                SUM(CASE WHEN sr.SupplierRfqStatus IN (30, 50) OR sr.ResponsePrice > 0
                         THEN 1.0 ELSE 0 END)
                / NULLIF(COUNT(sr.RfqId), 0) * 100, 1) AS DECIMAL(5,1))                      AS ResponseRate,
            SUM(CASE WHEN p.QuoteStatus = 30 THEN 1 ELSE 0 END)                              AS Won,
            CAST(ROUND(
                SUM(CASE WHEN p.QuoteStatus = 30 THEN 1.0 ELSE 0 END)
                / NULLIF(COUNT(sr.RfqId), 0) * 100, 1) AS DECIMAL(5,1))                      AS WinRate,
            AVG(CASE WHEN sr.ResponsePrice > 0 THEN sr.ResponsePrice END)                     AS AvgCost
        FROM SupplierRfqs sr
        JOIN Suppliers s         ON sr.SupplierId = s.SupplierId
        JOIN CustomerRfqParts p  ON sr.CustomerRfqPartId = p.RfqPartId
        JOIN CustomerRfqs r      ON p.CustomerRfqId = r.RfqId
        JOIN Mfrs m              ON p.MfrId = m.MfrId
        WHERE r.Deleted = 0
          AND LTRIM(RTRIM(COALESCE(m.MfrStdName, m.MfrName))) = %(mfr)s
          AND sr.CreatedDate >= %(start)s
          AND sr.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
        GROUP BY s.SupplierName, s.MFR, s.ATD, s.BRK, s.HYB
        ORDER BY Won DESC, ResponseRate DESC, TotalSRFQs DESC
    """
    return run_query(sql, params={"mfr": mfr_name, "start": start_date, "end": end_date})


def get_mfr_top_mpns(mfr_name: str, start_date: str, end_date: str, limit: int = 15) -> pd.DataFrame:
    sql = f"""
        SELECT TOP {limit}
            LTRIM(RTRIM(p.Mpn))                                              AS Mpn,
            COUNT(*)                                                          AS RequestCount,
            SUM(ISNULL(p.QtyRequested, 0))                                   AS TotalQty,
            SUM(CASE WHEN p.QuoteStatus = 30 THEN 1 ELSE 0 END)            AS Won
        FROM CustomerRfqParts p
        JOIN CustomerRfqs r ON p.CustomerRfqId = r.RfqId
        JOIN Mfrs m         ON p.MfrId = m.MfrId
        WHERE r.Deleted = 0
          AND LTRIM(RTRIM(COALESCE(m.MfrStdName, m.MfrName))) = %(mfr)s
          AND p.Mpn IS NOT NULL
          AND LEN(LTRIM(RTRIM(p.Mpn))) > 2
          AND LOWER(LTRIM(RTRIM(p.Mpn))) NOT IN ({DIRTY_MPN_SQL})
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {DATE_BOUNDS_CRFQ}
        GROUP BY LTRIM(RTRIM(p.Mpn))
        ORDER BY RequestCount DESC
    """
    return run_query(sql, params={"mfr": mfr_name, "start": start_date, "end": end_date})


def get_mfr_top_customers(mfr_name: str, start_date: str, end_date: str, limit: int = 10) -> pd.DataFrame:
    sql = f"""
        SELECT TOP {limit}
            c.CustomerName,
            COUNT(DISTINCT p.RfqPartId)                    AS RequestCount,
            SUM(ISNULL(p.QtyRequested, 0))                 AS TotalQty,
            SUM(CASE WHEN p.QuoteStatus = 30 THEN 1 ELSE 0 END) AS Won
        FROM CustomerRfqParts p
        JOIN CustomerRfqs r ON p.CustomerRfqId = r.RfqId
        JOIN Customers c    ON r.CustomerId = c.CustomerId
        JOIN Mfrs m         ON p.MfrId = m.MfrId
        WHERE r.Deleted = 0
          AND LTRIM(RTRIM(COALESCE(m.MfrStdName, m.MfrName))) = %(mfr)s
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {DATE_BOUNDS_CRFQ}
        GROUP BY c.CustomerName
        ORDER BY RequestCount DESC
    """
    return run_query(sql, params={"mfr": mfr_name, "start": start_date, "end": end_date})


def get_mfr_monthly_trend(mfr_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            FORMAT(r.CreatedDate, 'yyyy-MM') AS Month,
            COUNT(p.RfqPartId)               AS RequestCount,
            SUM(ISNULL(p.QtyRequested, 0))   AS TotalQty
        FROM CustomerRfqParts p
        JOIN CustomerRfqs r ON p.CustomerRfqId = r.RfqId
        JOIN Mfrs m         ON p.MfrId = m.MfrId
        WHERE r.Deleted = 0
          AND LTRIM(RTRIM(COALESCE(m.MfrStdName, m.MfrName))) = %(mfr)s
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {DATE_BOUNDS_CRFQ}
        GROUP BY FORMAT(r.CreatedDate, 'yyyy-MM')
        ORDER BY Month
    """
    return run_query(sql, params={"mfr": mfr_name, "start": start_date, "end": end_date})


def get_mfr_sourcing_breakdown(mfr_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            CASE p.SourcingStatus
                WHEN 0  THEN 'Not Started'
                WHEN 1  THEN 'Initiated'
                WHEN 10 THEN 'Sent to Sourcing'
                WHEN 20 THEN 'In Progress'
                WHEN 30 THEN 'Sourced'
                WHEN 40 THEN 'Quoted'
                WHEN 50 THEN 'Won'
                WHEN 60 THEN 'Closed'
                ELSE 'Other'
            END AS Label,
            COUNT(*) AS Count
        FROM CustomerRfqParts p
        JOIN CustomerRfqs r ON p.CustomerRfqId = r.RfqId
        JOIN Mfrs m         ON p.MfrId = m.MfrId
        WHERE r.Deleted = 0
          AND LTRIM(RTRIM(COALESCE(m.MfrStdName, m.MfrName))) = %(mfr)s
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {DATE_BOUNDS_CRFQ}
        GROUP BY p.SourcingStatus
        ORDER BY Count DESC
    """
    return run_query(sql, params={"mfr": mfr_name, "start": start_date, "end": end_date})


def get_mfr_sales_reps(mfr_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            COALESCE(u.DisplayName, 'Unassigned')               AS SalesRep,
            COUNT(DISTINCT p.RfqPartId)                          AS PartCount,
            SUM(CASE WHEN p.QuoteStatus = 30 THEN 1 ELSE 0 END) AS Won,
            SUM(ISNULL(p.QtyRequested, 0))                       AS TotalQty
        FROM CustomerRfqParts p
        JOIN CustomerRfqs r     ON p.CustomerRfqId = r.RfqId
        JOIN Mfrs m             ON p.MfrId = m.MfrId
        LEFT JOIN Users u       ON r.SalesRepId = u.UserId
        WHERE r.Deleted = 0
          AND LTRIM(RTRIM(COALESCE(m.MfrStdName, m.MfrName))) = %(mfr)s
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {DATE_BOUNDS_CRFQ}
        GROUP BY u.DisplayName
        ORDER BY PartCount DESC
    """
    return run_query(sql, params={"mfr": mfr_name, "start": start_date, "end": end_date})
