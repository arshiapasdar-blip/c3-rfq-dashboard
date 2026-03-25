import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from tools.db import run_query, DIRTY_MPN_SQL

CALENDAR_MONTHS = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr",
    5: "May", 6: "Jun", 7: "Jul", 8: "Aug",
    9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}

SRFQ_DATE_BOUNDS = ""
CRFQP_DATE_BOUNDS = ""

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


def get_srfq_kpis(start_date: str, end_date: str) -> dict:
    sql = f"""
        SELECT
            COUNT(*)                                                AS total_srfqs,
            SUM(CASE WHEN SupplierRfqStatus = 50 OR ResponsePrice IS NOT NULL THEN 1 ELSE 0 END) AS responded_count
        FROM SupplierRfqs sr
        WHERE sr.CreatedDate >= %(start)s
          AND sr.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {SRFQ_DATE_BOUNDS}
    """
    df = run_query(sql, params={"start": start_date, "end": end_date})
    if df.empty:
        return {"total_srfqs": 0, "response_rate": 0.0}
    row = df.iloc[0]
    total = int(row.get("total_srfqs", 0) or 0)
    responded = int(row.get("responded_count", 0) or 0)
    rate = (responded / total * 100) if total > 0 else 0.0
    return {
        "total_srfqs": total,
        "response_rate": round(rate, 1),
    }


def get_top_supplier_value(start_date: str, end_date: str) -> tuple:
    sql = f"""
        SELECT TOP 1
            s.SupplierName,
            SUM(ISNULL(sr.SalePrice, 0) * ISNULL(sr.SaleQty, 0)) AS TotalValue
        FROM SupplierRfqs sr
        JOIN Suppliers s            ON sr.SupplierId = s.SupplierId
        JOIN CustomerRfqParts p     ON sr.CustomerRfqPartId = p.RfqPartId
        WHERE p.QuoteStatus = 30
          AND sr.SalePrice IS NOT NULL
          AND sr.SaleQty IS NOT NULL
          AND sr.CreatedDate >= %(start)s
          AND sr.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {SRFQ_DATE_BOUNDS}
        GROUP BY s.SupplierName
        ORDER BY TotalValue DESC
    """
    df = run_query(sql, params={"start": start_date, "end": end_date})
    if df.empty:
        return ("N/A", 0.0)
    return (str(df.iloc[0]["SupplierName"]), float(df.iloc[0]["TotalValue"] or 0))


def get_top_suppliers(start_date: str, end_date: str, limit: int = 10) -> pd.DataFrame:
    sql = f"""
        SELECT TOP {limit}
            s.SupplierName,
            COUNT(sr.RfqId)  AS SrfqCount,
            SUM(CASE WHEN sr.SupplierRfqStatus = 50 OR sr.ResponsePrice IS NOT NULL THEN 1 ELSE 0 END) AS Responded
        FROM SupplierRfqs sr
        JOIN Suppliers s ON sr.SupplierId = s.SupplierId
        WHERE sr.CreatedDate >= %(start)s
          AND sr.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {SRFQ_DATE_BOUNDS}
        GROUP BY s.SupplierName
        ORDER BY SrfqCount DESC
    """
    return run_query(sql, params={"start": start_date, "end": end_date})


def get_supplier_response_rates(start_date: str, end_date: str, limit: int = 20) -> pd.DataFrame:
    sql = f"""
        SELECT TOP {limit}
            s.SupplierName,
            COUNT(sr.RfqId)                                              AS TotalSent,
            SUM(CASE WHEN sr.SupplierRfqStatus = 50 OR sr.ResponsePrice IS NOT NULL THEN 1 ELSE 0 END) AS TotalResponded
        FROM SupplierRfqs sr
        JOIN Suppliers s ON sr.SupplierId = s.SupplierId
        WHERE sr.CreatedDate >= %(start)s
          AND sr.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {SRFQ_DATE_BOUNDS}
        GROUP BY s.SupplierName
        ORDER BY TotalSent DESC
    """
    df = run_query(sql, params={"start": start_date, "end": end_date})
    if not df.empty:
        df["NoResponse"] = df["TotalSent"] - df["TotalResponded"]
        df["ResponseRate"] = (
            df["TotalResponded"] / df["TotalSent"].replace(0, float("nan")) * 100
        ).round(1).fillna(0)
    return df


def get_monthly_srfq_trend_range(start_date: str, end_date: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            FORMAT(CreatedDate, 'yyyy-MM') AS Period,
            COUNT(*) AS SrfqCount,
            SUM(CASE WHEN SupplierRfqStatus = 50 OR ResponsePrice IS NOT NULL THEN 1 ELSE 0 END) AS RespondedCount
        FROM SupplierRfqs
        WHERE CreatedDate >= %(start)s
          AND CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
        GROUP BY FORMAT(CreatedDate, 'yyyy-MM')
        ORDER BY Period
    """
    df = run_query(sql, params={"start": start_date, "end": end_date})
    if not df.empty:
        df["SrfqCount"] = df["SrfqCount"].astype(int)
        df["RespondedCount"] = df["RespondedCount"].astype(int)
    return df


def get_monthly_srfq_trend(year: int) -> pd.DataFrame:
    sql = """
        SELECT
            MONTH(CreatedDate) AS Month,
            COUNT(*)           AS SrfqCount,
            SUM(CASE WHEN SupplierRfqStatus = 50 OR ResponsePrice IS NOT NULL THEN 1 ELSE 0 END) AS RespondedCount
        FROM SupplierRfqs
        WHERE YEAR(CreatedDate) = %(year)s
        GROUP BY MONTH(CreatedDate)
        ORDER BY Month
    """
    df = run_query(sql, params={"year": year})
    if df.empty:
        return df
    all_months = pd.DataFrame({"Month": list(range(1, 13))})
    df = all_months.merge(df, on="Month", how="left").fillna(0)
    df["MonthLabel"] = df["Month"].map(CALENDAR_MONTHS)
    df["SrfqCount"] = df["SrfqCount"].astype(int)
    df["RespondedCount"] = df["RespondedCount"].astype(int)
    return df


def get_sourcing_status_breakdown(start_date: str, end_date: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            p.SourcingStatus,
            COUNT(*) AS Count
        FROM CustomerRfqParts p
        JOIN CustomerRfqs r ON p.CustomerRfqId = r.RfqId
        WHERE r.Deleted = 0
          {CRFQP_DATE_BOUNDS}
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
        GROUP BY p.SourcingStatus
        ORDER BY Count DESC
    """
    df = run_query(sql, params={"start": start_date, "end": end_date})
    if not df.empty and "SourcingStatus" in df.columns:
        df["StatusLabel"] = df["SourcingStatus"].map(
            lambda x: SOURCING_STATUS_LABELS.get(int(x), f"Status {x}") if x is not None else "Unknown"
        )
    return df


def get_top_sourced_mpns(start_date: str, end_date: str, limit: int = 20) -> pd.DataFrame:
    sql = f"""
        SELECT TOP {limit}
            LTRIM(RTRIM(p.Mpn)) AS Mpn,
            COUNT(*) AS SrfqCount
        FROM CustomerRfqParts p
        JOIN CustomerRfqs r ON p.CustomerRfqId = r.RfqId
        WHERE r.Deleted = 0
          {CRFQP_DATE_BOUNDS}
          AND p.SourcingStatus >= 30
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          AND p.Mpn IS NOT NULL
          AND LEN(LTRIM(RTRIM(p.Mpn))) > 2
          AND LOWER(LTRIM(RTRIM(p.Mpn))) NOT IN ({DIRTY_MPN_SQL})
        GROUP BY p.Mpn
        ORDER BY SrfqCount DESC
    """
    return run_query(sql, params={"start": start_date, "end": end_date})


def get_supplier_type_distribution(start_date: str, end_date: str) -> pd.DataFrame:
    sql = f"""
        SELECT 'ATD' AS SupplierType, COUNT(DISTINCT sr.SupplierId) AS Count
        FROM SupplierRfqs sr
        JOIN Suppliers s ON sr.SupplierId = s.SupplierId
        WHERE ISNULL(s.ATD, 0) = 1
          AND sr.CreatedDate >= %(start)s
          AND sr.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {SRFQ_DATE_BOUNDS}

        UNION ALL

        SELECT 'MFR', COUNT(DISTINCT sr.SupplierId)
        FROM SupplierRfqs sr
        JOIN Suppliers s ON sr.SupplierId = s.SupplierId
        WHERE ISNULL(s.MFR, 0) = 1
          AND sr.CreatedDate >= %(start)s
          AND sr.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {SRFQ_DATE_BOUNDS}

        UNION ALL

        SELECT 'BRK', COUNT(DISTINCT sr.SupplierId)
        FROM SupplierRfqs sr
        JOIN Suppliers s ON sr.SupplierId = s.SupplierId
        WHERE ISNULL(s.BRK, 0) = 1
          AND sr.CreatedDate >= %(start)s
          AND sr.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {SRFQ_DATE_BOUNDS}

        UNION ALL

        SELECT 'HYB', COUNT(DISTINCT sr.SupplierId)
        FROM SupplierRfqs sr
        JOIN Suppliers s ON sr.SupplierId = s.SupplierId
        WHERE ISNULL(s.HYB, 0) = 1
          AND sr.CreatedDate >= %(start)s
          AND sr.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {SRFQ_DATE_BOUNDS}
    """
    df = run_query(sql, params={"start": start_date, "end": end_date})
    if not df.empty:
        df = df[df["Count"] > 0]
    return df


def get_top_manufacturers(start_date: str, end_date: str, limit: int = 10) -> pd.DataFrame:
    sql = f"""
        SELECT TOP {limit}
            LTRIM(RTRIM(COALESCE(m.MfrStdName, m.MfrName))) AS Manufacturer,
            COUNT(*)                                          AS SrfqCount
        FROM SupplierRfqs sr
        JOIN Mfrs m ON sr.SupplierMfrId = m.MfrId
        WHERE sr.SupplierMfrId IS NOT NULL AND sr.SupplierMfrId > 0
          AND sr.CreatedDate >= %(start)s
          AND sr.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {SRFQ_DATE_BOUNDS}
        GROUP BY LTRIM(RTRIM(COALESCE(m.MfrStdName, m.MfrName)))
        ORDER BY SrfqCount DESC
    """
    return run_query(sql, params={"start": start_date, "end": end_date})


def get_margin_analysis(start_date: str, end_date: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            FORMAT(sr.CreatedDate, 'yyyy-MM') AS Period,
            SUM(ISNULL(sr.SalePrice, 0) * ISNULL(sr.SaleQty, 0))         AS SaleValue,
            SUM(ISNULL(sr.ResponsePrice, 0) * ISNULL(sr.ResponseQty, 0)) AS CostValue
        FROM SupplierRfqs sr
        JOIN CustomerRfqParts p ON sr.CustomerRfqPartId = p.RfqPartId
        WHERE p.QuoteStatus = 30
          AND sr.SalePrice IS NOT NULL
          AND sr.ResponsePrice IS NOT NULL
          AND sr.CreatedDate >= %(start)s
          AND sr.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {SRFQ_DATE_BOUNDS}
        GROUP BY FORMAT(sr.CreatedDate, 'yyyy-MM')
        ORDER BY Period
    """
    df = run_query(sql, params={"start": start_date, "end": end_date})
    if not df.empty:
        df["SaleValue"] = pd.to_numeric(df["SaleValue"], errors="coerce").fillna(0)
        df["CostValue"] = pd.to_numeric(df["CostValue"], errors="coerce").fillna(0)
        df["Margin"] = df["SaleValue"] - df["CostValue"]
        df["MarginPct"] = (
            (df["Margin"] / df["SaleValue"].replace(0, float("nan"))) * 100
        ).round(1)
    return df
