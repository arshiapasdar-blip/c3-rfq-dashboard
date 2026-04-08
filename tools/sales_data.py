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

DATE_BOUNDS = ""
DATE_BOUNDS_DIRECT = ""


def get_crfq_kpis(start_date: str, end_date: str, hub_sql: str = "") -> dict:
    sql = f"""
        SELECT
            COUNT(*)                        AS total_crfqs,
            COUNT(DISTINCT r.CustomerId)    AS active_customers,
            SUM(CASE WHEN p.QuoteStatus = 30 OR qp.CustomerRfqPartId IS NOT NULL
                     THEN 1 ELSE 0 END)     AS quoted_parts,
            COUNT(p.RfqPartId)              AS total_parts
        FROM CustomerRfqs r
        LEFT JOIN CustomerRfqParts p ON p.CustomerRfqId = r.RfqId
        LEFT JOIN (SELECT DISTINCT CustomerRfqPartId FROM CustomerQuoteParts) qp
               ON qp.CustomerRfqPartId = p.RfqPartId
        WHERE r.Deleted = 0
          {DATE_BOUNDS}
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {hub_sql}
    """
    df = run_query(sql, params={"start": start_date, "end": end_date})
    if df.empty:
        return {"total_crfqs": 0, "active_customers": 0, "quote_rate": 0.0}
    row = df.iloc[0]
    total_parts = int(row.get("total_parts", 0) or 0)
    quoted_parts = int(row.get("quoted_parts", 0) or 0)
    quote_rate = (quoted_parts / total_parts * 100) if total_parts > 0 else 0.0
    return {
        "total_crfqs": int(row.get("total_crfqs", 0) or 0),
        "active_customers": int(row.get("active_customers", 0) or 0),
        "quote_rate": round(quote_rate, 1),
    }


def get_top_customers(start_date: str, end_date: str, limit: int = 10, hub_sql: str = "") -> pd.DataFrame:
    sql = f"""
        SELECT TOP {limit}
            c.CustomerName,
            COUNT(DISTINCT r.RfqId)                              AS RfqCount,
            SUM(CASE WHEN p.QuoteStatus = 30 THEN 1 ELSE 0 END) AS WonCount
        FROM CustomerRfqs r
        JOIN Customers c ON r.CustomerId = c.CustomerId
        LEFT JOIN CustomerRfqParts p ON p.CustomerRfqId = r.RfqId
        WHERE r.Deleted = 0
          {DATE_BOUNDS}
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {hub_sql}
        GROUP BY c.CustomerName
        ORDER BY RfqCount DESC
    """
    return run_query(sql, params={"start": start_date, "end": end_date})


def get_monthly_crfq_trend_range(start_date: str, end_date: str, hub_sql: str = "") -> pd.DataFrame:
    sql = f"""
        SELECT
            FORMAT(r.CreatedDate, 'yyyy-MM') AS Period,
            COUNT(*) AS RfqCount
        FROM CustomerRfqs r
        WHERE r.Deleted = 0
          {DATE_BOUNDS}
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {hub_sql}
        GROUP BY FORMAT(r.CreatedDate, 'yyyy-MM')
        ORDER BY Period
    """
    df = run_query(sql, params={"start": start_date, "end": end_date})
    if not df.empty:
        df["RfqCount"] = df["RfqCount"].astype(int)
    return df


def get_monthly_crfq_volume(year: int) -> pd.DataFrame:
    sql = """
        SELECT
            MONTH(CreatedDate) AS Month,
            COUNT(*)           AS RfqCount
        FROM CustomerRfqs
        WHERE Deleted = 0
          AND YEAR(CreatedDate) = %(year)s
        GROUP BY MONTH(CreatedDate)
        ORDER BY Month
    """
    df = run_query(sql, params={"year": year})
    if df.empty:
        return df
    all_months = pd.DataFrame({"Month": list(range(1, 13))})
    df = all_months.merge(df, on="Month", how="left").fillna(0)
    df["MonthLabel"] = df["Month"].map(CALENDAR_MONTHS)
    df["RfqCount"] = df["RfqCount"].astype(int)
    return df


def get_rfq_result_breakdown(start_date: str, end_date: str, hub_sql: str = "") -> pd.DataFrame:
    sql = f"""
        SELECT
            CASE
                WHEN p.QuoteStatus = 30               THEN 'Won'
                WHEN qp.CustomerRfqPartId IS NOT NULL THEN 'Quoted'
                WHEN p.QuoteStatus = 10               THEN 'Lost'
                ELSE                                       'Not Quoted'
            END AS ResultLabel,
            COUNT(*) AS Count
        FROM CustomerRfqs r
        JOIN CustomerRfqParts p ON p.CustomerRfqId = r.RfqId
        LEFT JOIN (SELECT DISTINCT CustomerRfqPartId FROM CustomerQuoteParts) qp
               ON qp.CustomerRfqPartId = p.RfqPartId
        WHERE r.Deleted = 0
          {DATE_BOUNDS}
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {hub_sql}
        GROUP BY
            CASE
                WHEN p.QuoteStatus = 30               THEN 'Won'
                WHEN qp.CustomerRfqPartId IS NOT NULL THEN 'Quoted'
                WHEN p.QuoteStatus = 10               THEN 'Lost'
                ELSE                                       'Not Quoted'
            END
        ORDER BY Count DESC
    """
    return run_query(sql, params={"start": start_date, "end": end_date})


def get_top_mpns(start_date: str, end_date: str, limit: int = 20, hub_sql: str = "") -> pd.DataFrame:
    sql = f"""
        SELECT TOP {limit}
            LTRIM(RTRIM(p.Mpn)) AS Mpn,
            COUNT(*)            AS RequestCount,
            SUM(p.QtyRequested) AS TotalQty
        FROM CustomerRfqParts p
        JOIN CustomerRfqs r ON p.CustomerRfqId = r.RfqId
        WHERE r.Deleted = 0
          {DATE_BOUNDS}
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {hub_sql}
          AND {_mpn_filter('p')}
        GROUP BY p.Mpn
        ORDER BY RequestCount DESC
    """
    return run_query(sql, params={"start": start_date, "end": end_date})


def get_sales_rep_leaderboard(start_date: str, end_date: str, hub_sql: str = "") -> pd.DataFrame:
    sql = f"""
        SELECT
            COALESCE(u.DisplayName, 'Unassigned')                AS SalesRep,
            COUNT(DISTINCT r.RfqId)                              AS RfqCount,
            COUNT(p.RfqPartId)                                   AS TotalParts,
            SUM(CASE WHEN p.QuoteStatus = 30 THEN 1 ELSE 0 END) AS WonCount
        FROM CustomerRfqs r
        LEFT JOIN Users u            ON r.SalesRepId = u.UserId
        LEFT JOIN CustomerRfqParts p ON p.CustomerRfqId = r.RfqId
        WHERE r.Deleted = 0
          {DATE_BOUNDS}
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {hub_sql}
        GROUP BY u.DisplayName
        ORDER BY RfqCount DESC
    """
    df = run_query(sql, params={"start": start_date, "end": end_date})
    if not df.empty and "TotalParts" in df.columns:
        df["WinRate"] = (
            df["WonCount"] / df["TotalParts"].replace(0, float("nan")) * 100
        ).round(1).fillna(0)
    return df


def get_quote_value_by_customer(start_date: str, end_date: str, limit: int = 10, hub_sql: str = "") -> pd.DataFrame:
    sql = f"""
        SELECT TOP {limit}
            c.CustomerName,
            SUM(ISNULL(qp.SaleQty, 0) * ISNULL(qp.SalePrice, 0)) AS TotalQuoteValue,
            COUNT(qp.CustomerQuotePartId) AS QuotedParts
        FROM CustomerQuoteParts qp
        JOIN CustomerRfqs r ON qp.CustomerRfqId = r.RfqId
        JOIN Customers c ON r.CustomerId = c.CustomerId
        WHERE r.Deleted = 0
          AND qp.SalePrice > 0
          AND qp.SalePrice IS NOT NULL
          AND qp.SaleQty IS NOT NULL
          AND qp.CreatedDate >= %(start)s
          AND qp.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {hub_sql}
        GROUP BY c.CustomerName
        ORDER BY TotalQuoteValue DESC
    """
    df = run_query(sql, params={"start": start_date, "end": end_date})
    if not df.empty:
        df["TotalQuoteValue"] = pd.to_numeric(df["TotalQuoteValue"], errors="coerce").fillna(0)
    return df


def get_customer_country_distribution(start_date: str, end_date: str, hub_sql: str = "") -> pd.DataFrame:
    sql = f"""
        SELECT
            c.Country,
            COUNT(DISTINCT r.CustomerId) AS CustomerCount
        FROM CustomerRfqs r
        JOIN Customers c ON r.CustomerId = c.CustomerId
        WHERE r.Deleted = 0
          {DATE_BOUNDS}
          AND r.CreatedDate >= %(start)s
          AND r.CreatedDate < DATEADD(day, 1, CAST(%(end)s AS DATE))
          {hub_sql}
          AND c.Country IS NOT NULL
          AND LEN(LTRIM(RTRIM(c.Country))) > 2
          AND LTRIM(RTRIM(c.Country)) NOT IN ('string', 'I', '6', 'N/A', 'n/a')
        GROUP BY c.Country
        ORDER BY CustomerCount DESC
    """
    return run_query(sql, params={"start": start_date, "end": end_date})


def _mpn_filter(alias: str = "p") -> str:
    return f"""
        {alias}.Mpn IS NOT NULL
        AND LEN(LTRIM(RTRIM({alias}.Mpn))) > 2
        AND LOWER(LTRIM(RTRIM({alias}.Mpn))) NOT IN ({DIRTY_MPN_SQL})
    """
