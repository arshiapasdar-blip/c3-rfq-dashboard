import os
import sys
import queue
import logging
import threading
import pymssql
import pandas as pd
from dotenv import load_dotenv

try:
    from azure.identity import DefaultAzureCredential
    from azure.keyvault.secrets import SecretClient
    _AZURE_AVAILABLE = True
except ImportError:
    _AZURE_AVAILABLE = False

load_dotenv()

logger = logging.getLogger(__name__)

DIRTY_MPNS = {
    'test', 'test1', 'test2', 'test3', 'test12345',
    '12345', '5678', '123', 'abc', 't', 'maxm',
    'n/a', 'na', 'tbd', '-', '.', 'string',
}

DIRTY_MPN_SQL = ", ".join(f"'{m}'" for m in DIRTY_MPNS)

MPN_FILTER_SQL = f"""
    p.Mpn IS NOT NULL
    AND LEN(LTRIM(RTRIM(p.Mpn))) > 2
    AND LOWER(LTRIM(RTRIM(p.Mpn))) NOT IN ({DIRTY_MPN_SQL})
"""

VALID_DATE_FILTER = ""

# ─── Azure Key Vault (prod credentials) ───────────────────────────────────────
_PROD_VAULT_URL  = "https://j2-c3-euwe-kv-prod.vault.azure.net/"
_KV_SECRET_NAME  = "DefaultConnectionAz"
_credential_source: str = "unknown"  # set on first connection attempt


def _parse_connection_string(cs: str) -> dict:
    """Parse ADO.NET-style 'Key=Value;Key2=Value2;' connection string into a dict."""
    result = {}
    for part in cs.split(";"):
        part = part.strip()
        if "=" in part:
            k, v = part.split("=", 1)
            result[k.strip().lower()] = v.strip()
    return result


def _get_kv_credentials():
    """Fetch prod connection string from Azure Key Vault. Returns None on any failure."""
    if not _AZURE_AVAILABLE:
        return None
    try:
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=_PROD_VAULT_URL, credential=credential)
        secret = client.get_secret(_KV_SECRET_NAME)
        return _parse_connection_string(secret.value)
    except Exception as e:
        logger.warning("Key Vault fetch failed, falling back to .env: %s", e)
        return None


# ─── Connection pool ──────────────────────────────────────────────────────────
_POOL_SIZE = 8
_pool: queue.Queue = queue.Queue(maxsize=_POOL_SIZE)


def get_credential_source() -> str:
    """Return a human-readable string indicating which credential source is active."""
    return _credential_source


def _create_raw_connection() -> pymssql.Connection:
    global _credential_source
    # Try Azure Key Vault first (prod)
    kv = _get_kv_credentials()
    if kv:
        _credential_source = "🟢 Key Vault (prod)"
        server   = kv.get("server") or kv.get("data source")
        database = kv.get("database") or kv.get("initial catalog")
        user     = kv.get("user id") or kv.get("uid")
        password = kv.get("password") or kv.get("pwd")
        # Strip ADO.NET prefix/port: "tcp:host.database.windows.net,1433" → "host.database.windows.net"
        if server:
            if server.lower().startswith("tcp:"):
                server = server[4:]
            if "," in server:
                server = server.split(",")[0]
    else:
        # Fall back to .env (dev)
        _credential_source = "🟡 .env (dev/fallback)"
        server   = os.getenv("DB_SERVER")
        database = os.getenv("DB_NAME")
        user     = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")

    logger.info("Credential source: %s", _credential_source)
    logger.info("Connecting to server: %s, database: %s", server, database)

    if not all([server, database, user, password]):
        raise RuntimeError(
            "Missing database credentials. Key Vault unreachable and DB_SERVER/DB_NAME/"
            "DB_USER/DB_PASSWORD not set in .env."
        )

    try:
        return pymssql.connect(
            server=server,
            user=user,
            password=password,
            database=database,
            tds_version="7.3",
            timeout=30,
        )
    except Exception as e:
        raise RuntimeError(
            f"Cannot connect to database '{server}'. "
            f"Ensure you are on the J2 network or VPN. Error: {e}"
        ) from e


def get_connection() -> pymssql.Connection:
    """Return a pooled connection, creating a new one if the pool is empty."""
    try:
        conn = _pool.get_nowait()
        # Quick liveness check — reconnect if the connection has gone stale
        conn.cursor().execute("SELECT 1")
        return conn
    except Exception:
        return _create_raw_connection()


def _release(conn: pymssql.Connection) -> None:
    """Return a healthy connection to the pool, or close it if the pool is full."""
    try:
        _pool.put_nowait(conn)
    except queue.Full:
        conn.close()


def run_query(sql: str, params: dict = None) -> pd.DataFrame:
    conn = get_connection()
    try:
        cursor = conn.cursor(as_dict=True)
        # READ UNCOMMITTED avoids shared-lock contention with OLTP writes.
        # Dirty reads are acceptable for analytics dashboards.
        cursor.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        cursor.execute(sql, params or {})
        rows = cursor.fetchall()
        _release(conn)
        return pd.DataFrame(rows) if rows else pd.DataFrame()
    except RuntimeError:
        conn.close()
        raise
    except Exception as e:
        conn.close()
        logger.error("Query failed: %s", e)
        return pd.DataFrame()
