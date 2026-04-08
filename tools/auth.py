import os
import logging
import streamlit as st
import yaml
from tools.db import run_query

logger = logging.getLogger(__name__)

_CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config", "hubs.yaml")


def get_current_user_email() -> str:
    """Read logged-in user's email from Azure Easy Auth header, or DEV_USER_EMAIL for local dev."""
    try:
        return st.context.headers.get("X-Ms-Client-Principal-Name", "") or os.getenv("DEV_USER_EMAIL", "")
    except Exception:
        return os.getenv("DEV_USER_EMAIL", "")


@st.cache_data(ttl=300)
def load_hub_config() -> dict:
    """Load hub config from YAML file."""
    try:
        with open(_CONFIG_PATH, "r") as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        logger.warning("Failed to load hub config: %s", e)
        return {}


def get_hub_names() -> list:
    """Return list of hub names from config."""
    config = load_hub_config()
    return list(config.get("hubs", {}).keys())


def get_hub_user_ids(hub_name: str) -> list:
    """Return list of UserIds for a given hub."""
    config = load_hub_config()
    hub = config.get("hubs", {}).get(hub_name, {})
    return hub.get("user_ids", [])


def resolve_user_display(email: str) -> tuple:
    """Look up user in DB by PrincipalName. Returns (user_id, display_name) or (None, email)."""
    if not email:
        return (None, "Unknown")
    df = run_query(
        "SELECT TOP 1 UserId, DisplayName FROM Users WHERE LOWER(PrincipalName) = LOWER(%(email)s)",
        {"email": email},
    )
    if df.empty:
        return (None, email)
    return (int(df.iloc[0]["UserId"]), str(df.iloc[0]["DisplayName"]))


def build_hub_scope(hub_name: str, owner_col: str = "r.SalesRepId") -> str:
    """Build SQL WHERE fragment for hub filtering.

    Returns an empty string for 'All Hubs', or 'AND {owner_col} IN (id1,id2,...)'
    for a specific hub. IDs are integers from config — safe to inline.
    """
    if not hub_name or hub_name == "All Hubs":
        return ""
    ids = get_hub_user_ids(hub_name)
    if not ids:
        return ""
    id_list = ",".join(str(int(i)) for i in ids)
    return f"AND {owner_col} IN ({id_list})"
