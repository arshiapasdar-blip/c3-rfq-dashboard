"""
digikey.py — DigiKey API v4 integration.
Provides MPN lookup: pricing breaks, lead time, lifecycle status, stock, manufacturer.
"""
import os
import time
import logging
import requests

logger = logging.getLogger(__name__)

_token_cache: dict = {"access_token": None, "expires_at": 0.0}

_TOKEN_URL  = "https://api.digikey.com/v1/oauth2/token"
_SEARCH_URL = "https://api.digikey.com/products/v4/search/keyword"


def _get_token() -> str:
    """Return a valid OAuth2 bearer token (client-credentials), refreshing if needed."""
    client_id     = os.getenv("DIGIKEY_CLIENT_ID", "")
    client_secret = os.getenv("DIGIKEY_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        raise ValueError("DIGIKEY_CLIENT_ID / DIGIKEY_CLIENT_SECRET not set in .env")

    now = time.time()
    if _token_cache["access_token"] and now < _token_cache["expires_at"] - 30:
        return _token_cache["access_token"]

    resp = requests.post(
        _TOKEN_URL,
        data={
            "grant_type":    "client_credentials",
            "client_id":     client_id,
            "client_secret": client_secret,
        },
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    _token_cache["access_token"] = data["access_token"]
    _token_cache["expires_at"]   = now + int(data.get("expires_in", 3600))
    return _token_cache["access_token"]


def lookup_mpn(mpn: str) -> dict:
    """
    Search DigiKey for an MPN.

    Returns a dict:
      {"found": True,  "mpn": str, "products": [...]}
      {"found": False, "mpn": str, "error": str}

    Each product dict contains:
      digikey_pn, mfr_pn, manufacturer, description,
      lifecycle, qty_available, lead_weeks,
      pricing_breaks (list of {qty, unit_price}), url
    """
    client_id = os.getenv("DIGIKEY_CLIENT_ID", "")
    if not client_id:
        return {"found": False, "mpn": mpn,
                "error": "DigiKey credentials not configured (DIGIKEY_CLIENT_ID missing in .env)"}

    try:
        token = _get_token()
    except Exception as exc:
        return {"found": False, "mpn": mpn, "error": f"Auth error: {exc}"}

    headers = {
        "Authorization":         f"Bearer {token}",
        "X-DIGIKEY-Client-Id":   client_id,
        "X-DIGIKEY-Locale-Site": "US",
        "X-DIGIKEY-Locale-Language": "en",
        "X-DIGIKEY-Locale-Currency":  "USD",
        "Content-Type": "application/json",
    }

    payload = {
        "Keywords": mpn,
        "Limit":    5,
        "Offset":   0,
    }

    try:
        resp = requests.post(_SEARCH_URL, json=payload, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except requests.HTTPError as exc:
        logger.error("DigiKey HTTP error for %s: %s", mpn, exc)
        return {"found": False, "mpn": mpn, "error": f"DigiKey API error: {exc}"}
    except Exception as exc:
        logger.error("DigiKey error for %s: %s", mpn, exc)
        return {"found": False, "mpn": mpn, "error": str(exc)}

    raw_products = data.get("Products", [])
    if not raw_products:
        return {"found": False, "mpn": mpn, "error": "No results found on DigiKey"}

    products = []
    for p in raw_products[:4]:
        pricing_breaks = [
            {"qty": b.get("BreakQuantity", 0), "unit_price": b.get("UnitPrice", 0.0)}
            for b in p.get("StandardPricing", [])
        ]
        products.append({
            "digikey_pn":   p.get("DigiKeyPartNumber", ""),
            "mfr_pn":       p.get("ManufacturerProductNumber", ""),
            "manufacturer": (p.get("Manufacturer") or {}).get("Name", ""),
            "description":  p.get("ProductDescription", ""),
            "lifecycle":    (p.get("ProductStatus") or {}).get("Status", "Unknown"),
            "qty_available": p.get("QuantityAvailable", 0),
            "lead_weeks":   p.get("ManufacturerLeadWeeks"),
            "unit_price":   p.get("UnitPrice"),
            "pricing_breaks": pricing_breaks,
            "url":          p.get("ProductUrl", ""),
        })

    return {"found": True, "mpn": mpn, "products": products}


def format_result(result: dict) -> str:
    """Convert a lookup_mpn() result dict into a readable plain-text string."""
    if not result.get("found"):
        return f"DigiKey lookup: {result.get('error', 'No data available')}"

    lines = []
    for p in result["products"]:
        lines.append(f"{'─'*54}")
        lines.append(f"Part:         {p['mfr_pn']}  (DigiKey: {p['digikey_pn']})")
        lines.append(f"Manufacturer: {p['manufacturer']}")
        lines.append(f"Description:  {p['description']}")

        lc = p["lifecycle"]
        flag = " ⚠️  ATTENTION" if lc.upper() in ("NRND", "OBSOLETE", "LAST TIME BUY",
                                                    "NOT FOR NEW DESIGNS") else ""
        lines.append(f"Lifecycle:    {lc}{flag}")

        lines.append(f"Stock (DK):   {p['qty_available']:,} pcs")

        lw = p["lead_weeks"]
        lines.append(f"Lead time:    {lw} weeks" if lw is not None else "Lead time:    —")

        if p["pricing_breaks"]:
            lines.append("Pricing (USD):")
            for b in p["pricing_breaks"]:
                lines.append(f"   {b['qty']:>8,} pcs  →  ${b['unit_price']:.4f}/ea")
        elif p["unit_price"] is not None:
            lines.append(f"Unit price:   ${p['unit_price']:.4f}")

        if p["url"]:
            lines.append(f"DigiKey URL:  {p['url']}")

    lines.append(f"{'─'*54}")
    return "\n".join(lines)
