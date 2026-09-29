import hashlib
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlsplit, urlunsplit

_TRACKING_PARAMS = {"fbclid", "gclid", "mc_cid", "mc_eid"}


def normalize_url(url: str) -> str:
    """Same resource -> same string. Lowercases scheme and host (never the path:
    servers may be case-sensitive), drops the default port, the #fragment and
    tracking parameters, sorts the query, and encodes the path one consistent
    way so `a b.pdf` and `a%20b.pdf` match."""
    parts = urlsplit(url.strip())
    scheme = parts.scheme.lower()
    host = (parts.hostname or "").lower()
    port = parts.port
    if port and not ((scheme == "http" and port == 80) or (scheme == "https" and port == 443)):
        host = f"{host}:{port}"
    path = quote(unquote(parts.path), safe="/:@!$&'()*+,;=-._~") or "/"
    query = sorted(
        (k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
        if not k.lower().startswith("utm_") and k.lower() not in _TRACKING_PARAMS
    )
    return urlunsplit((scheme, host, path, urlencode(query), ""))


def url_key(normalized_url: str) -> str:
    return hashlib.sha256(normalized_url.encode("utf-8")).hexdigest()
