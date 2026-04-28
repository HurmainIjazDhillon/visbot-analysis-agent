from __future__ import annotations


ALLOWED_SQL_PREFIXES = ("select", "with")
BLOCKED_SQL_TOKENS = ("insert ", "update ", "delete ", "drop ", "alter ", "truncate ")


def validate_read_only_sql(query: str) -> str:
    normalized = " ".join(query.strip().lower().split())
    if not normalized.startswith(ALLOWED_SQL_PREFIXES):
        raise ValueError("Only read-only SELECT/WITH queries are allowed.")

    for token in BLOCKED_SQL_TOKENS:
        if token in normalized:
            raise ValueError(f"Blocked SQL token detected: {token.strip()}")

    return query.strip()
