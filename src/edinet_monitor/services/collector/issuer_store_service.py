from __future__ import annotations

import sqlite3


def upsert_issuer(conn: sqlite3.Connection, issuer: dict) -> None:
    conn.execute(
        """
        INSERT INTO issuer_master (
            edinet_code,
            security_code,
            company_name,
            market,
            industry,
            is_listed,
            exchange,
            listing_source,
            updated_at
        )
        VALUES (
            :edinet_code,
            :security_code,
            :company_name,
            :market,
            :industry,
            :is_listed,
            :exchange,
            :listing_source,
            :updated_at
        )
        ON CONFLICT(edinet_code) DO UPDATE SET
            security_code = excluded.security_code,
            company_name = excluded.company_name,
            exchange = CASE
                WHEN excluded.exchange <> '' THEN excluded.exchange
                ELSE issuer_master.exchange
            END,
            listing_source = CASE
                WHEN excluded.listing_source <> '' THEN excluded.listing_source
                ELSE issuer_master.listing_source
            END,
            updated_at = excluded.updated_at
        """,
        issuer,
    )


def upsert_issuers(conn: sqlite3.Connection, issuers: list[dict]) -> int:
    count = 0

    for issuer in issuers:
        if not issuer["edinet_code"]:
            continue
        upsert_issuer(conn, issuer)
        count += 1

    conn.commit()
    return count