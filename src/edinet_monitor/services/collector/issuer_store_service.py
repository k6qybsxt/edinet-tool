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
            industry_33,
            industry_17,
            is_listed,
            exchange,
            listing_category_raw,
            listing_source,
            updated_at
        )
        VALUES (
            :edinet_code,
            :security_code,
            :company_name,
            :market,
            :industry_33,
            :industry_17,
            :is_listed,
            :exchange,
            :listing_category_raw,
            :listing_source,
            :updated_at
        )
        ON CONFLICT(edinet_code) DO UPDATE SET
            security_code = excluded.security_code,
            company_name = excluded.company_name,
            market = excluded.market,
            industry_33 = excluded.industry_33,
            industry_17 = excluded.industry_17,
            is_listed = excluded.is_listed,
            exchange = CASE
                WHEN excluded.exchange <> '' THEN excluded.exchange
                ELSE issuer_master.exchange
            END,
            listing_category_raw = CASE
                WHEN excluded.listing_category_raw <> '' THEN excluded.listing_category_raw
                ELSE issuer_master.listing_category_raw
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