"""
Hivemind — WSB ticker mention tracker (ApeWisdom edition).

M1: pull aggregated WSB ticker mention data from apewisdom.io every
30 minutes, store as time-series snapshots, expose CLI + JSON API.

Usage:
  python server.py fetch                          # pull one snapshot now
  python server.py top [--limit N]                # current top tickers
  python server.py trending [--limit N]           # biggest rank movers vs 24h ago
  python server.py history TICKER [--hours N]     # mention count over time
  python server.py serve                          # Flask + 30-min scheduler
"""

from __future__ import annotations

import argparse
import html
import os
import sqlite3
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import requests
import yfinance as yf
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_from_directory
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

_vader = SentimentIntensityAnalyzer()

load_dotenv()

# ── Config ───────────────────────────────────────────────────────────
# On Fly (or any container deploy) the SQLite file lives on a mounted
# persistent volume. Locally it sits next to this script.
_DATA_DIR = Path(os.environ.get("CORTEX_DATA_DIR") or Path(__file__).parent)
_DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = _DATA_DIR / "cortex.db"
APEWISDOM_BASE_FMT = "https://apewisdom.io/api/v1.0/filter/{source}"
FETCH_INTERVAL_MIN = 30          # ApeWisdom refreshes every ~30 min
FETCH_PAGES = 3                  # 50 tickers per page → top 150
MENTION_FLOOR = 3                # ignore tickers below this many mentions (long-tail noise)
HTTP_TIMEOUT_S = 15
USER_AGENT = os.environ.get("USER_AGENT", "hivemind/0.1 (personal use)")

# Channels = ApeWisdom subreddit slugs. Order shown in the UI selector.
# 'all' is a virtual channel that aggregates across the real ones at query
# time — never used as a fetch target.
ALL_SOURCE = "all"
SOURCES = [
    {"key": "wallstreetbets", "label": "r/wallstreetbets", "short": "WSB"},
]
REAL_SOURCES = [s for s in SOURCES if s["key"] != ALL_SOURCE]
SOURCE_KEYS = {s["key"] for s in SOURCES}
# Single channel — r/wallstreetbets only. No channel selector in the UI.
DEFAULT_SOURCE = "wallstreetbets"
FETCH_DEFAULT = "wallstreetbets"


# ── DB ───────────────────────────────────────────────────────────────
SNAPSHOTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots (
    fetched_utc      INTEGER NOT NULL,
    source           TEXT NOT NULL DEFAULT 'wallstreetbets',
    ticker           TEXT NOT NULL,
    name             TEXT,
    mentions         INTEGER NOT NULL,
    upvotes          INTEGER,
    rank             INTEGER,
    rank_24h_ago     INTEGER,
    mentions_24h_ago INTEGER,
    PRIMARY KEY (fetched_utc, source, ticker)
);
"""

SNAPSHOTS_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_snapshots_ticker  ON snapshots (ticker);
CREATE INDEX IF NOT EXISTS idx_snapshots_fetched ON snapshots (fetched_utc);
CREATE INDEX IF NOT EXISTS idx_snapshots_source  ON snapshots (source);
"""

SCHEMA = """

CREATE TABLE IF NOT EXISTS sectors (
    ticker              TEXT PRIMARY KEY,
    sector              TEXT,           -- e.g. "Technology", "Healthcare"
    industry            TEXT,
    company_name        TEXT,
    last_refreshed_utc  INTEGER
);

-- Mainstream financial news per ticker. Sentiment scored locally with
-- VADER on the headline. Used as a secondary signal next to WSB chatter.
CREATE TABLE IF NOT EXISTS news_items (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker              TEXT NOT NULL,
    headline            TEXT,
    publisher           TEXT,
    url                 TEXT,
    published_utc       INTEGER,
    sentiment_compound  REAL,           -- VADER score: -1 (negative) .. +1 (positive)
    ingested_utc        INTEGER,
    UNIQUE (ticker, url)
);

CREATE INDEX IF NOT EXISTS idx_news_ticker    ON news_items (ticker);
CREATE INDEX IF NOT EXISTS idx_news_published ON news_items (published_utc);
"""

# In-memory price cache: ticker -> {price, previous_close, change_pct, expires_at}
_price_cache: dict[str, dict] = {}
PRICE_CACHE_S = 300   # 5 minutes


@contextmanager
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with db() as c:
        # 1) Ensure snapshots table exists (in new shape, if first-time install).
        c.executescript(SNAPSHOTS_SCHEMA)
        # 2) Migration: pre-multi-source DBs have snapshots WITHOUT a 'source'
        #    column. Detect and rebuild that table with the new schema, tagging
        #    all existing rows as 'wallstreetbets'.
        cols = [r[1] for r in c.execute("PRAGMA table_info(snapshots)").fetchall()]
        if "source" not in cols:
            print("migrating snapshots table to add 'source' column...",
                  file=sys.stderr)
            c.executescript("""
                CREATE TABLE snapshots_new (
                    fetched_utc      INTEGER NOT NULL,
                    source           TEXT NOT NULL DEFAULT 'wallstreetbets',
                    ticker           TEXT NOT NULL,
                    name             TEXT,
                    mentions         INTEGER NOT NULL,
                    upvotes          INTEGER,
                    rank             INTEGER,
                    rank_24h_ago     INTEGER,
                    mentions_24h_ago INTEGER,
                    PRIMARY KEY (fetched_utc, source, ticker)
                );
                INSERT INTO snapshots_new
                    (fetched_utc, source, ticker, name, mentions, upvotes,
                     rank, rank_24h_ago, mentions_24h_ago)
                  SELECT fetched_utc, 'wallstreetbets', ticker, name, mentions,
                         upvotes, rank, rank_24h_ago, mentions_24h_ago
                  FROM snapshots;
                DROP TABLE snapshots;
                ALTER TABLE snapshots_new RENAME TO snapshots;
            """)
        # 3) Now safely create indexes that reference the (now-existing) source.
        c.executescript(SNAPSHOTS_INDEXES)
        # 4) Rest of the schema (sectors, news_items, their indexes).
        c.executescript(SCHEMA)


def _resolve_source(s: str | None) -> str:
    """Validate + default a source key from the API/CLI."""
    if not s or s not in SOURCE_KEYS:
        return DEFAULT_SOURCE
    return s


# ── ApeWisdom client ─────────────────────────────────────────────────
def fetch_apewisdom_page(source: str, page: int = 1) -> dict:
    r = requests.get(
        f"{APEWISDOM_BASE_FMT.format(source=source)}/page/{page}",
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        timeout=HTTP_TIMEOUT_S,
    )
    r.raise_for_status()
    return r.json()


def fetch_snapshot(source: str = FETCH_DEFAULT) -> dict:
    """Pull the top N pages for one source and persist as a single snapshot."""
    source = _resolve_source(source)
    fetched_utc = int(time.time())
    rows_inserted = 0
    with db() as conn:
        for page in range(1, FETCH_PAGES + 1):
            try:
                data = fetch_apewisdom_page(source, page)
            except Exception as e:
                print(f"!! [{source}] page {page} failed: {e}", file=sys.stderr)
                break
            for r in data.get("results", []):
                # Skip the low-signal long tail — tickers below the mention
                # floor are mostly 1–2 mention noise and only pad the data.
                if int(r.get("mentions") or 0) < MENTION_FLOOR:
                    continue
                try:
                    conn.execute(
                        """INSERT OR REPLACE INTO snapshots
                           (fetched_utc, source, ticker, name, mentions, upvotes,
                            rank, rank_24h_ago, mentions_24h_ago)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (fetched_utc, source, r["ticker"],
                         html.unescape(r.get("name") or ""),
                         int(r.get("mentions") or 0),
                         int(r.get("upvotes") or 0) if r.get("upvotes") is not None else None,
                         int(r.get("rank") or 0),
                         int(r["rank_24h_ago"]) if r.get("rank_24h_ago") not in (None, "") else None,
                         int(r["mentions_24h_ago"]) if r.get("mentions_24h_ago") not in (None, "") else None),
                    )
                    rows_inserted += 1
                except Exception as e:
                    print(f"  ! row insert failed for {r.get('ticker')}: {e}",
                          file=sys.stderr)
            time.sleep(0.6)
    return {"source": source, "fetched_utc": fetched_utc,
            "tickers": rows_inserted}


def fetch_all_sources() -> list[dict]:
    """Scheduled tick: fetch each enabled REAL source serially.
    ('all' is a virtual aggregate channel — nothing to fetch.)"""
    results = []
    for s in REAL_SOURCES:
        results.append(fetch_snapshot(s["key"]))
    return results


# ── Queries ──────────────────────────────────────────────────────────
def latest_fetched_utc(source: str = DEFAULT_SOURCE) -> int | None:
    source = _resolve_source(source)
    with db() as conn:
        if source == ALL_SOURCE:
            row = conn.execute(
                "SELECT MAX(fetched_utc) FROM snapshots"
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT MAX(fetched_utc) FROM snapshots WHERE source = ?",
                (source,),
            ).fetchone()
        return row[0]


# When source = ALL, we aggregate across each real source's *own* latest
# snapshot (which may have different fetched_utc values), summing mentions
# per ticker. Wrapping with a CTE keeps the SQL readable.
_REAL_SOURCE_IN = ",".join("'%s'" % s["key"] for s in REAL_SOURCES)
# Scope cross-source aggregates to the currently-monitored channels, so data
# from a retired channel (e.g. an old wallstreetbets snapshot) can't leak in.
_LATEST_PER_SOURCE_CTE = """
WITH latest_per_source AS (
    SELECT source, MAX(fetched_utc) AS fu FROM snapshots
    WHERE source IN (%s)
    GROUP BY source
)
""" % _REAL_SOURCE_IN


def top_tickers(limit: int = 20, source: str = DEFAULT_SOURCE) -> list[dict]:
    """Current top tickers from the most-recent snapshot for a source."""
    source = _resolve_source(source)
    if source == ALL_SOURCE:
        with db() as conn:
            rows = conn.execute(
                _LATEST_PER_SOURCE_CTE +
                """SELECT s.ticker,
                          MAX(s.name)                AS name,
                          SUM(s.mentions)            AS mentions,
                          SUM(s.mentions_24h_ago)    AS mentions_24h_ago,
                          NULL                       AS rank,
                          NULL                       AS rank_24h_ago
                   FROM snapshots s
                   JOIN latest_per_source l
                     ON l.source = s.source AND l.fu = s.fetched_utc
                   GROUP BY s.ticker
                   ORDER BY mentions DESC
                   LIMIT ?""",
                (limit,),
            ).fetchall()
        # Assign synthetic ranks in Python (1..N).
        out = []
        for i, r in enumerate(rows, 1):
            d = dict(r); d["rank"] = i
            out.append(d)
        return out

    latest = latest_fetched_utc(source)
    if not latest:
        return []
    with db() as conn:
        rows = conn.execute(
            """SELECT ticker, name, mentions, rank, rank_24h_ago, mentions_24h_ago
               FROM snapshots
               WHERE source = ? AND fetched_utc = ?
               ORDER BY rank ASC
               LIMIT ?""",
            (source, latest, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def trending(limit: int = 20, source: str = DEFAULT_SOURCE) -> list[dict]:
    """Biggest rank improvers (negative delta = climbing up the list).
    For 'all', ranks across channels aren't directly comparable, so we
    sort by mention-ratio (now vs 24h ago) instead."""
    source = _resolve_source(source)
    if source == ALL_SOURCE:
        with db() as conn:
            rows = conn.execute(
                _LATEST_PER_SOURCE_CTE +
                """SELECT s.ticker,
                          MAX(s.name)              AS name,
                          SUM(s.mentions)          AS mentions,
                          SUM(s.mentions_24h_ago)  AS mentions_24h_ago,
                          NULL                     AS rank,
                          NULL                     AS rank_24h_ago,
                          NULL                     AS rank_delta,
                          CASE WHEN SUM(s.mentions_24h_ago) > 0
                               THEN SUM(s.mentions) * 1.0 / SUM(s.mentions_24h_ago)
                               ELSE NULL END       AS mention_ratio
                   FROM snapshots s
                   JOIN latest_per_source l
                     ON l.source = s.source AND l.fu = s.fetched_utc
                   GROUP BY s.ticker
                   HAVING SUM(s.mentions_24h_ago) > 0
                      AND SUM(s.mentions) >= 5
                   ORDER BY mention_ratio DESC
                   LIMIT ?""",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    latest = latest_fetched_utc(source)
    if not latest:
        return []
    with db() as conn:
        rows = conn.execute(
            """SELECT ticker, name, mentions, rank, rank_24h_ago,
                      (rank_24h_ago - rank) AS rank_delta,
                      mentions_24h_ago,
                      CASE WHEN mentions_24h_ago > 0
                           THEN (mentions * 1.0 / mentions_24h_ago)
                           ELSE NULL END AS mention_ratio
               FROM snapshots
               WHERE source = ? AND fetched_utc = ?
                 AND rank_24h_ago IS NOT NULL
               ORDER BY rank_delta DESC, mention_ratio DESC
               LIMIT ?""",
            (source, latest, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def history_bulk(tickers: list[str], hours: int = 48,
                 source: str = DEFAULT_SOURCE) -> dict[str, list[dict]]:
    """Time-series mentions for multiple tickers in one query.
    For 'all', mentions are summed per ticker per timestamp across sources."""
    source = _resolve_source(source)
    cutoff = int(time.time()) - hours * 3600
    out: dict[str, list[dict]] = {t.upper(): [] for t in tickers if t}
    if not out:
        return {}
    placeholders = ",".join("?" * len(out))
    with db() as conn:
        if source == ALL_SOURCE:
            rows = conn.execute(
                f"""SELECT ticker, fetched_utc, SUM(mentions) AS mentions
                    FROM snapshots
                    WHERE ticker IN ({placeholders}) AND fetched_utc >= ?
                    GROUP BY ticker, fetched_utc
                    ORDER BY ticker, fetched_utc ASC""",
                (*out.keys(), cutoff),
            ).fetchall()
        else:
            rows = conn.execute(
                f"""SELECT ticker, fetched_utc, mentions
                    FROM snapshots
                    WHERE source = ? AND ticker IN ({placeholders}) AND fetched_utc >= ?
                    ORDER BY ticker, fetched_utc ASC""",
                (source, *out.keys(), cutoff),
            ).fetchall()
    for r in rows:
        out[r["ticker"]].append({"fetched_utc": r["fetched_utc"],
                                 "mentions": r["mentions"]})
    return out


def history(ticker: str, hours: int = 48,
            source: str = DEFAULT_SOURCE) -> list[dict]:
    """Time-series of (fetched_utc, mentions, rank) for a given ticker."""
    source = _resolve_source(source)
    cutoff = int(time.time()) - hours * 3600
    with db() as conn:
        if source == ALL_SOURCE:
            rows = conn.execute(
                """SELECT fetched_utc, SUM(mentions) AS mentions, NULL AS rank
                   FROM snapshots
                   WHERE ticker = ? AND fetched_utc >= ?
                   GROUP BY fetched_utc
                   ORDER BY fetched_utc ASC""",
                (ticker.upper(), cutoff),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT fetched_utc, mentions, rank
                   FROM snapshots
                   WHERE source = ? AND ticker = ? AND fetched_utc >= ?
                   ORDER BY fetched_utc ASC""",
                (source, ticker.upper(), cutoff),
            ).fetchall()
    return [dict(r) for r in rows]


# ── Prices (yfinance) ────────────────────────────────────────────────
def _valid_ticker(t: str) -> bool:
    # Skip cashtag-only tokens that aren't real US equities/ETFs.
    return bool(t) and t.isalpha() and 1 <= len(t) <= 5


def get_prices(tickers: list[str]) -> dict[str, dict]:
    """Return {TICKER: {price, previous_close, change_pct}, ...} — cached 5 min."""
    out: dict[str, dict] = {}
    now = time.time()
    to_fetch: list[str] = []
    for t in tickers:
        t = t.upper()
        if not _valid_ticker(t):
            continue
        cached = _price_cache.get(t)
        if cached and cached["expires_at"] > now:
            out[t] = {k: cached.get(k)
                      for k in ("price", "previous_close", "change_pct")}
        else:
            to_fetch.append(t)
    if not to_fetch:
        return out
    try:
        df = yf.download(to_fetch, period="5d", interval="1d",
                         progress=False, threads=True,
                         group_by="ticker", auto_adjust=False)
    except Exception as e:
        print(f"!! yfinance fetch failed: {e}", file=sys.stderr)
        return out
    for t in to_fetch:
        try:
            if len(to_fetch) == 1:
                closes = df["Close"].dropna()
            else:
                closes = df[t]["Close"].dropna()
            if len(closes) >= 2:
                last = float(closes.iloc[-1])
                prev = float(closes.iloc[-2])
                rec = {"price": last, "previous_close": prev,
                       "change_pct": (last - prev) / prev * 100,
                       "expires_at": now + PRICE_CACHE_S}
            elif len(closes) == 1:
                rec = {"price": float(closes.iloc[-1]),
                       "previous_close": None, "change_pct": None,
                       "expires_at": now + PRICE_CACHE_S}
            else:
                continue
            _price_cache[t] = rec
            out[t] = {k: rec.get(k)
                      for k in ("price", "previous_close", "change_pct")}
        except Exception:
            # Negative-cache for 5 min so we don't keep refetching bad symbols.
            _price_cache[t] = {"price": None, "previous_close": None,
                               "change_pct": None, "expires_at": now + PRICE_CACHE_S}
    return out


# ── Sectors (yfinance) ───────────────────────────────────────────────
def refresh_sectors_for_top(limit: int = 100) -> int:
    """Look up sector/industry for any top-N tickers across ALL sources
    that we don't have yet. Slow (~0.3s per ticker)."""
    with db() as conn:
        # Pick the latest snapshot per source, take top-N from each.
        rows = conn.execute(
            """SELECT DISTINCT s.ticker FROM snapshots s
               LEFT JOIN sectors sec ON sec.ticker = s.ticker
               WHERE sec.ticker IS NULL
                 AND s.fetched_utc IN (
                     SELECT MAX(fetched_utc) FROM snapshots GROUP BY source
                 )
               ORDER BY s.rank ASC
               LIMIT ?""",
            (limit,),
        ).fetchall()
    fetched = 0
    for r in rows:
        t = r["ticker"]
        if not _valid_ticker(t):
            with db() as conn:
                conn.execute(
                    """INSERT OR REPLACE INTO sectors
                       (ticker, sector, industry, company_name, last_refreshed_utc)
                       VALUES (?, NULL, NULL, NULL, ?)""",
                    (t, int(time.time())),
                )
            continue
        sector = industry = name = None
        try:
            info = yf.Ticker(t).info
            sector = info.get("sector")
            industry = info.get("industry")
            name = info.get("longName") or info.get("shortName")
        except Exception:
            pass
        with db() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO sectors
                   (ticker, sector, industry, company_name, last_refreshed_utc)
                   VALUES (?, ?, ?, ?, ?)""",
                (t, sector, industry, name, int(time.time())),
            )
        fetched += 1
        time.sleep(0.25)
    return fetched


def sector_buzz(source: str = DEFAULT_SOURCE) -> list[dict]:
    """Aggregate latest-snapshot mentions by sector for a given source.
    Returns a `ticker_list` column with the top tickers in each sector
    (sorted by mentions, capped) for hover/expansion in the UI."""
    source = _resolve_source(source)
    if source == ALL_SOURCE:
        with db() as conn:
            rows = conn.execute(
                _LATEST_PER_SOURCE_CTE +
                """SELECT COALESCE(sec.sector, 'Unknown') AS sector,
                          SUM(s.mentions)                AS mentions,
                          SUM(s.mentions_24h_ago)        AS mentions_24h_ago,
                          COUNT(DISTINCT s.ticker)       AS tickers,
                          GROUP_CONCAT(DISTINCT s.ticker) AS ticker_list
                   FROM snapshots s
                   JOIN latest_per_source l
                     ON l.source = s.source AND l.fu = s.fetched_utc
                   LEFT JOIN sectors sec ON sec.ticker = s.ticker
                   GROUP BY COALESCE(sec.sector, 'Unknown')
                   ORDER BY mentions DESC"""
            ).fetchall()
    else:
        latest = latest_fetched_utc(source)
        if not latest:
            return []
        with db() as conn:
            rows = conn.execute(
                """SELECT
                       COALESCE(sec.sector, 'Unknown') AS sector,
                       SUM(s.mentions)                AS mentions,
                       SUM(s.mentions_24h_ago)        AS mentions_24h_ago,
                       COUNT(*)                       AS tickers,
                       GROUP_CONCAT(s.ticker)         AS ticker_list
                   FROM snapshots s
                   LEFT JOIN sectors sec ON sec.ticker = s.ticker
                   WHERE s.source = ? AND s.fetched_utc = ?
                   GROUP BY COALESCE(sec.sector, 'Unknown')
                   ORDER BY mentions DESC""",
                (source, latest),
            ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["tickers_list"] = [t for t in (d.pop("ticker_list") or "").split(",")
                             if t]
        out.append(d)
    return out


# ── News + sentiment (yfinance + VADER) ──────────────────────────────
def _extract_news_fields(it: dict) -> dict | None:
    """Normalize a yfinance news item across old + new (>=0.2.40) shapes.
    Returns {headline, publisher, url, published_utc} or None to skip."""
    # New shape: data nested under 'content'. Old: flat.
    c = it.get("content") if isinstance(it.get("content"), dict) else it

    headline = c.get("title") or it.get("title")
    if not headline:
        return None

    publisher = ""
    prov = c.get("provider")
    if isinstance(prov, dict):
        publisher = prov.get("displayName") or ""
    elif isinstance(it.get("publisher"), str):
        publisher = it["publisher"]

    url = ""
    cu = c.get("canonicalUrl")
    if isinstance(cu, dict):
        url = cu.get("url") or ""
    if not url:
        ct = c.get("clickThroughUrl")
        if isinstance(ct, dict):
            url = ct.get("url") or ""
    if not url:
        url = it.get("link") or ""

    pub = c.get("pubDate") or it.get("providerPublishTime")
    try:
        if isinstance(pub, str):
            from datetime import datetime
            pub_ts = int(datetime.fromisoformat(
                pub.replace("Z", "+00:00")).timestamp())
        elif isinstance(pub, (int, float)):
            pub_ts = int(pub)
        else:
            pub_ts = int(time.time())
    except Exception:
        pub_ts = int(time.time())

    return {"headline": headline, "publisher": publisher or None,
            "url": url, "published_utc": pub_ts}


def fetch_news_for_top(limit: int = 50, verbose: bool = False) -> int:
    """Pull recent news headlines for top-N tickers (across ALL sources),
    score each headline with VADER, persist. Returns count of new items."""
    with db() as conn:
        rows = conn.execute(
            """SELECT DISTINCT ticker FROM snapshots
               WHERE fetched_utc IN (
                   SELECT MAX(fetched_utc) FROM snapshots GROUP BY source
               )
               ORDER BY rank ASC
               LIMIT ?""",
            (limit,),
        ).fetchall()
    tickers = [r["ticker"] for r in rows if _valid_ticker(r["ticker"])]
    new = 0
    for i, t in enumerate(tickers, 1):
        try:
            items = yf.Ticker(t).news or []
        except Exception as e:
            if verbose:
                print(f"  [{i:>2}/{len(tickers)}] {t:<5} ! {e}", file=sys.stderr)
            continue
        added_this = 0
        for it in items:
            fields = _extract_news_fields(it)
            if not fields:
                continue
            compound = _vader.polarity_scores(fields["headline"])["compound"]
            try:
                with db() as conn:
                    cur = conn.execute(
                        """INSERT OR IGNORE INTO news_items
                           (ticker, headline, publisher, url, published_utc,
                            sentiment_compound, ingested_utc)
                           VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        (t, fields["headline"], fields["publisher"],
                         fields["url"], fields["published_utc"],
                         compound, int(time.time())),
                    )
                    if cur.rowcount:
                        added_this += 1
            except Exception as e:
                if verbose:
                    print(f"  ! insert failed for {t}: {e}", file=sys.stderr)
        new += added_this
        if verbose:
            print(f"  [{i:>2}/{len(tickers)}] {t:<5} +{added_this} headlines",
                  file=sys.stderr)
        time.sleep(0.15)
    return new


def sentiment_for_tickers(tickers: list[str], days: int = 7) -> dict[str, dict]:
    """Aggregate compound sentiment per ticker over the last N days."""
    cutoff = int(time.time()) - days * 86400
    out: dict[str, dict] = {}
    if not tickers:
        return out
    placeholders = ",".join("?" * len(tickers))
    with db() as conn:
        rows = conn.execute(
            f"""SELECT ticker,
                       AVG(sentiment_compound) AS avg_score,
                       COUNT(*)                AS n
                FROM news_items
                WHERE ticker IN ({placeholders}) AND published_utc >= ?
                GROUP BY ticker""",
            (*[t.upper() for t in tickers], cutoff),
        ).fetchall()
    for r in rows:
        s = r["avg_score"]
        if s is None:
            continue
        if s >= 0.20:
            label = "Bullish"
        elif s >= 0.05:
            label = "Mild bullish"
        elif s <= -0.20:
            label = "Bearish"
        elif s <= -0.05:
            label = "Mild bearish"
        else:
            label = "Neutral"
        out[r["ticker"]] = {"score": round(s, 3), "articles": r["n"],
                            "label": label}
    return out


def recent_news(ticker: str, limit: int = 8) -> list[dict]:
    with db() as conn:
        rows = conn.execute(
            """SELECT headline, publisher, url, published_utc, sentiment_compound
               FROM news_items
               WHERE ticker = ?
               ORDER BY published_utc DESC
               LIMIT ?""",
            (ticker.upper(), limit),
        ).fetchall()
    return [dict(r) for r in rows]


def fetch_all_sources_and_refresh_sectors() -> dict:
    """Scheduled job: pull a snapshot for every source, then lazily backfill
    sectors for any new tickers AND pull recent news headlines."""
    sources = fetch_all_sources()
    result: dict = {"sources": sources}
    try:
        result["sectors_added"] = refresh_sectors_for_top(limit=80)
    except Exception as e:
        print(f"!! sector refresh failed: {e}", file=sys.stderr)
    try:
        result["news_added"] = fetch_news_for_top(limit=80)
    except Exception as e:
        print(f"!! news fetch failed: {e}", file=sys.stderr)
    return result


# ── Flask ────────────────────────────────────────────────────────────
app = Flask(__name__)


@app.route("/")
def root():
    return send_from_directory(Path(__file__).parent, "index.html")


@app.route("/api/health")
def health():
    source = _resolve_source(request.args.get("source"))
    latest = latest_fetched_utc(source)
    with db() as conn:
        snapshots = conn.execute(
            "SELECT COUNT(DISTINCT fetched_utc) FROM snapshots WHERE source = ?",
            (source,),
        ).fetchone()[0]
        rows = conn.execute(
            "SELECT COUNT(*) FROM snapshots WHERE source = ?", (source,),
        ).fetchone()[0]
    return jsonify({
        "ok": True,
        "source": source,
        "snapshots": snapshots,
        "rows": rows,
        "latest_fetched_utc": latest,
        "age_minutes": (int(time.time()) - latest) // 60 if latest else None,
    })


@app.route("/api/sources")
def api_sources():
    """Channels available in the UI selector."""
    return jsonify(SOURCES)


@app.route("/api/top")
def api_top():
    return jsonify(top_tickers(
        limit=int(request.args.get("limit", 20)),
        source=request.args.get("source"),
    ))


@app.route("/api/trending")
def api_trending():
    return jsonify(trending(
        limit=int(request.args.get("limit", 20)),
        source=request.args.get("source"),
    ))


@app.route("/api/history/<ticker>")
def api_history(ticker):
    return jsonify(history(
        ticker,
        hours=int(request.args.get("hours", 48)),
        source=request.args.get("source"),
    ))


@app.route("/api/history-bulk")
def api_history_bulk():
    tickers = [t for t in (request.args.get("tickers") or "").split(",") if t]
    return jsonify(history_bulk(
        tickers,
        hours=int(request.args.get("hours", 24)),
        source=request.args.get("source"),
    ))


@app.route("/api/prices")
def api_prices():
    tickers = [t for t in (request.args.get("tickers") or "").split(",") if t]
    return jsonify(get_prices(tickers))


@app.route("/api/sectors")
def api_sectors():
    return jsonify(sector_buzz(source=request.args.get("source")))


@app.route("/api/sentiment")
def api_sentiment():
    tickers = [t for t in (request.args.get("tickers") or "").split(",") if t]
    days = int(request.args.get("days", 7))
    return jsonify(sentiment_for_tickers(tickers, days=days))


@app.route("/api/news/<ticker>")
def api_news(ticker):
    limit = int(request.args.get("limit", 8))
    return jsonify(recent_news(ticker, limit=limit))


@app.route("/api/channels/<ticker>")
def api_channels(ticker):
    """Per-source mention counts for a ticker — each source's latest snapshot.
    Used in the detail modal to show whether chatter is WSB-only or broad."""
    t = ticker.upper()
    with db() as conn:
        rows = conn.execute(
            _LATEST_PER_SOURCE_CTE +
            """SELECT s.source, s.mentions, s.rank
               FROM snapshots s
               JOIN latest_per_source l
                 ON l.source = s.source AND l.fu = s.fetched_utc
               WHERE s.ticker = ?""",
            (t,),
        ).fetchall()
    # Always return one row per known real source, even if 0 mentions.
    by_source = {r["source"]: dict(r) for r in rows}
    out = []
    for s in REAL_SOURCES:
        r = by_source.get(s["key"], {})
        out.append({
            "source": s["key"],
            "label": s["short"],
            "mentions": r.get("mentions") or 0,
            "rank": r.get("rank"),
        })
    return jsonify(out)


@app.route("/api/search")
def api_search():
    """Ticker / company-name search across the most recent snapshot of any
    source. Returns up to 20 matches ordered by mention count."""
    q = (request.args.get("q") or "").strip().upper()
    if not q:
        return jsonify([])
    pattern = f"%{q}%"
    # Match on either the ticker prefix (preferred) or anywhere in the name.
    with db() as conn:
        rows = conn.execute(
            """SELECT s.ticker,
                      MAX(s.name)     AS name,
                      SUM(s.mentions) AS mentions
               FROM snapshots s
               WHERE s.fetched_utc IN (
                       SELECT MAX(fetched_utc) FROM snapshots GROUP BY source
                     )
                 AND (s.ticker LIKE ? OR UPPER(s.name) LIKE ?)
               GROUP BY s.ticker
               ORDER BY
                   CASE WHEN s.ticker = ? THEN 0
                        WHEN s.ticker LIKE ? THEN 1
                        ELSE 2 END,
                   mentions DESC
               LIMIT 20""",
            (f"{q}%", pattern, q, f"{q}%"),
        ).fetchall()
    return jsonify([dict(r) for r in rows])


# ── CLI ──────────────────────────────────────────────────────────────
def _print_table(rows: list[dict], cols: list[tuple[str, str]]) -> None:
    if not rows:
        print("(no data)")
        return
    widths = []
    for key, header in cols:
        w = max(len(header), *(len(str(r.get(key, ''))) for r in rows))
        widths.append(w)
    print()
    print("  ".join(f"{h:<{w}}" for (_, h), w in zip(cols, widths)))
    print("  ".join("-" * w for w in widths))
    for r in rows:
        print("  ".join(f"{str(r.get(k, '')):<{w}}" for (k, _), w in zip(cols, widths)))
    print()


def cli() -> None:
    parser = argparse.ArgumentParser(prog="hivemind")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_f = sub.add_parser("fetch", help="Pull one snapshot from ApeWisdom now")
    p_f.add_argument("--source", default=FETCH_DEFAULT,
                     choices=[s["key"] for s in REAL_SOURCES])
    p_f.add_argument("--all-sources", action="store_true",
                     help="Fetch every channel in one tick")

    p_top = sub.add_parser("top", help="Show current top tickers")
    p_top.add_argument("--limit", type=int, default=20)
    p_top.add_argument("--source", default=DEFAULT_SOURCE,
                       choices=[s["key"] for s in SOURCES])

    p_tr = sub.add_parser("trending", help="Biggest rank movers vs 24h ago")
    p_tr.add_argument("--limit", type=int, default=20)
    p_tr.add_argument("--source", default=DEFAULT_SOURCE,
                      choices=[s["key"] for s in SOURCES])

    p_h = sub.add_parser("history", help="Time-series for one ticker")
    p_h.add_argument("ticker")
    p_h.add_argument("--hours", type=int, default=48)
    p_h.add_argument("--source", default=DEFAULT_SOURCE,
                     choices=[s["key"] for s in SOURCES])

    p_sec = sub.add_parser("sectors", help="Backfill sector data for top tickers")
    p_sec.add_argument("--limit", type=int, default=100)
    p_sec.add_argument("--source", default=DEFAULT_SOURCE,
                       choices=[s["key"] for s in SOURCES])

    p_news = sub.add_parser("news", help="Pull recent news headlines + score sentiment")
    p_news.add_argument("--limit", type=int, default=50)

    sub.add_parser("serve", help="Start Flask + background scheduler")

    args = parser.parse_args()
    init_db()

    if args.cmd == "fetch":
        t0 = time.time()
        if args.all_sources:
            results = fetch_all_sources()
            for r in results:
                print(f"  [{r['source']:<16}] +{r['tickers']} tickers")
        else:
            results = fetch_snapshot(args.source)
            print(f"  [{results['source']}] +{results['tickers']} tickers")
        print(f"fetched in {time.time()-t0:.1f}s")
        return

    if args.cmd == "top":
        rows = top_tickers(limit=args.limit, source=args.source)
        if not rows:
            print(f"(no snapshots yet for {args.source} — run `fetch` first)")
            return
        _print_table(rows, [("rank", "#"), ("ticker", "TICKER"),
                            ("name", "NAME"), ("mentions", "MENTIONS")])
        return

    if args.cmd == "trending":
        rows = trending(limit=args.limit, source=args.source)
        _print_table(rows, [("ticker", "TICKER"), ("name", "NAME"),
                            ("rank", "RANK"), ("rank_24h_ago", "WAS"),
                            ("rank_delta", "Δ"),
                            ("mentions", "NOW"), ("mentions_24h_ago", "24h")])
        return

    if args.cmd == "history":
        rows = history(args.ticker, hours=args.hours, source=args.source)
        if not rows:
            print(f"(no history for {args.ticker} in last {args.hours}h)")
            return
        for r in rows:
            ts = time.strftime("%Y-%m-%d %H:%M", time.localtime(r["fetched_utc"]))
            print(f"  {ts}  rank {r['rank']:>3}  mentions {r['mentions']:>5}")
        return

    if args.cmd == "sectors":
        t0 = time.time()
        n = refresh_sectors_for_top(limit=args.limit)
        print(f"sector lookup done in {time.time()-t0:.1f}s — added/refreshed {n}")
        rows = sector_buzz(source=args.source)
        _print_table(rows, [("sector", "SECTOR"), ("mentions", "MENTIONS"),
                            ("tickers", "TICKERS")])
        return

    if args.cmd == "news":
        t0 = time.time()
        n = fetch_news_for_top(limit=args.limit, verbose=True)
        print(f"\nnews fetch done in {time.time()-t0:.1f}s — added {n} new items")
        # Show the top tickers + their aggregated sentiment.
        with db() as conn:
            tickers = [r["ticker"] for r in conn.execute(
                "SELECT ticker FROM snapshots WHERE fetched_utc = ? "
                "ORDER BY rank ASC LIMIT 15",
                (latest_fetched_utc() or 0,),
            ).fetchall()]
        sentiment = sentiment_for_tickers(tickers, days=7)
        rows = [{"ticker": t, **(sentiment.get(t) or {})} for t in tickers]
        _print_table(rows, [("ticker", "TICKER"), ("label", "SENTIMENT"),
                            ("score", "SCORE"), ("articles", "ARTICLES")])
        return

    if args.cmd == "serve":
        scheduler = BackgroundScheduler(daemon=True)
        scheduler.add_job(fetch_all_sources_and_refresh_sectors,
                          "interval", minutes=FETCH_INTERVAL_MIN)
        scheduler.start()
        port = int(os.environ.get("PORT", 5052))
        print(f"Hivemind serving on http://127.0.0.1:{port}  "
              f"(fetching every {FETCH_INTERVAL_MIN}m)")
        app.run(host="0.0.0.0", port=port, debug=False)
        return


if __name__ == "__main__":
    cli()
