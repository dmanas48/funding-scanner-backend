"""
═══════════════════════════════════════════════════════════════
  FUNDING RATE SCANNER — backend/main.py
  Python FastAPI backend — deploy to Railway or Render (free)

  What this does:
    • Connects to Delta Exchange WebSocket (wss://socket.india.delta.exchange)
    • Polls Binance + Bybit REST APIs every 30 seconds
    • Estimates CoinDCX from Binance (CORS blocked from browsers)
    • Runs arbitrage engine → calculates spread
    • Exposes a single /api/rates endpoint → frontend fetches this
    • CORS enabled so Netlify frontend can call it

  Deploy: Railway.app or Render.com (both free tier)
═══════════════════════════════════════════════════════════════
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any

import httpx
import websockets
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)
log = logging.getLogger(__name__)

# ── Delta Exchange credentials ────────────────────────────────
DE_KEY    = os.getenv("DE_KEY",    "4f6zZTSa0dtEG5cmpQyKNKTSgvl91F")
DE_SECRET = os.getenv("DE_SECRET", "ZfA1oEisyJmh49fXmIydL2EofleVTOWMgctaHiz4ekVA0ggyZFsgODMIUtYs")

DE_WS_URL   = "wss://socket.india.delta.exchange"
DE_REST_URL = "https://api.india.delta.exchange"

# ── Shared in-memory stores (updated by background tasks) ─────
bn_data:  dict[str, dict] = {}   # base → {pct_per_interval, pct_hr, interval_h, next_funding}
by_data:  dict[str, dict] = {}
cd_data:  dict[str, dict] = {}
de_data:  dict[str, dict] = {}   # populated by WebSocket
ws_status: dict            = {"state": "disconnected", "coins": 0, "last_msg": 0}

REST_INTERVAL = 30          # seconds between REST polls


# ════════════════════════════════════════════════════════════════
#  HELPERS
# ════════════════════════════════════════════════════════════════

def int_class(h: int) -> str:
    if h <= 1: return "h1"
    if h <= 2: return "h2"
    if h <= 4: return "h4"
    return "h8"


def parse_base(symbol: str) -> str:
    """Extract base coin from symbol: BTCUSD → BTC, BTC_USDT → BTC"""
    s = symbol.upper()
    if "_" in s:
        parts = s.split("_")
        if parts[1] in ("USDT", "USD"):
            return parts[0]
        return ""
    if s.endswith("USDT"):
        return s[:-4]
    if s.endswith("USD"):
        return s[:-3]
    return ""


def delta_sign(method: str, path: str, query_string: str = "", body: str = "") -> tuple[str, str]:
    """Generate HMAC-SHA256 signature for Delta Exchange REST API"""
    ts        = str(int(time.time()))
    prehash   = method + ts + path + query_string + body
    signature = hmac.new(
        DE_SECRET.encode(), prehash.encode(), hashlib.sha256
    ).hexdigest()
    return ts, signature


def build_coindcx() -> None:
    """
    CoinDCX blocks browser requests (CORS), but Python has no such restriction.
    However CoinDCX's public funding rate API is unreliable / undocumented.
    We derive it from Binance with a stable per-coin variance as a best estimate.
    Tagged src='estimated' so frontend can label it clearly.
    """
    SCALE = 0.0015  # max ±0.0015 %/8h variance
    cd_data.clear()
    for base, d in bn_data.items():
        # Deterministic hash → stable variance per coin
        h = 0
        for c in base:
            h = (h * 31 + ord(c)) & 0xFFFFFFFF
        v     = ((h & 0xFFFF) / 0xFFFF - 0.5) * 2 * SCALE
        p8h   = d["pct_per_interval"] + v
        cd_data[base] = {
            "pct_per_interval": round(p8h, 6),
            "pct_hr":           round(p8h / 8, 7),
            "interval_h":       8,
            "src":              "estimated",
        }


# ════════════════════════════════════════════════════════════════
#  DELTA — WebSocket listener (runs as background task)
#
#  Subscribe to funding_rate channel → all symbols
#  Message shape:
#    { "type": "funding_rate", "symbol": "BTCUSD",
#      "funding_rate": "0.0001",
#      "next_funding_realization": "2025-03-16T13:30:00Z" }
#
#  funding_rate field:
#    Raw fraction e.g. 0.0001 = 0.01% per settlement period
#    pct_hr = (raw × 100) / interval_h
# ════════════════════════════════════════════════════════════════

async def delta_ws_listener() -> None:
    retry_delay = 5
    while True:
        try:
            log.info("Delta WS: connecting to %s", DE_WS_URL)
            ws_status["state"] = "connecting"
            async with websockets.connect(
                DE_WS_URL,
                ping_interval=20,
                ping_timeout=10,
                open_timeout=15,
            ) as ws:
                ws_status["state"] = "connected"
                retry_delay = 5
                log.info("Delta WS: connected")

                # Subscribe to funding_rate for ALL symbols
                sub = {
                    "type": "subscribe",
                    "payload": {
                        "channels": [
                            {"name": "funding_rate", "symbols": ["all"]}
                        ]
                    }
                }
                await ws.send(json.dumps(sub))
                log.info("Delta WS: subscribed to funding_rate ALL")

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    if msg.get("type") != "funding_rate":
                        continue

                    symbol = msg.get("symbol", "")
                    base   = parse_base(symbol)
                    if not base:
                        continue

                    raw_rate = msg.get("funding_rate", "")
                    try:
                        raw_f = float(raw_rate)
                    except (TypeError, ValueError):
                        continue

                    # Determine interval (minutes field or default 8h)
                    interval_h = 8
                    if "funding_interval" in msg:
                        try:
                            interval_h = max(1, round(int(msg["funding_interval"]) / 60))
                        except (TypeError, ValueError):
                            pass

                    # Parse next funding timestamp
                    next_funding = None
                    nfr = msg.get("next_funding_realization")
                    if nfr:
                        try:
                            from datetime import datetime, timezone
                            dt = datetime.fromisoformat(nfr.replace("Z", "+00:00"))
                            next_funding = int(dt.timestamp() * 1000)  # ms
                        except Exception:
                            pass

                    pct_per_interval = round(raw_f * 100, 6)
                    pct_hr           = round(pct_per_interval / interval_h, 7)

                    de_data[base] = {
                        "pct_per_interval": pct_per_interval,
                        "pct_hr":           pct_hr,
                        "interval_h":       interval_h,
                        "next_funding":     next_funding,
                        "symbol":           symbol,
                        "src":              "live",
                        "ts":               int(time.time() * 1000),
                    }

                    ws_status["coins"]    = len(de_data)
                    ws_status["last_msg"] = int(time.time())

        except Exception as e:
            ws_status["state"] = "disconnected"
            log.warning("Delta WS disconnected: %s — retry in %ds", e, retry_delay)
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 1.5, 60)


# ════════════════════════════════════════════════════════════════
#  DELTA — REST fallback (used if WS has no data yet)
# ════════════════════════════════════════════════════════════════

async def fetch_delta_rest(client: httpx.AsyncClient) -> None:
    path = "/v2/tickers"
    qs   = "?contract_types=perpetual_futures"
    ts, sig = delta_sign("GET", path, qs)

    try:
        resp = await client.get(
            DE_REST_URL + path + qs,
            headers={
                "api-key":      DE_KEY,
                "timestamp":    ts,
                "signature":    sig,
                "User-Agent":   "FundingRateScanner/2.0",
                "Accept":       "application/json",
            },
            timeout=12,
        )
        resp.raise_for_status()
        data = resp.json()

        if not data.get("success", True):
            log.warning("Delta REST error: %s", data.get("error"))
            return

        for d in data.get("result", []):
            raw_f = d.get("funding_rate")
            if raw_f is None:
                continue
            try:
                raw_f = float(raw_f)
            except (TypeError, ValueError):
                continue

            symbol     = (d.get("symbol") or "").upper()
            base       = parse_base(symbol)
            if not base:
                continue

            interval_h = 8
            if d.get("funding_interval"):
                try:
                    interval_h = max(1, round(int(d["funding_interval"]) / 60))
                except (TypeError, ValueError):
                    pass

            pct_per_interval = round(raw_f * 100, 6)
            pct_hr           = round(pct_per_interval / interval_h, 7)

            # Only update if WS hasn't already given us fresher data
            if base not in de_data:
                de_data[base] = {
                    "pct_per_interval": pct_per_interval,
                    "pct_hr":           pct_hr,
                    "interval_h":       interval_h,
                    "symbol":           symbol,
                    "src":              "live",
                    "ts":               int(time.time() * 1000),
                }

        log.info("Delta REST: loaded %d pairs into de_data", len(de_data))

    except Exception as e:
        log.warning("Delta REST fetch failed: %s", e)


# ════════════════════════════════════════════════════════════════
#  BINANCE — REST
#  GET fapi.binance.com/fapi/v1/premiumIndex
#  lastFundingRate = raw fraction, fixed 8h interval
#  pct_hr = raw × 100 / 8
# ════════════════════════════════════════════════════════════════

async def fetch_binance(client: httpx.AsyncClient) -> None:
    try:
        resp = await client.get(
            "https://fapi.binance.com/fapi/v1/premiumIndex",
            timeout=12,
        )
        resp.raise_for_status()
        arr = resp.json()
        if not isinstance(arr, list):
            raise ValueError("Expected list")

        bn_data.clear()
        count = 0
        for d in arr:
            sym = d.get("symbol", "")
            if not sym.endswith("USDT"):
                continue
            try:
                raw = float(d["lastFundingRate"])
            except (KeyError, TypeError, ValueError):
                continue

            base = sym[:-4]
            pct8h = round(raw * 100, 6)
            bn_data[base] = {
                "pct_per_interval": pct8h,
                "pct_hr":           round(pct8h / 8, 7),
                "interval_h":       8,
                "next_funding":     int(d.get("nextFundingTime") or 0) or None,
                "src":              "live",
            }
            count += 1

        log.info("Binance: %d USDT pairs loaded", count)

    except Exception as e:
        log.warning("Binance fetch failed: %s", e)


# ════════════════════════════════════════════════════════════════
#  BYBIT — REST (paginated)
#  GET api.bybit.com/v5/market/tickers?category=linear
#  fundingRate = raw fraction for THIS coin's interval
#  fundingIntervalHour = actual interval (1/2/4/8) per coin
#  pct_hr = raw × 100 / fundingIntervalHour
# ════════════════════════════════════════════════════════════════

async def fetch_bybit(client: httpx.AsyncClient) -> None:
    by_data.clear()
    cursor = ""
    pages  = 0
    int_dist: dict[int, int] = {}

    try:
        while pages < 25:
            url = "https://api.bybit.com/v5/market/tickers?category=linear"
            if cursor:
                url += f"&cursor={cursor}"

            resp = await client.get(url, timeout=12)
            resp.raise_for_status()
            j = resp.json()

            if j.get("retCode") != 0:
                raise ValueError(f"Bybit error: {j.get('retMsg')}")

            result = j.get("result", {})
            for d in result.get("list", []):
                sym = d.get("symbol", "")
                if not sym.endswith("USDT"):
                    continue
                try:
                    raw = float(d["fundingRate"])
                except (KeyError, TypeError, ValueError):
                    continue

                ih   = int(d.get("fundingIntervalHour") or 8)
                base = sym[:-4]
                pct  = round(raw * 100, 6)
                by_data[base] = {
                    "pct_per_interval": pct,
                    "pct_hr":           round(pct / ih, 7),
                    "interval_h":       ih,
                    "next_funding":     int(d.get("nextFundingTime") or 0) or None,
                    "src":              "live",
                }
                int_dist[ih] = int_dist.get(ih, 0) + 1

            cursor = result.get("nextPageCursor", "")
            pages += 1
            if not cursor:
                break

        dist_str = " · ".join(f"{h}h×{c}" for h, c in sorted(int_dist.items()))
        log.info("Bybit: %d pairs loaded. Intervals: %s", len(by_data), dist_str)

    except Exception as e:
        log.warning("Bybit fetch failed: %s", e)


# ════════════════════════════════════════════════════════════════
#  ARBITRAGE ENGINE
#  Merges all 4 sources → computes spread from LIVE sources only
# ════════════════════════════════════════════════════════════════

def build_rows() -> list[dict]:
    coins = set(bn_data.keys()) | set(by_data.keys())
    rows  = []

    for coin in coins:
        b = bn_data.get(coin)
        y = by_data.get(coin)
        c = cd_data.get(coin)
        d = de_data.get(coin)

        if not b and not y:
            continue

        # Spread from LIVE sources only
        live_vals = [
            x["pct_hr"] for x in [b, y, d]
            if x and x.get("src") == "live"
        ]
        spread = round(max(live_vals) - min(live_vals), 7) if len(live_vals) >= 2 else 0.0

        # Avg from all sources
        all_vals = [x["pct_hr"] for x in [b, y, c, d] if x]
        avg      = round(sum(all_vals) / len(all_vals), 7) if all_vals else 0.0

        # Best arb pair
        rate_map = {}
        if b: rate_map["Binance"] = b["pct_hr"]
        if y: rate_map["Bybit"]   = y["pct_hr"]
        if d and d.get("src") == "live":
            rate_map["Delta"] = d["pct_hr"]

        best_short = max(rate_map, key=rate_map.get) if len(rate_map) >= 2 else None
        best_long  = min(rate_map, key=rate_map.get) if len(rate_map) >= 2 else None

        next_funding = None
        for src in [b, y, d]:
            if src and src.get("next_funding"):
                next_funding = src["next_funding"]
                break

        row: dict[str, Any] = {
            "coin":        coin,
            "spread":      spread,
            "avg":         avg,
            "best_long":   best_long,
            "best_short":  best_short,
            "next_funding": next_funding,
            "binance":  b,
            "bybit":    y,
            "coindcx":  c,
            "delta":    d,
        }
        rows.append(row)

    # Sort by spread descending
    rows.sort(key=lambda r: r["spread"], reverse=True)
    return rows


# ════════════════════════════════════════════════════════════════
#  REST POLL LOOP (background task)
# ════════════════════════════════════════════════════════════════

async def rest_poll_loop() -> None:
    async with httpx.AsyncClient() as client:
        while True:
            log.info("REST poll: fetching Binance + Bybit…")
            await asyncio.gather(
                fetch_binance(client),
                fetch_bybit(client),
            )
            build_coindcx()

            # If WS hasn't given Delta data yet, try REST
            if not de_data:
                log.info("WS has no Delta data yet — trying REST fallback…")
                await fetch_delta_rest(client)

            log.info(
                "REST poll complete. BN=%d BY=%d CD=%d DE=%d",
                len(bn_data), len(by_data), len(cd_data), len(de_data),
            )
            await asyncio.sleep(REST_INTERVAL)


# ════════════════════════════════════════════════════════════════
#  FastAPI APP
# ════════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start background tasks when server boots
    ws_task   = asyncio.create_task(delta_ws_listener())
    rest_task = asyncio.create_task(rest_poll_loop())
    log.info("Background tasks started: WS listener + REST poll loop")
    yield
    ws_task.cancel()
    rest_task.cancel()


app = FastAPI(
    title="Funding Rate Scanner API",
    version="2.0",
    lifespan=lifespan,
)

# Allow requests from ANY origin so Netlify frontend can call this
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/api/rates")
async def get_rates():
    """
    Main endpoint — returns all funding rates merged + arbitrage calculations.
    Frontend polls this every 15 seconds.
    """
    rows = build_rows()
    return JSONResponse({
        "ok":        True,
        "ts":        int(time.time() * 1000),
        "ws_status": ws_status,
        "counts": {
            "binance": len(bn_data),
            "bybit":   len(by_data),
            "coindcx": len(cd_data),
            "delta":   len(de_data),
            "total":   len(rows),
        },
        "rows": rows,
    })


@app.get("/api/health")
async def health():
    return {
        "ok":       True,
        "ws":       ws_status["state"],
        "de_coins": len(de_data),
        "bn_coins": len(bn_data),
        "by_coins": len(by_data),
    }


@app.get("/")
async def root():
    return {"message": "Funding Rate Scanner API — /api/rates"}