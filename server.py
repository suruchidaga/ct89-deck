"""
Local read-only dashboard for Zerodha positions and orders via Kite Connect.

Run:  py server.py   then open http://127.0.0.1:5000
In your Kite Connect app settings, set the redirect URL to http://127.0.0.1:5000/callback

Market data comes from one of:
  - Dhan (if DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN are set in .env): quotes polled every second,
    option chain with previous-day OI, daily history. Works with the free Kite Connect Personal plan.
  - Kite (paid Kite Connect plan): KiteTicker stream, quotes and historical data.
Positions and orders always come from Kite and are re-fetched every few seconds.
This server never places, modifies or cancels orders.
"""
import json
import math
import os
import re
import sys
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, jsonify, redirect, request, send_file
from kiteconnect import KiteConnect, KiteTicker

import requests

from margin_calc import MarginCalc

load_dotenv(Path(__file__).with_name(".env"))
API_KEY = os.environ.get("KITE_API_KEY", "").strip()
API_SECRET = os.environ.get("KITE_API_SECRET", "").strip()
HERE = Path(__file__).parent
# KITE_SOURCE=mcp: account data and market data through Zerodha's Kite MCP server (no Kite Connect app needed)
USE_MCP = os.environ.get("KITE_SOURCE", "").strip().lower() == "mcp"
# Without Kite keys (and not using Kite MCP) the server still runs, in demo mode: the dashboard shows its
# sample book, and news and the margin calculator work for real.
DEMO_SERVER = not USE_MCP and not (API_KEY and API_SECRET)
DHAN_CLIENT_ID = os.environ.get("DHAN_CLIENT_ID", "").strip()
DHAN_ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN", "").strip()
mcp = None
if USE_MCP:
    from kite_mcp import KiteMCP, McpFeed, McpError, NotLoggedIn
    mcp = KiteMCP(HERE / ".kite_mcp_session.json")
    dhan = McpFeed(mcp, HERE)     # same interface as DhanFeed, so the Dhan code paths serve Kite MCP too
    DhanError = McpError
elif DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN:
    from dhan_feed import DhanFeed, DhanError
    dhan = DhanFeed(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, HERE)
else:
    dhan = None
    DhanError = Exception
DHAN_QUOTE_SECS = 2.0 if USE_MCP else 1.0   # Kite MCP is shared infrastructure: poll it gently
TOKEN_FILE = HERE / ".access_token.json"
PNL_RANGE_FILE = HERE / ".pnl_range.json"
HISTORY_FILE = HERE / ".pnl_history.json"
POLL_SECS = 5
HISTORY_MAX = 6 * 3600 // POLL_SECS  # ~6 hours of P&L samples

app = Flask(__name__)
kite = mcp if USE_MCP else KiteConnect(api_key=API_KEY or "demo")
margin_calc = MarginCalc()
lock = threading.Lock()
state = {
    "logged_in": False,
    "user": None,
    "positions": [],
    "orders": [],
    "ltp": {},          # instrument_token -> live price
    "history": [],      # [epoch_ms, total_pnl, day_pnl]
    "last_rest": None,
    "last_tick": None,
    "ticker": "disconnected",
    "error": None,
    "ohlc": {},         # instrument_token -> {open, high, low, close}
    "pnl_range": None,  # today's high/low of total and day P&L, see track_pnl_range
}
ticker = None
subscribed = set()
dhan_loop_started = False


# ---------- auth ----------

def save_token(token):
    TOKEN_FILE.write_text(json.dumps({"date": date.today().isoformat(), "access_token": token}))


def try_cached_token():
    if USE_MCP:
        # reuse today's Kite MCP session if it's still logged in
        if not mcp.resume():
            return False
        try:
            profile = mcp.profile()
        except Exception:
            return False
        on_login(None, profile)
        return True
    if not TOKEN_FILE.exists():
        return False
    cached = json.loads(TOKEN_FILE.read_text())
    if cached.get("date") != date.today().isoformat():
        return False
    kite.set_access_token(cached["access_token"])
    try:
        profile = kite.profile()
    except Exception:
        return False
    on_login(cached["access_token"], profile)
    return True


def on_login(token, profile):
    with lock:
        state["logged_in"] = True
        state["user"] = profile.get("user_name") or profile.get("user_id")
    refresh_rest()
    if dhan:
        global dhan_loop_started
        if not dhan_loop_started:     # one polling loop, however many times you log in
            dhan_loop_started = True
            threading.Thread(target=dhan_price_loop, daemon=True).start()
    else:
        start_ticker(token)


@app.route("/login")
def login():
    if USE_MCP:
        # Kite MCP: get a Zerodha login link for our MCP session, then watch for the login to complete.
        # Each new link cancels the previous one, so reuse the same link for 10 minutes: otherwise a second
        # tab (auto-opened page + button click) breaks the first with "missing MCP session_id".
        global mcp_login_link
        url, made = mcp_login_link
        if not url or time.time() - made > 600 or mcp_login_sid != mcp.sid:
            try:
                url = mcp.login_url()
            except Exception as e:
                return f"Couldn't get a login link from Kite MCP: {e}", 502
            mcp_login_link = (url, time.time())
            set_login_sid(mcp.sid)
        with lock:
            state["login_warning"] = mcp.login_warning
        watch_mcp_login()
        return redirect(url)
    return redirect(kite.login_url())


mcp_watch_running = False
mcp_login_link = (None, 0)   # (link, when it was issued)
mcp_login_sid = None         # MCP session the link belongs to


def set_login_sid(sid):
    global mcp_login_sid
    mcp_login_sid = sid


def watch_mcp_login(minutes=15):
    """After you open the Kite MCP login link, check every few seconds until the session is logged in."""
    global mcp_watch_running
    if mcp_watch_running:
        return
    mcp_watch_running = True

    def run():
        global mcp_watch_running
        try:
            end = time.time() + minutes * 60
            while time.time() < end:
                time.sleep(4)
                try:
                    profile = mcp.profile()
                except Exception:
                    continue
                print("Kite MCP login complete.")
                global mcp_login_link
                mcp_login_link = (None, 0)    # used up
                on_login(None, profile)
                return
        finally:
            mcp_watch_running = False
    threading.Thread(target=run, daemon=True).start()


login_page_opened_at = 0


def open_login_page(port=5000):
    """Open Kite's login page in the default browser (at most once every 10 minutes). Login stays manual:
    exchange rules require you to enter your password and TOTP yourself."""
    global login_page_opened_at
    if time.time() - login_page_opened_at < 600:
        return
    login_page_opened_at = time.time()
    import webbrowser
    threading.Timer(1.5, lambda: webbrowser.open(f"http://127.0.0.1:{port}/login")).start()


def is_session_expired(e):
    if USE_MCP and isinstance(e, NotLoggedIn):
        return True
    if USE_MCP and isinstance(e, McpError):
        # an expired Kite token can come back as a generic "Failed to execute ..." error: check the profile
        try:
            mcp.profile()
            return False
        except Exception:
            return True
    from kiteconnect import exceptions as kex
    return isinstance(e, kex.TokenException) or "TokenException" in type(e).__name__


def on_session_expired():
    """Kite sessions end every morning: drop the cached token, show the login screen, open the login page."""
    with lock:
        state["logged_in"] = False
        state["error"] = "Kite session expired. Log in again to resume live data."
    try:
        TOKEN_FILE.unlink()
    except FileNotFoundError:
        pass
    print("Kite session expired, opening the Kite login page...")
    open_login_page()


@app.route("/callback")
def callback():
    request_token = request.args.get("request_token")
    if not request_token:
        return "Missing request_token", 400
    session = kite.generate_session(request_token, api_secret=API_SECRET)
    token = session["access_token"]
    kite.set_access_token(token)
    save_token(token)
    on_login(token, session)
    return redirect("/")


# ---------- data ----------

def live_values(p, prices):
    """Live price, P&L and day P&L (m2m) for a Kite position."""
    mult = p.get("multiplier") or 1
    if dhan:
        # Kite Personal has no market data, so rebuild P&L from the position's values with Dhan prices
        # (same formulas Kite uses for pnl and m2m).
        price = prices.get(p["instrument_token"]) or p.get("last_price") or 0
        close = (state["ohlc"].get(p["instrument_token"]) or {}).get("close") or p.get("close_price") or 0
        pnl = (p["sell_value"] - p["buy_value"]) + p["quantity"] * price * mult
        m2m = ((p.get("day_sell_value") or 0) - (p.get("day_buy_value") or 0) + p["quantity"] * price * mult
               - (p.get("overnight_quantity") or 0) * close * mult)
        return price, pnl, m2m
    price = prices.get(p["instrument_token"], p["last_price"])
    move = p["quantity"] * (price - p["last_price"]) * mult
    return price, p["pnl"] + move, p["m2m"] + move


instruments = {}      # instrument_token -> instrument dump row
futures_by_name = {}  # (exchange, name) -> [future rows sorted by expiry]
options_by_name = {}  # (exchange, name) -> [option rows]
chain_cache = {}      # future token -> option chain summary for the simulator
by_symbol = {}        # (exchange, tradingsymbol) -> instrument row (maps Kite positions to Dhan IDs)
loaded_exchanges = set()


def load_instruments(exchanges):
    """Instrument list once per exchange (expiry, strike, lot size): from Dhan if configured, else Kite."""
    for ex in exchanges - loaded_exchanges:
        try:
            rows = dhan.instruments(ex) if dhan else kite.instruments(ex)
        except Exception as e:
            with lock:
                state["error"] = f"Instrument list for {ex} failed: {e}"
            continue
        for r in rows:
            instruments[r["instrument_token"]] = r
            by_symbol[(ex, r["tradingsymbol"])] = r
            if r["instrument_type"] == "FUT":
                futures_by_name.setdefault((ex, r["name"]), []).append(r)
            elif r["instrument_type"] in ("CE", "PE"):
                options_by_name.setdefault((ex, r["name"]), []).append(r)
        for lst in futures_by_name.values():
            lst.sort(key=lambda r: r["expiry"])
        loaded_exchanges.add(ex)


def underlying_future(inst):
    """The futures contract an option or future is priced off: same name, nearest expiry on/after its own."""
    if inst["instrument_type"] == "FUT":
        return inst
    futs = futures_by_name.get((inst["exchange"], inst["name"]), [])
    later = [f for f in futs if f["expiry"] >= inst["expiry"]]
    return later[0] if later else (futs[-1] if futs else None)


def option_chain(fut):
    """Nearest option expiry priced off this future, with its strikes (for the simulator)."""
    tok = fut["instrument_token"]
    if tok not in chain_cache:
        opts = [o for o in options_by_name.get((fut["exchange"], fut["name"]), [])
                if underlying_future(o) is fut]
        first = min((o["expiry"] for o in opts), default=None)
        chain_cache[tok] = {
            "opt_expiry": first.isoformat() if first else None,
            "strikes": sorted({o["strike"] for o in opts if o["expiry"] == first}),
            "lot_size": fut["lot_size"] or 1,
        }
    return chain_cache[tok]


def refresh_rest():
    try:
        positions = kite.positions()["net"]
        orders = kite.orders()
    except Exception as e:
        with lock:
            state["error"] = f"REST refresh failed: {e}"
        if is_session_expired(e):
            on_session_expired()
        return
    load_instruments({p["exchange"] for p in positions if p["exchange"] in ("NFO", "BFO", "MCX", "CDS", "BCD")})
    if dhan:
        # Key everything by Dhan security ID so prices and instrument details line up
        for p in positions:
            inst = by_symbol.get((p["exchange"], p["tradingsymbol"]))
            if inst:
                p["kite_token"], p["instrument_token"] = p["instrument_token"], inst["instrument_token"]
    # Underlying futures need prices too, for the payoff chart.
    und_tokens = set()
    for p in positions:
        inst = instruments.get(p["instrument_token"])
        fut = inst and underlying_future(inst)
        if fut:
            und_tokens.add(fut["instrument_token"])
    if und_tokens and not dhan:
        try:
            quotes = kite.ohlc([qkey(t) for t in und_tokens])
            with lock:
                for q in quotes.values():
                    state["ltp"].setdefault(q["instrument_token"], q["last_price"])
                    merge_ohlc(q["instrument_token"], q.get("ohlc"), q["last_price"])
        except Exception:
            pass
    trades = []
    try:
        trades = kite.trades()
        record_entry_fills(trades, positions, und_tokens)
    except Exception as e:
        with lock:
            state["error"] = f"Entry IV capture failed: {e}"
    with lock:
        state["trades"] = trades
    with lock:
        state["positions"] = positions
        state["orders"] = orders
        state["underlying_tokens"] = und_tokens
        state["last_rest"] = time.time()
        state["error"] = None
        if not dhan:
            for p in positions:
                state["ltp"].setdefault(p["instrument_token"], p["last_price"])
    if not dhan:
        sync_subscriptions()


# ---------- entry IV ----------
# Kite doesn't keep the IV at the time of a trade, so we record it ourselves: for every option fill,
# the futures price at that minute and the time to expiry give the IV implied by the fill price.
# Positions opened before the server ran can be given an entry IV by hand (POST /api/entry_iv).
ENTRY_FILE = HERE / ".entry_iv.json"
IST = timezone(timedelta(hours=5, minutes=30))
entry_book = {"fills": {}, "manual": {}}   # fills: trade_id -> fill; manual: symbol -> {"iv", "set_at", "note"}


def load_entry_book():
    global entry_book
    try:
        entry_book = json.loads(ENTRY_FILE.read_text())
        entry_book.setdefault("fills", {})
        entry_book.setdefault("manual", {})
    except Exception:
        pass


def save_entry_book():
    ENTRY_FILE.write_text(json.dumps(entry_book, indent=1))


def _ncdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def black76(F, K, T, vol, kind):
    if T <= 0 or vol <= 0:
        return max(0.0, F - K if kind == "CE" else K - F)
    sd = vol * math.sqrt(T)
    d1 = (math.log(F / K) + sd * sd / 2) / sd
    d2 = d1 - sd
    return F * _ncdf(d1) - K * _ncdf(d2) if kind == "CE" else K * _ncdf(-d2) - F * _ncdf(-d1)


def implied_vol(price, F, K, T, kind):
    if not (T > 0 and F > 0 and price > max(0.0, F - K if kind == "CE" else K - F) + 1e-6):
        return None
    lo, hi = 0.01, 5.0
    if black76(F, K, T, hi, kind) < price:
        return None
    for _ in range(60):
        mid = (lo + hi) / 2
        lo, hi = (lo, mid) if black76(F, K, T, mid, kind) > price else (mid, hi)
    return (lo + hi) / 2


def expiry_dt(d):
    return datetime(d.year, d.month, d.day, 23, 30, tzinfo=IST)


def futures_price_at(fut, when):
    """Futures price at a past moment: live price if it was moments ago, else 1-minute history."""
    if (datetime.now(IST) - when).total_seconds() < 120:
        return state["ltp"].get(fut["instrument_token"])
    try:
        if dhan:
            return dhan.price_at(fut["instrument_token"], fut["segment"], fut["dhan_instrument"], when)
        candles = kite.historical_data(fut["instrument_token"], when - timedelta(minutes=2),
                                       when + timedelta(minutes=1), "minute")
        before = [c for c in candles if c["date"] <= when]
        return (before[-1] if before else candles[0])["close"] if candles else None
    except Exception:
        return None


def fifo_inputs(p, trades):
    """
    What the dashboard needs to work out a FIFO cost: lots carried in from earlier days (qty, price)
    and today's fills in time order. Kite's own average blends booked lots in; FIFO doesn't.
    """
    carry_qty = p.get("overnight_quantity") or 0
    carry_price = None
    if carry_qty > 0:
        q = (p.get("buy_quantity") or 0) - (p.get("day_buy_quantity") or 0)
        if q > 0:
            carry_price = ((p.get("buy_price") or 0) * p["buy_quantity"]
                           - (p.get("day_buy_price") or 0) * (p.get("day_buy_quantity") or 0)) / q
    elif carry_qty < 0:
        q = (p.get("sell_quantity") or 0) - (p.get("day_sell_quantity") or 0)
        if q > 0:
            carry_price = ((p.get("sell_price") or 0) * p["sell_quantity"]
                           - (p.get("day_sell_price") or 0) * (p.get("day_sell_quantity") or 0)) / q
    if carry_qty and carry_price is None:
        carry_price = p.get("close_price") or p.get("average_price")
    fills = []
    for t in trades:
        if t["tradingsymbol"] != p["tradingsymbol"] or t["exchange"] != p["exchange"]:
            continue
        when = t.get("fill_timestamp") or t.get("exchange_timestamp")
        if isinstance(when, str):
            when = datetime.fromisoformat(when)
        ms = int(when.replace(tzinfo=IST).timestamp() * 1000) if when and when.tzinfo is None else \
             int(when.timestamp() * 1000) if when else 0
        qty = t["quantity"] if t["transaction_type"] == "BUY" else -t["quantity"]
        fills.append([ms, qty, t["average_price"]])
    fills.sort(key=lambda f: f[0])
    return {"carry_qty": carry_qty, "carry_price": carry_price, "fills": fills}


entry_attempts = {}   # trade_id -> failed price lookups (retry a few times before giving up)


def record_entry_fills(trades, positions, und_tokens):
    """Store IV for option fills we haven't seen; forget fills of positions that are now flat."""
    new = {}
    for t in trades:
        tid = str(t["trade_id"])
        if tid in entry_book["fills"]:
            continue
        inst = by_symbol.get((t["exchange"], t["tradingsymbol"])) or instruments.get(t["instrument_token"])
        if not inst or inst["instrument_type"] not in ("CE", "PE"):
            continue
        fut = underlying_future(inst)
        when = t.get("fill_timestamp") or t.get("exchange_timestamp")
        if isinstance(when, str):
            when = datetime.fromisoformat(when)
        when = when.replace(tzinfo=IST) if when.tzinfo is None else when
        F = futures_price_at(fut, when) if fut else None
        if F is None and entry_attempts.get(tid, 0) < 3:
            entry_attempts[tid] = entry_attempts.get(tid, 0) + 1
            continue
        T = (expiry_dt(inst["expiry"]) - when).total_seconds() / (365 * 86400)
        iv = implied_vol(t["average_price"], F, inst["strike"], T, inst["instrument_type"]) if F else None
        new[tid] = {
            "symbol": t["tradingsymbol"], "exchange": t["exchange"], "side": t["transaction_type"],
            "qty": t["quantity"], "price": t["average_price"], "time": int(when.timestamp() * 1000),
            "und_symbol": fut and fut["tradingsymbol"], "und_price": F, "iv": iv,
        }
    open_syms = {p["tradingsymbol"] for p in positions if p["quantity"] != 0}
    with lock:
        entry_book["fills"].update(new)
        if not positions:   # never wipe the record on an empty or partial response
            if new:
                save_entry_book()
            return
        stale = [k for k, f in entry_book["fills"].items() if f["symbol"] not in open_syms]
        for k in stale:
            del entry_book["fills"][k]
        stale_manual = [s for s in entry_book["manual"] if s not in open_syms]
        for sym in stale_manual:
            del entry_book["manual"][sym]
        if new or stale or stale_manual:
            save_entry_book()


def entry_iv_for(symbol, qty):
    """Entry IV for an open option position: opening fills we recorded, plus a manual value for the rest."""
    if not qty:
        return None
    fills = [f for f in entry_book["fills"].values() if f["symbol"] == symbol]
    fills.sort(key=lambda f: f["time"])
    # FIFO: closing fills take out the oldest open lots first, so only the lots still open count
    open_lots = []
    for f in fills:
        rem = f["qty"] if f["side"] == "BUY" else -f["qty"]
        while rem and open_lots and (open_lots[0]["q"] > 0) != (rem > 0):
            take = min(abs(rem), abs(open_lots[0]["q"]))
            open_lots[0]["q"] -= take if open_lots[0]["q"] > 0 else -take
            rem -= take if rem > 0 else -take
            if not open_lots[0]["q"]:
                open_lots.pop(0)
        if rem:
            open_lots.append({"q": rem, "f": f})
    known = [{**l["f"], "qty": abs(l["q"])} for l in open_lots
             if (l["q"] < 0) == (qty < 0) and l["f"]["iv"]]
    lots = sum(f["qty"] for f in known)
    manual = entry_book["manual"].get(symbol)
    need = abs(qty)
    parts = []   # (iv, weight)
    if known:
        parts.append((sum(f["iv"] * f["qty"] for f in known) / lots, min(lots, need)))
    rest = max(0, need - lots)
    if manual and (rest or not known):
        parts.append((manual["iv"], rest or need))
    if not parts:
        return {"iv": None, "source": None, "covered": 0, "fills": fills, "manual": manual}
    w = sum(p[1] for p in parts)
    source = "mixed" if len(parts) > 1 else ("fills" if known else "manual")
    return {"iv": sum(p[0] * p[1] for p in parts) / w, "source": source,
            "covered": min(need, lots + (rest if manual else 0)), "fills": fills, "manual": manual}


@app.route("/api/entry_iv", methods=["POST"])
def api_entry_iv():
    """Set or clear a manual entry IV (percent) for a symbol. Body: {"symbol", "iv" (or null), "note"}."""
    body = request.get_json(force=True) or {}
    sym = body.get("symbol")
    if not sym:
        return jsonify({"error": "symbol required"}), 400
    with lock:
        if body.get("iv") is None:
            entry_book["manual"].pop(sym, None)
        else:
            iv = float(body["iv"])
            entry_book["manual"][sym] = {"iv": iv / 100 if iv > 3 else iv, "note": body.get("note") or "",
                                         "set_at": int(time.time() * 1000)}
        save_entry_book()
    return jsonify({"ok": True})


# ---------- Dhan price feed ----------
dhan_quotes = {}  # security ID -> latest quote (price, ohlc, volume, oi)


def dhan_price_loop():
    """Poll Dhan quotes for positions and their underlying futures (Dhan allows 1 quote call per second)."""
    while True:
        with lock:
            tokens = {p["instrument_token"] for p in state["positions"]
                      if p["instrument_token"] in instruments} | state.get("underlying_tokens", set())
        by_seg = {}
        for t in tokens:
            by_seg.setdefault(instruments[t]["segment"], []).append(t)
        try:
            quotes = dhan.quotes(by_seg)
            with lock:
                for t, q in quotes.items():
                    if q["last_price"]:
                        state["ltp"][t] = q["last_price"]
                    merge_ohlc(t, q["ohlc"] if q["ohlc"]["high"] else None, q["last_price"])
                    dhan_quotes[t] = q
                state["last_tick"] = time.time()
                state["ticker"] = "connected"
                if state["error"] and state["error"].startswith("Dhan"):
                    state["error"] = None
        except Exception as e:
            with lock:
                state["ticker"] = "Dhan error"
                state["error"] = str(e) if isinstance(e, DhanError) else f"Dhan quote failed: {e}"
            time.sleep(5)
        time.sleep(DHAN_QUOTE_SECS)


def merge_ohlc(token, ohlc, last_price):
    """Call with lock held. Keeps the widest high/low seen from REST and ticks."""
    if not ohlc:
        return
    cur = state["ohlc"].get(token)
    high = max(ohlc["high"], last_price or 0, cur["high"] if cur else 0)
    lows = [v for v in (ohlc["low"], last_price, cur and cur["low"]) if v]
    state["ohlc"][token] = {"open": ohlc["open"], "high": high, "low": min(lows) if lows else 0,
                            "close": ohlc["close"]}


def load_pnl_range():
    try:
        saved = json.loads(PNL_RANGE_FILE.read_text())
        if saved.get("date") == date.today().isoformat():
            return saved
    except Exception:
        pass
    return None


def track_pnl_range(total, day):
    """Call with lock held. Tracks today's extremes; only sees what happens while the server runs."""
    now_ms = int(time.time() * 1000)
    r = state["pnl_range"]
    if not r or r.get("date") != date.today().isoformat():
        r = state["pnl_range"] = {"date": date.today().isoformat(), "since": now_ms,
                                  "total": {"high": [total, now_ms], "low": [total, now_ms]},
                                  "day": {"high": [day, now_ms], "low": [day, now_ms]}}
    for key, v in (("total", total), ("day", day)):
        if v > r[key]["high"][0]:
            r[key]["high"] = [v, now_ms]
        if v < r[key]["low"][0]:
            r[key]["low"] = [v, now_ms]


def current_totals():
    total = day = 0.0
    for p in state["positions"]:
        _, pnl, m2m = live_values(p, state["ltp"])
        total += pnl
        day += m2m
    return round(total, 2), round(day, 2)


def board_price(symbol):
    """Latest futures price from the large-prints scan (covers contracts the price loop doesn't poll)."""
    return next((f["ltp"] for f in state["prints"].get("futures") or [] if f["symbol"] == symbol), None)


def ng_ref():
    """Call with lock held. Front-month NG futures contract and its price (the P&L chart's second axis)."""
    futs = [f for f in futures_by_name.get(("MCX", "NATURALGAS"), []) if f["expiry"] >= date.today()]
    if not futs:
        return None, None
    f = futs[0]
    return f["tradingsymbol"], state["ltp"].get(f["instrument_token"]) or board_price(f["tradingsymbol"])


def record_history():
    with lock:
        if not state["positions"]:
            return
        total, day = current_totals()
        track_pnl_range(total, day)
        state["history"].append([int(time.time() * 1000), total, day, ng_ref()[1]])
        del state["history"][:-HISTORY_MAX]
        snapshot = dict(state["pnl_range"])
        history = list(state["history"]) if len(state["history"]) % 12 == 0 else None   # save about once a minute
    PNL_RANGE_FILE.write_text(json.dumps(snapshot))
    if history:
        HISTORY_FILE.write_text(json.dumps({"date": date.today().isoformat(), "history": history}))


def load_history():
    """Today's P&L / NG price samples, so a restart doesn't wipe the chart."""
    try:
        saved = json.loads(HISTORY_FILE.read_text())
        if saved.get("date") == date.today().isoformat():
            state["history"] = saved["history"][-HISTORY_MAX:]
    except Exception:
        pass


# ---------- booked P&L and capital added since a start date ----------
# Kite only gives today's positions and pay-ins, so earlier figures come from you once (Zerodha Console: P&L report
# for booked P&L, ledger for pay-ins), as a "seed" up to a date; after that the server records every day itself.
TRACK_FILE = HERE / ".tracking.json"
tracking = {"start": date.today().isoformat(), "seed": None, "days": {}}   # first run; change with "Set figures"


def load_tracking():
    global tracking
    try:
        tracking = {**tracking, **json.loads(TRACK_FILE.read_text())}
    except Exception:
        pass


def todays_booked():
    """Call with lock held. Booked (realised) P&L today, Kite-style: P&L minus the open lots' P&L vs their average."""
    total = 0.0
    for p in state["positions"]:
        price, pnl, _ = live_values(p, state["ltp"])
        total += pnl - p["quantity"] * (price - (p.get("average_price") or 0)) * (p.get("multiplier") or 1)
    return round(total, 2)


def open_pnl():
    """Call with lock held. P&L on lots still open, vs their Kite average (the unbooked part)."""
    total = 0.0
    for p in state["positions"]:
        price = live_values(p, state["ltp"])[0]
        total += p["quantity"] * (price - (p.get("average_price") or 0)) * (p.get("multiplier") or 1)
    return round(total, 2)


def record_tracking(with_funds):
    today = date.today().isoformat()
    with lock:
        if not state["positions"] and not with_funds:
            return
        day = tracking["days"].setdefault(today, {})
        day["booked"] = todays_booked()
        day["open"] = open_pnl()     # the last reading of the day = end-of-day open P&L (for the equity curve)
        day["mtm"] = current_totals()[1]   # the day's mark-to-market P&L (Kite's day P&L), for Sharpe / drawdown
    if with_funds:
        try:
            m = kite.margins()
            segs = [m.get(s) or {} for s in ("equity", "commodity")] if isinstance(m, dict) else []
            day["payin"] = round(sum((s.get("available") or {}).get("intraday_payin") or 0 for s in segs), 2)
            day["payout"] = round(sum((s.get("utilised") or {}).get("payout") or 0 for s in segs), 2)
            u = lambda k: round(sum((s.get("utilised") or {}).get(k) or 0 for s in segs), 2)
            a = lambda k: round(sum((s.get("available") or {}).get(k) or 0 for s in segs), 2)
            with lock:
                state["funds"] = {"at": int(time.time() * 1000), "opening": a("opening_balance"), "payin": a("intraday_payin"),
                                  "available": a("live_balance"), "collateral": a("collateral"),
                                  "span": u("span"), "exposure": u("exposure"), "premium": u("option_premium"),
                                  "m2m_realised": u("m2m_realised"), "m2m_unrealised": u("m2m_unrealised"),
                                  "payout": u("payout"), "debits": u("debits"),
                                  "net": round(sum(s.get("net") or 0 for s in segs), 2)}
        except Exception:
            pass
        TRACK_FILE.write_text(json.dumps(tracking, indent=1))


def record_weekly():
    """Investor figures as of each Friday's close (the fund's weekly reporting). Taken after 23:40 on Friday, or at
    the first chance before Monday's open if the server was off, while positions still stand as they closed."""
    now = datetime.now()
    wd, mins = now.weekday(), now.hour * 60 + now.minute
    if wd == 4 and mins >= 23 * 60 + 40:
        friday = now.date()
    elif wd in (5, 6) or (wd == 0 and mins < 9 * 60):
        friday = now.date() - timedelta(days=(wd - 4) % 7)
    else:
        return
    weekly = tracking.setdefault("weekly", {})
    if friday.isoformat() in weekly:
        return
    with lock:
        if not state["positions"]:
            return
        s = tracking_summary()
        op = open_pnl()
    capital = s["added"] - s["withdrawn"]
    weekly[friday.isoformat()] = {"capital": round(capital, 2), "booked": s["booked"], "open": op,
                                  "total": round(s["booked"] + op, 2), "at": int(time.time() * 1000)}
    TRACK_FILE.write_text(json.dumps(tracking, indent=1))


def tracking_summary():
    start, seed, days = tracking["start"], tracking.get("seed") or {}, tracking["days"]
    as_of = seed.get("as_of")
    after = {d: v for d, v in days.items() if d >= start and (not as_of or d > as_of)}
    booked_days = sum(v.get("booked") or 0 for v in after.values())
    today = days.get(date.today().isoformat(), {})
    # dated capital flows (for capital-weighted returns): your Console pay-ins up to the seed date, then recorded ones
    flows = [dict(f) for f in seed.get("flows") or []]
    for d, v in sorted(after.items()):
        if v.get("payin"):
            flows.append({"date": d, "amount": v["payin"]})
        if v.get("payout"):
            flows.append({"date": d, "amount": -v["payout"]})
    # daily P&L: Console ledger settlements up to the seed date, then the recorded mark-to-market of each day
    daily = [dict(x) for x in seed.get("daily") or []]
    daily += [{"date": d, "pnl": v["mtm"], "basis": "mtm"} for d, v in sorted(after.items()) if v.get("mtm") is not None]
    return {
        "weekly": [{"week_end": d, **v} for d, v in sorted((tracking.get("weekly") or {}).items())],
        "weekly_booked": tracking.get("weekly_booked") or {},
        "flows": flows, "daily": daily,
        "start": start, "seed": seed or None, "recorded_from": min(after) if after else None,
        "booked": round((seed.get("booked") or 0) + booked_days, 2), "booked_today": today.get("booked"),
        "added": round((seed.get("added") or 0) + sum(v.get("payin") or 0 for v in after.values()), 2),
        "withdrawn": round((seed.get("withdrawn") or 0) + sum(v.get("payout") or 0 for v in after.values()), 2),
        "days": {d: days[d] for d in sorted(after)},
    }


@app.route("/api/tracking", methods=["GET", "POST"])
def api_tracking():
    """GET: booked P&L and capital added since the start date. POST {"start", "seed": {"as_of", "booked", "added",
    "withdrawn", "source"}} sets the figures you bring from Console (seed null clears them)."""
    if request.method == "POST":
        body = request.get_json(force=True) or {}
        if body.get("start"):
            tracking["start"] = str(body["start"])[:10]
        if isinstance(body.get("weekly_booked"), dict):
            # booked P&L since the start date, as of each Friday (from Console), for the investor trail
            tracking["weekly_booked"] = {str(k)[:10]: float(v) for k, v in body["weekly_booked"].items()}
        if "seed" in body:
            s = body["seed"]
            tracking["seed"] = None if not s else {
                "as_of": str(s.get("as_of") or "")[:10] or None, "booked": float(s.get("booked") or 0),
                "added": float(s.get("added") or 0), "withdrawn": float(s.get("withdrawn") or 0),
                "charges": float(s.get("charges") or 0),
                "flows": [{"date": str(f["date"])[:10], "amount": float(f["amount"])}
                          for f in (s.get("flows") or []) if f.get("date") and f.get("amount")],
                "daily": [{"date": str(x["date"])[:10], "pnl": float(x["pnl"]), "basis": str(x.get("basis") or "settlement")[:20]}
                          for x in (s.get("daily") or []) if x.get("date") and x.get("pnl") is not None],
                "source": str(s.get("source") or "manual")[:40], "set_at": int(time.time() * 1000)}
        TRACK_FILE.write_text(json.dumps(tracking, indent=1))
    with lock:
        return jsonify(tracking_summary())


def poll_loop():
    n = 0
    while True:
        time.sleep(POLL_SECS)
        if state["logged_in"]:
            refresh_rest()
            record_history()
            try:
                record_tracking(with_funds=n % (60 // POLL_SECS) == 0)   # pay-ins once a minute
                record_weekly()
            except Exception as e:
                print("tracking failed:", e)
            if n % (FLOW_SECS // POLL_SECS) == 0:
                try:
                    refresh_flow()
                except Exception as e:
                    with lock:
                        state["flow"]["error"] = f"OI/volume refresh failed: {e}"
            n += 1


# ---------- OI & volume surge ----------
# Futures we track plus option strikes around the money for each future's nearest option expiry.
# Day changes are measured against yesterday's OI and the 10-day average volume (historical API);
# "spikes" compare the last 15 minutes against today's own average pace.

FLOW_SECS = 30
ATM_STRIKES = 6                    # strikes each side of ATM
SESSION = ((9, 0), (23, 30))       # MCX evening session ends 23:55 in US winter months
SPIKE_WINDOW = 15 * 60
FLOW_BASELINE_FILE = HERE / ".flow_baseline.json"
flow_baseline = {}                 # token -> {"prev_oi", "avg_vol", "source"}
flow_samples = {}                  # token -> [(t, oi, volume)] for today
last_alert = {}                    # token -> epoch of last alert
state["flow"] = {"futures": [], "options": [], "alerts": [], "updated": None, "error": None}


def qkey(token):
    inst = instruments[token]
    return f'{inst["exchange"]}:{inst["tradingsymbol"]}'


def session_fraction(now=None):
    lt = time.localtime(now or time.time())
    mins = lt.tm_hour * 60 + lt.tm_min
    start, end = SESSION[0][0] * 60 + SESSION[0][1], SESSION[1][0] * 60 + SESSION[1][1]
    return min(1.0, max(0.02, (mins - start) / (end - start)))


def flow_tokens():
    with lock:
        futs = sorted(state.get("underlying_tokens", set()))
    opts = []
    for ft in futs:
        fut = instruments[ft]
        chain = option_chain(fut)
        if not chain["opt_expiry"] or not chain["strikes"]:
            continue
        price = state["ltp"].get(ft) or 0
        strikes = chain["strikes"]
        atm = min(range(len(strikes)), key=lambda i: abs(strikes[i] - price))
        wanted = set(strikes[max(0, atm - ATM_STRIKES): atm + ATM_STRIKES + 1])
        for o in options_by_name.get((fut["exchange"], fut["name"]), []):
            if o["expiry"].isoformat() == chain["opt_expiry"] and o["strike"] in wanted and underlying_future(o) is fut:
                opts.append(o["instrument_token"])
    return futs, opts


def load_flow_baseline(tokens):
    """Yesterday's OI and 10-day average volume, once per day per token."""
    global flow_baseline
    today = date.today().isoformat()
    if not flow_baseline:
        try:
            saved = json.loads(FLOW_BASELINE_FILE.read_text())
            if saved.get("date") == today:
                flow_baseline = {int(k): v for k, v in saved["data"].items()}
        except Exception:
            pass
    missing = [t for t in tokens if t not in flow_baseline]
    if not missing:
        return
    from datetime import timedelta
    for t in missing:
        try:
            if dhan:
                inst = instruments[t]
                h = dhan.daily_history(t, inst["segment"], inst["dhan_instrument"])
                vols, ois = [v for v in h["volume"][-10:] if v], h["oi"]
            else:
                candles = kite.historical_data(t, date.today() - timedelta(days=21),
                                               date.today() - timedelta(days=1), "day", oi=True)
                vols = [c["volume"] for c in candles[-10:] if c.get("volume")]
                ois = [c.get("oi") for c in candles]
            flow_baseline[t] = {"prev_oi": ois[-1] if ois else None,
                                "avg_vol": sum(vols) / len(vols) if vols else None, "source": "historical"}
        except Exception:
            # No historical-data access (or a brand new contract): fall back to today's first reading
            flow_baseline[t] = {"prev_oi": None, "avg_vol": None, "source": "session"}
        if not dhan:
            time.sleep(0.35)  # Kite historical API allows ~3 requests/second
    FLOW_BASELINE_FILE.write_text(json.dumps({"date": today, "data": flow_baseline}))


def price_oi_signal(price_chg, oi_chg, kind):
    if not price_chg or not oi_chg:
        return ""
    up, oi_up = price_chg > 0, oi_chg > 0
    if kind == "FUT":
        return {(True, True): "Long buildup", (False, True): "Short buildup",
                (True, False): "Short covering", (False, False): "Long unwinding"}[(up, oi_up)]
    side = "Call" if kind == "CE" else "Put"
    return {(True, True): f"{side} buying", (False, True): f"{side} writing",
            (True, False): f"{side} short covering", (False, False): f"{side} unwinding"}[(up, oi_up)]


def gather_flow(futs, opts):
    """Quotes and baselines for the flow table: {token: quote}, {token: baseline}, {token: iv}."""
    if not dhan:
        tokens = futs + opts
        load_flow_baseline(tokens)
        quotes = {}
        for i in range(0, len(tokens), 200):
            for q in kite.quote([qkey(t) for t in tokens[i:i + 200]]).values():
                quotes[q["instrument_token"]] = q
        return quotes, {t: flow_baseline.get(t, {}) for t in tokens}, {}

    # Dhan: futures from the 1-second quote poll + daily history; options from the option chain,
    # which already carries previous-day OI and volume plus the exchange's IV.
    load_flow_baseline(futs)
    with lock:
        quotes = {t: dhan_quotes[t] for t in futs if t in dhan_quotes}
    base = {t: flow_baseline.get(t, {}) for t in futs}
    ivs, want = {}, set(opts)
    for ft in futs:
        fut = instruments[ft]
        expiry = option_chain(fut)["opt_expiry"]
        if not expiry:
            continue
        chain = {}
        for und in (fut["dhan_underlying"], ft):   # commodity-level ID first, then the future's own ID
            try:
                chain = dhan.option_chain(und, fut["segment"], expiry)
            except DhanError:
                raise
            except Exception:
                chain = {}
            if chain:
                break
        for o in options_by_name.get((fut["exchange"], fut["name"]), []):
            t = o["instrument_token"]
            if o["expiry"].isoformat() != expiry:
                continue    # this chain is one expiry: don't price Oct options off the Nov chain
            leg = chain.get((o["strike"], o["instrument_type"])) if t in want else None
            if not leg:
                continue
            quotes[t] = {"last_price": leg["last_price"], "ohlc": {"close": leg["prev_close"]},
                         "volume": leg["volume"], "oi": leg["oi"]}
            base[t] = {"prev_oi": leg["prev_oi"], "avg_vol": None, "prev_vol": leg["prev_volume"],
                       "source": "exchange"}
            if leg["iv"]:
                ivs[t] = leg["iv"]
    return quotes, base, ivs


def refresh_flow():
    futs, opts = flow_tokens()
    if not futs and not opts:
        return
    quotes, baselines, ivs = gather_flow(futs, opts)
    now = time.time()
    frac = session_fraction(now)
    rows, alerts = {}, []
    for t, q in quotes.items():
        inst = instruments[t]
        oi, vol = q.get("oi") or 0, q.get("volume") or 0
        samples = flow_samples.setdefault(t, [])
        if samples and time.localtime(samples[-1][0]).tm_yday != time.localtime(now).tm_yday:
            samples.clear()
        samples.append((now, oi, vol))
        while samples and samples[0][0] < now - 3 * 3600:
            samples.pop(0)
        base = baselines.get(t, {})
        prev_oi = base.get("prev_oi") or (samples[0][1] if samples else None)
        avg_vol = base.get("avg_vol")
        close = (q.get("ohlc") or {}).get("close") or 0
        chg_pct = (q["last_price"] - close) / close * 100 if close else None
        oi_chg = oi - prev_oi if prev_oi is not None else None
        # 15-minute window
        past = next((s for s in samples if s[0] >= now - SPIKE_WINDOW), None)
        oi_15 = oi - past[1] if past and past[0] < now - 60 else None
        vol_15 = vol - past[2] if past and past[0] < now - 60 else None
        mins_today = max(1.0, frac * (SESSION[1][0] * 60 + SESSION[1][1] - SESSION[0][0] * 60 - SESSION[0][1]))
        pace_15 = vol / mins_today * 15 if vol else None
        row = {
            "symbol": inst["tradingsymbol"], "name": inst["name"], "kind": inst["instrument_type"],
            "strike": inst["strike"], "expiry": inst["expiry"].isoformat(), "ltp": q["last_price"],
            "chg_pct": chg_pct, "volume": vol, "avg_vol": avg_vol,
            "vol_pace": vol / (avg_vol * frac) if avg_vol else None,
            "oi": oi, "prev_oi": prev_oi, "oi_chg": oi_chg,
            "oi_chg_pct": oi_chg / prev_oi * 100 if oi_chg is not None and prev_oi else None,
            "oi_15m": oi_15, "oi_15m_pct": oi_15 / (oi - oi_15) * 100 if oi_15 and oi - oi_15 else None,
            "vol_15m": vol_15, "vol_15m_ratio": vol_15 / pace_15 if vol_15 is not None and pace_15 else None,
            "baseline": base.get("source", "session"),
            "iv": ivs.get(t),
        }
        row["signal"] = price_oi_signal(chg_pct, oi_chg, row["kind"])
        rows[t] = row

        # Spike alerts, at most one per contract every 15 minutes
        is_fut = row["kind"] == "FUT"
        oi_th = 2.0 if is_fut else 10.0
        reasons = []
        if row["vol_15m_ratio"] and row["vol_15m_ratio"] >= 2.5 and (row["vol_15m"] or 0) >= (50 if is_fut else 100):
            reasons.append(f"volume {row['vol_15m_ratio']:.1f}× normal pace in 15 min")
        if row["oi_15m_pct"] and abs(row["oi_15m_pct"]) >= oi_th and abs(oi_15) >= (25 if is_fut else 100):
            reasons.append(f"OI {row['oi_15m_pct']:+.1f}% in 15 min")
        if reasons and now - last_alert.get(t, 0) > SPIKE_WINDOW:
            last_alert[t] = now
            alerts.append({"time": int(now * 1000), "symbol": row["symbol"], "kind": row["kind"],
                           "text": "; ".join(reasons), "signal": row["signal"]})
    with lock:
        f = state["flow"]
        f["futures"] = [rows[t] for t in futs if t in rows]
        f["options"] = [rows[t] for t in opts if t in rows]
        f["alerts"] = (alerts + f["alerts"])[:30]
        f["updated"] = int(now * 1000)
        f["error"] = None


# ---------- large prints ----------
# Every few seconds, quote all NG / NG Mini futures months and the next two months' options near the money, and log
# each new trade of at least PRINTS_MIN_LOTS lots: last traded quantity (LTQ), price and time. A quote only
# carries each contract's latest trade, so trades that are followed by another one before the next poll are
# missed; "vol_since" (volume traded since the previous poll) shows when that happened. The side is a guess
# from the best bid/ask in the same quote: at or above the offer = buyer lifted it, at or below the bid = seller hit it.

PRINTS_SECS = 3
PRINTS_MIN_LOTS = int(os.environ.get("PRINTS_MIN_LOTS", "2"))   # the dashboard filters further (default 5)
PRINTS_STRIKES = 15                # strikes each side of ATM
PRINTS_NAMES = ("NATURALGAS", "NATGASMINI")
LOT_UNITS = {"NATURALGAS": 1250, "NATGASMINI": 250}   # mmBtu per lot; the instrument list's lot size can read 1
PRINTS_KEEP = 500
PRINTS_FILE = HERE / ".large_prints.json"
prints_last = {}                   # token -> (last_trade_time, volume, last_price)
state["prints"] = {"items": [], "futures": [], "updated": None, "error": None, "min_lots": PRINTS_MIN_LOTS,
                   "contracts": 0}


def load_prints():
    try:
        saved = json.loads(PRINTS_FILE.read_text())
        if saved.get("date") == date.today().isoformat():
            state["prints"]["items"] = saved["items"][:PRINTS_KEEP]
    except Exception:
        pass


def prints_tokens():
    """All listed NG / NG Mini futures (near and far months), plus options within PRINTS_STRIKES of the money
    for the next two expiries."""
    if "MCX" not in loaded_exchanges:      # also works with no MCX positions open
        load_instruments({"MCX"})
    toks = []
    for name in PRINTS_NAMES:
        futs = [f for f in futures_by_name.get(("MCX", name), []) if f["expiry"] >= date.today()]
        toks += [f["instrument_token"] for f in futs]
        for fut in futs[:2]:
            ft = fut["instrument_token"]
            price = state["ltp"].get(ft) or (prints_last.get(ft) or (0, 0, 0))[2]
            chain = option_chain(fut)
            if not price or not chain["strikes"]:
                continue        # options join once the future has a price
            strikes = chain["strikes"]
            atm = min(range(len(strikes)), key=lambda i: abs(strikes[i] - price))
            wanted = set(strikes[max(0, atm - PRINTS_STRIKES): atm + PRINTS_STRIKES + 1])
            toks += [o["instrument_token"] for o in options_by_name.get(("MCX", name), [])
                     if o["expiry"].isoformat() == chain["opt_expiry"] and o["strike"] in wanted
                     and underlying_future(o) is fut]
    return toks


def full_quotes(tokens):
    """Kite quotes (with LTQ, last trade time and depth), keyed by token."""
    keymap = {qkey(t): t for t in tokens}
    keys, out = list(keymap), {}
    for i in range(0, len(keys), 200):
        if USE_MCP:
            data = mcp.call("get_quotes", {"instruments": keys[i:i + 200]})
            data = data.get("data", data) if isinstance(data, dict) else {}
        else:
            data = kite.quote(keys[i:i + 200])
        for k, q in data.items():
            if k in keymap:
                out[keymap[k]] = q
                if USE_MCP and q.get("instrument_token"):
                    dhan.kite_token[keymap[k]] = q["instrument_token"]   # lets history calls skip a quote lookup
    return out


def scan_prints(tokens):
    today = date.today().isoformat()
    now_ms = int(time.time() * 1000)
    new = []
    quotes = full_quotes(tokens)

    # futures board for calendar spreads: every listed month, with best bid/ask
    futs = []
    for t, q in quotes.items():
        inst = instruments[t]
        if inst["instrument_type"] != "FUT":
            continue
        depth = q.get("depth") or {}
        futs.append({"symbol": inst["tradingsymbol"], "name": inst["name"], "expiry": inst["expiry"].isoformat(),
                     "ltp": q.get("last_price") or 0, "close": (q.get("ohlc") or {}).get("close") or 0,
                     "bid": ((depth.get("buy") or [{}])[0]).get("price") or 0,
                     "ask": ((depth.get("sell") or [{}])[0]).get("price") or 0,
                     "bid_qty": ((depth.get("buy") or [{}])[0]).get("quantity") or 0,
                     "ask_qty": ((depth.get("sell") or [{}])[0]).get("quantity") or 0,
                     "ltq": q.get("last_quantity") or 0, "ltt": str(q.get("last_trade_time") or "")[:19],
                     "volume": q.get("volume") or 0, "oi": q.get("oi") or 0})
    futs.sort(key=lambda f: (f["name"], f["expiry"]))
    try:
        record_chain(quotes)
    except Exception as e:
        print("option chain stats failed:", e)
    front = {}
    for f in futs:
        front.setdefault(f["name"], f)

    for t, q in quotes.items():
        ltt = str(q.get("last_trade_time") or "").replace(" ", "T")
        vol, ltq, price = q.get("volume") or 0, q.get("last_quantity") or 0, q.get("last_price") or 0
        prev = prints_last.get(t)
        prints_last[t] = (ltt, vol, price)
        # first sight of a contract only sets the baseline; then a new trade shows as a new time or more volume
        if prev is None or not ltt.startswith(today) or (ltt == prev[0] and vol == prev[1]):
            continue
        if ltq < PRINTS_MIN_LOTS:
            continue
        depth = q.get("depth") or {}
        bid = ((depth.get("buy") or [{}])[0]).get("price") or 0
        ask = ((depth.get("sell") or [{}])[0]).get("price") or 0
        side = "buy" if ask and price >= ask else "sell" if bid and price <= bid else "mid"
        inst = instruments[t]
        fr = front.get(inst["name"])
        # a far-month futures print: its price minus the front month's at the time (calendar spread)
        spread = (round(price - fr["ltp"], 2) if inst["instrument_type"] == "FUT" and fr and fr["ltp"]
                  and fr["symbol"] != inst["tradingsymbol"] else None)
        new.append({
            "time": ltt[:19], "seen": now_ms, "symbol": inst["tradingsymbol"], "name": inst["name"],
            "spread": spread, "front": fr["symbol"] if spread is not None else None,
            "kind": inst["instrument_type"], "strike": inst["strike"], "expiry": inst["expiry"].isoformat(),
            "ltq": ltq, "price": price, "bid": bid, "ask": ask, "side": side,
            "value": round(ltq * LOT_UNITS.get(inst["name"], inst.get("lot_size") or 1) * price),
            "vol_since": vol - prev[1], "volume": vol, "oi": q.get("oi") or 0,
        })
    with lock:
        p = state["prints"]
        if new:
            new.sort(key=lambda r: r["time"], reverse=True)
            p["items"] = (new + p["items"])[:PRINTS_KEEP]
        p["updated"] = now_ms
        p["contracts"] = len(tokens)
        p["futures"] = futs
        p["error"] = None
        items = list(p["items"])
    if new:
        PRINTS_FILE.write_text(json.dumps({"date": today, "items": items}))


# ---------- option chain: OI by strike, PCR and ATM IV (for the NG chart) ----------
# Built from the same quotes as the large-prints scan (options within 15 strikes of the money, next two expiries).
# Kite quotes don't carry yesterday's OI, so "chg" is the change since the first reading of the day.
OPT_SERIES_FILE = HERE / ".opt_series.json"
chain_now = {}        # (name, expiry) -> {"t", "F", "pcr", "iv", "strikes": {strike: {"CE": {...}, "PE": {...}}}}
opt_series = {"date": None, "first_oi": {}, "series": {}}   # series: "name|expiry" -> [[minute_ms, pcr, atm_iv, call_oi, put_oi]]


def load_opt_series():
    global opt_series
    try:
        saved = json.loads(OPT_SERIES_FILE.read_text())
        if saved.get("date") == date.today().isoformat():
            opt_series = saved
    except Exception:
        pass


# OI by strike each minute: "name|expiry" -> {"<minute ms>": [futures price, {"<strike>": [call OI, put OI]}]}
OPT_STRIKES_FILE = HERE / ".opt_strikes.json"
opt_strikes = {"date": None, "groups": {}, "backfilled": {}}


def load_opt_strikes():
    global opt_strikes
    try:
        saved = json.loads(OPT_STRIKES_FILE.read_text())
        if saved.get("date") == date.today().isoformat():
            opt_strikes = saved
    except Exception:
        pass


def backfill_strikes(fut):
    """Today's minute OI for the option strikes near the money of one future's option expiry (Kite history).
    Fills the minutes before live recording began, and the PCR / IV series for them. About one Kite call per strike."""
    expiry = option_chain(fut)["opt_expiry"]
    key = f"{fut['name']}|{expiry}"
    status = opt_strikes["backfilled"]
    if status.get(key) in ("running", "done"):
        return
    status[key] = "running"
    try:
        snap = chain_now.get((fut["name"], expiry)) or {}
        strikes = sorted((snap.get("strikes") or {}).keys())
        opts = [o for o in options_by_name.get(("MCX", fut["name"]), [])
                if o["expiry"].isoformat() == expiry and o["strike"] in strikes]
        start = datetime.combine(date.today(), datetime.min.time()).replace(hour=9)
        end = datetime.now()
        hist = lambda t: (dhan._history(t, "minute", start, end, oi=True) if USE_MCP else
                          [{"time": int(c["date"].timestamp()), "close": c["close"], "oi": c.get("oi")}
                           for c in kite.historical_data(t, start, end, "minute", oi=True)])
        fc = hist(fut["instrument_token"])
        fprice = {c["time"] * 1000: c["close"] for c in fc}
        # futures OI and cumulative day volume per minute (same shape as live rows)
        fstat, cum = {}, 0
        for c in fc:
            cum += c.get("volume") or 0
            fstat[c["time"] * 1000] = (c.get("oi"), cum)
        series = {}       # minute -> {strike: [ce, pe]}, plus closes for IV
        closes = {}
        for o in opts:
            try:
                rows = hist(o["instrument_token"])
            except Exception:
                continue
            i = 0 if o["instrument_type"] == "CE" else 1
            last_oi, cum = None, 0
            for c in rows:
                m = c["time"] * 1000
                last_oi = c.get("oi") or last_oi
                cum += c.get("volume") or 0
                cell = series.setdefault(m, {}).setdefault(f"{o['strike']:g}", [None, None, None, None])
                cell[i], cell[i + 2] = last_oi, cum      # OI, and volume traded so far today
                closes.setdefault(m, {})[(o["strike"], o["instrument_type"])] = c.get("close")
            if not USE_MCP:
                time.sleep(0.35)
        rows = opt_strikes["groups"].setdefault(key, {})
        live_from = opt_strikes.setdefault("live_from", {}).get(key)
        all_s = sorted(series)
        # OI only changes on a trade, so before an option's first trade today it held the OI of that first candle
        carry = {}
        for m in all_s:
            for k, v in series[m].items():
                cur = carry.setdefault(k, [None, None, 0, 0])   # volume before the first trade is 0
                for i in (0, 1):
                    if cur[i] is None and v[i] is not None:
                        cur[i] = v[i]
        # options with no trade at all today: their OI is unchanged, so today's quote holds for every minute
        for o in opts:
            i = 0 if o["instrument_type"] == "CE" else 1
            cur = carry.setdefault(f"{o['strike']:g}", [None, None, 0, 0])
            if cur[i] is None:
                cur[i] = ((snap.get("strikes") or {}).get(o["strike"], {}).get(o["instrument_type"]) or {}).get("oi")
        carry_px, carry_f = None, (None, None)
        ser = opt_series["series"].setdefault(key, [])
        have = {r[0] for r in ser}
        new_rows = []
        for m in all_s:
            if live_from is not None and m >= live_from:
                break
            for k, v in series[m].items():   # options that didn't trade this minute keep their last OI and volume
                cur = carry.setdefault(k, [None, None, 0, 0])
                for i in range(4):
                    if v[i] is not None:
                        cur[i] = v[i]
            carry_px = fprice.get(m, carry_px)
            carry_f = fstat.get(m, carry_f)
            rows[str(m)] = [carry_px, {k: list(v) for k, v in carry.items()}, carry_f[0], carry_f[1]]
            if m not in have and carry_px:
                ce = sum(v[0] or 0 for v in carry.values()); pe = sum(v[1] or 0 for v in carry.values())
                k_atm = min(strikes, key=lambda s: abs(s - carry_px)) if strikes else None
                T = (datetime.fromisoformat(expiry + "T23:30:00+05:30").timestamp() - m / 1000) / (365 * 24 * 3600)
                ivs = [implied_vol(px, carry_px, k_atm, T, kind) for (s, kind), px in (closes.get(m) or {}).items()
                       if s == k_atm and px]
                ivs = [v for v in ivs if v]
                new_rows.append([m, round(pe / ce, 4) if ce else None, round(sum(ivs) / len(ivs), 4) if ivs else None, ce, pe])
        if new_rows:
            ser[:] = sorted(new_rows + ser, key=lambda r: r[0])
        # the day's first OI is the baseline for "change today"
        first = opt_series["first_oi"]
        for o in opts:
            for m in all_s:
                v = series[m].get(f"{o['strike']:g}")
                i = 0 if o["instrument_type"] == "CE" else 1
                if v and v[i] is not None:
                    first[o["tradingsymbol"]] = v[i]
                    break
        status[key] = "done"
        OPT_STRIKES_FILE.write_text(json.dumps(opt_strikes))
        OPT_SERIES_FILE.write_text(json.dumps(opt_series))
    except Exception as e:
        status[key] = f"failed: {e}"


# ---------- OI bias: the market's positioning today, on the same −100..+100 scale as the net bias score ----------
# Put OI added (writing) reads bullish and call OI added bearish, each weighted by the option's delta; futures count
# their OI change in the direction of the price move (long buildup / short covering bullish, short buildup / long
# unwinding bearish). In NG-lot equivalents. Score = net ÷ total absolute × 100.
def _delta(F, K, T, vol, kind):
    if T <= 0 or vol <= 0 or F <= 0:
        return (1.0 if F > K else 0.0) if kind == "CE" else (-1.0 if F < K else 0.0)
    sd = vol * math.sqrt(T)
    d1 = (math.log(F / K) + sd * sd / 2) / sd
    return _ncdf(d1) if kind == "CE" else _ncdf(d1) - 1


@app.route("/api/oibias")
def api_oibias():
    big = max(PRINTS_NAMES, key=lambda x: LOT_UNITS[x])
    with lock:
        snaps = dict(chain_now)
        futs = [dict(f) for f in state["flow"]["futures"]]
    rows, legs = {}, []
    now = time.time()
    for (name, expiry), s in snaps.items():
        if name not in PRINTS_NAMES or not s.get("F"):
            continue
        w = LOT_UNITS[name] / LOT_UNITS[big]
        T = (datetime.fromisoformat(expiry + "T23:30:00+05:30").timestamp() - now) / (365 * 24 * 3600)
        iv = s.get("iv") or 0.5
        r = rows.setdefault((expiry[:7], name), {"month": expiry[:7], "name": name, "opt_expiry": expiry, "pcr": s.get("pcr"),
                                                 "ce": 0, "pe": 0, "opt": 0, "fut": 0, "top_pe": None, "top_ce": None})
        for k, v in s["strikes"].items():
            for kind, leg in v.items():
                d = leg.get("chg") or 0
                if not d:
                    continue
                score = d * abs(_delta(s["F"], k, T, iv, kind)) * w * (1 if kind == "PE" else -1)
                legs.append(score)
                r["opt"] += score
                if kind == "CE":
                    r["ce"] += d
                    if d > 0 and (not r["top_ce"] or d > r["top_ce"][1]):
                        r["top_ce"] = [k, d]
                else:
                    r["pe"] += d
                    if d > 0 and (not r["top_pe"] or d > r["top_pe"][1]):
                        r["top_pe"] = [k, d]
    for f in futs:
        if f.get("name") not in PRINTS_NAMES or f.get("oi_chg") is None or not f.get("chg_pct"):
            continue
        w = LOT_UNITS[f["name"]] / LOT_UNITS[big]
        r = rows.setdefault((f["expiry"][:7], f["name"]), {"month": f["expiry"][:7], "name": f["name"], "opt_expiry": None, "pcr": None,
                                                           "ce": 0, "pe": 0, "opt": 0, "fut": 0, "top_pe": None, "top_ce": None})
        score = abs(f["oi_chg"]) * w * (1 if f["chg_pct"] > 0 else -1)
        legs.append(score)
        r.update({"fut": score, "fut_oi_chg": f["oi_chg"], "fut_chg_pct": f["chg_pct"], "fut_read": f.get("signal"), "fut_ltp": f.get("ltp")})
    net, gross = sum(legs), sum(abs(x) for x in legs)
    opt_net = sum(r["opt"] for r in rows.values()); fut_net = sum(r["fut"] for r in rows.values())
    history = oibias_hist["series"] if oibias_hist.get("date") == date.today().isoformat() else []
    return jsonify({"history": history,
                    "score": round(100 * net / gross) if gross else 0, "net": round(net, 1), "gross": round(gross, 1),
                    "opt_net": round(opt_net, 1), "fut_net": round(fut_net, 1), "unit": "NG lots",
                    "rows": sorted(rows.values(), key=lambda r: (r["month"], r["name"] != big)), "at": int(now * 1000)})


_hist_cache = {}


def strike_rows(fut, day=None):
    """This future's option-expiry rows by minute (starting today's backfill if it hasn't run).
    With `day` (an earlier date), the rows come from history/<day>/opt_strikes.json."""
    expiry = option_chain(fut)["opt_expiry"]
    key = f"{fut['name']}|{expiry}"
    if day and day != date.today().isoformat():
        p = HISTORY_DIR / day / "opt_strikes.json"
        try:
            mt = p.stat().st_mtime
            if _hist_cache.get(day, (None,))[0] != mt:
                _hist_cache[day] = (mt, json.loads(p.read_text()).get("groups") or {})
            return expiry, key, dict(_hist_cache[day][1].get(key) or {})
        except FileNotFoundError:
            return expiry, key, {}
    if not DEMO_SERVER and state["logged_in"] and key in opt_strikes["groups"] and not opt_strikes["backfilled"].get(key):
        threading.Thread(target=backfill_strikes, args=(fut,), daemon=True).start()
    with lock:
        return expiry, key, dict(opt_strikes["groups"].get(key, {}))


def combine_rows(parts):
    """parts: [(rows, weight)] -> one rows dict; each side keeps its last reading until it has a new one."""
    mins = sorted({int(m) for rows, _ in parts for m in rows})
    out, last = {}, [None] * len(parts)
    for m in mins:
        for j, (rows, _) in enumerate(parts):
            if str(m) in rows:
                last[j] = rows[str(m)]
        if any(x is None for x in last):
            continue
        strikes = {k for x in last for k in x[1]}
        merged = {}
        for k in strikes:
            v = [None, None, None, None]        # call OI, put OI, call volume, put volume
            for (rows, w), x in zip(parts, last):
                cell = x[1].get(k) or []
                for i in range(4):
                    o = cell[i] if len(cell) > i else None
                    if o is not None:
                        v[i] = (v[i] or 0) + o * w
            merged[k] = [round(a, 2) if a is not None else None for a in v]
        # futures OI and day volume, added up the same way (in the bigger contract's lots)
        fut = []
        for idx in (2, 3):
            vals = [(x[idx] if len(x) > idx else None, w) for (rows, w), x in zip(parts, last)]
            fut.append(round(sum(a * w for a, w in vals), 2) if all(a is not None for a, _ in vals) else None)
        out[str(m)] = [last[0][0], merged, fut[0], fut[1]]
    return out


@app.route("/api/oiprofile")
def api_oiprofile():
    """?symbol=<futures>&step=<minutes>&n=<strikes each side>[&combine=1]: OI by strike over today's session, bucketed.
    combine=1 adds the other NG contract (NG + NG Mini) of the same month, in lots of the bigger contract."""
    fut = by_symbol.get(("MCX", request.args.get("symbol", "")))
    if not fut or fut["instrument_type"] != "FUT":
        return jsonify({"error": "unknown futures contract"}), 404
    step = max(1, min(60, int(request.args.get("step", 15))))
    n = max(2, min(15, int(request.args.get("n", 6))))
    day = (request.args.get("date") or "")[:10] or None      # an earlier day, from history/
    if day == date.today().isoformat():
        day = None
    expiry, key, rows = strike_rows(fut, day)
    unit, combined = None, False
    bstat = "done" if day else opt_strikes["backfilled"].get(key)
    if request.args.get("combine") and fut["name"] in PRINTS_NAMES:
        other_name = [x for x in PRINTS_NAMES if x != fut["name"]][0]
        other = next((f for f in futures_by_name.get(("MCX", other_name), [])
                      if (f["expiry"].year, f["expiry"].month) == (fut["expiry"].year, fut["expiry"].month)), None)
        if other:
            o_exp, o_key, o_rows = strike_rows(other, day)
            if o_exp == expiry and o_rows:
                big = max(PRINTS_NAMES, key=lambda x: LOT_UNITS[x])
                w = lambda name: LOT_UNITS[name] / LOT_UNITS[big]
                ng_first = fut["name"] == big   # price line from the NG future
                parts = [(rows, w(fut["name"])), (o_rows, w(other_name))]
                rows = combine_rows(parts if ng_first else parts[::-1])
                unit, combined = "NG lots", True
                if not day:
                    bf = [opt_strikes["backfilled"].get(key), opt_strikes["backfilled"].get(o_key)]
                    bstat = "running" if "running" in bf else "done" if all(b == "done" for b in bf) else (bf[0] or bf[1])
    if not rows:
        return jsonify({"name": fut["name"], "expiry": expiry, "times": [], "strikes": [], "backfill": bstat, "combined": combined})
    mins = sorted(int(m) for m in rows)
    # last sample in each bucket
    buckets = {}
    for m in mins:
        buckets[m - m % (step * 60_000)] = m
    times = sorted(buckets)
    last = rows[str(mins[-1])]
    F = last[0] or 0
    all_k = sorted({float(k) for m in mins for k in rows[str(m)][1]})
    if not all_k:
        return jsonify({"name": fut["name"], "expiry": expiry, "times": [], "strikes": []})
    atm = min(range(len(all_k)), key=lambda i: abs(all_k[i] - F))
    ks = all_k[max(0, atm - n): atm + n + 1]
    ce, pe, px, foi, fvol, cev, pev = [], [], [], [], [], [], []
    for t in times:
        r = rows[str(buckets[t])]
        px.append(r[0])
        foi.append(r[2] if len(r) > 2 else None)
        fvol.append(r[3] if len(r) > 3 else None)   # futures volume so far today, at the end of the slot
        cells = [(r[1].get(f"{k:g}") or []) + [None] * 4 for k in ks]
        ce.append([c[0] for c in cells])
        pe.append([c[1] for c in cells])
        cev.append(sum(c[2] or 0 for c in cells) if any(c[2] is not None for c in cells) else None)
        pev.append(sum(c[3] or 0 for c in cells) if any(c[3] is not None for c in cells) else None)
    return jsonify({"name": fut["name"], "expiry": expiry, "step": step, "times": times, "strikes": ks, "price": px,
                    "ce": ce, "pe": pe, "first": rows[str(mins[0])][1] and {f"{k:g}": rows[str(mins[0])][1].get(f"{k:g}") for k in ks},
                    "opt_vol_ce": cev, "opt_vol_pe": pev,
                    "fut_oi": foi, "fut_vol": fvol, "fut_oi_first": (rows[str(mins[0])][2] if len(rows[str(mins[0])]) > 2 else None),
                    "backfill": bstat, "combined": combined, "unit": unit or "lots", "day": day,
                    "days": sorted(p.parent.name for p in HISTORY_DIR.glob("*/opt_strikes.json")) if HISTORY_DIR.exists() else []})


def record_chain(quotes):
    if datetime.now().weekday() >= 5:      # MCX is shut at weekends: don't start a new day of static quotes
        return
    now = time.time()
    today = date.today().isoformat()
    if opt_series["date"] != today:
        opt_series.update({"date": today, "first_oi": {}, "series": {}})
    if opt_strikes["date"] != today:
        opt_strikes.update({"date": today, "groups": {}, "backfilled": {}, "live_from": {}})
    first = opt_series["first_oi"]
    groups = {}
    for t, q in quotes.items():
        inst = instruments[t]
        if inst["instrument_type"] not in ("CE", "PE"):
            continue
        fut = underlying_future(inst)
        fq = (quotes.get(fut["instrument_token"]) or {}) if fut else {}
        g = groups.setdefault((inst["name"], inst["expiry"].isoformat()),
                              {"F": fq.get("last_price"), "fut_oi": fq.get("oi"), "fut_vol": fq.get("volume"), "strikes": {}})
        oi = q.get("oi") or 0
        base = first.setdefault(inst["tradingsymbol"], oi)
        g["strikes"].setdefault(inst["strike"], {})[inst["instrument_type"]] = {
            "oi": oi, "chg": oi - base, "ltp": q.get("last_price") or 0, "volume": q.get("volume") or 0}
    minute = int(now // 60) * 60_000
    new_minute = False
    snaps = {}
    for (name, expiry), g in groups.items():
        ce = sum(v["CE"]["oi"] for v in g["strikes"].values() if "CE" in v)
        pe = sum(v["PE"]["oi"] for v in g["strikes"].values() if "PE" in v)
        iv, F = None, g["F"]
        if F and g["strikes"]:
            T = (datetime.fromisoformat(expiry + "T23:30:00+05:30").timestamp() - now) / (365 * 24 * 3600)
            k = min(g["strikes"], key=lambda s: abs(s - F))
            ivs = [implied_vol(leg["ltp"], F, k, T, kind) for kind, leg in g["strikes"][k].items() if leg["ltp"]]
            ivs = [v for v in ivs if v]
            iv = sum(ivs) / len(ivs) if ivs else None
        pcr = pe / ce if ce else None
        snaps[(name, expiry)] = {"t": int(now * 1000), "F": F, "pcr": pcr, "iv": iv, "strikes": g["strikes"]}
        s = opt_series["series"].setdefault(f"{name}|{expiry}", [])
        row = [minute, round(pcr, 4) if pcr else None, round(iv, 4) if iv else None, ce, pe]
        if s and s[-1][0] == minute:
            s[-1] = row
        else:
            s.append(row)
            new_minute = True
    # per-strike OI each minute, for the OI buildup view
    for (name, expiry), g in groups.items():
        opt_strikes.setdefault("live_from", {}).setdefault(f"{name}|{expiry}", minute)   # backfill stops here
        rows = opt_strikes["groups"].setdefault(f"{name}|{expiry}", {})
        # [futures price, {strike: [call OI, put OI, call volume, put volume]}, futures OI, futures volume]
        # (volumes are what has traded so far today)
        rows[str(minute)] = [g["F"], {f"{k:g}": [(v.get("CE") or {}).get("oi"), (v.get("PE") or {}).get("oi"),
                                                 (v.get("CE") or {}).get("volume"), (v.get("PE") or {}).get("volume")]
                                      for k, v in g["strikes"].items()}, g.get("fut_oi"), g.get("fut_vol")]
    with lock:
        chain_now.update(snaps)
    try:
        check_iv_alerts(snaps)
    except Exception as e:
        print("IV alert check failed:", e)
    if new_minute:
        OPT_SERIES_FILE.write_text(json.dumps(opt_series))
        if minute // 60_000 % 5 == 0:
            OPT_STRIKES_FILE.write_text(json.dumps(opt_strikes))
        try:
            record_oibias(minute - 60_000)   # the minute that just completed
        except Exception as e:
            print("OI bias record failed:", e)


# ---------- housekeeping: daily archive, keep-awake, gap fill, OI bias history ----------
# 1. Archive: every day file is mirrored into history/<its date>/ whenever it changes, so nothing is lost when the
#    next session overwrites it (plus a daily copy of .tracking.json).
# 2. Keep-awake: while the server runs in MCX hours (weekdays 08:55-23:59) it asks Windows not to sleep.
# 3. Gap fill: gaps in today's minute recording (sleep, restarts) are filled from Kite's minute history after the
#    PC wakes, after 23:57, or on request. Quote-only data (large prints, spreads) can't be recovered.
# 4. The OI bias score is computed every minute from the recorded OI and kept for the day (.oibias.json).
import shutil

HISTORY_DIR = HERE / "history"
OIBIAS_FILE = HERE / ".oibias.json"
DAY_FILES = [".opt_strikes.json", ".opt_series.json", ".oibias.json", ".large_prints.json", ".pnl_history.json",
             ".pnl_range.json", ".flow_baseline.json"]
oibias_hist = {"date": None, "series": [], "fut_base": {}}   # series: [[minute_ms, score, options, futures]]
house = {"archived": 0, "last_archive": None, "fill": {"running": False, "last": None, "msg": ""}, "awake": False}
_arch_mtime = {}


def load_oibias():
    global oibias_hist
    try:
        saved = json.loads(OIBIAS_FILE.read_text())
        if saved.get("date") == date.today().isoformat():
            oibias_hist = saved
    except Exception:
        pass


def archive_files():
    """Mirror each day file into history/<date>/ when it has changed since the last copy."""
    n = 0
    for name in DAY_FILES:
        p = HERE / name
        try:
            mt = p.stat().st_mtime
        except FileNotFoundError:
            continue
        if _arch_mtime.get(name) == mt:
            continue
        try:
            d = json.loads(p.read_text()).get("date") or datetime.fromtimestamp(mt).date().isoformat()
        except Exception:
            continue
        dest = HISTORY_DIR / str(d)[:10] / name.lstrip(".")
        if not dest.exists() or dest.stat().st_mtime < mt:
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(p, dest)
            n += 1
        _arch_mtime[name] = mt
    if TRACK_FILE.exists():   # the investor record: one copy per day
        dest = HISTORY_DIR / date.today().isoformat() / "tracking.json"
        if not dest.exists() or dest.stat().st_mtime < TRACK_FILE.stat().st_mtime:
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(TRACK_FILE, dest)
            n += 1
    if n:
        house["archived"] += n
        house["last_archive"] = int(time.time() * 1000)


def keep_awake(on):
    """Ask Windows not to sleep (the screen can still turn off). Applies to the calling thread until changed."""
    if os.name != "nt" or house["awake"] == on:
        return
    import ctypes
    ES_CONTINUOUS, ES_SYSTEM_REQUIRED = 0x80000000, 0x00000001
    ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS | (ES_SYSTEM_REQUIRED if on else 0))
    house["awake"] = on


def update_fut_base():
    """Previous close and previous OI of each NG future (from the OI & volume table), for the OI bias."""
    with lock:
        futs = [dict(f) for f in state["flow"]["futures"]]
    for f in futs:
        if f.get("name") in PRINTS_NAMES and f.get("prev_oi") and f.get("chg_pct") is not None and f.get("ltp"):
            oibias_hist["fut_base"][f"{f['name']}|{f['expiry'][:7]}"] = [f["ltp"] / (1 + f["chg_pct"] / 100), f["prev_oi"]]


def oibias_at(t):
    """The OI bias score at minute t from the recorded rows (same method as /api/oibias)."""
    big = max(PRINTS_NAMES, key=lambda x: LOT_UNITS[x])
    legs, opt, fut = [], 0.0, 0.0
    for key, rows in list(opt_strikes["groups"].items()):
        name, expiry = key.split("|")
        if name not in PRINTS_NAMES or not rows:
            continue
        mins = sorted(int(m) for m in rows)
        at = [m for m in mins if m <= t]
        if not at:
            continue
        first, row = rows[str(mins[0])], rows[str(at[-1])]
        F = row[0]
        if not F:
            continue
        w = LOT_UNITS[name] / LOT_UNITS[big]
        ser = [r for r in opt_series["series"].get(key, []) if r[0] <= t and r[2]]
        iv = ser[-1][2] if ser else 0.5
        T = (datetime.fromisoformat(expiry + "T23:30:00+05:30").timestamp() - t / 1000) / (365 * 24 * 3600)
        for k, pair in row[1].items():
            f0 = first[1].get(k) or [None, None]
            for i, kind in ((0, "CE"), (1, "PE")):
                if pair[i] is None or f0[i] is None or pair[i] == f0[i]:
                    continue
                s = (pair[i] - f0[i]) * abs(_delta(F, float(k), T, iv, kind)) * w * (1 if kind == "PE" else -1)
                legs.append(s)
                opt += s
        base = oibias_hist["fut_base"].get(f"{name}|{expiry[:7]}")
        foi = row[2] if len(row) > 2 else None
        if base and foi is not None:
            s = abs(foi - base[1]) * w * (1 if F > base[0] else -1 if F < base[0] else 0)
            legs.append(s)
            fut += s
    gross = sum(abs(x) for x in legs)
    return [t, round(100 * sum(legs) / gross) if gross else 0, round(opt, 1), round(fut, 1)]


def record_oibias(minute):
    today = date.today().isoformat()
    if oibias_hist.get("date") != today:
        oibias_hist.update({"date": today, "series": [], "fut_base": oibias_hist.get("fut_base") or {}})
    update_fut_base()
    row = oibias_at(minute)
    s = oibias_hist["series"]
    if s and s[-1][0] == minute:
        s[-1] = row
    else:
        s.append(row)
    OIBIAS_FILE.write_text(json.dumps(oibias_hist))


def _hist_retry(inst, start, end):
    """Minute candles with OI for one instrument, backing off when Kite rate-limits (429)."""
    for attempt in range(6):
        try:
            if USE_MCP:
                return dhan._history(inst["instrument_token"], "minute", start, end, oi=True)
            return [{"time": int(c["date"].timestamp()), "close": c["close"], "oi": c.get("oi"), "volume": c.get("volume")}
                    for c in kite.historical_data(inst["instrument_token"], start, end, "minute", oi=True)]
        except Exception as e:
            if "429" not in str(e) or attempt == 5:
                raise
            time.sleep(2 * (attempt + 1))


def fill_window(key, a, b):
    """Fill minutes strictly between a and b (ms) for one option group from Kite's minute history."""
    rows = opt_strikes["groups"][key]
    name, expiry = key.split("|")
    base = rows[str(a)]
    exp = date.fromisoformat(expiry)
    fut = next((f for f in futures_by_name.get(("MCX", name), [])
                if (f["expiry"].year, f["expiry"].month) == (exp.year, exp.month)), None)
    opts = [o for o in options_by_name.get(("MCX", name), [])
            if o["expiry"].isoformat() == expiry and f"{o['strike']:g}" in base[1]]
    start, end = datetime.fromtimestamp(a / 1000) + timedelta(minutes=1), datetime.fromtimestamp(b / 1000)
    f_by_m, o_by_m, closes = {}, {}, {}
    if fut:
        for c in _hist_retry(fut, start, end):
            f_by_m[c["time"] * 1000] = (c.get("close"), c.get("oi"), c.get("volume") or 0)
    for o in opts:
        i = 0 if o["instrument_type"] == "CE" else 1
        k = f"{o['strike']:g}"
        cell = base[1].get(k) or []
        cum = (cell[i + 2] if len(cell) > i + 2 else None) or 0     # volume so far today at the start of the gap
        try:
            for c in _hist_retry(o, start, end):
                m = c["time"] * 1000
                cum += c.get("volume") or 0
                o_by_m.setdefault(m, []).append((k, i, c.get("oi"), cum))
                closes.setdefault(m, {})[(o["strike"], o["instrument_type"])] = c.get("close")
        except Exception as e:
            print("history failed for", o["tradingsymbol"], e)
    px, foi, fvol = base[0], base[2] if len(base) > 2 else None, base[3] if len(base) > 3 else None
    strikes = {k: list(v) for k, v in base[1].items()}
    ser = opt_series["series"].setdefault(key, [])
    have = {r[0] for r in ser}
    filled, new_ser = [], []
    m = a + 60_000
    while m < b:
        if m in f_by_m:
            c, oi, v = f_by_m[m]
            px = c or px
            foi = oi if oi is not None else foi
            fvol = fvol + v if fvol is not None else None
        for k, i, oi, vol in o_by_m.get(m, []):
            cell = strikes.setdefault(k, [None, None, None, None])
            cell += [None] * (4 - len(cell))
            if oi is not None:
                cell[i] = oi
            cell[i + 2] = vol
        if str(m) not in rows:
            rows[str(m)] = [px, {k: list(v) for k, v in strikes.items()}, foi, fvol]
            filled.append(m)
            if m not in have and px:
                ce = sum(v[0] or 0 for v in strikes.values()); pe = sum(v[1] or 0 for v in strikes.values())
                k_atm = min((float(k) for k in strikes), key=lambda s: abs(s - px))
                T = (datetime.fromisoformat(expiry + "T23:30:00+05:30").timestamp() - m / 1000) / (365 * 24 * 3600)
                ivs = [implied_vol(p, px, k_atm, T, kind) for (s, kind), p in (closes.get(m) or {}).items() if s == k_atm and p]
                ivs = [v for v in ivs if v]
                new_ser.append([m, round(pe / ce, 4) if ce else None, round(sum(ivs) / len(ivs), 4) if ivs else None, ce, pe])
        m += 60_000
    if new_ser:
        ser[:] = sorted(new_ser + ser, key=lambda r: r[0])
    return filled


def fill_gaps():
    """Find gaps of more than 3 minutes in today's recording and fill them from Kite's minute history."""
    fs = house["fill"]
    if fs["running"] or not state["logged_in"] or opt_strikes.get("date") != date.today().isoformat():
        return
    fs["running"] = True
    fs["msg"] = "Filling gaps from Kite history…"
    try:
        filled = set()
        for key in list(opt_strikes["groups"]):
            mins = sorted(int(m) for m in opt_strikes["groups"][key])
            for a, b in zip(mins, mins[1:]):
                if b - a > 3 * 60_000:
                    filled.update(fill_window(key, a, b))
        for m in sorted(filled):   # the OI bias for the filled minutes
            row = oibias_at(m)
            s = oibias_hist["series"]
            if not any(r[0] == m for r in s):
                s.append(row)
        oibias_hist["series"].sort(key=lambda r: r[0])
        OPT_STRIKES_FILE.write_text(json.dumps(opt_strikes))
        OPT_SERIES_FILE.write_text(json.dumps(opt_series))
        OIBIAS_FILE.write_text(json.dumps(oibias_hist))
        fs["msg"] = f"Filled {len(filled)} minutes from Kite history" if filled else "No gaps to fill"
        print(fs["msg"])
    except Exception as e:
        fs["msg"] = f"Gap fill failed: {e}"
        print(fs["msg"])
    finally:
        fs["running"], fs["last"] = False, int(time.time() * 1000)


def housekeeping_loop():
    last, eod_done, pending_fill = time.time(), None, time.time() + 120   # also check for gaps soon after a (re)start
    while True:
        try:
            now, dt = time.time(), datetime.now()
            today = dt.date().isoformat()
            if now - last > 180:           # the PC slept or the server stalled: fill the hole a little later
                pending_fill = now + 120
            last = now
            mins = dt.hour * 60 + dt.minute
            keep_awake(dt.weekday() < 5 and 8 * 60 + 55 <= mins <= 23 * 60 + 59)
            archive_files()
            if pending_fill and now >= pending_fill and state["logged_in"]:
                pending_fill = None
                threading.Thread(target=fill_gaps, daemon=True).start()
            if dt.weekday() < 5 and mins >= 23 * 60 + 57 and eod_done != today and state["logged_in"]:
                eod_done = today
                fill_gaps()
                archive_files()
        except Exception as e:
            print("housekeeping:", e)
        time.sleep(30)


@app.route("/api/housekeeping", methods=["GET", "POST"])
def api_housekeeping():
    """Status of archiving / keep-awake / gap filling. POST {"fill": true} fills today's gaps now."""
    if request.method == "POST" and (request.get_json(force=True) or {}).get("fill"):
        threading.Thread(target=fill_gaps, daemon=True).start()
    days = sorted(p.name for p in HISTORY_DIR.iterdir() if p.is_dir()) if HISTORY_DIR.exists() else []
    return jsonify({**house, "history_days": days})


# ---------- IV alert: current-month ATM IV at or above a level (e.g. to sell straddles) ----------
# Fires once when IV crosses up through the level, re-arms after it drops 1 point below it, and at most once every
# 30 minutes per underlying. Shows a Windows notification and a dashboard banner. Only reads data; places nothing.
ALERTS_FILE = HERE / ".alerts.json"
iv_alert = {"enabled": True, "iv_above": 58.0}
iv_alert_state = {}            # name -> {"armed": bool, "last": epoch}
state["iv_alerts"] = []        # newest first


def load_alerts():
    try:
        iv_alert.update(json.loads(ALERTS_FILE.read_text()))
    except Exception:
        pass


def windows_toast(title, text):
    """Native Windows notification through PowerShell (no extra packages)."""
    if os.name != "nt":
        return
    import subprocess
    x = lambda s: s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("'", "''")
    ps = ("[Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime] > $null;"
          "[Windows.Data.Xml.Dom.XmlDocument, Windows.Data.Xml.Dom.XmlDocument, ContentType = WindowsRuntime] > $null;"
          "$d = New-Object Windows.Data.Xml.Dom.XmlDocument;"
          f"$d.LoadXml('<toast scenario=\"reminder\"><visual><binding template=\"ToastGeneric\"><text>{x(title)}</text><text>{x(text)}</text></binding></visual>"
          "<actions><action content=\"Dismiss\" arguments=\"dismiss\" activationType=\"system\"/></actions><audio src=\"ms-winsoundevent:Notification.Looping.Alarm\"/></toast>');"
          "[Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier("
          "'{1AC14E77-02E7-4E5D-B744-2EB1AE5198B7}\\WindowsPowerShell\\v1.0\\powershell.exe').Show("
          "[Windows.UI.Notifications.ToastNotification]::new($d))")
    try:
        subprocess.Popen(["powershell", "-NoProfile", "-NonInteractive", "-Command", ps],
                         creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    except Exception as e:
        print("Windows notification failed:", e)


def check_iv_alerts(snaps):
    """snaps: (name, expiry) -> chain snapshot. Uses each underlying's nearest option expiry."""
    if not iv_alert.get("enabled"):
        return
    level = float(iv_alert.get("iv_above") or 0)
    front = {}
    for (name, expiry), s in snaps.items():
        if name not in PRINTS_NAMES or not s.get("iv") or not s.get("F"):
            continue
        if name not in front or expiry < front[name][0]:
            front[name] = (expiry, s)
    now = time.time()
    for name, (expiry, s) in front.items():
        iv = s["iv"] * 100
        st = iv_alert_state.setdefault(name, {"armed": True, "last": 0})   # already above at start: alert at once
        if iv < level - 1:
            st["armed"] = True
            continue
        if iv < level or not st["armed"] or now - st["last"] < 1800:
            continue
        st["armed"], st["last"] = False, now
        # the straddle at the strike nearest the futures price
        k = min(s["strikes"], key=lambda x: abs(x - s["F"]))
        ce, pe = (s["strikes"][k].get("CE") or {}).get("ltp") or 0, (s["strikes"][k].get("PE") or {}).get("ltp") or 0
        prem = ce + pe
        short = "NG" if name == "NATURALGAS" else "NG Mini"
        units = LOT_UNITS.get(name, 1)
        mon = datetime.fromisoformat(expiry).strftime("%d %b")
        a = {"time": int(now * 1000), "name": name, "expiry": expiry, "iv": round(iv, 2), "level": level,
             "strike": k, "ce": ce, "pe": pe, "premium": round(prem, 2), "per_lot": round(prem * units),
             "be_low": round(k - prem, 2), "be_high": round(k + prem, 2), "fut": s["F"],
             "text": f"{short} {mon} ATM IV {iv:.1f}% (alert at {level:g}%). {k:g} straddle {prem:.2f} "
                     f"(CE {ce:.2f} + PE {pe:.2f}) = ₹{prem * units:,.0f} per lot; breakevens {k - prem:.2f} / {k + prem:.2f}."}
        with lock:
            state["iv_alerts"] = ([a] + state["iv_alerts"])[:20]
        print("IV ALERT:", a["text"])
        windows_toast(f"CT89 Deck · {short} IV {iv:.1f}% ≥ {level:g}%", a["text"].split(". ", 1)[1])


@app.route("/api/alerts", methods=["GET", "POST"])
def api_alerts():
    """{"enabled": bool, "iv_above": percent}"""
    if request.method == "POST":
        body = request.get_json(force=True) or {}
        if "enabled" in body:
            iv_alert["enabled"] = bool(body["enabled"])
        if body.get("iv_above") is not None:
            iv_alert["iv_above"] = max(1.0, min(300.0, float(body["iv_above"])))
            iv_alert_state.clear()     # re-arm against the new level
        ALERTS_FILE.write_text(json.dumps(iv_alert))
    return jsonify({**iv_alert, "recent": state["iv_alerts"]})


@app.route("/api/optstats")
def api_optstats():
    """?symbol=<futures symbol>: OI by strike, PCR and ATM IV (now and per minute today) for its option expiry."""
    fut = by_symbol.get(("MCX", request.args.get("symbol", "")))
    if not fut or fut["instrument_type"] != "FUT":
        return jsonify({"error": "unknown futures contract"}), 404
    expiry = option_chain(fut)["opt_expiry"]
    with lock:
        snap = chain_now.get((fut["name"], expiry))
        series = list(opt_series["series"].get(f"{fut['name']}|{expiry}", []))
    profile = []
    if snap:
        for k in sorted(snap["strikes"]):
            v = snap["strikes"][k]
            profile.append({"strike": k, "ce": (v.get("CE") or {}).get("oi", 0), "pe": (v.get("PE") or {}).get("oi", 0),
                            "ce_chg": (v.get("CE") or {}).get("chg", 0), "pe_chg": (v.get("PE") or {}).get("chg", 0)})
    return jsonify({"name": fut["name"], "expiry": expiry, "updated": snap and snap["t"], "F": snap and snap["F"],
                    "pcr": snap and snap["pcr"], "iv": snap and snap["iv"], "profile": profile, "series": series})


def prints_loop():
    toks, toks_at = [], 0
    while True:
        time.sleep(PRINTS_SECS)
        if not state["logged_in"]:
            continue
        if not USE_MCP and dhan:
            state["prints"]["error"] = "Large prints need Kite quotes (Kite MCP, or the paid Kite Connect plan without Dhan)."
            time.sleep(60)
            continue
        try:
            has_opts = any(instruments[t]["instrument_type"] != "FUT" for t in toks)
            if not toks or not has_opts or time.time() - toks_at > 120:   # follow the money as prices move
                toks, toks_at = prints_tokens(), time.time()
            if toks:
                scan_prints(toks)
        except Exception as e:
            with lock:
                state["prints"]["error"] = f"Large-print scan failed: {e}"
            time.sleep(10)


# ---------- news ----------
NEWS_QUERY = '"natural gas" (Henry Hub OR LNG OR EIA OR futures OR storage) when:2d -site:tradingview.com'
NEWS_TTL = 600
news_cache = {"at": 0, "items": [], "error": None}


def fetch_news():
    """Google News RSS headlines; cached for 10 minutes."""
    import urllib.parse
    import urllib.request
    import xml.etree.ElementTree as ET
    from email.utils import parsedate_to_datetime
    if time.time() - news_cache["at"] < NEWS_TTL:
        return news_cache
    url = "https://news.google.com/rss/search?" + urllib.parse.urlencode(
        {"q": NEWS_QUERY, "hl": "en-US", "gl": "US", "ceid": "US:en"})
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (kite-live dashboard)"})
        with urllib.request.urlopen(req, timeout=10) as r:
            root = ET.fromstring(r.read())
        items = []
        for it in root.iter("item"):
            pub = it.findtext("pubDate")
            src = it.find("source")
            items.append({
                "title": it.findtext("title") or "",
                "link": it.findtext("link") or "",
                "source": src.text if src is not None else "",
                "published": int(parsedate_to_datetime(pub).timestamp() * 1000) if pub else None,
            })
        items.sort(key=lambda x: x["published"] or 0, reverse=True)
        news_cache.update(at=time.time(), items=items[:25], error=None)
    except Exception as e:
        news_cache.update(at=time.time() - NEWS_TTL + 60, error=f"News fetch failed: {e}")  # retry in a minute
    return news_cache


@app.route("/api/news")
def api_news():
    n = fetch_news()
    return jsonify({"items": n["items"], "error": n["error"], "fetched": int(n["at"] * 1000)})


# ---------- ticker ----------

def sync_subscriptions():
    with lock:
        wanted = {p["instrument_token"] for p in state["positions"]} | state.get("underlying_tokens", set())
    new = list(wanted - subscribed)
    if new and ticker is not None and ticker.is_connected():
        ticker.subscribe(new)
        # Quote mode includes the day's OHLC alongside the price
        ticker.set_mode(ticker.MODE_QUOTE, new)
        subscribed.update(new)


def start_ticker(token):
    global ticker
    if ticker is not None:
        # a new login means a new access token: replace the old stream
        try:
            ticker.close()
        except Exception:
            pass
        ticker = None
        subscribed.clear()
    ticker = KiteTicker(API_KEY, token)

    def on_ticks(ws, ticks):
        with lock:
            for t in ticks:
                state["ltp"][t["instrument_token"]] = t["last_price"]
                merge_ohlc(t["instrument_token"], t.get("ohlc"), t["last_price"])
            state["last_tick"] = time.time()

    def on_connect(ws, response):
        with lock:
            state["ticker"] = "connected"
        subscribed.clear()
        sync_subscriptions()

    def on_close(ws, code, reason):
        with lock:
            state["ticker"] = "disconnected"

    def on_reconnect(ws, attempts):
        with lock:
            state["ticker"] = f"reconnecting ({attempts})"

    ticker.on_ticks = on_ticks
    ticker.on_connect = on_connect
    ticker.on_close = on_close
    ticker.on_reconnect = on_reconnect
    ticker.connect(threaded=True)


# ---------- API ----------

@app.route("/")
def index():
    return send_file(HERE / "dashboard.html")


# ---------- price chart ----------
# MCX candles come from Dhan (or Kite's paid plan). NYMEX Henry Hub (NG=F) comes from Yahoo's public chart
# API and is converted to ₹ with USD/INR, so it works without any keys.
CHART_TF = {  # tf -> (Dhan interval, Kite interval, Yahoo interval, Yahoo range, days of history)
    "1m": ("1", "minute", "1m", "1d", 2), "5m": ("5", "5minute", "5m", "5d", 5),
    "15m": ("15", "15minute", "15m", "5d", 10), "1h": ("60", "60minute", "60m", "1mo", 30),
    "1d": ("D", "day", "1d", "6mo", 180),
}
chart_cache = {}


def yahoo_candles(symbol, interval, rng):
    r = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
                     params={"interval": interval, "range": rng}, timeout=(5, 15),
                     headers={"User-Agent": "Mozilla/5.0 (kite-live dashboard)"})
    res = r.json()["chart"]["result"][0]
    q = res["indicators"]["quote"][0]
    out = []
    for i, t in enumerate(res.get("timestamp") or []):
        if q["close"][i] is None:
            continue
        out.append({"time": t, "open": q["open"][i], "high": q["high"][i], "low": q["low"][i],
                    "close": q["close"][i], "volume": q["volume"][i] or 0})
    return out


@app.route("/api/chart")
def api_chart():
    """?source=mcx|nymex&tf=1m|5m|15m|1h|1d&symbol=<Kite-style futures symbol, for MCX>"""
    source, tf, symbol = request.args.get("source", "nymex"), request.args.get("tf", "5m"), request.args.get("symbol", "")
    if tf not in CHART_TF:
        return jsonify({"error": "bad timeframe"}), 400
    key = (source, tf, symbol)
    hit = chart_cache.get(key)
    if hit and time.time() - hit[0] < (60 if tf == "1d" else 15):
        return jsonify(hit[1])
    d_iv, k_iv, y_iv, y_rng, days = CHART_TF[tf]
    try:
        if source == "mcx":
            if DEMO_SERVER or not state["logged_in"] and not dhan:
                return jsonify({"error": "MCX candles need the live connection (Dhan or Kite keys in .env)."}), 409
            inst = by_symbol.get(("MCX", symbol))
            if not inst:
                return jsonify({"error": f"{symbol} isn't loaded yet"}), 404
            if dhan:
                candles = dhan.candles(inst["instrument_token"], inst["segment"], inst["dhan_instrument"], d_iv, days)
            else:
                rows = kite.historical_data(inst["instrument_token"], datetime.now() - timedelta(days=days), datetime.now(),
                                            k_iv, oi=True)
                candles = [{"time": int(c["date"].timestamp()), "open": c["open"], "high": c["high"], "low": c["low"],
                            "close": c["close"], "volume": c["volume"], "oi": c.get("oi") or 0} for c in rows]
            out = {"source": "mcx", "label": f"{symbol} · MCX", "currency": "INR", "candles": candles}
        else:
            ng = yahoo_candles("NG=F", y_iv, y_rng)
            fx = yahoo_candles("USDINR=X", y_iv if tf != "1m" else "5m", y_rng if tf != "1m" else "5d")
            # convert each NG candle with the latest USD/INR at or before its time
            j, rate, conv = 0, fx[0]["close"] if fx else None, []
            for c in ng:
                while j < len(fx) and fx[j]["time"] <= c["time"]:
                    rate = fx[j]["close"]
                    j += 1
                if rate:
                    conv.append({"time": c["time"], **{k: c[k] * rate for k in ("open", "high", "low", "close")},
                                 "volume": c["volume"], "usd": c["close"]})
            out = {"source": "nymex", "label": "NYMEX NG (NG=F) × USD/INR", "currency": "INR",
                   "usd_last": ng[-1]["close"] if ng else None, "fx_last": fx[-1]["close"] if fx else None, "candles": conv}
    except Exception as e:
        return jsonify({"error": f"Chart data failed: {e}"}), 502
    chart_cache[key] = (time.time(), out)
    return jsonify(out)


PLAN_FILE = HERE / (".plan_demo.json" if DEMO_SERVER else ".plan.json")


@app.route("/api/plan", methods=["GET", "POST"])
def api_plan():
    """Position plan: {"band": 10, "targets": {"<month>|<name>|<FUT|CE|PE>": lots}, "extra": [keys]}."""
    if request.method == "POST":
        plan = request.get_json(force=True)
        if not isinstance(plan, dict):
            return jsonify({"error": "expected an object"}), 400
        PLAN_FILE.write_text(json.dumps(plan, indent=1))
        return jsonify({"ok": True})
    try:
        return jsonify(json.loads(PLAN_FILE.read_text()))
    except Exception:
        return jsonify(None)


PAIRS_FILE = HERE / (".pairs_demo.json" if DEMO_SERVER else ".pairs.json")


@app.route("/api/pairs", methods=["GET", "POST"])
def api_pairs():
    """Your position pairs/groups: [{"id", "name", "legs": [{"symbol", "lots"}]}], in display order."""
    if request.method == "POST":
        pairs = request.get_json(force=True)
        if not isinstance(pairs, list):
            return jsonify({"error": "expected a list"}), 400
        PAIRS_FILE.write_text(json.dumps(pairs, indent=1))
        return jsonify({"ok": True})
    try:
        return jsonify(json.loads(PAIRS_FILE.read_text()))
    except Exception:
        return jsonify(None)


@app.route("/api/margin", methods=["POST"])
def api_margin():
    """
    Body: {"legs": [{"symbol", "qty"}], "sims": [{"symbol", "qty"}]} with qty in signed lots.
    Returns the netted portfolio margin for the legs, gross (each leg alone), NG / NG Mini / month subsets,
    and, with sims, the portfolio margin including them. Uses the exchange SPAN files via Zerodha's calculator.
    """
    body = request.get_json(force=True) or {}
    legs = [(l["symbol"], int(l["qty"])) for l in body.get("legs", []) if l.get("qty")]
    sims = [(l["symbol"], int(l["qty"])) for l in body.get("sims", []) if l.get("qty")]
    syms = [s for s, _ in legs]
    groups = {}
    for name in sorted({margin_name(s) for s in syms}):
        groups[name] = [s for s in syms if margin_name(s) == name]
    for month in sorted({s[len(margin_name(s)):][:5] for s in syms}, key=month_sort):
        groups[month] = [s for s in syms if s[len(margin_name(s)):][:5] == month]
    try:
        if body.get("quick"):
            # just the netted totals, without and with the extra legs (used to compare adjustment plans)
            merged = {}
            for s, q in legs + sims:
                merged[s] = merged.get(s, 0) + q
            base = margin_calc.portfolio_only(legs)
            withs = margin_calc.portfolio_only([(s, q) for s, q in merged.items() if q])
            return jsonify({"portfolio": base, "with_sims": withs})
        out = margin_calc.portfolio(legs, groups) or {}
        if sims:
            # merge simulated lots into the same contracts so offsets are counted
            merged = {}
            for s, q in legs + sims:
                merged[s] = merged.get(s, 0) + q
            withs = margin_calc.portfolio([(s, q) for s, q in merged.items() if q])
            out["with_sims"] = withs and withs["portfolio"]
    except Exception as e:
        return jsonify({"error": f"Margin calculation failed: {e}"}), 502
    if not DEMO_SERVER and state["logged_in"]:
        try:
            m = kite.margins("commodity")
            out["funds"] = {"net": m.get("net"), "available": (m.get("available") or {}).get("live_balance"),
                            "utilised": {k: (m.get("utilised") or {}).get(k) for k in ("span", "exposure", "option_premium", "debits")}}
        except Exception:
            pass
    return jsonify(out)


def margin_name(symbol):
    m = re.match(r"^([A-Z]+?)\d{2}[A-Z]{3}", symbol)
    return m.group(1) if m else symbol


def month_sort(yymon):
    months = "JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split()
    return (yymon[:2], months.index(yymon[2:]) if yymon[2:] in months else 99)


# ---------- margin optimiser: same exposure, less margin ----------
# Looks for three patterns and prices each step on the SPAN calculator (read-only; it never places orders):
#  1. long futures + short calls (covered calls)      == short puts at the same strikes   (put-call parity)
#  2. short futures + short puts                       == short calls at the same strikes
#  3. NG and NG Mini futures of the same month on opposite sides -> one net position (1 Mini lot = 0.2 NG lot)
OPT_RE = re.compile(r"^([A-Z]+?)(\d{2}[A-Z]{3})(\d+(?:\.\d+)?)(CE|PE)$")
margin_opt_cache = {"at": 0, "data": None}


def _book_legs():
    """Open NG legs: symbol -> {name, base (e.g. NATGASMINI26OCT), kind, strike, qty, avg, ltp, mult}."""
    with lock:
        prices = dict(state["ltp"])
        out = {}
        for p in state["positions"]:
            if not p["quantity"] or margin_name(p["tradingsymbol"]) not in PRINTS_NAMES:
                continue
            sym = p["tradingsymbol"]
            m = OPT_RE.match(sym)
            out[sym] = {"name": margin_name(sym), "base": sym[:-3] if sym.endswith("FUT") else m.group(1) + m.group(2),
                        "kind": "FUT" if sym.endswith("FUT") else m.group(4), "strike": float(m.group(3)) if m else None,
                        "qty": p["quantity"], "avg": p.get("average_price") or 0, "ltp": live_values(p, prices)[0],
                        "mult": p.get("multiplier") or LOT_UNITS.get(margin_name(sym), 1)}
    return out


def _opt_quote(base, strike, kind):
    """(ltp, futures price, years to expiry) for an option from the live chain (for new legs' delta)."""
    name = re.match(r"^[A-Z]+?(?=\d)", base).group(0)
    for (n, expiry), s in chain_now.items():
        if n == name and date.fromisoformat(expiry).strftime("%y%b").upper() == base[len(name):]:
            leg = ((s.get("strikes") or {}).get(strike) or {}).get(kind) or {}
            T = (datetime.fromisoformat(expiry + "T23:30:00+05:30").timestamp() - time.time()) / (365 * 24 * 3600)
            return leg.get("ltp"), s.get("F"), T
    return None, None, None


def _book_delta(book, legs):
    """Net delta of a book in NG lots, by month (options via Black-76). Uses each held option's own price for
    its IV; new strikes use the month's median IV. Works when the live chain is empty (weekends)."""
    big = LOT_UNITS["NATURALGAS"]

    def fut_px(base):
        inst = by_symbol.get(("MCX", base + "FUT"))
        return (state["ltp"].get(inst["instrument_token"]) if inst else None) or board_price(base + "FUT")

    def opt_T(sym):
        inst = by_symbol.get(("MCX", sym))
        if not inst or not inst.get("expiry"):
            return None
        return (datetime.fromisoformat(inst["expiry"].isoformat() + "T23:30:00+05:30").timestamp() - time.time()) / (365 * 24 * 3600)

    ivs = {}                     # month base -> IVs of held options
    for sym, l in legs.items():
        if l["kind"] != "FUT" and l["ltp"]:
            F, T = fut_px(l["base"]), opt_T(sym)
            iv = implied_vol(l["ltp"], F, l["strike"], T, l["kind"]) if F and T and T > 0 else None
            if iv:
                ivs.setdefault(l["base"], []).append(iv)
    out = {}
    for sym, q in book.items():
        if not q:
            continue
        name = margin_name(sym)
        units = LOT_UNITS.get(name, 1)
        if sym.endswith("FUT"):
            d, base = 1.0, sym[:-3]
        else:
            m = OPT_RE.match(sym)
            base, k, kind = m.group(1) + m.group(2), float(m.group(3)), m.group(4)
            F, T = fut_px(base), opt_T(sym)
            iv = None
            if sym in legs and legs[sym]["ltp"] and F and T and T > 0:
                iv = implied_vol(legs[sym]["ltp"], F, k, T, kind)
            if not iv:
                xs = sorted(ivs.get(base) or [0.5])
                iv = xs[len(xs) // 2]
            d = _delta(F, k, T, iv, kind) if F and T else 0
        mon = base[len(name):]
        out[mon] = round(out.get(mon, 0) + q * d * units / big, 2)
    return out


def optimise_margin():
    legs = _book_legs()
    book = {s: l["qty"] for s, l in legs.items()}
    steps = []

    def add_step(title, why, trades):
        trades = [t for t in trades if t["qty"]]
        if trades:
            steps.append({"title": title, "why": why, "trades": trades})
            for t in trades:
                book[t["symbol"]] = book.get(t["symbol"], 0) + t["qty"]

    bases = sorted({l["base"] for l in legs.values()})
    for base in bases:
        name = margin_name(base)
        fut = next((s for s, l in legs.items() if l["base"] == base and l["kind"] == "FUT"), None)
        f = book.get(fut, 0) if fut else 0
        calls = sorted([s for s, l in legs.items() if l["base"] == base and l["kind"] == "CE" and l["qty"] < 0], key=lambda s: legs[s]["strike"])
        puts = sorted([s for s, l in legs.items() if l["base"] == base and l["kind"] == "PE" and l["qty"] < 0], key=lambda s: -legs[s]["strike"])
        if f > 0 and calls:              # 1. covered calls -> short puts (deepest in-the-money calls first)
            n, trades = f, []
            for c in calls:
                q = min(n, -book[c])
                if q <= 0:
                    break
                k = legs[c]["strike"]
                trades += [{"symbol": c, "qty": q, "action": "Buy back"}, {"symbol": f"{base}{k:g}PE", "qty": -q, "action": "Sell"}]
                n -= q
            used = f - n
            add_step(f"{shortn(name)} {base[-3:]}: covered calls → short puts",
                     f"{used} long futures + {used} short calls behave exactly like {used} short puts at the same strikes",
                     [{"symbol": fut, "qty": -used, "action": "Sell"}] + trades)
        elif f < 0 and puts:             # 2. short futures + short puts -> short calls
            n, trades = -f, []
            for p in puts:
                q = min(n, -book[p])
                if q <= 0:
                    break
                k = legs[p]["strike"]
                trades += [{"symbol": p, "qty": q, "action": "Buy back"}, {"symbol": f"{base}{k:g}CE", "qty": -q, "action": "Sell"}]
                n -= q
            used = -f - n
            add_step(f"{shortn(name)} {base[-3:]}: short futures + short puts → short calls",
                     f"{used} short futures + {used} short puts behave exactly like {used} short calls at the same strikes",
                     [{"symbol": fut, "qty": used, "action": "Buy"}] + trades)
    # 3. NG vs NG Mini futures of the same month on opposite sides -> one net position
    for mon in sorted({b[len(margin_name(b)):] for b in bases}, key=month_sort):
        ng, mini = f"NATURALGAS{mon}FUT", f"NATGASMINI{mon}FUT"
        a, b = book.get(ng, 0), book.get(mini, 0)
        if a and b and (a > 0) != (b > 0):
            net = a + b * LOT_UNITS["NATGASMINI"] / LOT_UNITS["NATURALGAS"]
            new_ng = int(net)
            new_mini = round((net - new_ng) * LOT_UNITS["NATURALGAS"] / LOT_UNITS["NATGASMINI"])
            add_step(f"{mon[-3:]}: merge NG and NG Mini futures",
                     f"NG {a:+d} and Mini {b:+d} net to {net:+.1f} NG lots; held as NG {new_ng:+d}, Mini {new_mini:+d}",
                     [{"symbol": ng, "qty": new_ng - a, "action": "Buy" if new_ng > a else "Sell"},
                      {"symbol": mini, "qty": new_mini - b, "action": "Buy" if new_mini > b else "Sell"}])
    # price each step on the SPAN calculator, cumulatively
    start = {s: l["qty"] for s, l in legs.items()}
    m0 = margin_calc.portfolio_only(list(start.items()))
    cur, prev = dict(start), (m0 or {}).get("total")
    for st in steps:
        for t in st["trades"]:
            cur[t["symbol"]] = cur.get(t["symbol"], 0) + t["qty"]
        m = margin_calc.portfolio_only([(s, q) for s, q in cur.items() if q])
        st["margin_after"] = m and m["total"]
        st["saving"] = round(prev - m["total"], 2) if prev is not None and m else None
        prev = m and m["total"]
        # booked P&L from closing existing lots, and a rough cost (one tick per lot + ₹20 per order)
        booked = cost = 0.0
        for t in st["trades"]:
            l = legs.get(t["symbol"])
            units = LOT_UNITS.get(margin_name(t["symbol"]), 1)
            if l and (l["qty"] > 0) != (t["qty"] > 0):
                closed = min(abs(t["qty"]), abs(l["qty"]))
                booked += closed * (1 if l["qty"] > 0 else -1) * (l["ltp"] - l["avg"]) * l["mult"]
            cost += abs(t["qty"]) * units * (0.10 if t["symbol"].endswith("FUT") else 0.05) + 20
            t["label"] = t["symbol"]
        st["booked"], st["cost"] = round(booked), round(cost)
    after = {s: q for s, q in cur.items() if q}
    return {"at": int(time.time() * 1000), "steps": steps,
            "margin_now": (m0 or {}).get("total"), "margin_after": prev,
            "skipped": (m0 or {}).get("skipped") or [],
            "delta_before": _book_delta(start, legs), "delta_after": _book_delta(after, legs),
            "booked": sum(s["booked"] for s in steps), "cost": sum(s["cost"] for s in steps)}


def shortn(name):
    return {"NATURALGAS": "NG", "NATGASMINI": "Mini"}.get(name, name)


@app.route("/api/margin_opt")
def api_margin_opt():
    """Margin optimiser: steps that keep the same exposure and need less margin (cached 5 minutes)."""
    if DEMO_SERVER or not state["logged_in"]:
        return jsonify({"error": "Needs the live connection"}), 409
    if request.args.get("refresh") or time.time() - margin_opt_cache["at"] > 300 or not margin_opt_cache["data"]:
        try:
            margin_opt_cache.update({"at": time.time(), "data": optimise_margin()})
        except Exception as e:
            return jsonify({"error": f"Margin optimiser failed: {e}"}), 502
    return jsonify(margin_opt_cache["data"])


@app.route("/api/state")
def api_state():
    if DEMO_SERVER:
        return jsonify({"demo_server": True})
    with lock:
        prices = dict(state["ltp"])
        positions = []
        for p in state["positions"]:
            price, pnl, m2m = live_values(p, prices)
            mult = p.get("multiplier") or 1
            row = {
                "symbol": p["tradingsymbol"],
                "exchange": p["exchange"],
                "product": p["product"],
                "qty": p["quantity"],
                "avg": p["average_price"],
                "ltp": price,
                "pnl": round(pnl, 2),
                "day_pnl": round(m2m, 2),
                "mult": mult,
                # Realised part: P&L = base + qty * mult * price
                "base": round(pnl - p["quantity"] * mult * price, 2),
            }
            inst = instruments.get(p["instrument_token"])
            if inst and inst["instrument_type"] in ("FUT", "CE", "PE"):
                fut = underlying_future(inst)
                row.update({
                    "name": inst["name"],
                    "kind": inst["instrument_type"],
                    "strike": inst["strike"],
                    "expiry": inst["expiry"].isoformat() if inst["expiry"] else None,
                    "und_symbol": fut and fut["tradingsymbol"],
                    "und_ltp": fut and prices.get(fut["instrument_token"]),
                })
                if inst["instrument_type"] in ("CE", "PE"):
                    row["entry_iv"] = entry_iv_for(p["tradingsymbol"], p["quantity"])
            row["fifo_in"] = fifo_inputs(p, state.get("trades") or [])
            positions.append(row)
        orders = [{
            "time": str(o.get("order_timestamp") or ""),
            "side": o["transaction_type"],
            "symbol": o["tradingsymbol"],
            "exchange": o["exchange"],
            "product": o["product"],
            "type": o["order_type"],
            "filled": o["filled_quantity"],
            "qty": o["quantity"],
            "price": o["average_price"] or o["price"],
            "status": o["status"],
        } for o in state["orders"]]
        underlyings = []
        # futures behind your positions, plus NG / NG Mini for the next two months so the simulator can
        # build positions from scratch (their prices come from the large-prints scan)
        held = state.get("underlying_tokens", set())
        extra = {f["instrument_token"] for name in PRINTS_NAMES
                 for f in [f for f in futures_by_name.get(("MCX", name), []) if f["expiry"] >= date.today()][:2]}
        for tok in sorted(held | extra, key=lambda t: (instruments[t]["name"], instruments[t]["expiry"])):
            inst, o = instruments[tok], state["ohlc"].get(tok)
            underlyings.append({
                "symbol": inst["tradingsymbol"], "name": inst["name"],
                "expiry": inst["expiry"].isoformat(),
                "ltp": prices.get(tok) or board_price(inst["tradingsymbol"]),
                "held": tok in held, "mult": LOT_UNITS.get(inst["name"]),
                **({k: o[k] for k in ("open", "high", "low", "close")} if o else {}),
                **option_chain(inst),
            })
        history_ref = ng_ref()[0]
        if state["positions"]:
            track_pnl_range(*current_totals())
        return jsonify({
            "underlyings": underlyings,
            "pnl_range": state["pnl_range"],
            "flow": state["flow"],
            "prints": {**state["prints"], "items": state["prints"]["items"][:300]},
            "logged_in": state["logged_in"],
            "user": state["user"],
            "positions": positions,
            "orders": orders,
            "history": state["history"],
            "history_ref": history_ref,
            "iv_alerts": state["iv_alerts"][:5],
            "funds": state.get("funds"),
            "iv_alert": iv_alert,
            "last_rest": state["last_rest"],
            "last_tick": state["last_tick"],
            "ticker": state["ticker"],
            "feed": "Kite MCP" if USE_MCP else "Dhan" if dhan else "Kite",
            "login_warning": state.get("login_warning"),
            "error": state["error"],
        })


if __name__ == "__main__":
    if DEMO_SERVER:
        print("No KITE_API_KEY / KITE_API_SECRET in .env, so running in DEMO mode:\n"
              "sample positions with simulated prices; news and the margin calculator are live.")
    # --port lets a second copy run for testing; Kite's login redirect expects the default 5000
    port = int(sys.argv[sys.argv.index("--port") + 1]) if "--port" in sys.argv else 5000
    # Windows lets two servers share a port, which splits requests between them: refuse to start a second one
    import socket
    with socket.socket() as probe:
        probe.settimeout(1)
        if probe.connect_ex(("127.0.0.1", port)) == 0:
            print(f"The dashboard is already running at http://127.0.0.1:{port}, opening it instead.")
            if "--no-browser" not in sys.argv:
                import webbrowser
                webbrowser.open(f"http://127.0.0.1:{port}")
            sys.exit(0)
    if not DEMO_SERVER:
        state["pnl_range"] = load_pnl_range()
        load_history()
        load_tracking()
        load_entry_book()
        if not try_cached_token() and "--no-browser" not in sys.argv:
            # No session for today: open Kite's login page (you still enter password + TOTP yourself)
            print("No Kite session for today, opening the Kite login page in your browser...")
            open_login_page(port)
        threading.Thread(target=poll_loop, daemon=True).start()
        load_prints()
        load_opt_series()
        load_opt_strikes()
        load_oibias()
        load_alerts()
        threading.Thread(target=housekeeping_loop, daemon=True).start()
        threading.Thread(target=prints_loop, daemon=True).start()
    print(f"Dashboard: http://127.0.0.1:{port}")
    app.run(host="127.0.0.1", port=port, threaded=True)
