"""
Read-only access to your Zerodha account through Kite MCP (https://mcp.kite.trade/mcp), Zerodha's official
MCP server. No Kite Connect app or subscription is needed: you log in through Zerodha's own page (password +
TOTP) using a link from the `login` tool.

Two pieces:
  KiteMCP  - a kiteconnect-like client for positions, orders, trades, margins and profile.
  McpFeed  - market data with the same interface as DhanFeed (quotes, option chain, candles, history),
             so the server's Dhan code paths work unchanged. Instrument details (expiry, strike) come from
             Dhan's public instrument list, which needs no login.

Kite MCP also exposes order tools (place/modify/cancel orders, GTTs). This client refuses to call anything
outside READ_TOOLS, so nothing here can trade.
"""
import json
import re
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

from dhan_feed import DhanFeed

URL = "https://mcp.kite.trade/mcp"
READ_TOOLS = {
    "login", "get_profile", "get_positions", "get_orders", "get_trades", "get_margins",
    "get_quotes", "get_ltp", "get_ohlc", "get_historical_data", "search_instruments",
}
NOT_LOGGED_IN = re.compile(r"log ?in first|not logged in|login required|session (has )?expired|access_token|TokenException", re.I)


class McpError(Exception):
    pass


class NotLoggedIn(McpError):
    """Kite MCP needs (a new) login. The server treats this like an expired Kite session."""


class KiteMCP:
    def __init__(self, session_file):
        self.http = requests.Session()
        self.session_file = Path(session_file)
        self.sid = None
        self.login_warning = None
        self._id = 0
        self._lock = threading.Lock()
        self._last = 0.0

    # ---- MCP plumbing ----
    def _post(self, payload, timeout=30):
        headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
        if self.sid:
            headers["mcp-session-id"] = self.sid
        r = self.http.post(URL, headers=headers, json=payload, timeout=timeout)
        return r

    @staticmethod
    def _json(r):
        text = r.text
        if "data:" in text[:50]:   # server-sent events framing
            text = "".join(l[5:].strip() for l in text.splitlines() if l.startswith("data:"))
        return json.loads(text) if text.strip() else {}

    def _initialize(self):
        self.sid = None
        r = self._post({"jsonrpc": "2.0", "id": 0, "method": "initialize",
                        "params": {"protocolVersion": "2025-03-26", "capabilities": {},
                                   "clientInfo": {"name": "kite-live-dashboard", "version": "1.0"}}})
        r.raise_for_status()
        self.sid = r.headers.get("mcp-session-id")
        if not self.sid:
            raise McpError("Kite MCP didn't return a session ID")
        self._post({"jsonrpc": "2.0", "method": "notifications/initialized"})
        self.session_file.write_text(json.dumps({"sid": self.sid, "date": date.today().isoformat()}))

    def resume(self):
        """Reuse today's MCP session if there is one (so a restart doesn't need a new login)."""
        try:
            saved = json.loads(self.session_file.read_text())
            if saved.get("date") == date.today().isoformat():
                self.sid = saved["sid"]
                return True
        except Exception:
            pass
        return False

    def call(self, tool, args=None, timeout=30):
        if tool not in READ_TOOLS:
            raise McpError(f"{tool} is not a read-only tool; the dashboard never calls it")
        with self._lock:
            wait = self._last + 0.35 - time.time()   # stay well under Kite's per-second limits
            if wait > 0:
                time.sleep(wait)
            self._last = time.time()
            if not self.sid:
                self._initialize()
            self._id += 1
            payload = {"jsonrpc": "2.0", "id": self._id, "method": "tools/call",
                       "params": {"name": tool, "arguments": args or {}}}
            r = self._post(payload, timeout)
            if r.status_code in (400, 404) and "session" in r.text.lower():
                # the server forgot our session (e.g. it restarted): start a new one, which needs a new login
                self._initialize()
                if tool != "login":
                    raise NotLoggedIn("Kite MCP session ended. Log in again.")
                r = self._post(payload, timeout)
            r.raise_for_status()
            msg = self._json(r)
        if "error" in msg:
            raise McpError(f"{tool}: {msg['error'].get('message')}")
        res = msg.get("result") or {}
        text = "".join(c.get("text", "") for c in res.get("content", []) if c.get("type") == "text")
        if res.get("isError"):
            if NOT_LOGGED_IN.search(text):
                raise NotLoggedIn(text.strip()[:200])
            raise McpError(f"{tool}: {text.strip()[:300]}")
        if tool == "login":
            return text
        try:
            return json.loads(text)
        except ValueError:
            return text

    # ---- login ----
    def login_url(self):
        """A fresh Zerodha login link for this MCP session (you log in yourself: password + TOTP)."""
        text = self.call("login")
        m = re.search(r"https://mcp\.kite\.trade/authorize\?[^\s)\]]+", text)
        if not m:
            raise McpError("Kite MCP's login reply didn't contain a link")
        w = re.search(r"\*\*(WARNING:[^*]+)\*\*", text)
        self.login_warning = w.group(1).strip() if w else None
        return m.group(0)

    # ---- kiteconnect-like account calls ----
    @staticmethod
    def _rows(data, key=None):
        """Kite MCP may wrap lists for pagination; return the plain list."""
        if isinstance(data, dict):
            if key and key in data:
                return data[key]
            for k in ("data", "items", "results"):
                if isinstance(data.get(k), list):
                    return data[k]
        return data if isinstance(data, list) else []

    def _paged(self, tool, key=None, limit=500):
        out, start = [], 0
        while True:
            data = self.call(tool, {"from": start, "limit": limit})
            rows = self._rows(data, key)
            out += rows
            more = isinstance(data, dict) and (data.get("pagination") or {}).get("has_more")
            if not more or not rows:
                return out
            start += len(rows)

    def profile(self):
        return self.call("get_profile")

    def positions(self):
        data = self.call("get_positions", {"from": 0, "limit": 500})
        if isinstance(data, dict) and "net" in data:
            return data
        if isinstance(data, dict) and isinstance(data.get("data"), dict) and "net" in data["data"]:
            return data["data"]
        # Kite MCP returns Kite's "net" and "day" lists merged into one, so anything traded today shows
        # twice. Keep one row per contract, preferring the net row (it carries the overnight quantity).
        best = {}
        for r in self._rows(data):
            key = (r.get("exchange"), r.get("tradingsymbol"), r.get("product"))
            cur = best.get(key)
            if cur is None or (not cur.get("overnight_quantity") and r.get("overnight_quantity")):
                best[key] = r
        return {"net": list(best.values()), "day": []}

    def orders(self):
        return _with_dates(self._paged("get_orders"), ("order_timestamp", "exchange_timestamp"))

    def trades(self):
        return _with_dates(self._paged("get_trades"), ("fill_timestamp", "exchange_timestamp"))

    def margins(self, segment=None):
        m = self.call("get_margins")
        return m.get(segment, m) if segment and isinstance(m, dict) else m


def _with_dates(rows, fields):
    for r in rows:
        for f in fields:
            v = r.get(f)
            if isinstance(v, str):
                try:
                    r[f] = datetime.fromisoformat(v.replace("Z", "+00:00").replace(" ", "T"))
                except ValueError:
                    pass
    return rows


class McpFeed:
    """Market data via Kite MCP with DhanFeed's interface (instrument tokens are Dhan security IDs)."""
    INTERVALS = {"1": "minute", "5": "5minute", "15": "15minute", "60": "60minute", "D": "day"}

    def __init__(self, mcp, cache_dir):
        self.mcp = mcp
        self.dhan_list = DhanFeed("", "", cache_dir)   # only its public instrument list is used
        self.rows = {}            # Dhan security ID -> instrument row
        self.kite_token = {}      # Dhan security ID -> Kite instrument_token (learned from quotes)

    def instruments(self, exchange="MCX"):
        rows = self.dhan_list.instruments(exchange)
        for r in rows:
            self.rows[r["instrument_token"]] = r
        return rows

    def _key(self, sid):
        r = self.rows[sid]
        return f'{r["exchange"]}:{r["tradingsymbol"]}'

    def quotes(self, by_segment):
        sids = [s for lst in by_segment.values() for s in lst if s in self.rows]
        out = {}
        for i in range(0, len(sids), 200):
            chunk = sids[i:i + 200]
            data = self.mcp.call("get_quotes", {"instruments": [self._key(s) for s in chunk]})
            data = data.get("data", data) if isinstance(data, dict) else {}
            for s in chunk:
                q = data.get(self._key(s))
                if not q:
                    continue
                if q.get("instrument_token"):
                    self.kite_token[s] = q["instrument_token"]
                ohlc = q.get("ohlc") or {}
                out[s] = {"last_price": q.get("last_price") or 0, "volume": q.get("volume") or q.get("volume_traded") or 0,
                          "oi": q.get("oi") or 0,
                          "ohlc": {k: ohlc.get(k) or 0 for k in ("open", "high", "low", "close")}}
        return out

    def option_chain(self, underlying_sid, segment, expiry):
        """Quotes for every strike of one expiry. Kite quotes carry today's OI but not yesterday's, so
        prev_oi stays empty and the dashboard measures OI change from the day's first reading."""
        fut = self.rows.get(underlying_sid)
        if not fut:
            return {}   # a commodity-level ID can be shared by NG and NG Mini; the caller retries with the future's own ID
        name = fut["name"]
        opts = [s for s, r in self.rows.items() if r["name"] == name and r["instrument_type"] in ("CE", "PE")
                and r["expiry"].isoformat() == expiry]
        q = self.quotes({segment: opts})
        return {(self.rows[s]["strike"], self.rows[s]["instrument_type"]): {
            "security_id": s, "last_price": v["last_price"], "prev_close": v["ohlc"]["close"],
            "oi": v["oi"], "prev_oi": None, "volume": v["volume"], "prev_volume": None, "iv": None}
            for s, v in q.items()}

    def _token(self, sid):
        if sid not in self.kite_token:
            self.quotes({"MCX_COMM": [sid]})
        tok = self.kite_token.get(sid)
        if not tok:
            raise McpError(f"no Kite instrument token for {self.rows.get(sid, {}).get('tradingsymbol', sid)}")
        return tok

    def _history(self, sid, interval, start, end, oi=False):
        data = self.mcp.call("get_historical_data", {
            "instrument_token": self._token(sid), "interval": interval, "oi": oi, "continuous": False,
            "from_date": start.strftime("%Y-%m-%d %H:%M:%S"), "to_date": end.strftime("%Y-%m-%d %H:%M:%S")})
        rows = data if isinstance(data, list) else KiteMCP._rows(data, "candles")
        out = []
        for c in rows:
            if isinstance(c, list):   # [date, open, high, low, close, volume, (oi)]
                c = dict(zip(("date", "open", "high", "low", "close", "volume", "oi"), c))
            d = c.get("date")
            if isinstance(d, str):
                d = datetime.fromisoformat(d.replace("Z", "+00:00").replace(" ", "T"))
            out.append({**c, "time": int(d.timestamp()) if d else 0})
        return out

    def candles(self, sid, segment, instrument, interval, days):
        now = datetime.now()
        rows = self._history(sid, self.INTERVALS[interval], now - timedelta(days=days), now, oi=True)
        return [{"time": c["time"], **{k: c.get(k) or 0 for k in ("open", "high", "low", "close", "volume", "oi")}} for c in rows]

    def daily_history(self, sid, segment, instrument, days=21):
        today = datetime.combine(date.today(), datetime.min.time())
        rows = self._history(sid, "day", today - timedelta(days=days), today - timedelta(seconds=1), oi=True)
        return {"volume": [c.get("volume") or 0 for c in rows], "oi": [c.get("oi") or 0 for c in rows]}

    def price_at(self, sid, segment, instrument, when):
        naive = when.replace(tzinfo=None)
        rows = self._history(sid, "minute", naive - timedelta(minutes=2), naive + timedelta(minutes=2))
        before = [c for c in rows if c["time"] <= when.timestamp()]
        return (before[-1] if before else rows[0])["close"] if rows else None
