"""
Market data from DhanHQ (Data API) for the dashboard: instruments, quotes, option chain, daily history.

Read-only: only market-data endpoints are used. Positions and orders still come from Kite.
Docs: https://dhanhq.co/docs/v2/
"""
import csv
import io
import json
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

BASE = "https://api.dhan.co/v2"
SEGMENTS = {"MCX": "MCX_COMM", "NFO": "NSE_FNO", "BFO": "BSE_FNO"}
MONTHS = "JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split()


class DhanError(Exception):
    pass


class DhanFeed:
    def __init__(self, client_id, access_token, cache_dir):
        self.s = requests.Session()
        self.s.headers.update({"access-token": access_token, "client-id": client_id,
                               "Accept": "application/json", "Content-Type": "application/json"})
        self.cache_dir = Path(cache_dir)
        self._last_call = {}

    # ---- plumbing ----
    def _throttle(self, key, gap):
        wait = self._last_call.get(key, 0) + gap - time.time()
        if wait > 0:
            time.sleep(wait)
        self._last_call[key] = time.time()

    def _post(self, path, body, gap):
        self._throttle(path, gap)
        r = self.s.post(BASE + path, data=json.dumps(body), timeout=10)
        if r.status_code in (401, 403):
            raise DhanError("Dhan rejected the access token (expired or wrong client ID). "
                            "Generate a new one in web.dhan.co and update DHAN_ACCESS_TOKEN in .env.")
        if r.status_code != 200:
            raise DhanError(f"Dhan {path} HTTP {r.status_code}: {r.text[:200]}")
        return r.json()

    # ---- instruments ----
    def instruments(self, exchange="MCX"):
        """Instrument rows shaped like Kite's dump, with Dhan security IDs as instrument_token."""
        seg = SEGMENTS[exchange]
        cache = self.cache_dir / f".dhan_{seg}.csv"
        today = date.today().isoformat()
        text = None
        if cache.exists() and time.strftime("%Y-%m-%d", time.localtime(cache.stat().st_mtime)) == today:
            text = cache.read_text(encoding="utf-8")
        if text is None:
            r = requests.get(f"{BASE}/instrument/{seg}", timeout=60)
            r.raise_for_status()
            text = r.text
            cache.write_text(text, encoding="utf-8")
        rows = []
        for r in csv.DictReader(io.StringIO(text)):
            kind = {"FUTCOM": "FUT", "FUTIDX": "FUT", "FUTSTK": "FUT"}.get(r["INSTRUMENT"])
            if not kind and r["OPTION_TYPE"] in ("CE", "PE"):
                kind = r["OPTION_TYPE"]
            if not kind or not r["SM_EXPIRY_DATE"]:
                continue
            exp = datetime.strptime(r["SM_EXPIRY_DATE"][:10], "%Y-%m-%d").date()
            name = r["UNDERLYING_SYMBOL"] or r["SYMBOL_NAME"]
            strike = float(r["STRIKE_PRICE"] or 0)
            yy, mon = exp.strftime("%y"), MONTHS[exp.month - 1]
            # Kite's MCX monthly symbols: NATURALGAS26OCTFUT, NATURALGAS26OCT300CE
            sym = f"{name}{yy}{mon}FUT" if kind == "FUT" else f"{name}{yy}{mon}{strike:g}{kind}"
            rows.append({
                "instrument_token": int(r["SECURITY_ID"]), "tradingsymbol": sym, "name": name,
                "exchange": exchange, "segment": seg, "instrument_type": kind, "strike": strike,
                "expiry": exp, "lot_size": float(r["LOT_SIZE"] or 1),
                "dhan_instrument": r["INSTRUMENT"], "dhan_underlying": r["UNDERLYING_SECURITY_ID"],
            })
        return rows

    # ---- quotes ----
    def quotes(self, by_segment):
        """by_segment: {"MCX_COMM": [sid, ...]} -> {sid: {last_price, ohlc, volume, oi}}. Max 1 call/s."""
        body = {seg: [int(s) for s in sids] for seg, sids in by_segment.items() if sids}
        if not body:
            return {}
        data = self._post("/marketfeed/quote", body, 1.05).get("data", {})
        out = {}
        for seg_data in data.values():
            for sid, q in seg_data.items():
                ohlc = q.get("ohlc") or {}
                out[int(sid)] = {"last_price": q.get("last_price") or 0, "volume": q.get("volume") or 0,
                                 "oi": q.get("oi") or 0,
                                 "ohlc": {k: ohlc.get(k) or 0 for k in ("open", "high", "low", "close")}}
        return out

    def option_chain(self, underlying_sid, segment, expiry):
        """{(strike, 'CE'|'PE'): {...}} with today's and previous-day OI/volume and IV. One call per 3 s."""
        body = {"UnderlyingScrip": int(underlying_sid), "UnderlyingSeg": segment, "Expiry": expiry}
        data = self._post("/optionchain", body, 3.1).get("data", {})
        out = {}
        for strike, legs in (data.get("oc") or {}).items():
            for kind in ("ce", "pe"):
                leg = legs.get(kind)
                if leg:
                    out[(float(strike), kind.upper())] = {
                        "security_id": leg.get("security_id"), "last_price": leg.get("last_price") or 0,
                        "prev_close": leg.get("previous_close_price") or 0,
                        "oi": leg.get("oi") or 0, "prev_oi": leg.get("previous_oi"),
                        "volume": leg.get("volume") or 0, "prev_volume": leg.get("previous_volume"),
                        "iv": leg.get("implied_volatility"),
                    }
        return out

    def price_at(self, sid, segment, instrument, when):
        """Close of the 1-minute candle covering `when` (a datetime in IST), or None."""
        start = when - timedelta(minutes=2)
        body = {"securityId": str(sid), "exchangeSegment": segment, "instrument": instrument,
                "interval": "1", "oi": False,
                "fromDate": start.strftime("%Y-%m-%d %H:%M:%S"),
                "toDate": (when + timedelta(minutes=2)).strftime("%Y-%m-%d %H:%M:%S")}
        d = self._post("/charts/intraday", body, 0.25)
        ts, closes = d.get("timestamp") or [], d.get("close") or []
        target = when.timestamp()
        best = None
        for t, c in zip(ts, closes):
            if t <= target:
                best = c
        return best if best is not None else (closes[0] if closes else None)

    def candles(self, sid, segment, instrument, interval, days):
        """OHLCV candles: interval "1"/"5"/"15"/"60" minutes, or "D" for daily."""
        now = datetime.now()
        if interval == "D":
            body = {"securityId": str(sid), "exchangeSegment": segment, "instrument": instrument, "expiryCode": 0,
                    "oi": False, "fromDate": (date.today() - timedelta(days=days)).isoformat(),
                    "toDate": (date.today() + timedelta(days=1)).isoformat()}
            d = self._post("/charts/historical", body, 0.25)
        else:
            body = {"securityId": str(sid), "exchangeSegment": segment, "instrument": instrument, "interval": interval,
                    "oi": True, "fromDate": (now - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S"),
                    "toDate": now.strftime("%Y-%m-%d %H:%M:%S")}
            d = self._post("/charts/intraday", body, 0.25)
        keys = ("open", "high", "low", "close", "volume")
        ois = d.get("open_interest") or []
        return [{"time": int(t), **{k: d[k][i] for k in keys}, "oi": ois[i] if i < len(ois) else 0}
                for i, t in enumerate(d.get("timestamp") or [])]

    def daily_history(self, sid, segment, instrument, days=21):
        """Daily candles up to yesterday: {"volume": [...], "oi": [...]}."""
        body = {"securityId": str(sid), "exchangeSegment": segment, "instrument": instrument,
                "expiryCode": 0, "oi": True,
                "fromDate": (date.today() - timedelta(days=days)).isoformat(),
                "toDate": date.today().isoformat()}  # toDate is exclusive
        d = self._post("/charts/historical", body, 0.25)
        return {"volume": d.get("volume") or [], "oi": d.get("open_interest") or []}
