"""
SPAN + exposure margin for MCX positions, via Zerodha's public SPAN calculator
(https://zerodha.com/margin-calculator/SPAN/), which runs the exchange's SPAN files.

A list of legs is sent together, so the result is the portfolio margin with offsets between legs.
Legs are also priced one by one to show how much the netting saves.
No login is needed and nothing is ordered.
"""
import json
import re
import threading
import time
from urllib.parse import urlencode

import requests

PAGE = "https://zerodha.com/margin-calculator/SPAN/"
ENDPOINT = "https://zerodha.com/margin-calculator/SPAN"
CACHE_SECS = 300           # SPAN files change a few times a day
TIMEOUT = (5, 15)          # connect, read (seconds) for each call to the calculator
LOCK_WAIT = 45             # give up rather than queue forever behind a stuck request
SYMBOL_RE = re.compile(r"^([A-Z]+?)(\d{2}[A-Z]{3})(?:(\d+(?:\.\d+)?)(CE|PE)|FUT)$")


class MarginCalc:
    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": "Mozilla/5.0 (kite-live dashboard)",
                               "X-Requested-With": "XMLHttpRequest", "Referer": PAGE})
        self.lots = {}        # "NATURALGAS26OCT" -> units per lot
        self.lots_at = 0
        self.cache = {}       # key -> (time, result)
        self.lock = threading.Lock()

    def _load_lots(self):
        if time.time() - self.lots_at < 6 * 3600 and self.lots:
            return
        html = self.s.get(PAGE, timeout=TIMEOUT).text
        for scrip, _label, lot in re.findall(r'\["([A-Z0-9]+)","([^"]+)",(\d+),', html):
            self.lots[scrip] = int(lot)
        self.lots_at = time.time()

    def _leg_params(self, symbol, lots):
        m = SYMBOL_RE.match(symbol)
        if not m:
            raise ValueError(f"Unsupported symbol {symbol}")
        name, yymon, strike, kind = m.groups()
        scrip = f"{name}{yymon}"
        unit = self.lots.get(scrip)
        if not unit:
            raise ValueError(f"{scrip} not in the SPAN calculator's contract list")
        return [("exchange[]", "MCX"), ("product[]", "OPT" if kind else "FUT"), ("scrip[]", scrip),
                ("option_type[]", kind or "CE"), ("strike_price[]", strike or ""),
                ("qty[]", abs(lots) * unit), ("trade[]", "sell" if lots < 0 else "buy")]

    def _post(self, legs):
        key = json.dumps(sorted(legs))
        hit = self.cache.get(key)
        if hit and time.time() - hit[0] < CACHE_SECS:
            return hit[1]
        q = [("action", "calculate")]
        for sym, lots in legs:
            q += self._leg_params(sym, lots)
        r = self.s.post(ENDPOINT, data=urlencode(q), timeout=TIMEOUT,
                        headers={"Content-Type": "application/x-www-form-urlencoded"})
        data = r.json()
        if "total" not in data:
            raise ValueError(f"SPAN calculator rejected the request: {str(data)[:200]}")
        t = data["total"]
        res = {k: round(float(t.get(k) or 0), 2) for k in ("span", "exposure", "netoptionvalue", "spread", "total")}
        self.cache[key] = (time.time(), res)
        time.sleep(0.2)  # be gentle with a public endpoint
        return res

    def _locked(self):
        if not self.lock.acquire(timeout=LOCK_WAIT):
            raise TimeoutError("margin calculator is busy; try again shortly")
        return self.lock

    def _priceable(self, legs):
        """Split legs into those the calculator lists and those it doesn't (it only carries the near months)."""
        ok, skipped = [], []
        for s, q in legs:
            m = SYMBOL_RE.match(s)
            (ok if m and f"{m.group(1)}{m.group(2)}" in self.lots else skipped).append((s, q))
        return ok, skipped

    def portfolio_only(self, legs):
        """Netted margin for the legs, without pricing each leg alone (legs the calculator doesn't list are left out)."""
        legs = [(s, int(q)) for s, q in legs if q]
        if not legs:
            return None
        lock = self._locked()
        try:
            self._load_lots()
            legs, skipped = self._priceable(legs)
            res = self._post(legs) if legs else {"span": 0, "exposure": 0, "netoptionvalue": 0, "spread": 0, "total": 0}
            return {**res, "skipped": [s for s, _ in skipped]}
        finally:
            lock.release()

    def portfolio(self, legs, groups=None):
        """
        legs: [(tradingsymbol, signed lots)], e.g. [("NATGASMINI26OCT305CE", -9)].
        groups: optional {label: [symbols]} to also price subsets (e.g. NG alone, NG Mini alone).
        Returns the netted portfolio margin, each leg alone, the gross sum and each group.
        """
        legs = [(s, int(q)) for s, q in legs if q]
        if not legs:
            return None
        lock = self._locked()
        try:
            self._load_lots()
            legs, skipped = self._priceable(legs)
            if not legs:
                return None
            out = {"portfolio": self._post(legs), "legs": [], "groups": {}, "calculated": int(time.time() * 1000),
                   "skipped": [s for s, _ in skipped]}   # e.g. far months the calculator doesn't carry
            for sym, lots in legs:
                out["legs"].append({"symbol": sym, "lots": lots, **self._post([(sym, lots)])})
            out["gross"] = round(sum(l["total"] for l in out["legs"]), 2)
            for label, syms in (groups or {}).items():
                sub = [(s, q) for s, q in legs if s in syms and (s, q) not in skipped]
                if sub:
                    out["groups"][label] = self._post(sub)
            return out
        finally:
            lock.release()
