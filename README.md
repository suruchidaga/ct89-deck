# CT89 Deck

A local, read-only dashboard for Zerodha positions and orders. It never places, modifies or cancels orders.

## What it shows
- **OI buildup by strike**: for the chosen contract's option expiry, a grid of strikes × time slots (5/15/30 min)
  with a bubble for the OI added (filled) or cut (hollow) in each slot, calls red and puts green, sized by lots; the
  futures price drawn on the strike scale; each strike's OI change today on the right; and cumulative call vs put OI
  change below. Recorded every minute (`.opt_strikes.json`); the first time a contract is opened each day, the
  earlier part of the day is filled from Kite's minute history (about one Kite call per strike).
- **OI bias**: the market's positioning today on the net-bias scale (−100 bearish … +100 bullish). Option OI change
  since the day's first reading, delta-weighted (put OI added = bullish, call OI added = bearish), plus futures OI
  change vs yesterday in the direction of the price move; NG-lot equivalents, by month and contract.
- **Margin optimiser** (Fund manager view): finds covered calls (long futures + short calls → short puts), short
  futures + short puts (→ short calls) and opposite NG / NG Mini futures in the same month (→ one net position),
  prices each step on the SPAN calculator in order, and lists the trades, margin freed, net delta before → after,
  booked-P&L impact and a rough cost. Read-only: it never places orders. Far months the calculator doesn't list
  (e.g. Dec) are shown but not priced.
- **IV alert** (set next to the NG chart, default 58%): when current-month ATM IV of NG or NG Mini reaches the level,
  a Windows notification and a dashboard banner give the ATM straddle (CE + PE, ₹ per lot, breakevens). Fires once per
  crossing (re-arms 1 point below, at most every 30 min per underlying); saved in `.alerts.json`. It never places orders.
- **Since a date** (top strip, default 15 Aug): booked P&L and capital added (net of withdrawals), and booked ÷ capital.
  Kite only exposes today, so figures up to a date are entered once from Zerodha Console ("Set figures": Realised P&L
  from the P&L report; pay-ins typed or imported from the ledger CSV). After that the server records each day's
  booked P&L (Kite-style, average cost) and pay-ins / payouts from Kite's margins in `.tracking.json`.
- Total and day's P&L, with the day's high and low
- Underlying NG / NG Mini futures with their day range
- **NG chart** (TradingView Lightweight Charts 5): with "OI profile, OI, PCR & IV" on, the chart shows option OI by
  strike on the right of the price pane (calls red to the left, puts green to the right, a tick where each bar stood at
  the day's first reading), and panes below for futures OI, volume, put-call ratio and ATM IV. PCR / IV cover the
  selected contract's option expiry, from options within 15 strikes of the money, sampled every minute while the
  server runs (kept for the day in `.opt_series.json`).
  Candles at 1m / 5m / 15m / 1h / 1D with volume, and your levels
  drawn on it: futures entries, option strikes with net lots, expiry breakevens, day high/low. Source is MCX
  futures from Dhan/Kite when live, or NYMEX NG=F × USD/INR from Yahoo Finance (works without keys), optionally
  shifted by the current MCX–NYMEX gap. `py server.py --port 5001` runs a second copy for testing.
- Net bias score (futures + option delta, NG and NG Mini on one scale, by month), with each futures position's
  Kite average, **FIFO average** and current price
- **Position plan** (switched off for now, with the adjustment planner and the news card): a target in lots for each instrument (month × NG / NG Mini × futures / calls / puts), the trade
  needed to reach it, and a ceiling per instrument: the lots that keep the net bias score within a band (±10 by
  default) while everything else sits at its target. Flags instruments that are too long or too short now.
  Saved in `.plan.json` on the server (or in the browser when there's no server).
- **FIFO cost** alongside Kite's average (which blends booked lots in): each sale closes the oldest open lots
  first, so the FIFO average is what the lots you still hold actually cost. Built from lots carried in from
  earlier days (Kite's overnight position fields) and today's fills (Kite trade book); hover to see the lots.
- Margin calculator: SPAN + exposure for the whole NG book priced together (so NG vs NG Mini and Oct vs Nov
  offsets count), compared with each leg alone, each contract alone and each month alone; shows how much
  simulated positions add or free, and your commodity funds when logged in. Uses the exchange SPAN files via
  Zerodha's public SPAN calculator (`margin_calc.py`), so it needs no login.
- The large-prints and OI & volume surge cards are switched off; their quote scan still runs in the background
  because the OI buildup, OI bias and the chart's OI panes are built from it.
- **OI buildup by strike** also shows futures OI change, futures volume and options volume per time slot, and a
  **Day** picker to view earlier days kept in `history/`.
- P&L over time with the front-month NG futures price on a second axis (kept for the day across restarts in `.pnl_history.json`), and a payoff chart (at expiry and today) with underlying and month toggles
- A what-if simulator for new positions (calculated in the browser only); works with no open positions too, using NG / NG Mini futures for the next two months
- **My pairs** view: group positions into your own pairs/strategies (e.g. long Oct futures with the short calls
  written against them), using all or part of a position's lots; reorder pairs and legs with ↑ ↓; each pair shows
  combined P&L, day's P&L, net delta and netted SPAN margin; leftover lots appear under Unpaired. Saved in
  `.pairs.json` on the server (or in the browser when there's no server).
- Positions with current IV and **entry IV** (IV when the trade was made) plus the change since entry, and today's orders with each fill's P&L impact at current prices

## Entry IV
Kite doesn't store the IV at the time of a trade, so the server records it: for every option fill it takes
the futures price at that minute (Dhan 1-minute history, or the live price for fresh fills) and the time to
expiry, and solves Black-76 for the IV of the fill price. Records are kept in `.entry_iv.json` and survive restarts.
Lots opened before the server was running can be given an entry IV by clicking the Entry IV cell: type the IV, or
enter the entry price, futures price and date/time to have it calculated. A `*` means some lots have no entry IV yet.

## Data sources
- **Kite MCP (recommended, no Kite Connect app needed):** set `KITE_SOURCE=mcp` in `.env`. The server talks to
  Zerodha's official Kite MCP server (`kite_mcp.py`): positions, orders, trades, margins, quotes, OHLC and
  historical candles. Prices are polled every ~2 s (not a tick stream). You log in through Zerodha's page each day
  (the server opens it); the session is reused across restarts that day. Only read-only tools are ever called:
  Kite MCP also offers order tools, and the client refuses them. Kite MCP merges Kite's net and day position
  lists, so positions traded today are de-duplicated. Options' previous-day OI isn't available this way, so their
  OI change counts from the day's first reading.
- **Positions and orders:** Kite Connect. The free *Personal* plan is enough when Dhan supplies market data.
- **Market data:** Dhan Data API (prices every second, option chain with previous-day OI and IV, daily history).
  Without Dhan, it falls back to the paid Kite Connect plan's price stream and quotes.

## Setup (Windows)
1. Install Python 3, then run: `py -m pip install -r requirements.txt`
2. Copy `.env.example` to `.env` and fill in:
   - `KITE_API_KEY` and `KITE_API_SECRET` from developers.kite.trade. Set the app's redirect URL to `http://127.0.0.1:5000/callback`.
   - `DHAN_CLIENT_ID` and `DHAN_ACCESS_TOKEN` from web.dhan.co › My Profile › Access DhanHQ APIs. Tokens expire, so replace the token when the dashboard reports it as rejected.
3. Run `py server.py` (or double-click `Start CT89 Deck.bat`). If there's no Kite session for today it opens Kite's login page in your browser; log in
   (password + TOTP) and the dashboard opens. Restarts on the same day reuse the session. When the session
   expires (every morning) the dashboard says so and the login page opens again.
   The login itself stays manual on purpose: exchange rules require you to log in yourself at least once a day,
   so the password/TOTP step is not automated. Add `--no-browser` to skip opening the login page.
   Starting it while it's already running just opens the dashboard (two copies on one port would split requests).

## Start automatically on weekdays (optional)
A Windows scheduled task can start the server at 08:50, Monday to Friday, while you're signed in:
```powershell
$py = (Get-Command py).Source
Register-ScheduledTask -TaskName "Kite Live dashboard" `
  -Action (New-ScheduledTaskAction -Execute $py -Argument "server.py" -WorkingDirectory (Get-Location).Path) `
  -Trigger (New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At "08:50") `
  -Settings (New-ScheduledTaskSettingsSet -StartWhenAvailable -ExecutionTimeLimit ([TimeSpan]::Zero) -MultipleInstances IgnoreNew)
```
Remove it with `Unregister-ScheduledTask -TaskName "Kite Live dashboard" -Confirm:$false`.

## Demo mode
- `py server.py` with no Kite keys in `.env` runs in demo mode: sample positions with simulated prices, but live
  news headlines and live margin calculations (including simulated positions).
- Opening `dashboard.html` directly also works, with a saved margin result for the sample book and no headlines.

The sample book is made up for illustration. A banner marks demo mode.

## Files
- `server.py`: local server (Kite login, positions/orders, P&L tracking, OI/volume, news)
- `dhan_feed.py`: Dhan market-data client
- `margin_calc.py`: SPAN margin via Zerodha's public SPAN calculator
- `dashboard.html`: the dashboard page
- `tracker.py`: a simpler terminal-only tracker (Kite data only)
