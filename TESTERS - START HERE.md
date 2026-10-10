# CT89 Deck: tester guide

CT89 Deck is a dashboard for MCX natural gas positions (NATURALGAS and NATGASMINI, futures and options) in
your Zerodha account. It runs on your own PC. A role switcher at the top (Investor, Inner circle, Fund manager,
Trader, All) shows the cards each role cares about:

- **Trader**: live P&L, the NG chart with option OI by strike and futures OI / volume / PCR / IV panes,
  OI buildup by strike (calls and puts added or cut in each time slot, futures and options volume, earlier days),
  OI bias (the market's positioning today), your net bias, payoff, simulator, positions and orders
- **Fund manager**: risk and returns, margin, the margin optimiser (same exposure, less margin), payoff
- **Investor / Inner circle**: weekly fund performance, P&L since a start date, capital and cash flow

**It is read-only.** It never places, modifies or cancels orders. It connects to Zerodha's official Kite MCP
server (mcp.kite.trade) and only calls its read tools (positions, orders, trades, margins, quotes, history); the
order tools are blocked in the code (`kite_mcp.py`, `READ_TOOLS`). The margin optimiser only suggests trades.

**Your login stays yours.** You log in on Zerodha's own page with your own password and TOTP. CT89 Deck never
sees your password, nothing is sent to anyone else, and the dashboard is only reachable from your PC
(127.0.0.1). Never share your Zerodha login or TOTP with anyone, including the person who sent you this.

## What you need
- A Windows PC
- Python 3.10 or newer: https://www.python.org/downloads/ (during install, tick **Add python.exe to PATH**)
- Your own Zerodha account. No Kite Connect API key or paid plan is needed.

## Start it
1. Unzip the folder anywhere (for example your Documents folder).
2. Double-click **Start CT89 Deck.bat**. The first run installs the required packages, which takes a minute.
3. Your browser opens Zerodha's login page. Zerodha shows a warning about giving an app access to your
   account: that is Zerodha's standard notice for Kite MCP. Log in with your password and TOTP.
4. The dashboard opens at http://127.0.0.1:5000 within a few seconds of logging in.
   If it doesn't, open that address yourself.

Keep the black window open while you use the dashboard; closing it stops CT89 Deck.
You log in once a day (Zerodha's sessions expire every morning). If the dashboard ever shows "Log in with
Kite" again during the day, click it and log in.

**First run notes**
- The OI buildup card fills in the earlier part of the day from Kite's history the first time you open it
  (about a minute). PCR, IV and the OI bias history build up minute by minute while the dashboard runs.
- Each day's data is kept in the `history` folder, so earlier days can be picked on the OI buildup card.
- While it runs on weekdays during MCX hours, it keeps the PC from sleeping; gaps (for example after a restart)
  are filled from Kite's history automatically.
- The "since" strip and Fund performance start from the day you first run it. Use **Set figures** on the strip to
  add your own earlier booked P&L and capital from Zerodha Console (optional).
- The IV alert (next to the NG chart, 58% by default) pops a Windows notification; change or untick it as you like.

**No NG positions?** The chart, OI cards and simulator still work. To look around with a made-up sample book
first, open `dashboard.html` directly in your browser.

## Please check and report back
1. Do positions, quantities, average prices and P&L match Kite's Positions page? (Note any difference in ₹.)
2. Does the day's P&L match Kite?
3. Does the margin card come close to Kite's used margin? Do the margin optimiser's suggestions make sense?
4. Do the OI buildup and OI bias cards match what you see in the option chain?
5. Do prices update every couple of seconds during market hours?
6. Anything confusing, missing, slow or broken. Screenshots help.
7. Which role view and which cards did you actually use, and which would you remove?

Send your notes and screenshots to the person who shared CT89 Deck with you. Blur or crop anything you
don't want to share; the dashboard shows your real positions and P&L.

## If something goes wrong
- **"Python is not installed"**: install it as above, with *Add python.exe to PATH* ticked, then restart the PC.
- **Login page says "missing MCP session_id"**: close that tab and use the newest login link the window opened
  (or open http://127.0.0.1:5000/login). Only the latest link works.
- **Browser says 127.0.0.1 refused to connect**: the black window was closed or showed an error. Run
  Start CT89 Deck.bat again and send the window's text if it fails.
- **Port 5000 already in use**: another program uses it. Run `py server.py --port 5001` in this folder and open
  http://127.0.0.1:5001.

## Stop or remove it
Close the black window. To remove CT89 Deck, delete the folder. The Kite MCP session expires by itself the next
morning.

More detail about every card is in README.md.
