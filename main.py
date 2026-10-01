import asyncio
import math
import time
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI(title="AKE MEXC adatfigyelő")
SYMBOL = "AKEUSDT"
BASE_URL = "https://api.mexc.com"
cache: dict[str, Any] = {"until": 0.0, "status": 503, "data": {}}
lock = asyncio.Lock()


def iso(milliseconds: int) -> str:
    return datetime.fromtimestamp(milliseconds / 1000, timezone.utc).isoformat()


def number(value: Any) -> float:
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise ValueError("Érvénytelen szám érkezett a tőzsdétől.")
    return result


async def get_json(client: httpx.AsyncClient, path: str, **params: Any) -> Any:
    response = await client.get(path, params=params)
    response.raise_for_status()
    data = response.json()
    if isinstance(data, dict) and "code" in data:
        raise ValueError("A MEXC API nem adta vissza a kért piaci adatot.")
    return data


async def snapshot() -> dict[str, Any]:
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=8.0) as client:
        clock, trades, candles, book = await asyncio.wait_for(
            asyncio.gather(
                get_json(client, "/api/v3/time"),
                get_json(client, "/api/v3/trades", symbol=SYMBOL, limit=500),
                get_json(client, "/api/v3/klines", symbol=SYMBOL,
                         interval="1m", limit=100),
                get_json(client, "/api/v3/depth", symbol=SYMBOL, limit=20),
            ),
            timeout=12.0,
        )

    now = int(clock["serverTime"])
    if abs(time.time() * 1000 - now) > 120000:
        raise ValueError("A tőzsdei szerveridő frissessége nem igazolható.")
    trades = sorted(trades, key=lambda item: int(item["time"]))
    closed = sorted(
        (row for row in candles if int(row[6]) < now),
        key=lambda row: int(row[0]),
    )
    if not trades or len(closed) < 61:
        raise ValueError("Nincs elég kötés vagy lezárt gyertya az elemzéshez.")

    last = trades[-1]
    trade_time = int(last["time"])
    candle_time = int(closed[-1][6])
    if not -5000 <= now - trade_time <= 120000:
        raise ValueError("Az utolsó kötés időpontja régi vagy nem ellenőrizhető.")
    if not 0 <= now - candle_time <= 120000:
        raise ValueError("A gyertyaadatok több mint kétpercesek.")
    recent = closed[-61:]
    if any(int(b[0]) - int(a[0]) != 60000
           for a, b in zip(recent, recent[1:])):
        raise ValueError("Hiányos az egyperces gyertyasor; nincs értékelés.")

    bids = sorted(
        [(number(p), number(q)) for p, q in book["bids"]], reverse=True
    )[:20]
    asks = sorted(
        [(number(p), number(q)) for p, q in book["asks"]]
    )[:20]
    if not bids or not asks or bids[0][0] <= 0 or asks[0][0] <= bids[0][0]:
        raise ValueError("Hiányos vagy érvénytelen az ajánlati könyv.")

    closes = [number(row[4]) for row in recent]
    if min(closes) <= 0:
        raise ValueError("Érvénytelen záróár érkezett.")
    price = number(last["price"])
    if price <= 0:
        raise ValueError("Érvénytelen utolsó kötésár.")
    sma20 = sum(closes[-20:]) / 20
    change5 = (closes[-1] / closes[-6] - 1) * 100
    bid_value = sum(p * q for p, q in bids)
    ask_value = sum(p * q for p, q in asks)
    total_book = bid_value + ask_value
    bid_share = 100 * bid_value / total_book if total_book else None

    buy_share = None
    if all(type(t.get("isBuyerMaker")) is bool for t in trades):
        amounts = [number(t["price"]) * number(t["qty"]) for t in trades]
        total = sum(amounts)
        buys = sum(v for t, v in zip(trades, amounts)
                   if not t["isBuyerMaker"])
        buy_share = 100 * buys / total if total else None

    position = "felett" if closes[-1] > sma20 else "alatt"
    if closes[-1] == sma20:
        position = "szintjén"
    summary = (
        f"Az utolsó lezárt 1 perces záróár az SMA20 {position} van. "
        f"Az 5 perces záróárváltozás {change5:+.2f}%. "
        "Ez leíró adat, nem igazolt kitörés–visszateszt vagy vételi jel."
    )
    return {
        "ok": True,
        "symbol": SYMBOL,
        "source": "MEXC Spot REST API",
        "received_at": iso(int(time.time() * 1000)),
        "exchange_time": iso(now),
        "last_trade_time": iso(trade_time),
        "last_trade_age_seconds": round(max(0, now - trade_time) / 1000, 1),
        "last_price": price,
        "best_bid": bids[0][0],
        "best_ask": asks[0][0],
        "spread_pct": (asks[0][0] / bids[0][0] - 1) * 100,
        "sma20": sma20,
        "change_5m_pct": change5,
        "low_60m": min(number(row[3]) for row in closed[-60:]),
        "high_60m": max(number(row[2]) for row in closed[-60:]),
        "closed_candle_time": iso(candle_time),
        "bid_share_top20_pct": bid_share,
        "bid_value_top20_usdt": bid_value,
        "ask_value_top20_usdt": ask_value,
        "taker_buy_share_pct": buy_share,
        "trade_sample_count": len(trades),
        "trade_sample_seconds": (trade_time - int(trades[0]["time"])) / 1000,
        "summary": summary,
        "signal": "KIVÁRÁS – a teljes belépési radar még nincs kiértékelve.",
        "futures_long_short_ratio": None,
        "liquidation_levels": None,
        "warnings": [
            "A könyv legfeljebb 20-20 árszint mintája, nem a teljes likviditás.",
            "A könyvválaszhoz nincs tőzsdei időbélyeg; frissessége nem igazolt.",
            "Az ajánlatok visszavonhatók. A vételi arány nem long/short arány.",
            "A REST-lekérések nem teljesen egyidejűek.",
        ],
    }


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "scope": "web service only"}


@app.get("/api/data")
async def api_data() -> JSONResponse:
    async with lock:
        if time.monotonic() >= cache["until"]:
            ttl = 10
            try:
                data = await snapshot()
                status = 200
            except Exception as exc:
                status = 503
                ttl = 15
                message = "Adatkapcsolati hiba. Nincs igazolt friss adat."
                if isinstance(exc, httpx.HTTPStatusError):
                    code = exc.response.status_code
                    message = f"A MEXC API HTTP {code} hibát adott."
                    if code in (418, 429):
                        ttl = 60
                elif isinstance(exc, ValueError):
                    message = str(exc)
                data = {"ok": False, "symbol": SYMBOL,
                        "message": message, "signal": "KIVÁRÁS"}
            cache.update(until=time.monotonic() + ttl, status=status, data=data)
        return JSONResponse(cache["data"], status_code=cache["status"],
                            headers={"Cache-Control": "no-store"})


HTML = """<!doctype html>
<html lang="hu">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AKE / MEXC adatfigyelő</title>
<style>
body {font-family:system-ui,sans-serif;margin:20px auto;padding:0 16px;
      max-width:760px;line-height:1.5;background:#101725;color:#f3f5fa;}
h1 {font-size:25px;} button {padding:12px 18px;font-size:16px;cursor:pointer;}
table {width:100%;border-collapse:collapse;}
td {padding:10px 4px;border-bottom:1px solid #42506a;overflow-wrap:anywhere;}
td:last-child {text-align:right;} small {display:block;margin:16px 0;}
</style>
</head>
<body>
<h1>AKE / MEXC adatfigyelő</h1>
<p>Nyilvános spotadatok. Lekérdezés 15 másodpercenként, amíg az oldal aktív.</p>
<button id="refresh" onclick="refreshData()">Frissítés most</button>
<h2 id="status">Adatok betöltése…</h2>
<p id="summary"></p>
<table><tbody id="rows"></tbody></table>
<small id="warnings"></small>
<small>Ez még részleges radar. Nem küld megbízást vagy értesítést,
nem kapcsolódik automatikusan a ChatGPT-hez. Futures long/short arányt
és likvidálási szinteket nem számol.</small>
<script>
let busy = false;
let lastSuccess = 0;
const el = id => document.getElementById(id);
const fmt = (v, digits=8) => v == null ? "Nincs adat" :
  Number(v).toLocaleString("hu-HU", {maximumFractionDigits:digits});
const localTime = v => new Date(v).toLocaleString("hu-HU");
function clearData(message) {
  el("status").textContent = message;
  el("summary").textContent = "";
  el("rows").replaceChildren();
  el("warnings").textContent = "";
}
async function refreshData() {
  if (busy || document.hidden) return;
  busy = true;
  el("refresh").disabled = true;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 20000);
  try {
    const response = await fetch("/api/data", {
      cache:"no-store", signal:controller.signal
    });
    const d = await response.json();
    if (!response.ok || !d.ok) throw new Error(d.message || "Adathiány.");
    el("status").textContent = d.signal;
    el("summary").textContent = d.summary;
    el("warnings").textContent = d.warnings.join(" ");
    const rows = [
      ["Utolsó kötés ára", fmt(d.last_price) + " USDT"],
      ["Legjobb vételi / eladási ajánlat", fmt(d.best_bid)+" / "+fmt(d.best_ask)],
      ["Spread", fmt(d.spread_pct,3)+"%"],
      ["5 perces záróárváltozás", fmt(d.change_5m_pct,2)+"%"],
      ["SMA20 – lezárt 1 perces gyertyák", fmt(d.sma20)],
      ["60 lezárt perc minimum / maximum", fmt(d.low_60m)+" / "+fmt(d.high_60m)],
      ["Könyvminta vételi értékaránya", fmt(d.bid_share_top20_pct,1)+"%"],
      ["Könyvminta vételi értéke", fmt(d.bid_value_top20_usdt,2)+" USDT"],
      ["Könyvminta eladási értéke", fmt(d.ask_value_top20_usdt,2)+" USDT"],
      ["Vevői kezdeményezés aránya a kötésmintában",
        d.taker_buy_share_pct == null ? "Nincs adat" : fmt(d.taker_buy_share_pct,1)+"%"],
      ["Kötésminta mérete / időtartama", d.trade_sample_count+" kötés / "+fmt(d.trade_sample_seconds,1)+" mp"],
      ["Utolsó kötés időpontja", localTime(d.last_trade_time)],
      ["Utolsó lezárt gyertya", localTime(d.closed_candle_time)],
      ["Tőzsdei szerveridő", localTime(d.exchange_time)],
      ["Lekérés befejezése", localTime(d.received_at)],
      ["Futures long/short és likvidálási szintek", "Nincs bekötve"]
    ];
    el("rows").replaceChildren();
    for (const row of rows) {
      const tr = document.createElement("tr");
      for (const value of row) {
        const td = document.createElement("td");
        td.textContent = value;
        tr.appendChild(td);
      }
      el("rows").appendChild(tr);
    }
    lastSuccess = performance.now();
  } catch (error) {
    lastSuccess = 0;
    clearData("KIVÁRÁS – " + (error.name === "AbortError" ?
      "A lekérés túllépte az időkorlátot." : error.message));
  } finally {
    clearTimeout(timeout);
    busy = false;
    el("refresh").disabled = false;
  }
}
setInterval(refreshData, 15000);
setInterval(() => {
  if (lastSuccess && performance.now() - lastSuccess > 45000) {
    lastSuccess = 0;
    clearData("KIVÁRÁS – nem érkezett időben új adat.");
  }
}, 1000);
document.addEventListener("visibilitychange", () => {
  if (!document.hidden) {
    clearData("Friss adatok lekérése…");
    refreshData();
  }
});
refreshData();
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    return HTMLResponse(HTML, headers={"Cache-Control": "no-store"})
