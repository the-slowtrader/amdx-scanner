#!/usr/bin/env python3
"""
Weekly Disagreement Scanner
Finds stocks exhibiting weekly DOL reversals: a candle that sweeps 2+ consecutive
weeks of lows (or highs) then closes back inside the previous week's range.
"""

import os
import sys
import json
import argparse
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

import requests

FMP_BASE = "https://financialmodelingprep.com/stable"
FMP_API_KEY = os.environ.get("FMP_API_KEY", "9ZZSogafEDXEw5Z3NQnbK4i0NRuQz8yZ")

CORE_TICKERS = [
    "SPY", "XOM", "UPS", "RIVN", "NVDA", "AMD", "AAPL", "HOOD",
    "AVGO", "SOFI", "AFRM", "MCD", "BE", "DIS", "CROX",
]

# Matt's watchlist (Reese's Pieces)
MATT_TICKERS = [
    "INTC","MU","AAPL","NVDA","MSFT","META","AMD","ASML","ARM","MCHP","AVGO","MRVL",
    "COIN","JPM","AXP","MA","GS","WFC","IBKR","CI","AIG","PFE","LLY","UNH","ABBV",
    "JNJ","HD","TGT","CHWY","MCD","DIS","NKE","BBY","CROX","LULU","CMG","ABNB","COST",
    "FIVE","FUBO","XOM","COP","EOG","OXY","AES","BAP","DEO","CAT","BA","DE","FLR",
    "DHI","GNRC","LMT","W","RGR","TSLA","EVGO","ZS","HOOD","DLO","SOUN","CSCO","MARA",
    "MET","SRPT","FANG","RCAT","NEON","PLD","TSM","TWLO","DELL","AMAT","NU","HLT","FN",
    "FIG","SMH","CNC","SNOW","FDX","INTU","ORCL","AI","QQQ","CVX","AVAV","IREN","CIFR",
    "MKTX","LYTS","SNAP","DUOL","FSLR","FTNT","JD","PLTR","CRCL","MP","WELL","SPOT",
    "ABAT","RIVN","RXRX","BRK.B","TOST","BIRK","RGTI","FRHC","LSCC","UI","IOT","NOW",
    "CAVA","ENPH","SOFI","KTOS","DOMO","CVS","MSTR","DKNG","IONQ","ZM","OSCR","DRI",
    "DAVE","AXTI","LCID","EOSE","DNTH","M","RBLX","HAL","CELH","TTWO","POOL","NET",
    "RDDT","UBER","BABA","CORZ","CDNS","RKLB","VRT","DOCU","CRWD","AMGN",
]

# S&P 500 tickers (hardcoded — FMP constituent endpoint requires higher tier)
SP500_TICKERS = [
    "AAPL","ABBV","ABT","ACN","ADBE","ADI","ADM","ADP","ADSK","AEE","AEP","AES",
    "AFL","AIG","AIZ","AJG","AKAM","ALB","ALGN","ALK","ALL","ALLE","AMAT","AMCR",
    "AMD","AME","AMGN","AMP","AMT","AMZN","ANET","ANSS","AON","AOS","APA","APD",
    "APH","APTV","ARE","ATO","ATVI","AVB","AVGO","AVY","AWK","AXP","AZO","BA",
    "BAC","BAX","BBWI","BBY","BDX","BEN","BF.B","BIIB","BIO","BK","BKNG","BKR",
    "BLK","BMY","BR","BRK.B","BRO","BSX","BWA","BXP","C","CAG","CAH","CARR",
    "CAT","CB","CBOE","CBRE","CCI","CCL","CDAY","CDNS","CDW","CE","CEG","CF",
    "CFG","CHD","CHRW","CHTR","CI","CINF","CL","CLX","CMA","CMCSA","CME","CMG",
    "CMI","CMS","CNC","CNP","COF","COO","COP","COST","CPB","CPRT","CPT","CRL",
    "CRM","CSCO","CSGP","CSX","CTAS","CTLT","CTRA","CTSH","CTVA","CVS","CVX",
    "CZR","D","DAL","DD","DE","DFS","DG","DGX","DHI","DHR","DIS","DISH","DLR",
    "DLTR","DOV","DOW","DPZ","DRI","DTE","DUK","DVA","DVN","DXC","DXCM","EA",
    "EBAY","ECL","ED","EFX","EIX","EL","EMN","EMR","ENPH","EOG","EPAM","EQIX",
    "EQR","EQT","ES","ESS","ETN","ETR","ETSY","EVRG","EW","EXC","EXPD","EXPE",
    "EXR","F","FANG","FAST","FBHS","FCX","FDS","FDX","FE","FFIV","FIS","FISV",
    "FITB","FLT","FMC","FOX","FOXA","FRC","FRT","FTNT","FTV","GD","GE","GILD",
    "GIS","GL","GLW","GM","GNRC","GOOG","GOOGL","GPC","GPN","GRMN","GS","GWW",
    "HAL","HAS","HBAN","HCA","HD","HOLX","HON","HPE","HPQ","HRL","HSIC","HST",
    "HSY","HUM","HWM","IBM","ICE","IDXX","IEX","IFF","ILMN","INCY","INTC","INTU",
    "INVH","IP","IPG","IQV","IR","IRM","ISRG","IT","ITW","IVZ","J","JBHT","JCI",
    "JKHY","JNJ","JNPR","JPM","K","KDP","KEY","KEYS","KHC","KIM","KLAC","KMB",
    "KMI","KMX","KO","KR","L","LDOS","LEN","LH","LHX","LIN","LKQ","LLY","LMT",
    "LNC","LNT","LOW","LRCX","LUMN","LUV","LVS","LW","LYB","LYV","MA","MAA",
    "MAR","MAS","MCD","MCHP","MCK","MCO","MDLZ","MDT","MET","META","MGM","MHK",
    "MKC","MKTX","MLM","MMC","MMM","MNST","MO","MOH","MOS","MPC","MPWR","MRK",
    "MRNA","MRO","MS","MSCI","MSFT","MSI","MTB","MTCH","MTD","MU","NCLH","NDAQ",
    "NDSN","NEE","NEM","NFLX","NI","NKE","NOC","NOW","NRG","NSC","NTAP","NTRS",
    "NUE","NVDA","NVR","NWL","NWS","NWSA","NXPI","O","ODFL","OGN","OKE","OMC",
    "ON","ORCL","ORLY","OTIS","OXY","PARA","PAYC","PAYX","PCAR","PCG","PEAK",
    "PEG","PEP","PFE","PFG","PG","PGR","PH","PHM","PKG","PKI","PLD","PM","PNC",
    "PNR","PNW","POOL","PPG","PPL","PRU","PSA","PSX","PTC","PVH","PWR","PXD",
    "PYPL","QCOM","QRVO","RCL","RE","REG","REGN","RF","RHI","RJF","RL","RMD",
    "ROK","ROL","ROP","ROST","RSG","RTX","SBAC","SBNY","SBUX","SCHW","SEE","SHW",
    "SIVB","SJM","SLB","SNA","SNPS","SO","SPG","SPGI","SRE","STE","STT","STX",
    "STZ","SWK","SWKS","SYF","SYK","SYY","T","TAP","TDG","TDY","TECH","TEL",
    "TER","TFC","TFX","TGT","TMO","TMUS","TPR","TRGP","TRMB","TROW","TRV","TSCO",
    "TSLA","TSN","TT","TTWO","TXN","TXT","TYL","UAL","UDR","UHS","ULTA","UNH",
    "UNP","UPS","URI","USB","V","VFC","VICI","VLO","VMC","VNO","VRSK","VRSN",
    "VRTX","VTR","VTRS","VZ","WAB","WAT","WBA","WBD","WDC","WEC","WELL","WFC",
    "WHR","WM","WMB","WMT","WRB","WRK","WST","WTW","WY","WYNN","XEL","XOM",
    "XRAY","XYL","YUM","ZBH","ZBRA","ZION","ZTS",
]


# ---------------------------------------------------------------------------
# FMP helpers
# ---------------------------------------------------------------------------

def fmp_get(endpoint, params=None):
    params = params or {}
    params["apikey"] = FMP_API_KEY
    url = f"{FMP_BASE}/{endpoint}"
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def get_sp500_tickers():
    return list(SP500_TICKERS)


def get_daily_history(ticker, days=740):
    today = datetime.date.today()
    from_date = today - datetime.timedelta(days=days)
    data = fmp_get("historical-price-eod/full", {
        "symbol": ticker,
        "from": from_date.isoformat(),
        "to": today.isoformat(),
    })
    if not data or not isinstance(data, list):
        return []
    data.sort(key=lambda r: r["date"])
    return data


# ---------------------------------------------------------------------------
# Resample daily -> weekly
# ---------------------------------------------------------------------------

def resample_weekly(daily_rows):
    """Group daily bars into ISO-week buckets (Mon-Fri)."""
    buckets = defaultdict(list)
    for row in daily_rows:
        d = datetime.date.fromisoformat(row["date"])
        iso_year, iso_week, _ = d.isocalendar()
        key = (iso_year, iso_week)
        buckets[key].append(row)

    weeks = []
    for key in sorted(buckets.keys()):
        bars = buckets[key]
        if not bars:
            continue
        week_open = bars[0]["open"]
        week_close = bars[-1]["close"]
        week_high = max(b["high"] for b in bars)
        week_low = min(b["low"] for b in bars)
        week_volume = sum(b.get("volume", 0) for b in bars)
        week_date = bars[0]["date"]  # Monday (or first trading day)
        weeks.append({
            "date": week_date,
            "open": week_open,
            "high": week_high,
            "low": week_low,
            "close": week_close,
            "volume": week_volume,
        })
    return weeks


# ---------------------------------------------------------------------------
# Pattern detection
# ---------------------------------------------------------------------------

def detect_signals(ticker, weeks, min_swept=2):
    """Detect bullish and bearish disagreement patterns."""
    signals = []
    for i in range(1, len(weeks)):
        candle = weeks[i]
        pw = weeks[i - 1]
        pw_range = pw["high"] - pw["low"]
        if pw_range <= 0:
            continue

        pw_body_top = max(pw["open"], pw["close"])
        pw_body_bot = min(pw["open"], pw["close"])
        pw_ce = (pw["high"] + pw["low"]) / 2

        # Rolling 4-week range for discount/premium
        lookback = weeks[max(0, i - 4):i]
        rolling_high = max(w["high"] for w in lookback)
        rolling_low = min(w["low"] for w in lookback)
        monthly_ce = (rolling_high + rolling_low) / 2

        # --- Bullish (sell-side sweep) ---
        if candle["low"] < pw["low"] and candle["close"] > pw["low"] and candle["close"] <= pw["high"]:
            count = 0
            swept_lows = []
            for j in range(i - 1, -1, -1):
                if candle["low"] < weeks[j]["low"]:
                    count += 1
                    swept_lows.append({"week": weeks[j]["date"], "low": weeks[j]["low"]})
                else:
                    break

            if count >= min_swept:
                bullish_candle = candle["close"] > candle["open"]
                in_discount = candle["close"] < monthly_ce
                closed_above_pw_body = candle["close"] > pw_body_bot
                sweep_depth_pct = round((pw["low"] - candle["low"]) / pw_range * 100, 1)
                close_vs_pwl_pct = round((candle["close"] - pw["low"]) / pw_range * 100, 1)

                score = _score_signal(
                    weeks_swept=count,
                    closed_body=closed_above_pw_body,
                    candle_confirms=bullish_candle,
                    discount_premium=in_discount,
                    sweep_depth_ratio=(pw["low"] - candle["low"]) / pw_range,
                    close_strength_ratio=(candle["close"] - pw["low"]) / pw_range,
                )
                grade = "A" if score >= 60 else ("B" if score >= 40 else "C")

                signals.append({
                    "ticker": ticker,
                    "date": candle["date"],
                    "direction": "BULLISH",
                    "type": "Sell-Side Disagree",
                    "price": candle["close"],
                    "weeks_swept": count,
                    "swept_lows": swept_lows,
                    "swept_level": pw["low"],
                    "sweep_low": candle["low"],
                    "sweep_depth_pct": sweep_depth_pct,
                    "close_vs_pwl_pct": close_vs_pwl_pct,
                    "closed_above_pw_body": closed_above_pw_body,
                    "bullish_candle": bullish_candle,
                    "in_discount": in_discount,
                    "score": round(score, 1),
                    "grade": grade,
                    "dol_targets": {
                        "pw_ce": round(pw_ce, 2),
                        "pwh": pw["high"],
                        "monthly_ce": round(monthly_ce, 2),
                    },
                    "weekly_open": candle["open"],
                    "weekly_high": candle["high"],
                    "weekly_low": candle["low"],
                    "weekly_close": candle["close"],
                    "pw_high": pw["high"],
                    "pw_low": pw["low"],
                    "pw_ce": round(pw_ce, 2),
                })

        # --- Bearish (buy-side sweep) ---
        if candle["high"] > pw["high"] and candle["close"] < pw["high"] and candle["close"] >= pw["low"]:
            count = 0
            swept_highs = []
            for j in range(i - 1, -1, -1):
                if candle["high"] > weeks[j]["high"]:
                    count += 1
                    swept_highs.append({"week": weeks[j]["date"], "high": weeks[j]["high"]})
                else:
                    break

            if count >= min_swept:
                bearish_candle = candle["close"] < candle["open"]
                in_premium = candle["close"] > monthly_ce
                closed_below_pw_body = candle["close"] < pw_body_top
                sweep_depth_pct = round((candle["high"] - pw["high"]) / pw_range * 100, 1)
                close_vs_pwh_pct = round((pw["high"] - candle["close"]) / pw_range * 100, 1)

                score = _score_signal(
                    weeks_swept=count,
                    closed_body=closed_below_pw_body,
                    candle_confirms=bearish_candle,
                    discount_premium=in_premium,
                    sweep_depth_ratio=(candle["high"] - pw["high"]) / pw_range,
                    close_strength_ratio=(pw["high"] - candle["close"]) / pw_range,
                )
                grade = "A" if score >= 60 else ("B" if score >= 40 else "C")

                signals.append({
                    "ticker": ticker,
                    "date": candle["date"],
                    "direction": "BEARISH",
                    "type": "Buy-Side Disagree",
                    "price": candle["close"],
                    "weeks_swept": count,
                    "swept_highs": swept_highs,
                    "swept_level": pw["high"],
                    "sweep_high": candle["high"],
                    "sweep_depth_pct": sweep_depth_pct,
                    "close_vs_pwh_pct": close_vs_pwh_pct,
                    "closed_below_pw_body": closed_below_pw_body,
                    "bearish_candle": bearish_candle,
                    "in_premium": in_premium,
                    "score": round(score, 1),
                    "grade": grade,
                    "dol_targets": {
                        "pw_ce": round(pw_ce, 2),
                        "pwl": pw["low"],
                        "monthly_ce": round(monthly_ce, 2),
                    },
                    "weekly_open": candle["open"],
                    "weekly_high": candle["high"],
                    "weekly_low": candle["low"],
                    "weekly_close": candle["close"],
                    "pw_high": pw["high"],
                    "pw_low": pw["low"],
                    "pw_ce": round(pw_ce, 2),
                })

    return signals


def _score_signal(weeks_swept, closed_body, candle_confirms, discount_premium,
                  sweep_depth_ratio, close_strength_ratio):
    score = 0.0
    # Weeks swept: 15 per week, cap 45
    score += min(weeks_swept * 15, 45)
    # Closed above PW body bottom (bull) / below PW body top (bear)
    if closed_body:
        score += 10
    # Candle direction confirms
    if candle_confirms:
        score += 10
    # In discount (bull) / premium (bear)
    if discount_premium:
        score += 10
    # Sweep depth: ratio * 20, cap 15
    score += min(sweep_depth_ratio * 20, 15)
    # Close strength: ratio * 15, cap 10
    score += min(close_strength_ratio * 15, 10)
    return score


# ---------------------------------------------------------------------------
# Scan orchestration
# ---------------------------------------------------------------------------

def scan_ticker(ticker, min_swept=2):
    """Fetch data + detect signals for one ticker."""
    try:
        daily = get_daily_history(ticker)
        if len(daily) < 10:
            return ticker, [], None
        weeks = resample_weekly(daily)
        if len(weeks) < 3:
            return ticker, [], None

        # Price filter
        last_close = weeks[-1]["close"]
        if last_close < 15 or last_close > 500:
            return ticker, [], None

        sigs = detect_signals(ticker, weeks, min_swept=min_swept)
        return ticker, sigs, None
    except Exception as e:
        return ticker, [], str(e)


def run_scan(tickers=None, core_only=False, matt_only=False,
             specific_tickers=None, min_swept=2, output_path=None):
    """Run the full scan and return results dict."""
    if specific_tickers:
        universe = [t.upper() for t in specific_tickers]
    elif core_only:
        universe = list(CORE_TICKERS)
    elif matt_only:
        universe = list(set(MATT_TICKERS + CORE_TICKERS))
    else:
        sp500 = get_sp500_tickers()
        universe = list(set(sp500 + CORE_TICKERS + MATT_TICKERS))
    universe.sort()

    print(f"Scanning {len(universe)} tickers (min swept = {min_swept})...")
    all_signals = []
    errors = 0
    done = 0

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(scan_ticker, t, min_swept): t for t in universe}
        for future in as_completed(futures):
            ticker, sigs, err = future.result()
            if err:
                errors += 1
            all_signals.extend(sigs)
            done += 1
            if done % 50 == 0:
                print(f"  Progress: {done}/{len(universe)} tickers scanned, "
                      f"{len(all_signals)} signals found...")

    # Split active vs historical
    all_signals.sort(key=lambda s: s["date"], reverse=True)
    unique_dates = sorted(set(s["date"] for s in all_signals), reverse=True)
    active_dates = set(unique_dates[:2]) if len(unique_dates) >= 2 else set(unique_dates)

    active = [s for s in all_signals if s["date"] in active_dates]
    historical = [s for s in all_signals if s["date"] not in active_dates][:200]

    active_bull = sum(1 for s in active if s["direction"] == "BULLISH")
    active_bear = sum(1 for s in active if s["direction"] == "BEARISH")
    a_grade = sum(1 for s in active if s["grade"] == "A")
    b_grade = sum(1 for s in active if s["grade"] == "B")

    result = {
        "scan_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "tickers_scanned": len(universe),
        "errors": errors,
        "total_signals": len(all_signals),
        "active_setups": active,
        "historical_signals": historical,
        "summary": {
            "active_bullish": active_bull,
            "active_bearish": active_bear,
            "a_grade_active": a_grade,
            "b_grade_active": b_grade,
        },
    }

    # Determine output path
    if not output_path:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_path = os.path.join(script_dir, "disagreement_data.json")

    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\nScan complete. {len(all_signals)} total signals found.")
    print(f"  Active: {len(active)} ({active_bull} bull, {active_bear} bear)")
    print(f"  A-grade active: {a_grade}, B-grade active: {b_grade}")
    print(f"  Historical: {len(historical)}")
    print(f"  Errors: {errors}")
    print(f"  Output: {output_path}")

    # Print summary table
    _print_table(active)

    return result


def _print_table(signals):
    if not signals:
        return
    print(f"\n{'Ticker':<8} {'Grade':<6} {'Score':<7} {'Price':<10} {'Wks':<5} {'Type':<22} {'Date'}")
    print("-" * 75)
    for s in sorted(signals, key=lambda x: x["score"], reverse=True):
        print(f"{s['ticker']:<8} {s['grade']:<6} {s['score']:<7} "
              f"${s['price']:<9.2f} {s['weeks_swept']:<5} {s['type']:<22} {s['date']}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Weekly Disagreement Scanner")
    parser.add_argument("--core", action="store_true", help="Scan core 15 tickers only")
    parser.add_argument("--matt", action="store_true", help="Scan Matt's watchlist + core tickers")
    parser.add_argument("--tickers", nargs="+", help="Scan specific tickers")
    parser.add_argument("--min-swept", type=int, default=2, help="Minimum weeks swept (default: 2)")
    parser.add_argument("--output", help="Custom output JSON path")
    args = parser.parse_args()

    if not FMP_API_KEY:
        print("ERROR: FMP_API_KEY environment variable not set.")
        sys.exit(1)

    run_scan(
        core_only=args.core,
        matt_only=args.matt,
        specific_tickers=args.tickers,
        min_swept=args.min_swept,
        output_path=args.output,
    )


if __name__ == "__main__":
    main()
