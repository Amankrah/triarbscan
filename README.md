# TriArbScan

*A measurement instrument for the apparent-opportunity surface of triangular arbitrage on cryptocurrency exchanges.*

---

## What this is

TriArbScan is a multi-venue triangular-arbitrage scanner built as the measurement instrument behind the working paper *The Mirage of Triangular Arbitrage on Cryptocurrency Exchanges* (in [`manuscript/`](manuscript/)).

It targets the public REST and WebSocket endpoints of Binance, Kraken, and KuCoin at the retail taker fee tier and instruments the scan loop with the filters, fee model, and microstructure diagnostics the paper relies on. Operated as a measurement tool rather than a trading bot, it produces per-scan diagnostics and per-event arrival logs intended for offline analysis of the apparent-opportunity surface.

The headline empirical claim, established with this instrument:

> Under retail conditions on Binance, Kraken, and KuCoin, the apparent triangular-arbitrage opportunity surface is dominated by measurement artifacts of four structural kinds — stale low-volume quotes, tick-quantization staircases, segmented fiat-cross dislocations, and still-quote pinning on slow-refreshing high-volume legs — not by genuine cross-rate dislocations. Once these are removed and common scanner implementation errors are corrected (fee handling, exchange-symbol parsing, leg ordering), no cycle clears the retail net hurdle over multi-hundred-scan continuous runs.

See the manuscript for the full argument, tables, and references.

---

## Entry points

Four executable scanners cover the matrix of (single-venue ↔ multi-venue) × (REST polling ↔ WebSocket streaming):

| Script | Cadence | Venues | Notes |
| --- | --- | --- | --- |
| `main.py` | REST, ~10 s | Poloniex only | Original single-venue scanner; preserved as Poloniex-only mode |
| `main_multi.py` | REST, ~10–15 s per scan | Binance, Kraken, KuCoin (configurable) | Source of paper figures and tables except §3.4.4 |
| `main_ws.py` | WebSocket, event-driven up to 20 Hz | Binance | Single-venue streaming variant; L2 partial-book on a precursor watchlist (§3.4.4 probe) |
| `main_ws_multi.py` | WebSocket, event-driven up to 20 Hz per venue | Binance + Kraken + KuCoin in parallel | Three independent scan loops + combined heartbeat |

The WebSocket scanners apply the same filtering and fee methodology as the REST scanner — they differ only in observation cadence and in producing additional arrival-process logs. The WS cadence is what makes §3.4.4 (still-quote pinning) observable.

---

## Setup

```bash
python -m venv venv
.\venv\Scripts\activate          # Windows PowerShell
# source venv/bin/activate       # macOS / Linux

pip install -r requirements.txt
```

Python 3.10+ recommended. **No exchange API keys are required** — all observations use unauthenticated public endpoints.

---

## Quick start

```bash
python main_multi.py             # REST multi-venue, periodic
python main_ws_multi.py          # WebSocket multi-venue, event-driven
```

Each scanner prints a per-venue diagnostic block: pairs evaluated, positive-surface count, depth gate, best surface / real / net rates, the route currently driving best-surface, and aggregate-trajectory statistics. WebSocket scanners additionally print **per-leg quote ages** on the best-surface route (the §3.4.4 still-quote-pinning diagnostic).

Stop with Ctrl+C. Output artifacts (CSV logs, JSON triangle dumps) accumulate in the working directory and are covered by `.gitignore`.

---

## Methodology

The scanner enforces a layered pipeline; each layer removes a known source of apparent-but-non-tradeable opportunity, as documented in the manuscript. Defaults reflect the calibrations reported there.

**Triangular cycle enumeration.** O(n²) via a currency index, deduplicated by sorted-pair-name hash. Cycles are persisted on each bootstrap as `triangular_pairs_<venue>.json` for inspection.

**Volume filter** (§3.4.1). Pairs whose 24-hour USD volume falls below `MIN_USD_VOLUME` (default $50K) are dropped *before* triangle construction. Fiat-quoted pairs are held to a higher floor `MIN_USD_VOLUME_BY_QUOTE` (default $250K for EUR / GBP / JPY / CAD / AUD / CHF) because fiat-quoted thin altcoins are disproportionately prone to stale quotes.

**Tick-ratio filter** (§3.4.2, §3.5). Cycles whose legs include a pair with `tick / mid ≥ 2 × 10⁻⁴` are excluded from precursor tracking and the aggregate trajectory. This removes the tick-quantization staircase artifact that affects coarse-grid pairs even at high volume (the XDG_BTC case in §3.4.2). The 2×10⁻⁴ threshold is calibrated against a venue-by-venue sweep in §3.5.

**Per-pair taker fee** (§2.7, §3.3). Fees are sourced live from each venue's metadata where the API exposes them — Kraken's `AssetPairs` entry-tier schedule and KuCoin's per-pair `feeCategory` (Class A/B/C → 0.10%/0.16%/0.24%). For Binance, where no unauthenticated fee endpoint exists, a verified constant is used and labelled as such. The net rate sums the three legs' actual rates rather than multiplying a single per-exchange rate by three.

**Depth gate** (§2.6). Order-book walks are gated behind a per-venue surface threshold derived from `3·τ_min + slippage − margin`. Cycles below the gate cannot net positive and are not depth-checked, eliminating ~90% of per-scan HTTP cost without changing the opportunity set.

**Leg ordering** (§3.2). The depth walk routes through `order_legs_for_execution`, so the three legs are walked in their actual produce→consume sequence with the starting notional in the genuinely first-consumed currency. Without this, naive scanners systematically mis-test the cycle in one direction and produce spurious book-exhaustion in the other.

**Surface time-series tracking** (§2.9). For eligible cycles, the WebSocket scanners maintain a per-route moving average (precursor detection) and a per-venue aggregate trajectory (mean + dispersion across routes — separating systemic from idiosyncratic dynamics). Both are logged to daily-rotated CSVs for offline analysis.

**Arrival event log** (§4.6). Gate-crossings (up/down) and precursor enter/exit transitions are written to a daily-rotated event log with millisecond timestamps — the input dataset for the arrival-process characterization the manuscript advertises as the next study.

**Per-leg quote-age diagnostic** (§3.4.4). The WebSocket feeds record the wall-clock of each pair's last *inside-price* change. Heartbeats print these ages alongside the best-surface route — a leg ageing into multi-second territory while its cycle-mates tick sub-second is the signature of still-quote pinning.

---

## Configuration

Edit constants at the top of each entry script.

### REST scanner (`main_multi.py`)

| Constant | Default | Purpose |
| --- | --- | --- |
| `ENABLED_EXCHANGES` | `['binance', 'kraken', 'kucoin']` | Active venues |
| `MIN_USD_VOLUME` | `50000` | Per-pair 24h USD volume floor |
| `MIN_USD_VOLUME_BY_QUOTE` | EUR/GBP/JPY/CAD/AUD/CHF → `250000` | Per-quote-currency override |
| `MIN_NET_RATE` | `0.0` | Net hurdle for opportunity classification |
| `SLIPPAGE_BUFFER` | `0.10` | % subtracted on top of fees |
| `AUTO_DEPTH_GATE` | `True` | Derive gate from per-venue fee hurdle |
| `DEPTH_GATE_MARGIN` | `0.10` | Gate sits this far below strict hurdle |
| `NOTIONAL_USD` | `5000` | Notional used in dollar-profit display |

### WebSocket scanners (`main_ws.py`, `main_ws_multi.py`)

All of the above plus:

| Constant | Default | Purpose |
| --- | --- | --- |
| `EVENT_SCAN_HZ` | `20` | Cap on event-driven scan rate per venue |
| `AGGREGATE_LOG_SECONDS` | `1.0` | Slow-tick interval (tracker, aggregate, CSV) |
| `HEARTBEAT_SECONDS` | `5.0` | Console summary cadence |
| `ENABLE_L2_WATCHLIST` | `True` | Stream L2 partial-book for precursor cycles (Binance only) |
| `LOG_AGGREGATE_CSV` | `True` | Append aggregate rows to daily CSV |
| `LOG_ARRIVAL_EVENTS` | `True` | Append gate/precursor events to daily CSV |

### Microstructure thresholds (`multi_exchange_manager.py`)

| Constant | Default | Purpose |
| --- | --- | --- |
| `MAX_TICK_RATIO` | `2e-4` | Per-leg tick-quantization exclusion threshold |
| `PRECURSOR_WINDOW` | `10` | Scans of surface history kept per cycle |
| `PRECURSOR_GATE_FRACTION` | `0.5` | Flag a cycle when its MA reaches this × gate |
| `AGGREGATE_TREND_WINDOW` | `20` | Slow ticks used to compute the aggregate-trend flag |

---

## Output artifacts

All files are created in the working directory and covered by `.gitignore`.

| File pattern | Producer | Contents |
| --- | --- | --- |
| `triangular_pairs_<venue>.json` | All scanners | Triangle dump from bootstrap, one per venue, rewritten each run |
| `surface_aggregate_log_<YYYY-MM-DD>.csv` | `main_multi.py` | REST aggregate: 1 row per scan per venue (timestamp, exchange, mean MA, dispersion, route count) |
| `surface_aggregate_log_ws_<YYYY-MM-DD>.csv` | `main_ws.py` | Binance-only WS aggregate, 1 row per second |
| `surface_aggregate_log_ws_<venue>_<YYYY-MM-DD>.csv` | `main_ws_multi.py` | Per-venue WS aggregate |
| `arrival_events_ws_<YYYY-MM-DD>.csv` | `main_ws.py` | Gate-crossings + precursor transitions, ms-precision |
| `arrival_events_ws_<venue>_<YYYY-MM-DD>.csv` | `main_ws_multi.py` | Per-venue arrival events |

All CSVs write a header on first creation and are safe to ingest with `pandas.read_csv` directly.

---

## Tests and utilities

```bash
python test_all_exchanges.py     # Ticker / symbol-parse / orderbook smoke per adapter
python fee_calculator.py         # Prints the verified-fallback breakeven table
```

---

## Project layout

```
exchange_adapter.py        # Abstract adapter: fees, tick sizes, get_tradeable_pairs
poloniex_adapter.py        # Poloniex
binance_adapter.py         # Binance (verified fallback fee; tick sizes from exchangeInfo)
kraken_adapter.py          # Kraken (live fees + tick sizes from AssetPairs; wsname map)
kucoin_adapter.py          # KuCoin (per-pair fees + tick sizes from /api/v2/symbols)

func_arbitrage.py          # Cycle enumeration, surface calc, depth walk, leg reordering
fee_calculator.py          # Verified fallback fee schedule; per-cycle net calc
surface_tracker.py         # Per-route MA + aggregate trajectory
multi_exchange_manager.py  # Volume/fiat filter, microstructure constants, REST orchestrator

ws_binance.py              # Binance L1 bookTicker + L2 partial-book depth feeds
ws_kraken.py               # Kraken WS v2 ticker channel
ws_kucoin.py               # KuCoin bullet-token feed (auto token refresh)

main.py                    # REST single-venue (Poloniex, legacy)
main_multi.py              # REST multi-venue scanner — paper §3.1–§3.7
main_ws.py                 # WS single-venue (Binance) — §3.4.4 probe
main_ws_multi.py           # WS multi-venue scanner — arrival-process logging

test_all_exchanges.py      # Adapter smoke tests
manuscript/                # Working paper draft (v0.2)
```

---

## Manuscript

The working paper lives at [`manuscript/triarbscan_manuscript_v0.2.md`](manuscript/triarbscan_manuscript_v0.2.md). The scanner is the measurement instrument it describes; section numbers referenced throughout this README correspond to the manuscript's structure.

Pre-compiled `.docx` versions in the same directory may lag the markdown; regenerate with `pandoc triarbscan_manuscript_v0.2.md -o triarbscan_manuscript_v0.2.docx` when needed.

---

## Limits

- Observations are conditional on the retail taker fee tier, public REST/WebSocket endpoints, and the cadences and filter parameters above. Results should not be read as claims about maker-tier strategies, VIP volume-tier fees, or co-located streaming infrastructure.
- Surface rates use top-of-book quotes; real rates walk live order books for a fixed notional. Neither models withdrawal limits, deposit times, custody, or counterparty risk.
- L2 partial-book streaming on a precursor watchlist is currently Binance-only; Kraken and KuCoin run at L1 (inside quote) for now. The extension is mechanically straightforward via each venue's depth channel.
- The scanner is a measurement tool. It does not place trades.

---

## Provenance

This scanner originated from the public [CryptoWizardsNet/poloniex-tri-arb](https://github.com/CryptoWizardsNet/poloniex-tri-arb) scaffold and has been substantially rewritten for multi-exchange operation, asynchronous and event-driven scanning, live per-pair fee sourcing, microstructure-aware filtering, and the surface-rate time-series instrumentation documented in the manuscript.
