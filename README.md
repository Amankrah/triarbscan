# TriArbScan

Scans crypto exchanges for triangular arbitrage opportunities using public market data (no API keys required).

## Supported exchanges

| Exchange  | Entry point   |
|-----------|---------------|
| Poloniex  | `main.py`     |
| Poloniex, Binance, Kraken, KuCoin | `main_multi.py` |

## Setup

```bash
python -m venv venv
venv\Scripts\activate          # Windows
# source venv/bin/activate     # macOS/Linux

pip install -r requirements.txt
```

## Usage

**Single exchange (Poloniex):**

```bash
python main.py
```

**Multiple exchanges:**

```bash
python main_multi.py
```

Edit the config block at the top of each entry script:

- `MIN_SURFACE_RATE` — minimum surface profit % before fetching order books
- `MIN_REAL_RATE` — minimum depth-adjusted profit % to print (use ~0.35% to cover typical taker fees on three legs)
- `ENABLED_EXCHANGES` — list of exchanges for `main_multi.py` (`poloniex`, `binance`, `kraken`, `kucoin`)

Triangular pair caches are written as `structured_triangular_pairs.json` or `triangular_pairs_<exchange>.json` on first run (gitignored).

## Fee analysis

```bash
python fee_calculator.py
```

Shows breakeven rates per exchange after three taker fees.

## Tests

```bash
python test_all_exchanges.py
```

Verifies ticker, symbol parsing, and order book access for each adapter.

## Project layout

```
exchange_adapter.py      # Abstract adapter interface
poloniex_adapter.py      # Poloniex
binance_adapter.py       # Binance
kraken_adapter.py        # Kraken
kucoin_adapter.py        # KuCoin
multi_exchange_manager.py
func_arbitrage.py        # Pair discovery and arb math
fee_calculator.py
main.py                  # Poloniex scanner
main_multi.py            # Multi-exchange scanner
test_all_exchanges.py
```

## Notes

- Surface rates use top-of-book bid/ask; real rates walk the order book for a fixed notional per base asset.
- Opportunities are informational only — latency, fees, withdrawal limits, and execution risk are not modeled in the scanner loop.
- Based on [CryptoWizardsNet/poloniex-tri-arb](https://github.com/CryptoWizardsNet/poloniex-tri-arb), extended for multiple exchanges and async scanning.
