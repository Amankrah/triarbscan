import sys

if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        pass

# Verified static fallback fee schedule (entry tier, % per trade). The scanner
# prefers live fees from each adapter where the public API exposes them
# (Kraken AssetPairs, KuCoin symbols feeCategory); this dict is used only when
# no live rate is available. Verified May 2026 against each exchange.
EXCHANGE_FEES = {
    'poloniex': {'maker': 0.155, 'taker': 0.155,
                 'vip_levels': {'VIP1': {'maker': 0.145, 'taker': 0.145},
                                'VIP2': {'maker': 0.135, 'taker': 0.135},
                                'VIP3': {'maker': 0.125, 'taker': 0.125}}},
    'binance': {'maker': 0.10, 'taker': 0.10,
                'vip_levels': {'VIP1': {'maker': 0.08, 'taker': 0.08},
                               'VIP2': {'maker': 0.06, 'taker': 0.06},
                               'VIP3': {'maker': 0.02, 'taker': 0.02}}},
    # Standard = entry tier (0-volume), verified against Kraken's public
    # AssetPairs `fees`/`fees_maker` arrays: [[0, 0.40], [10000, 0.35], ...].
    'kraken': {'maker': 0.25, 'taker': 0.40,
               'vip_levels': {'VIP1': {'maker': 0.14, 'taker': 0.24},
                              'VIP2': {'maker': 0.12, 'taker': 0.22},
                              'VIP3': {'maker': 0.10, 'taker': 0.20}}},
    'kucoin': {'maker': 0.10, 'taker': 0.10,
               'vip_levels': {'VIP1': {'maker': 0.08, 'taker': 0.08},
                              'VIP2': {'maker': 0.06, 'taker': 0.06},
                              'VIP3': {'maker': 0.04, 'taker': 0.04}}},
}


def calculate_fee_impact(exchange, real_rate_perc, fee_type='taker', vip_level=None,
                         total_fee_perc=None):
    """Net profit after fees.

    `total_fee_perc` is the exact summed cost of all 3 legs — pass it when the
    caller has live per-pair fees (legs can carry different rates, e.g. KuCoin
    fee categories). When omitted, fall back to the static EXCHANGE_FEES table
    (one uniform rate x 3).
    """
    exchange = exchange.lower()
    if total_fee_perc is None:
        if exchange not in EXCHANGE_FEES:
            return {'error': f'Unknown exchange: {exchange}', 'net_profit_perc': None, 'is_profitable': False}
        fee_info = EXCHANGE_FEES[exchange]
        if vip_level and vip_level in fee_info.get('vip_levels', {}):
            fee_rate = fee_info['vip_levels'][vip_level][fee_type]
        else:
            fee_rate = fee_info[fee_type]
        total_fee_perc = fee_rate * 3

    net_profit_perc = real_rate_perc - total_fee_perc
    return {
        'exchange': exchange,
        'fee_type': fee_type,
        'vip_level': vip_level or 'Standard',
        'fee_per_trade': total_fee_perc / 3,
        'total_fee_3_trades': total_fee_perc,
        'real_profit_perc': real_rate_perc,
        'net_profit_perc': net_profit_perc,
        'is_profitable': net_profit_perc > 0,
        'breakeven_rate': total_fee_perc,
    }


def calculate_dollar_profit(net_profit_perc, capital_usd):
    return (net_profit_perc / 100) * capital_usd


def get_breakeven_rates(fee_type='taker'):
    breakeven = {}
    for exchange, fees in EXCHANGE_FEES.items():
        fee_rate = fees[fee_type]
        total_fee = fee_rate * 3
        breakeven[exchange] = {
            'breakeven_rate': total_fee,
            'fee_per_trade': fee_rate,
            'total_fee': total_fee,
        }
    return breakeven


def format_fee_analysis(opportunity, capital_usd=10000):
    exchange = opportunity['exchange']
    real_rate = opportunity['real_rate_perc']
    standard = calculate_fee_impact(exchange, real_rate, 'taker')
    vip1 = calculate_fee_impact(exchange, real_rate, 'taker', 'VIP1')

    lines = [
        f"\n{'=' * 60}",
        f"FEE ANALYSIS — {exchange.upper()}",
        f"{'=' * 60}",
        f"Route: {opportunity['contract_1']} -> {opportunity['contract_2']} -> {opportunity['contract_3']}",
        f"Real profit (before fees): {real_rate:.4f}%",
        "",
        "Standard account:",
        f"  Fee per trade: {standard['fee_per_trade']:.2f}%",
        f"  Total fees (3 trades): {standard['total_fee_3_trades']:.2f}%",
        f"  Net profit: {standard['net_profit_perc']:.4f}%",
    ]
    dollar = calculate_dollar_profit(standard['net_profit_perc'], capital_usd)
    status = "✓ PROFITABLE" if standard['is_profitable'] else "✗ LOSS"
    lines.append(f"  P/L on ${capital_usd:,}: ${dollar:.2f} {status}")

    if vip1['fee_per_trade'] != standard['fee_per_trade']:
        lines.extend([
            "",
            "VIP1 account:",
            f"  Net profit: {vip1['net_profit_perc']:.4f}%",
        ])

    lines.extend([
        "",
        f"Breakeven: {standard['breakeven_rate']:.2f}%",
        f"{'=' * 60}",
    ])
    return '\n'.join(lines)


def print_breakeven_table():
    print("\n" + "=" * 70)
    print("BREAKEVEN RATES (3 taker trades)")
    print("=" * 70)
    print(f"{'Exchange':<15} {'Taker/trade':<12} {'3-trade cost':<15} {'Min real rate':<15}")
    print("-" * 70)
    for exchange in EXCHANGE_FEES:
        taker = EXCHANGE_FEES[exchange]['taker']
        total = taker * 3
        print(f"{exchange.capitalize():<15} {taker:.2f}%{' ' * 7} {total:.2f}%{' ' * 10} >{total:.2f}%")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    print_breakeven_table()
    example = {
        'exchange': 'kucoin',
        'contract_1': 'USDC_USDT',
        'contract_2': 'LUNC_USDC',
        'contract_3': 'LUNC_USDT',
        'real_rate_perc': 0.2327,
    }
    print(format_fee_analysis(example))
