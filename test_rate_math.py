"""Sanity checks for calc_triangular_arb_surface_rate.

Triangle shape: pair_a = A_B, pair_b = B_C, pair_c = A_C
(same shape as DASH_BTC / BTC_USDT / DASH_USDT).
Consistent mids: 1 A = 2 B, 1 B = 3 C  =>  1 A = 6 C.
"""
from func_arbitrage import calc_triangular_arb_surface_rate

T_PAIR = {
    "a_base": "A", "a_quote": "B",
    "b_base": "B", "b_quote": "C",
    "c_base": "A", "c_quote": "C",
    "pair_a": "A_B", "pair_b": "B_C", "pair_c": "A_C",
}


def prices(ab, bc, ac, half_spread):
    """Build a prices_dict from mid prices and a fractional half-spread."""
    def leg(mid):
        return mid * (1 - half_spread), mid * (1 + half_spread)  # bid, ask
    ab_b, ab_a = leg(ab)
    bc_b, bc_a = leg(bc)
    ac_b, ac_a = leg(ac)
    return {
        "pair_a_bid": ab_b, "pair_a_ask": ab_a,
        "pair_b_bid": bc_b, "pair_b_ask": bc_a,
        "pair_c_bid": ac_b, "pair_c_ask": ac_a,
    }


def run():
    failures = 0

    # 1. Zero spread, perfectly consistent cycle -> profit must be ~0%.
    _, best = calc_triangular_arb_surface_rate(T_PAIR, prices(2, 3, 6, 0.0))
    ok = best is not None and abs(best) < 1e-9
    print(f"[1] zero-spread 1:1:1 cycle   best_surface={best!r}  "
          f"{'PASS' if ok else 'FAIL (expected ~0%)'}")
    failures += not ok

    # 2. Real spread, still no-arb -> best path must be NEGATIVE
    #    (crossing 3 spreads costs money). A positive result here means the
    #    bid/ask sides are swapped somewhere.
    _, best = calc_triangular_arb_surface_rate(T_PAIR, prices(2, 3, 6, 0.005))
    ok = best is not None and best < 0
    print(f"[2] 0.5% spread, no-arb       best_surface={best:.4f}%  "
          f"{'PASS' if ok else 'FAIL (expected < 0% — bid/ask likely inverted)'}")
    failures += not ok

    # 3. Genuine arb: A_C richer than the consistent 6.0 -> profit must be > 0.
    surface, best = calc_triangular_arb_surface_rate(T_PAIR, prices(2, 3, 6.3, 0.005))
    ok = best is not None and best > 0 and bool(surface)
    print(f"[3] A_C mispriced (6.3)       best_surface={best:.4f}%  "
          f"{'PASS' if ok else 'FAIL (expected > 0%)'}")
    failures += not ok

    print(f"\n{'ALL PASS' if failures == 0 else str(failures) + ' FAILURE(S)'}")
    return failures


if __name__ == "__main__":
    raise SystemExit(run())
