"""Per-route surface-rate history.

Tracks how each triangular cycle's raw surface rate evolves across scans, so a
cycle building toward the depth-check gate can be spotted *before* it crosses.
Only cycles whose legs price smoothly enough to carry a real signal should be
recorded here — coarse-tick legs produce staircase artifacts, not precursors.
"""
from collections import deque
from typing import Deque, Dict, List, Optional, Tuple


class SurfaceTracker:
    """Rolling window of raw surface rates for one exchange's cycles."""

    def __init__(self, window: int = 10):
        self.window = window
        self._history: Dict[str, Deque[float]] = {}

    def record(self, route: str, surface_perc: float) -> None:
        """Append one scan's raw (un-rounded) surface rate for a cycle."""
        dq = self._history.get(route)
        if dq is None:
            dq = deque(maxlen=self.window)
            self._history[route] = dq
        dq.append(surface_perc)

    def moving_average(self, route: str) -> Optional[float]:
        dq = self._history.get(route)
        return sum(dq) / len(dq) if dq else None

    def aggregate(self, min_samples: int = 1) -> Optional[Dict[str, float]]:
        """Mean and dispersion of the moving-average surface across every
        tracked route.

        A rising mean with stable dispersion points to a venue-wide move; a
        flat mean with wide dispersion points to independent, idiosyncratic
        route spikes. Returns {mean_ma, dispersion, route_count} or None.
        """
        mas = [sum(dq) / len(dq) for dq in self._history.values()
               if len(dq) >= min_samples]
        if not mas:
            return None
        n = len(mas)
        mean = sum(mas) / n
        variance = sum((x - mean) ** 2 for x in mas) / n
        return {'mean_ma': mean, 'dispersion': variance ** 0.5, 'route_count': n}

    def precursors(self, threshold: float,
                   min_samples: int = 4) -> List[Tuple[str, float, bool]]:
        """Cycles whose moving-average surface rate has reached `threshold`.

        Returns (route, moving_average, rising) sorted by MA descending.
        `rising` compares the newer half of the window to the older half — it
        separates a genuine build-up from a cycle merely parked above the line.
        """
        out: List[Tuple[str, float, bool]] = []
        for route, dq in self._history.items():
            if len(dq) < min_samples:
                continue
            ma = sum(dq) / len(dq)
            if ma < threshold:
                continue
            values = list(dq)
            half = len(values) // 2
            older, newer = values[:half], values[half:]
            rising = sum(newer) / len(newer) > sum(older) / len(older)
            out.append((route, ma, rising))
        out.sort(key=lambda item: item[1], reverse=True)
        return out
