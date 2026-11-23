"""NFR-1/NFR-5/AC-6: platform budget ceiling and measured intake latency.

Monthly infrastructure ceiling USD 150: alert at 80%, refuse non-essential
spend at 100%. Intake/outbox latency tracker for the AC-6 numbers, measured
on this host during the repair eval.
"""

from __future__ import annotations

from dataclasses import dataclass, field

MONTHLY_CAP_USD = 150.0
ALERT_FRACTION = 0.8


@dataclass
class PlatformBudget:
    monthly_cap_usd: float = MONTHLY_CAP_USD
    spent_usd: float = 0.0

    @property
    def status(self) -> str:
        if self.spent_usd >= self.monthly_cap_usd:
            return "stop"
        if self.spent_usd >= self.monthly_cap_usd * ALERT_FRACTION:
            return "alert"
        return "ok"

    def spend(self, amount_usd: float, essential: bool = False) -> bool:
        """Record spend. Non-essential spend is refused once stopped."""
        if self.status == "stop" and not essential:
            return False
        self.spent_usd += amount_usd
        return True


def p95_ms(values_ms: list[float]) -> float:
    if not values_ms:
        return 0.0
    ordered = sorted(values_ms)
    return ordered[min(len(ordered) - 1, int(0.95 * len(ordered)))]


@dataclass
class LatencyTracker:
    samples_ms: list[float] = field(default_factory=list)

    def record(self, elapsed_ms: float) -> None:
        self.samples_ms.append(elapsed_ms)

    @property
    def p95(self) -> float:
        return p95_ms(self.samples_ms)
