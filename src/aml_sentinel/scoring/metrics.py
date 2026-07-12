"""Score fired alerts against per-ring ground truth.

Matching rule: an alert is a true positive when its evidence contains at
least one injected fraud transaction; a ring counts as detected by a
typology when any of that typology's alerts includes one of the ring's
hops. Detection latency is simulated time from a ring's last hop to the
earliest matching alert (0 = caught the moment the cycle closed; negative =
flagged before the ring even completed).
"""

from __future__ import annotations

import statistics
from dataclasses import asdict, dataclass

from aml_sentinel.models import Alert, RingGroundTruth


@dataclass(frozen=True, slots=True)
class TypologyMetrics:
    typology: str
    alerts: int
    true_positives: int
    false_positives: int
    rings_detected: int
    rings_total: int
    precision: float | None  # None when the typology fired no alerts
    recall: float
    mean_latency_s: float | None  # None when no ring was detected
    median_latency_s: float | None


@dataclass(frozen=True, slots=True)
class ScoreReport:
    per_typology: tuple[TypologyMetrics, ...]
    overall: TypologyMetrics

    def to_dict(self) -> dict[str, object]:
        return {
            "per_typology": [asdict(m) for m in self.per_typology],
            "overall": asdict(self.overall),
        }

    def to_markdown(self) -> str:
        header = (
            "| Typology | Alerts | TP | FP | Precision | Rings detected | Recall "
            "| Latency mean | Latency median |\n"
            "|:---|---:|---:|---:|---:|---:|---:|---:|---:|"
        )
        rows = [_markdown_row(m) for m in (*self.per_typology, self.overall)]
        return "\n".join([header, *rows])


def _fmt_ratio(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.2f}"


def _fmt_latency(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.0f}s"


def _markdown_row(m: TypologyMetrics) -> str:
    return (
        f"| {m.typology} | {m.alerts} | {m.true_positives} | {m.false_positives} "
        f"| {_fmt_ratio(m.precision)} | {m.rings_detected}/{m.rings_total} "
        f"| {_fmt_ratio(m.recall)} | {_fmt_latency(m.mean_latency_s)} "
        f"| {_fmt_latency(m.median_latency_s)} |"
    )


def _metrics_for(
    typology: str, alerts: list[Alert], ground_truth: list[RingGroundTruth]
) -> TypologyMetrics:
    ring_tx: dict[str, set[str]] = {gt.ring_id: set(gt.tx_ids) for gt in ground_truth}
    all_fraud_tx = set().union(*ring_tx.values()) if ring_tx else set()

    true_positives = 0
    latencies: list[float] = []
    detected: set[str] = set()
    # earliest matching alert per ring drives the latency number
    first_hit: dict[str, Alert] = {}

    for alert in alerts:
        evidence = {e.tx_id for e in alert.evidence_edges}
        if evidence & all_fraud_tx:
            true_positives += 1
        for gt in ground_truth:
            if evidence & ring_tx[gt.ring_id]:
                detected.add(gt.ring_id)
                current = first_hit.get(gt.ring_id)
                if current is None or alert.fired_at < current.fired_at:
                    first_hit[gt.ring_id] = alert

    for gt in ground_truth:
        hit = first_hit.get(gt.ring_id)
        if hit is not None:
            latencies.append((hit.fired_at - gt.last_hop_time).total_seconds())

    n_alerts = len(alerts)
    n_rings = len(ground_truth)
    return TypologyMetrics(
        typology=typology,
        alerts=n_alerts,
        true_positives=true_positives,
        false_positives=n_alerts - true_positives,
        rings_detected=len(detected),
        rings_total=n_rings,
        precision=(true_positives / n_alerts) if n_alerts else None,
        recall=(len(detected) / n_rings) if n_rings else 0.0,
        mean_latency_s=statistics.fmean(latencies) if latencies else None,
        median_latency_s=statistics.median(latencies) if latencies else None,
    )


def score_alerts(alerts: list[Alert], ground_truth: list[RingGroundTruth]) -> ScoreReport:
    typologies = sorted({a.typology for a in alerts})
    per_typology = tuple(
        _metrics_for(name, [a for a in alerts if a.typology == name], ground_truth)
        for name in typologies
    )
    overall = _metrics_for("overall", alerts, ground_truth)
    return ScoreReport(per_typology=per_typology, overall=overall)
