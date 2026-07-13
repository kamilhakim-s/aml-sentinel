from datetime import datetime, timedelta

from aml_sentinel.detect import (
    CycleDetection,
    DetectionService,
    HighValueDegreeOutlier,
    Structuring,
)
from aml_sentinel.models import Transaction

T0 = datetime(2024, 1, 1)


def tx(
    tx_id: str,
    src: str,
    dst: str,
    *,
    minutes: float,
    amount: float = 100.0,
    description: str = "test",
) -> Transaction:
    return Transaction(
        tx_id=tx_id,
        src=src,
        dst=dst,
        amount=amount,
        event_time=T0 + timedelta(minutes=minutes),
        description=description,
    )


def cycle_only_service(window_hours: float = 72) -> DetectionService:
    return DetectionService(window=timedelta(hours=window_hours), typologies=[CycleDetection()])


class TestCycleDetection:
    def test_three_hop_cycle_fires_on_closing_edge(self) -> None:
        svc = cycle_only_service()
        assert svc.process(tx("t1", "a", "b", minutes=0, amount=9999)) == []
        assert svc.process(tx("t2", "b", "c", minutes=10, amount=9999)) == []
        alerts = svc.process(tx("t3", "c", "a", minutes=20, amount=9999))
        assert len(alerts) == 1
        alert = alerts[0]
        assert alert.typology == "cycle"
        assert set(alert.accounts) == {"a", "b", "c"}
        assert [e.tx_id for e in alert.evidence_edges] == ["t3", "t1", "t2"]
        assert alert.score == 1.0  # uniform amounts
        assert alert.fired_at == T0 + timedelta(minutes=20)

    def test_no_alert_without_cycle(self) -> None:
        svc = cycle_only_service()
        svc.process(tx("t1", "a", "b", minutes=0))
        svc.process(tx("t2", "b", "c", minutes=1))
        svc.process(tx("t3", "c", "d", minutes=2))
        assert svc.alerts == []

    def test_cycle_broken_by_window_expiry_does_not_fire(self) -> None:
        svc = cycle_only_service(window_hours=1)
        svc.process(tx("t1", "a", "b", minutes=0))
        svc.process(tx("t2", "b", "c", minutes=30))
        # closing edge arrives after t1 fell out of the window
        alerts = svc.process(tx("t3", "c", "a", minutes=90))
        assert alerts == []

    def test_depth_cap_respected(self) -> None:
        rule = CycleDetection(max_depth=3)
        svc = DetectionService(typologies=[rule])
        for i, (s, d) in enumerate([("a", "b"), ("b", "c"), ("c", "d")]):
            svc.process(tx(f"t{i}", s, d, minutes=i))
        # closing edge makes a 4-cycle; cap is 3 -> nothing fires
        assert svc.process(tx("t9", "d", "a", minutes=10)) == []

    def test_two_hop_roundtrip_fires(self) -> None:
        svc = cycle_only_service()
        svc.process(tx("t1", "a", "b", minutes=0))
        alerts = svc.process(tx("t2", "b", "a", minutes=5))
        assert len(alerts) == 1
        assert set(alerts[0].accounts) == {"a", "b"}

    def test_self_loop_ignored(self) -> None:
        svc = cycle_only_service()
        assert svc.process(tx("t1", "a", "a", minutes=0)) == []

    def test_nonuniform_amounts_score_lower(self) -> None:
        svc = cycle_only_service()
        svc.process(tx("t1", "a", "b", minutes=0, amount=100))
        svc.process(tx("t2", "b", "c", minutes=1, amount=9000))
        alerts = svc.process(tx("t3", "c", "a", minutes=2, amount=50))
        assert len(alerts) == 1
        assert alerts[0].score < 0.5


class TestStructuring:
    def make_service(self) -> DetectionService:
        return DetectionService(typologies=[Structuring(min_count=3)])

    def test_fires_at_threshold_within_24h(self) -> None:
        svc = self.make_service()
        svc.process(tx("t1", "m", "x1", minutes=0, amount=9500))
        svc.process(tx("t2", "m", "x2", minutes=60, amount=9999))
        alerts = svc.process(tx("t3", "x3", "m", minutes=120, amount=9200))
        assert [a.typology for a in alerts] == ["structuring"]
        assert "m" in alerts[0].accounts
        assert {e.tx_id for e in alerts[0].evidence_edges} == {"t1", "t2", "t3"}

    def test_does_not_fire_across_24h(self) -> None:
        svc = self.make_service()
        svc.process(tx("t1", "m", "x1", minutes=0, amount=9500))
        svc.process(tx("t2", "m", "x2", minutes=60, amount=9999))
        # third band tx more than 24h after the first
        alerts = svc.process(tx("t3", "x3", "m", minutes=25 * 60, amount=9200))
        assert alerts == []

    def test_amount_outside_band_ignored(self) -> None:
        svc = self.make_service()
        svc.process(tx("t1", "m", "x1", minutes=0, amount=10_000))
        svc.process(tx("t2", "m", "x2", minutes=10, amount=8_999))
        alerts = svc.process(tx("t3", "m", "x3", minutes=20, amount=9_500))
        assert alerts == []

    def test_cooldown_prevents_refiring_every_tx(self) -> None:
        svc = self.make_service()
        for i in range(6):
            svc.process(tx(f"t{i}", "m", f"x{i}", minutes=i * 10, amount=9500))
        structuring = [a for a in svc.alerts if "m" in a.accounts]
        assert len(structuring) == 1


class TestHighValueDegreeOutlier:
    def test_hub_account_triggers(self) -> None:
        rule = HighValueDegreeOutlier(min_amount=5000, z_threshold=3.0, min_population=10)
        svc = DetectionService(typologies=[rule])
        # background: 30 low-degree account pairs
        for i in range(30):
            svc.process(tx(f"bg{i}", f"p{i}", f"q{i}", minutes=i, amount=100))
        # hub receives from many senders, then a high-value hit
        for i in range(25):
            svc.process(tx(f"fan{i}", f"s{i}", "hub", minutes=100 + i, amount=100))
        alerts = svc.process(tx("big", "s99", "hub", minutes=130, amount=9000))
        assert any(a.typology == "high_value_degree_outlier" for a in alerts)
        hit = next(a for a in alerts if a.typology == "high_value_degree_outlier")
        assert hit.accounts == ("hub",)
        assert 0 < len(hit.evidence_edges) <= 20

    def test_low_value_never_fires(self) -> None:
        rule = HighValueDegreeOutlier(min_amount=5000, min_population=1)
        svc = DetectionService(typologies=[rule])
        for i in range(25):
            svc.process(tx(f"fan{i}", f"s{i}", "hub", minutes=i, amount=100))
        assert svc.alerts == []
