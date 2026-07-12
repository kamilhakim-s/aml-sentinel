"""Command-line interface for AML Sentinel."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import timedelta
from pathlib import Path

from aml_sentinel import __version__
from aml_sentinel.detect import DetectionService, DetectorSink
from aml_sentinel.replay import (
    CollectingSink,
    SynthesisConfig,
    load_dataset,
    replay,
    synthesize,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="aml-sentinel")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    rp = sub.add_parser("replay", help="Stream the generated dataset in event-time order.")
    rp.add_argument("--data-dir", type=Path, default=Path("data"), help="Dataset directory.")
    rp.add_argument(
        "--speed",
        type=float,
        default=0,
        help="Simulated seconds per wall second (3600 = 1h/s). <=0 emits at full speed.",
    )
    rp.add_argument(
        "--direct",
        action="store_true",
        help="Bypass Kafka and emit in-process. (Kafka transport lands in a later phase.)",
    )
    rp.add_argument("--seed", type=int, default=42, help="RNG seed for time synthesis.")
    rp.add_argument("--horizon-days", type=float, default=30, help="Simulated horizon length.")
    rp.add_argument(
        "--ground-truth-out",
        type=Path,
        default=None,
        help="Where to write per-ring ground truth JSON (default: <data-dir>/ground_truth.json).",
    )
    dt = sub.add_parser("detect", help="Replay the dataset straight into the detection service.")
    dt.add_argument("--data-dir", type=Path, default=Path("data"), help="Dataset directory.")
    dt.add_argument("--seed", type=int, default=42, help="RNG seed for time synthesis.")
    dt.add_argument("--horizon-days", type=float, default=30, help="Simulated horizon length.")
    dt.add_argument(
        "--window-hours", type=float, default=72, help="Rolling graph window (simulated hours)."
    )
    dt.add_argument(
        "--alerts-out",
        type=Path,
        default=None,
        help="Where to write fired alerts JSON (default: <data-dir>/alerts.json).",
    )
    return parser


def _run_detect(args: argparse.Namespace) -> int:
    dataset = load_dataset(args.data_dir)
    config = SynthesisConfig(horizon=timedelta(days=args.horizon_days), seed=args.seed)
    stream, _ = synthesize(dataset, config)

    service = DetectionService(window=timedelta(hours=args.window_hours))
    stats = asyncio.run(replay(stream, DetectorSink(service), speed=0))
    detection = service.stats()

    alerts_path: Path = args.alerts_out or args.data_dir / "alerts.json"
    alerts_path.write_text(json.dumps([a.to_dict() for a in service.alerts], indent=2))

    rate = stats.count / stats.wall_seconds if stats.wall_seconds > 0 else float("inf")
    print(f"processed {stats.count} transactions in {stats.wall_seconds:.2f}s ({rate:,.0f} tx/s)")
    print(
        f"per-tx latency: p50 {detection.latency_p50_us:.0f}us, "
        f"p99 {detection.latency_p99_us:.0f}us, max {detection.latency_max_us:.0f}us"
    )
    total = len(service.alerts)
    by_typology = (
        ", ".join(f"{name}={count}" for name, count in sorted(detection.alerts_by_typology.items()))
        or "none"
    )
    print(f"alerts fired: {total} ({by_typology}) -> {alerts_path}")
    return 0


def _run_replay(args: argparse.Namespace) -> int:
    if not args.direct:
        print("only --direct mode is implemented so far; pass --direct", file=sys.stderr)
        return 2

    dataset = load_dataset(args.data_dir)
    config = SynthesisConfig(horizon=timedelta(days=args.horizon_days), seed=args.seed)
    stream, ground_truth = synthesize(dataset, config)

    gt_path: Path = args.ground_truth_out or args.data_dir / "ground_truth.json"
    gt_path.write_text(json.dumps([gt.to_dict() for gt in ground_truth], indent=2))

    sink = CollectingSink()
    stats = asyncio.run(replay(stream, sink, speed=args.speed))

    sim_days = stats.simulated_span_seconds / 86400
    rate = stats.count / stats.wall_seconds if stats.wall_seconds > 0 else float("inf")
    print(
        f"replayed {stats.count} transactions covering {sim_days:.1f} simulated days "
        f"in {stats.wall_seconds:.2f}s wall ({rate:,.0f} tx/s)"
    )
    print(f"rings in ground truth: {len(ground_truth)} -> {gt_path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "replay":
        return _run_replay(args)
    if args.command == "detect":
        return _run_detect(args)
    raise AssertionError(f"unhandled command {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
