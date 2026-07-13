"""Command-line interface for AML Sentinel."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from datetime import timedelta
from pathlib import Path

from aml_sentinel import __version__
from aml_sentinel.detect import DetectionService, DetectorSink
from aml_sentinel.detect.poster import AlertPoster, PostingDetectorSink
from aml_sentinel.replay import (
    CollectingSink,
    ReplayStats,
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
        "--transport",
        choices=["direct", "kafka"],
        default="direct",
        help="Where to emit: in-process (direct) or a Kafka/Redpanda topic.",
    )
    rp.add_argument(
        "--direct",
        action="store_true",
        help="Alias for --transport direct (kept for compatibility).",
    )
    rp.add_argument("--brokers", default="localhost:9092", help="Kafka bootstrap servers.")
    rp.add_argument("--topic", default="transactions", help="Kafka topic to produce to.")
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
    dt.add_argument(
        "--speed",
        type=float,
        default=0,
        help="Simulated seconds per wall second while detecting. <=0 = full speed.",
    )
    dt.add_argument(
        "--api-url",
        default=None,
        help="Case API base URL; when set, alerts are POSTed there as they fire.",
    )

    cn = sub.add_parser(
        "consume", help="Consume the transactions topic into the detector (compose demo)."
    )
    cn.add_argument("--brokers", default="localhost:9092", help="Kafka bootstrap servers.")
    cn.add_argument("--topic", default="transactions", help="Kafka topic to consume.")
    cn.add_argument("--api-url", default="http://127.0.0.1:8000", help="Case API base URL.")
    cn.add_argument(
        "--window-hours", type=float, default=72, help="Rolling graph window (simulated hours)."
    )
    cn.add_argument("--group", default="aml-sentinel-detector", help="Consumer group id.")

    sc = sub.add_parser(
        "score", help="Run the detection pipeline and score alerts against ground truth."
    )
    sc.add_argument("--data-dir", type=Path, default=Path("data"), help="Dataset directory.")
    sc.add_argument("--seed", type=int, default=42, help="RNG seed for time synthesis.")
    sc.add_argument("--horizon-days", type=float, default=30, help="Simulated horizon length.")
    sc.add_argument(
        "--window-hours", type=float, default=72, help="Rolling graph window (simulated hours)."
    )
    sc.add_argument(
        "--report-out",
        type=Path,
        default=None,
        help="Also write the report (markdown + .json sibling) to this path.",
    )

    sv = sub.add_parser("serve", help="Run the case API (FastAPI/uvicorn).")
    sv.add_argument("--db", default="sqlite:///cases.db", help="SQLAlchemy database URL.")
    sv.add_argument("--host", default="127.0.0.1")
    sv.add_argument("--port", type=int, default=8000)
    return parser


def _run_detect(args: argparse.Namespace) -> int:
    dataset = load_dataset(args.data_dir)
    config = SynthesisConfig(horizon=timedelta(days=args.horizon_days), seed=args.seed)
    stream, _ = synthesize(dataset, config)

    service = DetectionService(window=timedelta(hours=args.window_hours))
    sink: DetectorSink | PostingDetectorSink
    if args.api_url is not None:
        sink = PostingDetectorSink(service, AlertPoster(args.api_url))
    else:
        sink = DetectorSink(service)
    stats = asyncio.run(replay(stream, sink, speed=args.speed))
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


def _run_score(args: argparse.Namespace) -> int:
    from aml_sentinel.scoring import score_alerts

    dataset = load_dataset(args.data_dir)
    config = SynthesisConfig(horizon=timedelta(days=args.horizon_days), seed=args.seed)
    stream, ground_truth = synthesize(dataset, config)

    service = DetectionService(window=timedelta(hours=args.window_hours))
    asyncio.run(replay(stream, DetectorSink(service), speed=0))

    report = score_alerts(service.alerts, ground_truth)
    print(report.to_markdown())

    if args.report_out is not None:
        args.report_out.write_text(report.to_markdown() + "\n")
        json_path = args.report_out.with_suffix(".json")
        json_path.write_text(json.dumps(report.to_dict(), indent=2))
        print(f"\nreport written to {args.report_out} and {json_path}")
    return 0


def _run_consume(args: argparse.Namespace) -> int:
    from aml_sentinel.detect.consumer import consume_transactions

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    service = asyncio.run(
        consume_transactions(
            args.brokers,
            args.topic,
            args.api_url,
            window=timedelta(hours=args.window_hours),
            group_id=args.group,
        )
    )
    detection = service.stats()
    print(f"consumed {detection.transactions} transactions, {len(service.alerts)} alerts")
    return 0


def _run_serve(args: argparse.Namespace) -> int:
    import uvicorn

    from aml_sentinel.api import create_app, make_engine

    app = create_app(make_engine(args.db))
    uvicorn.run(app, host=args.host, port=args.port)
    return 0


def _run_replay(args: argparse.Namespace) -> int:
    transport = "direct" if args.direct else args.transport

    dataset = load_dataset(args.data_dir)
    config = SynthesisConfig(horizon=timedelta(days=args.horizon_days), seed=args.seed)
    stream, ground_truth = synthesize(dataset, config)

    gt_path: Path = args.ground_truth_out or args.data_dir / "ground_truth.json"
    gt_path.write_text(json.dumps([gt.to_dict() for gt in ground_truth], indent=2))

    if transport == "kafka":
        from aml_sentinel.replay.kafka import KafkaSink

        async def _produce() -> ReplayStats:
            async with KafkaSink(args.brokers, args.topic) as sink:
                return await replay(stream, sink, speed=args.speed)

        stats = asyncio.run(_produce())
    else:
        stats = asyncio.run(replay(stream, CollectingSink(), speed=args.speed))

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
    if args.command == "score":
        return _run_score(args)
    if args.command == "consume":
        return _run_consume(args)
    if args.command == "serve":
        return _run_serve(args)
    raise AssertionError(f"unhandled command {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
