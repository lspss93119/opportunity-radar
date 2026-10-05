from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path

from radar.config import load_config
from radar.history.manual_opportunity import replay_manual_opportunity


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    offset = parsed.utcoffset()
    if parsed.tzinfo is None or offset is None:
        raise argparse.ArgumentTypeError("timestamp must be timezone-aware")
    if offset.total_seconds() != 0:
        raise argparse.ArgumentTypeError("timestamp must be UTC")
    return parsed.astimezone(UTC)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Replay Manual Opportunity v1 from BBO history")
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--start", type=_timestamp, required=True)
    parser.add_argument("--end", type=_timestamp, required=True)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/tmp/opportunity-radar-manual-v1-replay"),
    )
    args = parser.parse_args(argv)
    result = replay_manual_opportunity(
        data_root=args.data_root,
        config=load_config(args.config),
        start=args.start,
        end=args.end,
        output_dir=args.output_dir,
    )
    import json

    print(json.dumps(result.report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
