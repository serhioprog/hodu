"""
Bake-off runner: evaluate one architecture on train or holdout, write
results to results/bake_off_{train,holdout}.json keyed by architecture name.

Usage:
    cd ~/hodu
    venv/Scripts/python.exe -m src.services.engine_v2.bake_off \
        --db-host localhost --architecture rule_based --split train

When an architecture is added (Phase 2 LLM, Phase 3 ML, Phase 4 hybrid),
register it in `ARCHITECTURES` and run with --architecture <name>.

Holdout warning: --split holdout prints a SINGLE-SHOT warning (spec §5.4
forbids tuning on holdout).
"""
from __future__ import annotations

import os
import sys


def _bootstrap_db_host_from_argv() -> None:
    """--db-host argv override before src.* imports."""
    args = sys.argv[1:]
    for i, a in enumerate(args):
        if a == "--db-host" and i + 1 < len(args):
            os.environ["POSTGRES_HOST"] = args[i + 1]
            return
        if a.startswith("--db-host="):
            os.environ["POSTGRES_HOST"] = a.split("=", 1)[1]
            return


_bootstrap_db_host_from_argv()

import argparse  # noqa: E402
import asyncio   # noqa: E402

from .engine_db import async_session_maker  # noqa: E402
from .evaluation import (  # noqa: E402
    evaluate,
    load_holdout,
    load_train,
    print_metrics_summary,
    write_results,
)
from .scoring.rule_based import score_pair as score_rule_based   # noqa: E402
from .scoring.llm_tier import score_pair as score_llm_tier       # noqa: E402
from .scoring.classical_ml import (                              # noqa: E402
    prepare as prepare_classical_ml,
    score_pair as score_classical_ml,
)

ARCHITECTURES: dict[str, dict] = {
    "rule_based":   {"score": score_rule_based,   "prepare": None},
    "llm_tier":     {"score": score_llm_tier,     "prepare": None},
    "classical_ml": {"score": score_classical_ml, "prepare": prepare_classical_ml},
    # "hybrid": ... (Phase 4 — design only this session)
}


async def main(arch: str, split: str) -> int:
    if arch not in ARCHITECTURES:
        print(f"unknown architecture: {arch}; choices: {sorted(ARCHITECTURES)}",
              file=sys.stderr)
        return 1

    if split == "holdout":
        print(
            "WARNING: running on HOLDOUT. Spec §5.4: single-shot only, "
            "no tuning afterward.\n",
            file=sys.stderr,
        )
        pairs = load_holdout()
    else:
        pairs = load_train()

    cfg = ARCHITECTURES[arch]
    async with async_session_maker() as session:
        if cfg["prepare"] is not None:
            await cfg["prepare"](session)
        m = await evaluate(session, cfg["score"], pairs, architecture=arch)

    print_metrics_summary(m)

    out_file = "bake_off_train.json" if split == "train" else "bake_off_holdout.json"
    path = write_results(out_file, arch, m)
    print(f"\nWrote: {path}")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="bake_off")
    parser.add_argument("--db-host", help="Override POSTGRES_HOST")
    parser.add_argument(
        "--architecture", required=True, choices=sorted(ARCHITECTURES.keys()),
    )
    parser.add_argument("--split", choices=["train", "holdout"], default="train")
    args = parser.parse_args()
    sys.exit(asyncio.run(main(args.architecture, args.split)))
