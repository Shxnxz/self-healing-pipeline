"""python -m evaluation: the evaluation experiment, its report, and the demo.

  python -m evaluation run                      # 2 rounds of data/faults/evaluation.json, oracle engineer
  python -m evaluation run --scenario jule --rounds 3 --label jule-3
  python -m evaluation run --engineer none      # nobody answers: autonomous behaviour only
  python -m evaluation report eval/results/<label>   # recompute report.md from the saved JSON
  python -m evaluation demo [--auto]            # the Jule story, step by step

Runs on the host, from the repo root, with Python 3.8+ and Docker (standard library
only); every command it issues runs in the evaluation stack (docker-compose.eval.yml),
which must be up. Results go to eval/results/<label>/ (git-ignored): round-N.json (raw
status and workflows), metrics.json and report.md. See docs/evaluation.md.
"""
import argparse
import os
import re
import sys
from datetime import datetime, timezone

from evaluation import run as runner
from evaluation import stack


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m evaluation", description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    p_run = sub.add_parser("run", help="inject, let the system work, measure, report")
    p_run.add_argument("--scenario", default="evaluation", help="data/faults/<name>.json (default evaluation)")
    p_run.add_argument("--rounds", type=int, default=2, help="times the scenario is injected (default 2)")
    p_run.add_argument("--label", help="results folder and run id prefix (default eval-<time>)")
    p_run.add_argument("--engineer", choices=["oracle", "none"], default="oracle",
                       help="who answers escalated cases (default: the ground-truth oracle)")
    p_run.add_argument("--no-promote", action="store_true", help="engineer fixes heal only, never promote")
    p_run.add_argument("--timeout", type=float, default=1500, help="seconds to wait per round (default 1500)")
    p_run.add_argument("--poll", type=float, default=15, help="seconds between checks (default 15)")
    p_run.add_argument("--quiet", type=float, default=60,
                       help="seconds nothing may move before a round counts as done (default 60)")
    p_run.add_argument("--seed", type=int, default=7)
    p_run.add_argument("--out", default=runner.DEFAULT_OUT)
    p_rep = sub.add_parser("report", help="recompute the report from a results folder")
    p_rep.add_argument("folder")
    p_demo = sub.add_parser("demo", help="the Jule story, end to end")
    p_demo.add_argument("--auto", action="store_true", help="no pauses")
    p_demo.add_argument("--scenario", default="jule")
    args = parser.parse_args(argv)

    try:
        if args.command == "report":
            rounds = runner.load(args.folder)
            path = runner.write(args.folder, rounds, f"Evaluation: {os.path.basename(args.folder.rstrip('/'))}",
                                runner.context_lines(rounds))
            print(f"report: {path}")
            return 0
        if args.command == "demo":
            from evaluation import demo

            return demo.main(args.auto, args.scenario)

        label = args.label or "eval-" + datetime.now(timezone.utc).strftime("%Y%m%dt%H%M")
        if not re.match(r"^[a-z0-9][a-z0-9-]{0,50}$", label):
            parser.error("--label: lower-case letters, digits and '-' only (it becomes the run ids)")
        stack.check_running()
        out_dir = os.path.join(args.out, label)
        rounds = []
        for i in range(1, args.rounds + 1):
            rounds.append(runner.run_round(args.scenario, f"{label}-r{i}", label, i, args.engineer,
                                           not args.no_promote, args.timeout, args.poll, args.quiet, args.seed))
            runner.write(out_dir, rounds, f"Evaluation: {label}", runner.context_lines(rounds))
        path = runner.write(out_dir, rounds, f"Evaluation: {label}", runner.context_lines(rounds))
        print(f"\nreport: {path}")
        return 0
    except stack.StackError as exc:
        print(f"[evaluation] {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("\n[evaluation] stopped", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
