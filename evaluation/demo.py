"""The demo: the spec's "Jule" example, end to end, on the evaluation stack.

  python -m evaluation demo            # pauses before each step (press Enter)
  python -m evaluation demo --auto     # no pauses, e.g. to record it

  1. "Jule" arrives in 12 June records (3 micro-batches). It is one edit from JUNE and
     JULY, so fuzzy matching refuses to guess and Silver 2 quarantines the records.
  2. The lookup finds no past incident; the agent asks the reasoning model (when one is
     running) and the policy gate decides; without a confident, checked proposal the
     case goes to an engineer with the whole context.
  3. The engineer decides (here: JUNE, from the records' crash_date). Remediate stages
     the fix, Validate replays the records through Silver 2's own code and releases
     them, Learn stores the incident in Neo4j and promotes JULE -> JUNE.
  4. "Jule" arrives again: Silver 2 maps it itself. No quarantine, no model, no engineer.
  5. Gold has the healed records; the Gold metrics leave injected copies out.

Each step prints the command it runs, so the same story can be told by hand from
docs/demo.md.
"""
import time
from datetime import datetime, timezone

from evaluation import metrics, oracle, stack

ENGINEER = "demo-engineer"


def _pause(auto: bool, text: str) -> None:
    stack.say("\n" + "=" * 78 + f"\n{text}\n" + "=" * 78)
    if not auto:
        input("Press Enter to run it... ")


def _show(args, runner=stack.tools) -> str:
    stack.say("$ " + " ".join(stack.COMPOSE + (["run", "--rm", "tools", "python"] if runner is stack.tools
                                                else ["exec", "recovery-agent", "python"]) + args))
    out = runner(args).stdout
    lines = [l for l in out.splitlines() if not l.startswith((":: ", "\t", "Ivy", "downloading", "found "))]
    stack.say("\n".join(lines[-40:]))
    return out


def _wait(run_id: str, done, what: str, timeout: float = 900, poll: float = 20, on_tick=None) -> dict:
    stack.say(f"... waiting until {what}")
    started = time.monotonic()
    status = {}
    while time.monotonic() - started < timeout:
        status = stack.status(run_id)
        records = status.get("records") or []
        if records and done(records):
            return status
        if on_tick and records:
            on_tick(records)
        stack.say(f"   [{int(time.monotonic() - started)}s] {status.get('summary', {}).get('outcomes')}")
        time.sleep(poll)
    stack.say(f"   still not there after {timeout:.0f}s; carrying on")
    return status


def _case(records):
    sigs = {metrics.record_signature(r) for r in records}
    for _ in range(45):
        waiting = [w for w in stack.workflows() if w.get("waiting") and metrics.workflow_signature(w) in sigs]
        if waiting:
            return waiting[0]
        time.sleep(10)
    return None


def _answer_the_rest(answered: set):
    """Answer the run's other open cases the same way (until Task 109 attaches later
    micro-batches to the open workflow, each batch of "Jule" is a case of its own)."""
    def tick(records):
        expected = metrics.expected_by_signature(records)
        sigs = set(expected)
        for wf in stack.workflows():
            key = (wf["failure_id"], wf.get("escalation_rounds"))
            if not wf.get("waiting") or key in answered or metrics.workflow_signature(wf) not in sigs:
                continue
            decision = oracle.decide(wf, expected)
            if decision:
                args = [a if a != oracle.ENGINEER else ENGINEER for a in decision["command"]]
                done = stack.review(args)
                answered.add(key)
                stack.say(f"   engineer: {wf['failure_id']} (a later micro-batch of the same value) -> "
                          f"{decision['why']} (exit {done.returncode})")
    return tick


def main(auto: bool = False, scenario: str = "jule") -> int:
    stack.check_running()
    stamp = datetime.now(timezone.utc).strftime("%H%M%S")
    first, second = f"demo-{stamp}-first", f"demo-{stamp}-again"

    _pause(auto, "1. Send 12 crash records whose crash_month says 'Jule' (they happened in June).")
    stack.say(stack.inject(scenario, first).strip())
    status = _wait(first, lambda rs: all(r["outcome"] not in metrics.NOT_SETTLED for r in rs),
                   "Silver 2 has seen every record")
    _show(["-m", "tools.fault_injector", "status", first, "--records"])
    stack.say("'Jule' is one edit from JUNE and from JULY: fuzzy matching will not guess, so the records "
              "wait in quarantine instead of reaching Silver 2 with a wrong month.")

    _pause(auto, "2. The recovery agent picks the failure up: lookup, diagnosis, policy gate.")
    case = _case(status.get("records") or [])
    if case is None:
        stack.say("No case is waiting for an engineer: the agent may have healed it on its own "
                  "(a confident, checked diagnosis), see the status below.")
    else:
        _show(["-m", "agent.review", "list"], runner=stack.agent)
        _show(["-m", "agent.review", "show", case["failure_id"]], runner=stack.agent)

        _pause(auto, "3. The engineer decides from the records' crash_date (June, so 'Jule' means JUNE).")
        decision = oracle.decide(case, metrics.expected_by_signature(status.get("records") or []))
        args = (decision or {}).get("command") or ["fix", case["failure_id"], "JUNE"]
        args = [a if a != oracle.ENGINEER else ENGINEER for a in args]
        _show(["-m", "agent.review"] + args, runner=stack.agent)
    answered = {(case["failure_id"], case.get("escalation_rounds"))} if case else set()
    status = _wait(first, lambda rs: all(r["outcome"] != "QUARANTINED" for r in rs),
                   "Validate has released the records into Silver 2", on_tick=_answer_the_rest(answered))
    _show(["-m", "tools.fault_injector", "status", first, "--records"])
    _show(["-m", "agent.workflows"], runner=stack.agent)

    _pause(auto, "4. 'Jule' arrives again in 12 new records.")
    stack.say(stack.inject(scenario, second).strip())
    _wait(second, lambda rs: all(r["outcome"] not in metrics.NOT_SETTLED for r in rs), "Silver 2 has seen them")
    _show(["-m", "tools.fault_injector", "status", second, "--records"])
    stack.say("Outcome CLEAN: Silver 2 mapped JULE -> JUNE from ref_canonical_mappings. No quarantine, "
              "no model, no engineer: the system only had to solve it once.")

    _pause(auto, "5. Gold: the healed records are there; the business metrics leave injected copies out.")
    _show(["-m", "evaluation.peek", "gold", "--where", f"crash_record_id LIKE 'FAULT-{first}-%'",
           "--columns", "crash_record_id,crash_date,crash_month,is_injected", "--limit", "5"])
    stack.say("$ " + " ".join(stack.COMPOSE + ["run", "--rm", "tools", "python", "spark-jobs/gold_metrics.py"]))
    stack.say("\n".join(l for l in stack.gold_metrics().splitlines() if l.startswith("gold_")))
    _show(["-m", "evaluation.peek", "gold_daily_summary", "--order", "crash_date", "--limit", "10"])
    stack.say("\nDemo done. Runs: " + first + ", " + second)
    return 0
