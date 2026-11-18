"""The evaluation stack, driven from the host with `docker compose` (standard library only).

Every command runs in a container of the evaluation stack (docker-compose.eval.yml):
the fault injector and its `status` in the `tools` service (Spark), the workflow
export and the engineer's decisions in `recovery-agent`. Nothing here touches the
main stack: the compose project, Kafka, Delta folder and Neo4j are the evaluation's.
"""
import json
import os
import subprocess
import sys
from typing import List, Optional

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COMPOSE = ["docker", "compose", "-f", "docker-compose.yml", "-f", "docker-compose.eval.yml"]


class StackError(RuntimeError):
    pass


def compose(args: List[str], timeout: float = 900, check: bool = True, echo: bool = False) -> subprocess.CompletedProcess:
    cmd = COMPOSE + args
    if echo:
        print("$ " + " ".join(cmd), flush=True)
    try:
        done = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True, encoding="utf-8",
                              errors="replace", timeout=timeout)
    except FileNotFoundError as exc:
        raise StackError("docker is not installed or not on PATH") from exc
    except subprocess.TimeoutExpired as exc:
        raise StackError(f"timed out after {timeout:.0f}s: {' '.join(args)}") from exc
    if check and done.returncode != 0:
        tail = "\n".join((done.stderr or done.stdout or "").strip().splitlines()[-15:])
        raise StackError(f"`{' '.join(args)}` exited {done.returncode}:\n{tail}")
    return done


def extract_json(text: str):
    """The JSON document a command printed, skipping log lines printed before it
    (Spark and Ivy write some to stdout)."""
    decoder = json.JSONDecoder()
    lines = text.splitlines(keepends=True)
    offset = 0
    for line in lines:
        if line.lstrip().startswith(("{", "[")):
            try:
                value, _ = decoder.raw_decode(text[offset + len(line) - len(line.lstrip()):])
                return value
            except ValueError:
                pass
        offset += len(line)
    raise StackError("no JSON in the command's output:\n" + "\n".join(text.strip().splitlines()[-10:]))


def tools(args: List[str], timeout: float = 900) -> subprocess.CompletedProcess:
    return compose(["run", "--rm", "-T", "tools", "python"] + args, timeout=timeout)


def agent(args: List[str], timeout: float = 300, check: bool = True) -> subprocess.CompletedProcess:
    return compose(["exec", "-T", "recovery-agent", "python"] + args, timeout=timeout, check=check)


def check_running() -> None:
    done = compose(["ps", "--status", "running", "--services"], timeout=60, check=False)
    running = set((done.stdout or "").split())
    needed = {"kafka", "bronze-silver1-job", "silver2-gold-job", "lookup-scanner", "deterministic-lookup",
              "recovery-agent", "neo4j"}
    missing = sorted(needed - running)
    if missing:
        raise StackError(
            f"the evaluation stack is not running ({', '.join(missing)} not up). Start it with\n"
            f"  {' '.join(COMPOSE)} up -d --build\nand wait until the producer has exited "
            "(docs/evaluation.md).")


def inject(scenario: str, run_id: str, seed: int = 7) -> str:
    done = tools(["-m", "tools.fault_injector", "inject", scenario, "--run-id", run_id, "--seed", str(seed)],
                 timeout=1800)
    return done.stdout


def run_exists(run_id: str) -> bool:
    """Was this run id injected already (its ground truth is in fault_injections)?"""
    done = compose(["run", "--rm", "-T", "tools", "python", "-m", "tools.fault_injector", "status", run_id,
                    "--json"], check=False)
    if done.returncode == 0:
        return True
    if "no run" in (done.stderr or "") or "no fault_injections table" in (done.stderr or ""):
        return False
    raise StackError(f"could not check run {run_id}:\n" + "\n".join((done.stderr or "").strip().splitlines()[-10:]))


def status(run_id: str) -> dict:
    return extract_json(tools(["-m", "tools.fault_injector", "status", run_id, "--json"]).stdout)


def workflows() -> List[dict]:
    return extract_json(agent(["-m", "agent.workflows", "--json"]).stdout)


def review(args: List[str]) -> subprocess.CompletedProcess:
    return agent(["-m", "agent.review"] + args, check=False)


def gold_metrics() -> str:
    return tools(["spark-jobs/gold_metrics.py"]).stdout


def say(text: str = "", end: str = "\n") -> None:
    print(text, end=end, flush=True)
    sys.stdout.flush()


def env_flag(name: str) -> Optional[str]:
    return os.environ.get(name)
