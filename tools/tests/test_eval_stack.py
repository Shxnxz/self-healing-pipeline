"""The evaluation stack (docker-compose.eval.yml) must not touch the main stack's state.

Reads the merged configuration with `docker compose config`, so it checks what Compose
really runs (the `!override` lists included). Skipped where the Compose CLI is missing.
"""
import json
import os
import shutil
import subprocess

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MAIN_STATE = [os.path.join(REPO, d) for d in ("delta", "checkpoints", "incidents")]
EVAL_ROOT = os.path.join(REPO, "eval")


def _config(*files):
    if shutil.which("docker") is None:
        pytest.skip("docker compose is not installed")
    args = ["docker", "compose", "--project-directory", REPO]
    for f in files:
        args += ["-f", os.path.join(REPO, f)]
    args += ["--profile", "tools", "config", "--format", "json"]
    proc = subprocess.run(args, capture_output=True, text=True, cwd=REPO)
    if proc.returncode != 0 and "is not a docker command" in proc.stderr:
        pytest.skip("docker compose is not installed")
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


@pytest.fixture(scope="module")
def main():
    return _config("docker-compose.yml")


@pytest.fixture(scope="module")
def evaluation():
    return _config("docker-compose.yml", "docker-compose.eval.yml")


def _binds(service):
    return [v for v in service.get("volumes", []) if v.get("type") == "bind"]


def _under(path, root):
    path, root = os.path.realpath(path), os.path.realpath(root)
    return path == root or path.startswith(root + os.sep)


def test_no_eval_service_mounts_the_main_tables(evaluation):
    leaks = [(name, v["source"], v["target"])
             for name, svc in evaluation["services"].items() for v in _binds(svc)
             if any(_under(v["source"], d) for d in MAIN_STATE)]
    assert not leaks, f"evaluation services mount the main stack's state: {leaks}"


def test_every_table_mount_comes_from_eval(evaluation):
    for name, svc in evaluation["services"].items():
        for v in _binds(svc):
            if v["target"].startswith(("/data/delta", "/data/checkpoints", "/data/incidents")):
                assert _under(v["source"], EVAL_ROOT), (name, v["source"], v["target"])


def test_eval_agent_mounts_every_table_the_main_agent_mounts(main, evaluation):
    """A table later mounted for the main agent must get its eval twin, or the eval
    agent cannot write it (and before !override, it would have used the main one)."""
    targets = lambda svc: {v["target"]: v.get("read_only", False) for v in _binds(svc)
                           if v["target"].startswith("/data/delta")}
    main_agent = targets(main["services"]["recovery-agent"])
    eval_agent = targets(evaluation["services"]["recovery-agent"])
    missing = {t: ro for t, ro in main_agent.items() if eval_agent.get(t) != ro}
    assert not missing, f"docker-compose.eval.yml recovery-agent lacks: {missing}"


def test_separate_project_names_and_ports(main, evaluation):
    assert evaluation["name"] != main["name"]
    for name, svc in evaluation["services"].items():
        if "container_name" in svc:
            assert svc["container_name"].startswith("eval-"), name
    published = lambda cfg: {p.get("published") for s in cfg["services"].values() for p in s.get("ports", [])}
    assert not (published(main) & published(evaluation))


def test_only_the_eval_tools_may_inject(main, evaluation):
    assert main["services"]["tools"]["environment"]["PIPELINE_STACK"] == "main"
    assert evaluation["services"]["tools"]["environment"]["PIPELINE_STACK"] == "evaluation"
