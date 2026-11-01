"""Run from the repo root:  python -m pytest lookup/tests"""
import sys
from pathlib import Path

LOOKUP = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LOOKUP.parent))  # agent.memory.incidents
sys.path.insert(0, str(LOOKUP))         # flat modules, as in the container
