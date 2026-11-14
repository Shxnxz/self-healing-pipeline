"""The fault catalog: what the fault injector can break, and what the right outcome is.

Pure Python, no Spark: tools/fault_injector.py takes clean records from the
pipeline, asks this module for a corrupted copy, and records the ground truth next
to it, so an evaluation (Task 111) can say whether the pipeline and the agent did
the right thing with each injected record.

Each kind reproduces one failure mode of the spec (problem statement 2.1 and 2.2,
workflow sections 1 and 3):

  kind               what changes in the record                     where it should end up
  typo               one edit in a closed-vocabulary value          fuzzy-fixed in Silver 2 (IQ6, IQ7)
                     (JUNE -> JNUE)
  ambiguous          a value one edit from two canonical values     quarantine; the agent's fix is
                     (JUNE -> Jule, MAY -> Ma)                      ADD_VARIANT to the true value
  unseen_variant     a real synonym the reference table lacks       quarantine; ADD_VARIANT to the
                     (RAIN -> RAINING, JULY -> JULIO)               true value
  unseen_format      a value no parser reads                        quarantine; no canonical set, so
                     (crash_date -> "22nd of June 2021")            an engineer decides
  unknown_value      a value with no right answer                   quarantine; an engineer decides
                     (weather -> MISTY DARK)
  missing_mandatory  crash_date left empty                          quarantine (MISSING_MANDATORY_VALUE)
  schema_rename      a payload column renamed (crash_date ->        Silver 1 dead letter; an engineer
                     CrashDate)                                     renames, tools.replay re-runs it
  schema_missing     a payload column dropped                       Silver 1 dead letter
  schema_extra       an unexpected payload column added             Silver 1 dead letter
  corrupt_record     the Kafka message is not valid JSON            Silver 1 dead letter (CORRUPT_RECORD)

The true value of a record is what Silver 2 made of the clean original, so it is
never guessed: the injector only corrupts records that reached Silver 2 cleanly.
"""
import json
import random
from typing import Dict, List, Optional

# Where a record should end up (intended_path) and where the pipeline's own code says
# it will (predicted_path, computed by the injector with Silver 1 + Silver 2's code).
CLEAN = "CLEAN"                # exact lookup / parser: straight to Silver 2
FUZZY_FIXED = "FUZZY_FIXED"    # Silver 2's fuzzy match corrected it
QUARANTINED = "QUARANTINED"
DEAD_LETTER = "DEAD_LETTER"    # Silver 1 schema guard parked it

# Closed-vocabulary columns: the reference table has canonical values for them, so the
# agent's ADD_VARIANT can fix them (agent/actions.py).
VOCABULARY_COLUMNS = ("weather_condition", "lighting_condition", "street_direction", "crash_month",
                      "crash_day_of_week", "hit_and_run_i", "damage", "num_units")
FUZZY_COLUMNS = ("weather_condition", "lighting_condition", "street_direction", "crash_month",
                 "crash_day_of_week")

# One edit away from two canonical values, or too short to tell: fuzzy matching refuses.
AMBIGUOUS: Dict[str, Dict[str, List[str]]] = {
    "crash_month": {
        "JUNE": ["Jule", "Juny", "Ju"],
        "JULY": ["Jule", "Juny", "Ju"],
        "MARCH": ["Ma", "Mary"],
        "MAY": ["Ma", "Mary"],
    },
    "crash_day_of_week": {
        "TUESDAY": ["T"],
        "THURSDAY": ["T"],
        "SATURDAY": ["S"],
        "SUNDAY": ["S"],
    },
}

# Real ways to write the value that the reference table does not know and that are
# too far from every anchor for the fuzzy match.
UNSEEN_VARIANTS: Dict[str, Dict[str, List[str]]] = {
    "weather_condition": {
        "RAIN": ["RAINING", "RAINFALL", "SHOWERS"],
        "SNOW": ["SNOWING", "SNOWFALL", "BLIZZARD"],
        "CLEAR": ["SUNNY", "FAIR"],
        "CLOUDY": ["OVERCAST SKIES", "GREY SKY"],
        "FOG": ["FOGGY", "MISTY"],
    },
    "lighting_condition": {
        "DAYLIGHT": ["DAYTIME", "BROAD DAYLIGHT"],
        "DARKNESS": ["NIGHTTIME", "PITCH DARK"],
        "DARKNESS, LIGHTED ROAD": ["STREETLIGHTS ON", "LIT ROAD AT NIGHT"],
        "DUSK": ["SUNSET", "TWILIGHT"],
        "DAWN": ["SUNRISE", "DAYBREAK"],
    },
    "crash_month": {
        "JANUARY": ["Enero"], "FEBRUARY": ["Febrero"], "MARCH": ["Marzo"], "APRIL": ["Apr."],
        "MAY": ["Mayo"], "JUNE": ["Junio"], "JULY": ["Julio"], "AUGUST": ["Agosto"],
        "SEPTEMBER": ["Sept"], "OCTOBER": ["Octubre"], "NOVEMBER": ["Noviembre"], "DECEMBER": ["Diciembre"],
    },
    "crash_day_of_week": {
        "MONDAY": ["Lunes"], "TUESDAY": ["Tues"], "WEDNESDAY": ["Weds"], "THURSDAY": ["Thurs"],
        "FRIDAY": ["Viernes"], "SATURDAY": ["Sabado"], "SUNDAY": ["Domingo"],
    },
    "street_direction": {
        "NORTH": ["NORTHBOUND", "NB"], "SOUTH": ["SOUTHBOUND", "SB"],
        "EAST": ["EASTBOUND", "EB"], "WEST": ["WESTBOUND", "WB"],
    },
    "damage": {
        "OVER $1,500": ["MORE THAN $1500"],
        "$501 - $1,500": ["$501 TO $1500"],
        "$500 OR LESS": ["UNDER $500"],
    },
    "hit_and_run_i": {"TRUE": ["AFFIRMATIVE"], "FALSE": ["NEGATIVE"]},
}

# Values with no correct canonical value at all.
UNKNOWN_VALUES: Dict[str, List[str]] = {
    "weather_condition": ["MISTY DARK", "VOLCANIC ASH", "SOLAR ECLIPSE"],
    "lighting_condition": ["STROBE LIGHTS", "SOLAR ECLIPSE"],
    "damage": ["PRICELESS"],
}

_MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September",
           "October", "November", "December"]
_NUMBER_WORDS = {5: "five", 10: "ten", 15: "fifteen", 20: "twenty", 25: "twenty-five", 30: "thirty",
                 35: "thirty-five", 40: "forty", 45: "forty-five", 50: "fifty", 55: "fifty-five",
                 60: "sixty", 65: "sixty-five", 70: "seventy"}


def _ordinal(day: int) -> str:
    suffix = "th" if 11 <= day % 100 <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
    return f"{day}{suffix}"


def _words_date(iso: str) -> str:
    """2021-06-22 -> '22nd of June 2021' (the spec's own example of a format no parser reads)."""
    year, month, day = (int(p) for p in iso[:10].split("-"))
    return f"{_ordinal(day)} of {_MONTHS[month - 1]} {year}"


def _unseen_format(column: str, truth: str) -> Optional[str]:
    if column == "crash_date":
        return _words_date(truth)
    if column == "date_police_notified":
        return f"{_words_date(truth)} at {truth[11:16]}"
    if column == "crash_hour":            # Silver 2 writes "19.00"
        hour = int(truth.split(".")[0])
        return f"{hour % 12 or 12} o'clock {'in the evening' if hour >= 12 else 'in the morning'}"
    if column == "posted_speed_limit":
        return _NUMBER_WORDS.get(int(truth))
    return None


UNSEEN_FORMAT_COLUMNS = ("crash_date", "date_police_notified", "crash_hour", "posted_speed_limit")


def _typo(truth: str, rng: random.Random, accept=None) -> Optional[str]:
    """One edit (swap two neighbours, drop or double a letter), never the first letter.

    `accept(column_value)` can reject an edit (the injector passes Silver 2's fuzzy
    guardrails, so a typo that would tie between two values, MARCH -> Marh, is not used)."""
    word = truth.title() if rng.random() < 0.5 else truth
    letters = [i for i in range(1, len(word)) if word[i].isalpha()]
    if len(word) < 4 or not letters:
        return None
    for _ in range(20):
        op, i = rng.choice(("swap", "drop", "double")), rng.choice(letters)
        if op == "swap" and i + 1 < len(word) and word[i + 1].isalpha() and word[i] != word[i + 1]:
            out = word[:i] + word[i + 1] + word[i] + word[i + 2:]
        elif op == "drop":
            out = word[:i] + word[i + 1:]
        elif op == "double":
            out = word[:i] + word[i] + word[i:]
        else:
            continue
        if out.upper() != truth.upper() and (accept is None or accept(out)):
            return out
    return None


def camel(column: str) -> str:
    """crash_date -> CrashDate: the kind of rename an upstream API change makes."""
    return "".join(part.capitalize() for part in column.split("_"))


# --------------------------------------------------------------------------- kinds
KINDS = {
    "typo": {"intended": FUZZY_FIXED, "columns": FUZZY_COLUMNS, "needs_value": True},
    "ambiguous": {"intended": QUARANTINED, "columns": tuple(AMBIGUOUS), "needs_value": True},
    "unseen_variant": {"intended": QUARANTINED, "columns": tuple(UNSEEN_VARIANTS), "needs_value": True},
    "unseen_format": {"intended": QUARANTINED, "columns": UNSEEN_FORMAT_COLUMNS, "needs_value": True},
    "unknown_value": {"intended": QUARANTINED, "columns": tuple(UNKNOWN_VALUES), "needs_value": False},
    "missing_mandatory": {"intended": QUARANTINED, "columns": ("crash_date",), "needs_value": True},
    "schema_rename": {"intended": DEAD_LETTER, "columns": None, "needs_value": False},
    "schema_missing": {"intended": DEAD_LETTER, "columns": None, "needs_value": False},
    "schema_extra": {"intended": DEAD_LETTER, "columns": None, "needs_value": False},
    "corrupt_record": {"intended": DEAD_LETTER, "columns": None, "needs_value": False},
}


class FaultSpecError(ValueError):
    """A scenario entry the injector cannot run."""


def canonical(column: str, value) -> Optional[str]:
    """Silver 2's value as the reference table writes the canonical value:
    hit_and_run_i True -> 'TRUE', num_units Decimal('2.0') -> '2.0', crash_month 'JUNE'."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return str(value)


def normalize_spec(entry: dict, index: int, known_columns) -> dict:
    """Check one scenario entry and fill in its defaults."""
    spec = dict(entry)
    kind = spec.get("kind")
    if kind not in KINDS:
        raise FaultSpecError(f"fault {index}: kind {kind!r} is not one of {sorted(KINDS)}")
    spec.setdefault("source", "crash1")
    spec["source"] = str(spec["source"]).strip().lower()
    spec["count"] = int(spec.get("count", 5))
    spec["batches"] = int(spec.get("batches", 1))
    if spec["count"] < 1 or spec["batches"] < 1:
        raise FaultSpecError(f"fault {index}: count and batches must be at least 1")
    column = spec.get("column")
    allowed = KINDS[kind]["columns"]
    if kind == "corrupt_record":
        spec["column"] = None
    elif kind == "schema_extra":
        spec["column"] = column or "extra_field"
        if spec["column"] in known_columns:
            raise FaultSpecError(f"fault {index}: schema_extra must add a column the pipeline does not know")
    elif kind == "missing_mandatory":
        spec["column"] = column or "crash_date"
    elif not column:
        raise FaultSpecError(f"fault {index}: {kind} needs a column")
    if spec.get("column") and kind != "schema_extra" and spec["column"] not in known_columns:
        raise FaultSpecError(f"fault {index}: unknown column {spec['column']!r}")
    if allowed is not None and spec["column"] not in allowed:
        raise FaultSpecError(f"fault {index}: {kind} works on {list(allowed)}, not {spec['column']!r}")
    if kind == "schema_rename":
        spec["to"] = spec.get("to") or camel(spec["column"])
        if spec["to"] in known_columns:
            raise FaultSpecError(f"fault {index}: rename {spec['column']} to a name the pipeline does not know")
    if spec.get("base") is not None:
        spec["base"] = str(spec["base"]).strip().upper()
    if kind == "ambiguous" and spec.get("base") is None and spec.get("value") is None:
        # Base rows must have a value the ambiguous spelling can stand for.
        spec["base_any"] = sorted(AMBIGUOUS[spec["column"]])
    if kind == "ambiguous" and spec.get("value") is not None and spec.get("base") is None:
        spec["base_any"] = sorted(truth for truth, values in AMBIGUOUS[spec["column"]].items()
                                  if spec["value"].upper() in (v.upper() for v in values))
        if not spec["base_any"]:
            raise FaultSpecError(f"fault {index}: give `base` (the true value) for ambiguous value "
                                 f"{spec['value']!r}")
    if kind == "unseen_variant" and spec.get("base") is None and spec.get("value") is None:
        spec["base_any"] = sorted(UNSEEN_VARIANTS[spec["column"]])
    return spec


def corrupt(spec: dict, truth: Optional[str], rng: random.Random, accept_typo=None) -> Optional[str]:
    """The injected raw value for a value fault, or None when this record cannot carry it
    (the base row's true value has no such variant). `accept_typo(column, value)` filters
    generated typos (see _typo)."""
    kind, column = spec["kind"], spec.get("column")
    if spec.get("value") is not None and kind not in ("missing_mandatory",):
        return str(spec["value"])
    if kind == "missing_mandatory":
        return ""
    if truth is None:
        return None
    if kind == "typo":
        return _typo(truth, rng, (lambda v: accept_typo(column, v)) if accept_typo else None)
    if kind == "ambiguous":
        options = AMBIGUOUS[column].get(truth.upper())
        return rng.choice(options) if options else None
    if kind == "unseen_variant":
        options = UNSEEN_VARIANTS[column].get(truth.upper())
        return rng.choice(options) if options else None
    if kind == "unseen_format":
        return _unseen_format(column, truth)
    if kind == "unknown_value":
        return rng.choice(UNKNOWN_VALUES[column])
    return None


def expected_fix(spec: dict, truth: Optional[str]) -> dict:
    """What a correct recovery does with this record (ground truth for Task 111).

    action ADD_VARIANT: the agent may fix it itself (ADD_VARIANT to `canonical_value`).
    action NONE:        the pipeline itself must fix it (fuzzy match), no agent.
    action RENAME_COLUMN: an engineer renames, then tools.replay re-runs the batch.
    action ESCALATE:    only an engineer can decide (no canonical set, no right answer,
                        a missing mandatory value, a broken record).
    """
    kind, column = spec["kind"], spec.get("column")
    if kind == "typo":
        return {"action": "NONE", "params": {}}
    if kind in ("ambiguous", "unseen_variant") and column in VOCABULARY_COLUMNS and truth:
        return {"action": "ADD_VARIANT", "params": {"canonical_value": truth}}
    if kind == "schema_rename":
        return {"action": "RENAME_COLUMN", "params": {"renames": {spec["to"]: column}}}
    return {"action": "ESCALATE", "params": {}}


def build_payload(silver1_row: dict, columns: List[str]) -> Dict[str, str]:
    """A producer-style payload from a Silver 1 row: every column as a string, '' for null."""
    return {c: ("" if silver1_row.get(c) is None else str(silver1_row[c])) for c in columns}


def apply_fault(spec: dict, payload: Dict[str, str], injected: Optional[str]) -> Dict[str, str]:
    """The payload with the fault in it (value faults and schema faults)."""
    out = dict(payload)
    kind, column = spec["kind"], spec.get("column")
    if kind == "schema_rename":
        out[spec["to"]] = out.pop(column)
    elif kind == "schema_missing":
        out.pop(column)
    elif kind == "schema_extra":
        out[column] = str(spec.get("value", "unexpected"))
    elif kind != "corrupt_record":
        out[column] = injected
    return out


def message(envelope: dict, spec: dict) -> str:
    """The Kafka message value. corrupt_record cuts the JSON in half (after record_id)."""
    text = json.dumps(envelope, ensure_ascii=False)
    if spec["kind"] == "corrupt_record":
        return text[: max(len(text) // 2, text.index(envelope["record_id"]) + len(envelope["record_id"]) + 2)]
    return text
