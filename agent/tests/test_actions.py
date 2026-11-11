"""The pre-approved action set: python -m pytest agent/tests"""
from agent import actions
from agent.tests.helpers import MONTHS, schema_item, stack_item

JULE = stack_item()["failure"]
RENAMED = schema_item()["failure"]


def test_only_add_variant_runs_without_an_engineer():
    assert actions.SAFE_ACTIONS == {"ADD_VARIANT"}
    assert set(actions.CATALOGUE) == {"ADD_VARIANT", "RENAME_COLUMN", "ESCALATE"}


def test_settings_can_narrow_the_safe_set_but_never_widen_it():
    assert actions.safe_actions(None) == {"ADD_VARIANT"}
    assert actions.safe_actions([]) == set()
    assert actions.safe_actions(["rename_column", "ESCALATE", " add_variant "]) == {"ADD_VARIANT"}


def test_the_model_is_offered_only_actions_that_can_apply_to_the_failure():
    assert actions.allowed_for(JULE, MONTHS) == ["ADD_VARIANT", "ESCALATE"]
    assert actions.allowed_for(RENAMED, []) == ["RENAME_COLUMN", "ESCALATE"]
    # crash_date is parsed, not looked up: it has no canonical values to map to
    assert actions.allowed_for({**JULE, "column_name": "crash_date"}, []) == ["ESCALATE"]
    assert actions.allowed_for({**JULE, "raw_value": None, "mandatory": True}, MONTHS) == ["ESCALATE"]
    assert actions.allowed_for({**JULE, "family": "RECORD"}, MONTHS) == ["ESCALATE"]


def test_add_variant_must_name_a_canonical_value_of_the_column():
    assert actions.check_params("ADD_VARIANT", {"canonical_value": "JULY"}, JULE, MONTHS) == \
        ({"canonical_value": "JULY"}, [])
    for bad in ({"canonical_value": "JULLY"}, {"canonical_value": "july"}, {"canonical_value": None}, {}, None):
        params, problems = actions.check_params("ADD_VARIANT", bad, JULE, MONTHS)
        assert params is None and "not a canonical value of crash_month" in problems[0]


def test_add_variant_is_refused_without_a_vocabulary_or_a_raw_value():
    assert actions.check_params("ADD_VARIANT", {"canonical_value": "JULY"}, JULE, [])[0] is None
    for failure in ({**JULE, "raw_value": "  "}, {**JULE, "raw_value": "MISSING_MANDATORY_VALUE"},
                    {**JULE, "family": "SCHEMA"}, {**JULE, "column_name": None}):
        assert actions.check_params("ADD_VARIANT", {"canonical_value": "JULY"}, failure, MONTHS)[0] is None


def test_checked_parameters_drop_anything_else_the_model_wrote():
    params, _ = actions.check_params("ADD_VARIANT", {"canonical_value": "JULY", "also": "DROP TABLE"}, JULE, MONTHS)
    assert params == {"canonical_value": "JULY"}


def test_rename_must_use_the_columns_of_the_failure():
    good = {"renames": {"CrashDate": "crash_date", "Weather": "weather_condition"}}
    assert actions.check_params("RENAME_COLUMN", good, RENAMED, []) == (good["renames"], [])
    for bad in ({"renames": {"Crash_Date": "crash_date"}}, {"renames": {"CrashDate": "crash_day"}},
                {"renames": {"CrashDate": "crash_date", "Weather": "crash_date"}}, {"renames": {}}, {}):
        assert actions.check_params("RENAME_COLUMN", bad, RENAMED, [])[0] is None
    assert actions.check_params("RENAME_COLUMN", good, JULE, [])[0] is None


def test_an_unknown_action_is_never_accepted():
    params, problems = actions.check_params("DROP_TABLE", {}, JULE, MONTHS)
    assert params is None and "not a pre-approved action" in problems[0]
