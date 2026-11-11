import copy

import pytest

from people_counter.fabric_sjd_definition import validate_schedule_readback


def schedule():
    return {
        "enabled": False,
        "configuration": {
            "type": "Cron", "interval": 5, "localTimeZoneId": "UTC",
            "startDateTime": "2026-11-11T00:05:00Z",
            "endDateTime": "2026-12-11T00:05:00Z",
        },
        "executionData": {"commandLineArguments": "--artifacts checked-target"},
    }


@pytest.mark.parametrize("suffix", ["", "+00:00", "Z"])
def test_equivalent_utc_datetime_formats_do_not_break_readback(suffix):
    expected = schedule()
    observed = copy.deepcopy(expected)
    observed["id"] = "server-assigned"
    observed["owner"] = {"type": "User"}
    for key in ("startDateTime", "endDateTime"):
        observed["configuration"][key] = expected["configuration"][key].rstrip("Z") + suffix
    validate_schedule_readback(observed, expected)


def test_aware_datetime_offsets_are_compared_as_instants_not_text():
    expected = schedule()
    observed = copy.deepcopy(expected)
    observed["configuration"]["startDateTime"] = "2026-11-11T01:05:00+01:00"
    validate_schedule_readback(observed, expected)


@pytest.mark.parametrize("change", ["enabled", "interval", "interval_bool", "namespace", "zone", "start", "end"])
def test_changed_schedule_semantics_still_fail_closed(change):
    expected = schedule()
    observed = copy.deepcopy(expected)
    if change == "enabled":
        observed["enabled"] = 0
    elif change == "interval":
        observed["configuration"]["interval"] = 10
    elif change == "interval_bool":
        observed["configuration"]["interval"] = True
    elif change == "namespace":
        observed["executionData"]["commandLineArguments"] = "--artifacts another-target"
    elif change == "zone":
        observed["configuration"]["localTimeZoneId"] = "Central Standard Time"
    else:
        observed["configuration"][change + "DateTime"] = "2027-01-01T00:00:00Z"
    with pytest.raises(ValueError, match="schedule"):
        validate_schedule_readback(observed, expected)


@pytest.mark.parametrize("bad", [None, {}, {"enabled": False}, {"enabled": False, "configuration": []}])
def test_missing_or_malformed_readback_is_not_success(bad):
    with pytest.raises(ValueError, match="schedule"):
        validate_schedule_readback(bad, schedule())


def test_disabling_a_weekly_schedule_preserves_its_configuration_without_assuming_cron():
    expected = schedule()
    expected["configuration"].pop("interval")
    expected["configuration"].update(type="Weekly", times=["10:00"], weekdays=["Monday"])
    validate_schedule_readback(copy.deepcopy(expected), expected)
