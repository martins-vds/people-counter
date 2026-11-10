import pytest


def pytest_addoption(parser):
    parser.addoption("--portal-e2e", action="store_true", help="Run opt-in operator portal browser tests")


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--portal-e2e"):
        skip = pytest.mark.skip(reason="Portal browser tests require --portal-e2e")
        for item in items:
            if item.get_closest_marker("ui"):
                item.add_marker(skip)
