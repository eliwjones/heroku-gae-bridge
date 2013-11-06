import pytest

def pytest_addoption(parser):
    parser.addoption("--env", action="store", default="local",
        help="Do testing against remote database if configured.")
