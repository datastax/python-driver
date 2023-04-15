import pytest
import logging

# from https://github.com/streamlit/streamlit/pull/5047/files
def pytest_sessionfinish():
    # We're not waiting for scriptrunner threads to cleanly close before ending the PyTest,
    # which results in raised exception ValueError: I/O operation on closed file.
    # This is well known issue in PyTest, check out these discussions for more:
    # * https://github.com/pytest-dev/pytest/issues/5502
    # * https://github.com/pytest-dev/pytest/issues/5282
    # To prevent the exception from being raised on pytest_sessionfinish
    # we disable exception raising in logging module
    logging.raiseExceptions = False
