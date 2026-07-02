def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "integration: exercises the dev DB / Martin tile server; "
        "skips when unreachable",
    )
