"""Pytest configuration for service tests."""

import pytest


@pytest.fixture
def anyio_backend():
    """Use only asyncio backend (trio not installed)."""
    return "asyncio"
