import pytest
from unittest.mock import MagicMock

@pytest.fixture(autouse=True)
def mock_ee(request, monkeypatch):
    """
    Mocks the Earth Engine library for all tests, unless a test is
    marked with 'no_mock_ee'.
    """
    if 'no_mock_ee' in request.node.keywords:
        yield  # Allow the test to run with the real 'ee'
        return

    mock_ee_instance = MagicMock()
    # Mock 'ee' in both prep and utils where it is used
    monkeypatch.setattr("src.prep.ee", mock_ee_instance)
    monkeypatch.setattr("src.utils.ee", mock_ee_instance)
    yield mock_ee_instance