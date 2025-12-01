import pytest
import os
import sys
from pathlib import Path

# Add src to python path so tests can import modules
sys.path.append(str(Path(__file__).parent.parent))

@pytest.fixture(scope="session")
def project_root():
    """Return the project root directory."""
    return Path(__file__).parent.parent

@pytest.fixture
def api_client():
    """Return a test client for the API (if using TestClient)."""
    from fastapi.testclient import TestClient
    from src.api.main import app
    return TestClient(app)

@pytest.fixture
def mock_db_conn(mocker):
    """Mock the database connection."""
    mock_conn = mocker.Mock()
    mocker.patch("src.db.core.connect", return_value=mock_conn)
    return mock_conn
