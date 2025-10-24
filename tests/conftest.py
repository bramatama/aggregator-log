import pytest
from fastapi.testclient import TestClient
import os
import time
from datetime import datetime

def create_test_event(event_id: str, topic: str = "test-topic"):
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.now().isoformat() + "Z",
        "source": "pytest",
        "payload": {"test_id": event_id}
    }

@pytest.fixture(scope="function")
def test_client(tmp_path):
    """
    Fixture Pytest yang membuat TestClient FastAPI baru untuk SETIAP test function.
    
    PENTING: Fixture ini mengatur env var DATABASE_FILE 
    untuk menunjuk ke file database temporer yang unik (via tmp_path).
    Ini memastikan setiap tes berjalan terisolasi (database bersih).
    """
    
    # Tentukan path database temporer yang unik untuk tes ini
    temp_db_path = tmp_path / "test_aggregator.db"
    
    # Set environment variable SEBELUM app di-import
    os.environ["DATABASE_FILE"] = str(temp_db_path)
    
    # Import app SETELAH env var di-set
    # Ini penting agar app membaca DATABASE_FILE yang benar saat startup
    from src.main import app 
    
    # 'with TestClient(app) as client' akan menjalankan logic 'lifespan' (startup/shutdown)
    with TestClient(app) as client:
        yield client # Ini adalah 'client' yang digunakan oleh tes
    
    # Cleanup: Hapus env var setelah tes selesai
    del os.environ["DATABASE_FILE"]

