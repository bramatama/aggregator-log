import os
import time
from fastapi.testclient import TestClient
from datetime import datetime, timezone

# Helper function (duplicate from conftest for isolation)
def create_test_event(event_id: str, topic: str = "test-topic") -> dict:
    """Helper to create a valid event structure."""
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test-suite",
        "payload": {"test_data": event_id}
    }

# Tes 7
def test_dedup_store_persists_across_restarts(tmp_path):
    """
    Tes [Toleransi Crash] (Poin c):
    Mensimulasikan restart server dan memastikan dedup store persisten.

    Ini adalah tes khusus yang TIDAK menggunakan fixture 'test_client'
    karena kita perlu mengontrol path DB secara manual.
    """

    persistent_db_path = tmp_path / "persistent_test.db"
    os.environ["DATABASE_FILE"] = str(persistent_db_path)

    from src.main import app

    event = create_test_event("id-persist-1")

    # --- 2. Simulasi Run Pertama (Sebelum Restart) ---
    print("Menjalankan TestClient pertama...")
    with TestClient(app) as client1:
        res = client1.post("/publish", json=[event])
        assert res.status_code == 200

        time.sleep(0.1) # Beri waktu consumer memproses

        stats1 = client1.get("/stats").json()
        assert stats1["received"] == 1
        assert stats1["unique_processed"] == 1
        assert stats1["duplicate_dropped"] == 0

    # --- 3. Simulasi Run Kedua (Setelah Restart) ---
    print("Menjalankan TestClient kedua (simulasi restart)...")
    with TestClient(app) as client2:
        stats2_pre = client2.get("/stats").json()
        # Stats in-memory (received, dropped) harus reset
        assert stats2_pre["received"] == 0
        assert stats2_pre["duplicate_dropped"] == 0
        # Stats persisten (unique, topics) HARUS tetap ada
        assert stats2_pre["unique_processed"] == 1
        assert "test-topic" in stats2_pre["topics"]

        # --- 4. Kirim Duplikat (Setelah Restart) ---
        res = client2.post("/publish", json=[event])
        assert res.status_code == 200

        time.sleep(0.1) 

        stats2_post = client2.get("/stats").json()
        assert stats2_post["received"] == 1 # 1 event diterima di run ini
        assert stats2_post["unique_processed"] == 1 # Total unik TETAP 1
        assert stats2_post["duplicate_dropped"] == 1 # 1 duplikat terdeteksi

