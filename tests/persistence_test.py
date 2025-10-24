import pytest
from fastapi.testclient import TestClient
import os
import time
from datetime import datetime # <-- 1. TAMBAHKAN IMPORT INI

# Helper function (disalin dari conftest karena file ini tidak menggunakannya)
def create_test_event(event_id: str, topic: str = "test-topic"):
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.now().isoformat() + "Z",
        "source": "pytest",
        "payload": {"test_id": event_id}
    }

# Tes 7
def test_dedup_store_persists_across_restarts(tmp_path):
    """
    Tes [Toleransi Crash] (Poin c):
    Mensimulasikan restart server dan memastikan dedup store persisten.
    
    Ini adalah tes khusus yang TIDAK menggunakan fixture 'test_client'
    karena kita perlu mengontrol path DB secara manual.
    """
    
    # 1. Tentukan path DB yang akan dipakai bersama
    persistent_db_path = tmp_path / "persistent_test.db"
    os.environ["DATABASE_FILE"] = str(persistent_db_path)
    
    # Import app SETELAH env var di-set
    from src.main import app
    
    event = create_test_event("id-persist-1")
    
    # --- 2. Simulasi Run Pertama (Sebelum Restart) ---
    print("Menjalankan TestClient pertama...")
    with TestClient(app) as client1:
        # Periksa status code di sini untuk debugging
        res = client1.post("/publish", json=[event])
        assert res.status_code == 200
        
        time.sleep(0.1) # Waktu proses
        
        # Cek stats sebelum shutdown
        stats1 = client1.get("/stats").json()
        assert stats1["unique_processed"] == 1
        assert stats1["received"] == 1
        
    # 'with' block ditutup -> 'lifespan' shutdown dipanggil.
    print("TestClient pertama shutdown.")

    # --- 3. Simulasi Run Kedua (Setelah Restart) ---
    # Kita Buka TestClient baru (mensimulasikan server restart).
    # Karena DATABASE_FILE env var masih sama,
    # 'lifespan' startup akan membaca file DB yang *sudah ada*.
    print("Menjalankan TestClient kedua (simulasi restart)...")
    with TestClient(app) as client2:
        
        # Cek stats saat startup (sebelum kirim event baru)
        # load_initial_stats() seharusnya berjalan
        stats2_startup = client2.get("/stats").json()
        
        # In-memory stats (received, dropped) harus 0
        assert stats2_startup["received"] == 0
        assert stats2_startup["duplicate_dropped"] == 0
        
        # Persistent stats (unique) harus di-load dari DB
        assert stats2_startup["unique_processed"] == 1 
        assert stats2_startup["topics"] == ["test-topic"]
        
        # --- 4. Kirim Event Duplikat (Setelah Restart) ---
        # Kirim event "id-persist-1" YANG SAMA
        print("Mengirim event duplikat setelah restart...")
        
        # Periksa status code di sini untuk debugging
        res = client2.post("/publish", json=[event])
        assert res.status_code == 200

        time.sleep(0.1) # Waktu proses
        
        # Cek stats akhir
        stats2_final = client2.get("/stats").json()
        
        # 'received' bertambah
        assert stats2_final["received"] == 1
        
        # 'unique_processed' TETAP 1 (dedup store persisten bekerja!)
        assert stats2_final["unique_processed"] == 1 
        
        # 'duplicate_dropped' bertambah
        assert stats2_final["duplicate_dropped"] == 1
    
    print("TestClient kedua shutdown.")
    # Cleanup env var
    del os.environ["DATABASE_FILE"]

