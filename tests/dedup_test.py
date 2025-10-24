from fastapi.testclient import TestClient
from tests.conftest import create_test_event
import time

# Tes 5
def test_publish_single_duplicate(test_client: TestClient):
    """
    Tes [Idempotency] (Poin b): Kirim event yang sama dua kali.
    Memastikan event hanya diproses sekali.
    """
    event = create_test_event("id-dup-1")
    
    # Kirim pertama kali
    res1 = test_client.post("/publish", json=[event])
    assert res1.status_code == 200
    
    time.sleep(0.1) 
    
    # Cek stats
    stats1 = test_client.get("/stats").json()
    assert stats1["received"] == 1
    assert stats1["unique_processed"] == 1
    assert stats1["duplicate_dropped"] == 0
    
    # Kirim kedua kali (duplikat)
    res2 = test_client.post("/publish", json=[event])
    assert res2.status_code == 200
    
    time.sleep(0.1)
    
    # Cek stats lagi
    stats2 = test_client.get("/stats").json()
    assert stats2["received"] == 2 # Diterima 2x
    assert stats2["unique_processed"] == 1 # Diproses 1x
    assert stats2["duplicate_dropped"] == 1 # Dibuang 1x

# Tes 6
def test_publish_batch_with_duplicates_and_timing(test_client: TestClient):
    """
    Tes [Idempotency] (Poin b): Kirim batch berisi duplikat.
    Tes [Stress Kecil] (Poin f): Memastikan batch diproses dalam waktu wajar.
    """
    event1 = create_test_event("id-batch-1")
    event2 = create_test_event("id-batch-2")
    
    batch = [event1, event2, event1, event1, event2] # 5 total, 2 unik
    
    start_time = time.time()
    
    res = test_client.post("/publish", json=batch)
    assert res.status_code == 200
    
    time.sleep(0.2) 
    
    end_time = time.time()
    duration = end_time - start_time
    
    # --- Poin f: Assert Waktu Eksekusi ---
    # Memproses 5 event harus sangat cepat (kurang dari 1 detik)
    print(f"Waktu pemrosesan batch: {duration:.4f}s")
    assert duration < 1.0, "Pemrosesan batch terlalu lambat (melebihi 1 detik)"
    
    stats = test_client.get("/stats").json()
    
    assert stats["received"] == 5
    assert stats["unique_processed"] == 2 # (id-batch-1 dan id-batch-2)
    assert stats["duplicate_dropped"] == 3 # (2 duplikat id-1, 1 duplikat id-2)
    
    events_res = test_client.get("/events?topic=test-topic")
    assert len(events_res.json()["events"]) == 2

