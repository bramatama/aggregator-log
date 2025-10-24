from fastapi.testclient import TestClient
from tests.conftest import create_test_event

# Tes 1
def test_get_stats_initial(test_client: TestClient):
    """
    Tes [GET /stats] pada startup (database bersih).
    Memastikan semua statistik awal adalah 0.
    """
    response = test_client.get("/stats")
    assert response.status_code == 200
    data = response.json()
    assert data["received"] == 0
    assert data["unique_processed"] == 0
    assert data["duplicate_dropped"] == 0
    assert data["topics"] == []

# Tes 2
def test_get_events_empty(test_client: TestClient):
    """
    Tes [GET /events] pada startup (database bersih).
    Memastikan list event kosong.
    """
    response = test_client.get("/events?topic=test-topic")
    assert response.status_code == 200
    data = response.json()
    assert data["topic"] == "test-topic"
    assert data["events"] == []

# Tes 3
def test_publish_valid_event(test_client: TestClient):
    """
    Tes [POST /publish] dengan satu event valid.
    Memastikan API merespons dengan benar.
    """
    event = create_test_event("id-valid-1")
    response = test_client.post("/publish", json=[event])
    assert response.status_code == 200
    assert response.json() == {"status": "events queued", "count": 1}

# Tes 4
def test_publish_empty_list_fails(test_client: TestClient):
    """
    Tes [POST /publish] dengan list kosong [].
    Memastikan API mengembalikan error 400 (Bad Request).
    """
    response = test_client.post("/publish", json=[])
    assert response.status_code == 400 # 400 Bad Request
    assert "Event list tidak boleh kosong" in response.json()["detail"]
