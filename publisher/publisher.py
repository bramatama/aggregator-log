import requests
import uuid
import time
import random
import os
from datetime import datetime

# --- Konfigurasi Tes ---
TOTAL_EVENTS = 5000
DUPLICATE_PERCENTAGE = 0.20 # 20%
BATCH_SIZE = 100
# BACA URL DARI ENVIRONMENT VARIABLE (disediakan oleh docker-compose)
BASE_URL = os.getenv("AGGREGATOR_URL", "http://localhost:8080")

def generate_event(topic, event_id):
    """Membuat satu event dummy."""
    return {
        "topic": topic,
        "event_id": str(event_id),
        "timestamp": datetime.now().isoformat() + "Z",
        "source": "docker-publisher-service",
        "payload": {"run_id": str(uuid.uuid4())}
    }

def run_load_test():
    """Menjalankan load test utama."""
    
    print(f"--- Publisher Service Dimulai ---")
    print(f"Target Aggregator: {BASE_URL}")
    print(f"Mulai Load Test: {TOTAL_EVENTS} event, {DUPLICATE_PERCENTAGE*100}% duplikasi.")
    
    num_unique = int(TOTAL_EVENTS * (1 - DUPLICATE_PERCENTAGE))
    num_duplicates = TOTAL_EVENTS - num_unique
    
    # 1. Buat event unik
    unique_events = [generate_event("loadtest", uuid.uuid4()) for _ in range(num_unique)]
    
    # 2. Ambil sampel acak dari event unik untuk dijadikan duplikat
    duplicate_events = [random.choice(unique_events) for _ in range(num_duplicates)]
    
    # 3. Gabungkan dan kocok
    all_events_to_send = unique_events + duplicate_events
    random.shuffle(all_events_to_send)
    
    print(f"Total event yang akan dikirim: {len(all_events_to_send)}")
    
    # 4. Cek stats awal
    try:
        stats_before = requests.get(f"{BASE_URL}/stats").json()
        print(f"Stats Aggregator Awal -> Unik: {stats_before['unique_processed']}, Duplikat: {stats_before['duplicate_dropped']}")
    except Exception as e:
        print(f"Gagal menghubungi aggregator di {BASE_URL}. Error: {e}")
        return

    # 5. Mulai mengirim
    start_time = time.time()
    
    sent_count = 0
    for i in range(0, len(all_events_to_send), BATCH_SIZE):
        batch = all_events_to_send[i:i+BATCH_SIZE]
        try:
            res = requests.post(f"{BASE_URL}/publish", json=batch, timeout=5.0)
            if res.status_code != 200:
                print(f"Error mengirim batch: {res.status_code} - {res.text}")
            sent_count += len(batch)
            print(f"Mengirim event... {sent_count}/{TOTAL_EVENTS}", end="\r")
        except Exception as e:
            print(f"\nError koneksi saat mengirim: {e}")
            time.sleep(1)

    end_time = time.time()
    print(f"\nSelesai mengirim {TOTAL_EVENTS} event dalam {end_time - start_time:.2f} detik.")
    
    print("Menunggu consumer memproses (5 detik)...")
    time.sleep(5) # Beri waktu consumer untuk memproses sisa antrian

    # 6. Dapatkan stats akhir
    stats_after = requests.get(f"{BASE_URL}/stats").json()
    print("\n--- Hasil Publisher ---")
    
    total_unique_processed = stats_after['unique_processed'] - stats_before['unique_processed']
    total_duplicates_dropped = stats_after['duplicate_dropped'] - stats_before['duplicate_dropped']
    
    print(f"Total Unik Diproses (sesuai laporan aggregator): {total_unique_processed} (Diharapkan: {num_unique})")
    print(f"Total Duplikat Dibuang (sesuai laporan aggregator): {total_duplicates_dropped} (Diharapkan: {num_duplicates})")
        
    if total_unique_processed == num_unique:
        print("\nStatus Poin (d): BERHASIL! (Jumlah unik sesuai)")
    else:
        print(f"\nStatus Poin (d): GAGAL! (Jumlah unik {total_unique_processed}, diharapkan {num_unique})")

    print("--- Publisher Service Selesai ---")

if __name__ == "__main__":
    # Coba hubungi aggregator 5x sebelum menyerah
    connected = False
    for i in range(5):
        try:
            requests.get(f"{BASE_URL}/stats", timeout=3.0)
            print("Koneksi ke Aggregator berhasil.")
            connected = True
            break
        except Exception:
            print(f"Menunggu Aggregator... (upaya {i+1}/5)")
            time.sleep(3)
            
    if connected:
        run_load_test()
    else:
        print("Tidak dapat terhubung ke Aggregator. Publisher exiting.")
