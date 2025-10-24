import requests
import uuid
import time
import random
import threading

# --- Konfigurasi Tes ---
TOTAL_EVENTS = 5000
DUPLICATE_PERCENTAGE = 0.20 # 20%
BATCH_SIZE = 100 # Mengirim dalam batch agar lebih efisien
BASE_URL = "http://127.0.0.1:8080"

# --- Variabel Global untuk Cek Responsivitas ---
responsive_check_passed = None

def generate_event(topic, event_id):
    """Membuat satu event dummy."""
    return {
        "topic": topic,
        "event_id": str(event_id),
        "timestamp": datetime.now().isoformat() + "Z",
        "source": "load-tester",
        "payload": {"test_run": "scale-test"}
    }

def check_responsiveness():
    """
    Thread terpisah yang mengecek /stats saat load test berjalan.
    """
    global responsive_check_passed
    try:
        print("\n[Responsiveness Check] Mengecek GET /stats...")
        start_time = time.time()
        res = requests.get(f"{BASE_URL}/stats", timeout=2.0) # Timeout 2 detik
        end_time = time.time()
        
        if res.status_code == 200:
            print(f"[Responsiveness Check] BERHASIL! /stats merespons dalam {end_time - start_time:.2f} detik.")
            responsive_check_passed = True
        else:
            print(f"[Responsiveness Check] GAGAL! /stats mengembalikan {res.status_code}")
            responsive_check_passed = False
    except requests.exceptions.Timeout:
        print("[Responsiveness Check] GAGAL! /stats timeout (> 2 detik).")
        responsive_check_passed = False
    except Exception as e:
        print(f"[Responsiveness Check] GAGAL! Error: {e}")
        responsive_check_passed = False

def run_load_test():
    """Menjalankan load test utama."""
    global responsive_check_passed
    
    print(f"Memulai Load Test: {TOTAL_EVENTS} event, {DUPLICATE_PERCENTAGE*100}% duplikasi.")
    
    num_unique = int(TOTAL_EVENTS * (1 - DUPLICATE_PERCENTAGE))
    num_duplicates = TOTAL_EVENTS - num_unique
    
    print(f"Unik: {num_unique}, Duplikat: {num_duplicates}")
    
    # 1. Buat event unik
    unique_events = [generate_event("loadtest", uuid.uuid4()) for _ in range(num_unique)]
    
    # 2. Ambil sampel acak dari event unik untuk dijadikan duplikat
    duplicate_events = [random.choice(unique_events) for _ in range(num_duplicates)]
    
    # 3. Gabungkan dan kocok
    all_events_to_send = unique_events + duplicate_events
    random.shuffle(all_events_to_send)
    
    print(f"Total event yang akan dikirim: {len(all_events_to_send)}")
    
    # 4. Dapatkan stats awal
    try:
        stats_before = requests.get(f"{BASE_URL}/stats").json()
        print(f"Stats Awal -> Unik: {stats_before['unique_processed']}, Duplikat: {stats_before['duplicate_dropped']}")
    except Exception as e:
        print(f"Tidak bisa menghubungi server di {BASE_URL}. Pastikan server berjalan.")
        return

    # 5. Mulai mengirim
    start_time = time.time()
    
    # Mulai thread pengecek responsivitas di tengah-tengah proses
    threading.Timer(5.0, check_responsiveness).start()
    threading.Timer(25.0, check_responsiveness).start()
    threading.Timer(60.0, check_responsiveness).start()
    
    sent_count = 0
    for i in range(0, len(all_events_to_send), BATCH_SIZE):
        batch = all_events_to_send[i:i+BATCH_SIZE]
        try:
            res = requests.post(f"{BASE_URL}/publish", json=batch)
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
    print("\n--- Hasil Load Test ---")
    
    total_unique_processed = stats_after['unique_processed'] - stats_before['unique_processed']
    total_duplicates_dropped = stats_after['duplicate_dropped'] - stats_before['duplicate_dropped']
    
    print(f"Total Unik Diproses   : {total_unique_processed} (Diharapkan: {num_unique})")
    print(f"Total Duplikat Dibuang: {total_duplicates_dropped} (Diharapkan: {num_duplicates})")
    
    if responsive_check_passed is None:
        print("[Responsiveness Check] Pengecekan tidak sempat berjalan.")
    elif responsive_check_passed:
        print("[Responsiveness Check] Sistem TETAP RESPONSIF selama tes.")
    else:
        print("[Responsiveness Check] Sistem TIDAK RESPONSIF selama tes.")
        
    if total_unique_processed == num_unique and total_duplicates_dropped == num_duplicates:
        print("\nStatus Poin (d): BERHASIL!")
    else:
        print("\nStatus Poin (d): GAGAL! Angka tidak sesuai.")

if __name__ == "__main__":
    from datetime import datetime
    run_load_test()
