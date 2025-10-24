import asyncio
import sqlite3
import json
import time
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, ValidationError
from typing import List, Dict, Any, Set
from contextlib import asynccontextmanager
from datetime import datetime
import os

# --- Konstanta & Konfigurasi ---
# DATABASE_FILE = os.getenv("DATABASE_FILE", "aggregator.db")
# ^^^ DIHAPUS DARI SINI. Variabel ini akan dibaca di dalam fungsi.

# --- Model Data (Pydantic) ---
# Sesuai spesifikasi tugas
class Event(BaseModel):
    topic: str
    event_id: str  # string-unik
    timestamp: str # ISO8601
    source: str
    payload: Dict[str, Any]

# --- State Aplikasi ---
# Variabel ini akan diinisialisasi saat startup
app_state = {
    "event_queue": None,
    "stats": {
        "received": 0,
        "unique_processed": 0,
        "duplicate_dropped": 0,
        "topics": set()
    },
    "start_time": datetime.now()
}

# --- Fungsi Database (SQLite) ---

def init_db():
    """
    Menginisialisasi database SQLite dan membuat tabel jika belum ada.
    Ini adalah kunci untuk persistensi (tahan restart).
    """
    # --- PERBAIKAN ---
    # Baca env var di dalam fungsi, bukan di level global
    DATABASE_FILE = os.getenv("DATABASE_FILE", "aggregator.db")
    
    print(f"Menggunakan database file: {DATABASE_FILE}") # Log untuk debugging
    with sqlite3.connect(DATABASE_FILE) as conn:
        cursor = conn.cursor()
        
        # Tabel 1: Dedup Store (Hanya untuk cek idempotency)
        # PRIMARY KEY (topic, event_id) secara otomatis mencegah duplikasi.
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS dedup_store (
            topic TEXT,
            event_id TEXT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (topic, event_id)
        )
        """)
        
        # Tabel 2: Processed Events (Untuk melayani GET /events)
        # Menyimpan data lengkap dari event yang unik.
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_events (
            topic TEXT,
            event_id TEXT,
            timestamp TEXT,
            source TEXT,
            payload TEXT, -- Menyimpan payload JSON sebagai string
            UNIQUE(topic, event_id)
        )
        """)
        conn.commit()

def load_initial_stats():
    """
    Memuat statistik persisten (event unik & topik) dari DB saat startup.
    Fungsi ini HANYA memuat stats persisten.
    """
    # --- PERBAIKAN ---
    DATABASE_FILE = os.getenv("DATABASE_FILE", "aggregator.db")
    
    # Reset stats persisten di memori sebelum memuat
    app_state["stats"]["unique_processed"] = 0
    app_state["stats"]["topics"] = set()

    if not os.path.exists(DATABASE_FILE):
        print("Database file not found, stats persisten starts from zero.")
        return

    try:
        with sqlite3.connect(DATABASE_FILE) as conn:
            cursor = conn.cursor()
            
            # Hitung event unik yang sudah ada
            cursor.execute("SELECT COUNT(*) FROM dedup_store")
            unique_count = cursor.fetchone()[0]
            app_state["stats"]["unique_processed"] = unique_count
            
            # Ambil daftar topik unik yang sudah ada
            cursor.execute("SELECT DISTINCT topic FROM dedup_store")
            topics = cursor.fetchall()
            app_state["stats"]["topics"] = {row[0] for row in topics}
            
            print(f"Loaded {unique_count} unique events and {len(topics)} topics from DB.")
    except sqlite3.OperationalError as e:
        print(f"Error memuat stats (DB mungkin terkunci atau belum siap): {e}")
        # Biarkan stats 0 jika gagal memuat

def process_event_in_db(event: Event) -> bool:
    """
    Mencoba memproses dan menyimpan event.
    Ini adalah inti dari logika idempotency dan deduplication.
    
    Mengembalikan:
        True: Jika event baru dan berhasil diproses.
        False: Jika event adalah duplikat.
    """
    # --- PERBAIKAN ---
    DATABASE_FILE = os.getenv("DATABASE_FILE", "aggregator.db")
    
    try:
        with sqlite3.connect(DATABASE_FILE) as conn:
            cursor = conn.cursor()
            
            # 1. Coba masukkan ke dedup_store. 
            # Jika gagal (IntegrityError), itu adalah duplikat.
            cursor.execute(
                "INSERT INTO dedup_store (topic, event_id) VALUES (?, ?)",
                (event.topic, event.event_id)
            )
            
            # 2. Jika berhasil (event baru), masukkan data lengkap ke processed_events
            cursor.execute(
                "INSERT INTO processed_events (topic, event_id, timestamp, source, payload) VALUES (?, ?, ?, ?, ?)",
                (event.topic, event.event_id, event.timestamp, event.source, json.dumps(event.payload))
            )
            
            conn.commit()
            return True  # Event baru
            
    except sqlite3.IntegrityError:
        # Ini terjadi jika PRIMARY KEY (topic, event_id) sudah ada.
        # Ini adalah duplikat yang diharapkan.
        return False  # Event duplikat
    except Exception as e:
        print(f"Error saat memproses event di DB: {e}")
        return False # Gagal (bukan duplikat, tapi error)

# --- Consumer (Background Task) ---

async def consumer():
    """
    Task background yang berjalan terus-menerus (seperti Koki).
    Mengambil event dari antrian dan memprosesnya.
    """
    print("Consumer task dimulai...")
    
    while True:
        try:
            # 1. Menunggu event baru dari antrian
            # Periksa apakah queue ada sebelum mencoba mengambil
            if app_state["event_queue"] is None:
                await asyncio.sleep(0.1)
                continue
                
            queue = app_state["event_queue"]
            event = await queue.get()
            
            # 2. Proses event di DB (fungsi ini menangani dedup)
            is_new = process_event_in_db(event)
            
            # 3. Perbarui statistik berdasarkan hasil
            if is_new:
                app_state["stats"]["unique_processed"] += 1
                app_state["stats"]["topics"].add(event.topic)
                # print(f"Event baru diproses: {event.topic}/{event.event_id}")
            else:
                app_state["stats"]["duplicate_dropped"] += 1
                # Logging yang jelas untuk duplikasi (sesuai permintaan)
                print(f"[DUPLICATE] Event duplikat terdeteksi dan dibuang: {event.topic}/{event.event_id}")
            
            # 4. Menandai tugas di antrian selesai
            queue.task_done()
            
        except Exception as e:
            print(f"Error di consumer: {e}")
            # Pastikan loop terus berjalan
            await asyncio.sleep(1)

# --- FastAPI Lifecycle (Startup & Shutdown) ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Logika yang dijalankan SEBELUM aplikasi melayani request (Startup)
    print("Aplikasi startup...")
    
    # --- PERBAIKAN KUNCI ---
    # Reset statistik IN-MEMORY setiap kali aplikasi (atau TestClient) dimulai.
    # Ini memastikan isolasi antar tes.
    print("Me-reset statistik in-memory...")
    app_state["stats"]["received"] = 0
    app_state["stats"]["duplicate_dropped"] = 0
    app_state["start_time"] = datetime.now()
    app_state["event_queue"] = asyncio.Queue()
    # -----------------------
    
    # 1. Inisialisasi Database (tetap)
    init_db()
    
    # 2. Muat statistik PERSISTEN dari DB (tetap)
    # Ini akan me-set 'unique_processed' dan 'topics'
    load_initial_stats()
    
    # 4. Mulai consumer task di background
    # (Pindahkan ini setelah antrian dibuat)
    asyncio.create_task(consumer())
    
    yield
    
    # Logika yang dijalankan SETELAH aplikasi berhenti (Shutdown)
    print("Aplikasi shutdown...")
    # (Tidak ada cleanup khusus yang diperlukan untuk file ini)

# --- Inisialisasi Aplikasi FastAPI ---
app = FastAPI(lifespan=lifespan)

# --- API Endpoints ---

@app.post("/publish")
async def publish_events(events: List[Event], request: Request):
    """
    Menerima satu atau batch event dan memasukkannya ke antrian internal.
    """
    queue = app_state["event_queue"]
    
    # Validasi custom untuk memastikan setidaknya satu event dikirim
    if not events:
        raise HTTPException(status_code=400, detail="Event list tidak boleh kosong")
        
    for event in events:
        await queue.put(event)
        app_state["stats"]["received"] += 1 # Tambah statistik 'received'
        
    return {"status": "events queued", "count": len(events)}

@app.get("/events")
async def get_events(topic: str):
    """
    Mengembalikan daftar event unik yang telah diproses untuk topik tertentu.
    Data diambil dari DB persisten.
    """
    # --- PERBAIKAN ---
    DATABASE_FILE = os.getenv("DATABASE_FILE", "aggregator.db")
    
    try:
        with sqlite3.connect(DATABASE_FILE) as conn:
            conn.row_factory = sqlite3.Row # Memudahkan konversi ke dict
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT * FROM processed_events WHERE topic = ?",
                (topic,)
            )
            rows = cursor.fetchall()
            
            # Konversi hasil DB (yang payload-nya string) kembali ke format Event
            events_list = []
            for row in rows:
                event_data = dict(row)
                event_data["payload"] = json.loads(event_data["payload"]) # Konversi string JSON kembali ke dict
                events_list.append(event_data)
                
            return {"topic": topic, "events": events_list}
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error mengambil data: {e}")

@app.get("/stats")
async def get_stats():
    """
    Menampilkan statistik operasional.
    'unique_processed' dan 'topics' diambil dari state yang persisten/di-load.
    'received' dan 'duplicate_dropped' adalah in-memory dan akan reset saat restart.
    """
    uptime_delta = datetime.now() - app_state["start_time"]
    
    # PERBAIKAN: Hapus logika load-ulang dari sini.
    # get_stats() harus melaporkan state 'in-memory' saat ini,
    # yang sudah diinisialisasi oleh 'lifespan' dan diperbarui oleh 'consumer'.
    
    # if os.path.exists(DATABASE_FILE):
    #     ... (Logika yang dihapus) ...
    
    return {
        "received": app_state["stats"]["received"],
        "unique_processed": app_state["stats"]["unique_processed"],
        "duplicate_dropped": app_state["stats"]["duplicate_dropped"],
        "topics": list(app_state["stats"]["topics"]),
        "uptime": f"{uptime_delta.total_seconds():.2f}s"
    }

# --- Untuk menjalankan (jika file ini dieksekusi langsung) ---
if __name__ == "__main__":
    import uvicorn
    # Inisialisasi DB secara manual jika dijalankan langsung
    init_db() 
    print("Menjalankan server Uvicorn di http://127.0.D0.1:8080")
    uvicorn.run(app, host="0.0.0.0", port=8080)

