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

# --- Model Data (Pydantic) ---
class Event(BaseModel):
    topic: str
    event_id: str
    timestamp: str
    source: str
    payload: Dict[str, Any]

# --- State Aplikasi ---
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
    DATABASE_FILE = os.getenv("DATABASE_FILE", "aggregator.db")
    
    print(f"Menggunakan database file: {DATABASE_FILE}")
    with sqlite3.connect(DATABASE_FILE) as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS dedup_store (
            topic TEXT,
            event_id TEXT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (topic, event_id)
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_events (
            topic TEXT,
            event_id TEXT,
            timestamp TEXT,
            source TEXT,
            payload TEXT,
            UNIQUE(topic, event_id)
        )
        """)
        conn.commit()

def load_initial_stats():
    """
    Memuat statistik persisten (event unik & topik) dari DB saat startup.
    Fungsi ini HANYA memuat stats persisten.
    """
    DATABASE_FILE = os.getenv("DATABASE_FILE", "aggregator.db")
    
    app_state["stats"]["unique_processed"] = 0
    app_state["stats"]["topics"] = set()

    if not os.path.exists(DATABASE_FILE):
        return

    try:
        with sqlite3.connect(DATABASE_FILE) as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM dedup_store")
            unique_count = cursor.fetchone()[0]
            app_state["stats"]["unique_processed"] = unique_count
            
            cursor.execute("SELECT DISTINCT topic FROM dedup_store")
            topics = cursor.fetchall()
            app_state["stats"]["topics"] = {row[0] for row in topics}
            
            print(f"Loaded {unique_count} unique events and {len(topics)} topics from DB.")
    except sqlite3.OperationalError as e:
        print(f"Error memuat stats (DB mungkin terkunci atau belum siap): {e}")

def process_event_in_db(event: Event) -> bool:
    """
    Mencoba memproses dan menyimpan event.
    Ini adalah inti dari logika idempotency dan deduplication.
    
    Mengembalikan:
        True: Jika event baru dan berhasil diproses.
        False: Jika event adalah duplikat.
    """
    DATABASE_FILE = os.getenv("DATABASE_FILE", "aggregator.db")
    
    try:
        with sqlite3.connect(DATABASE_FILE) as conn:
            cursor = conn.cursor()
            
            cursor.execute(
                "INSERT INTO dedup_store (topic, event_id) VALUES (?, ?)",
                (event.topic, event.event_id)
            )
            
            cursor.execute(
                "INSERT INTO processed_events (topic, event_id, timestamp, source, payload) VALUES (?, ?, ?, ?, ?)",
                (event.topic, event.event_id, event.timestamp, event.source, json.dumps(event.payload))
            )
            
            conn.commit()
            return True
            
    except sqlite3.IntegrityError:
        return False
    except Exception as e:
        print(f"Error saat memproses event di DB: {e}")
        return False

# --- Consumer (Background Task) ---

async def consumer():
    """
    Task background yang berjalan terus-menerus (seperti Koki).
    Mengambil event dari antrian dan memprosesnya.
    """
    print("Consumer task dimulai...")
    
    while True:
        try:
            if app_state["event_queue"] is None:
                await asyncio.sleep(0.1)
                continue
                
            queue = app_state["event_queue"]
            event = await queue.get()
            
            is_new = process_event_in_db(event)
            
            if is_new:
                app_state["stats"]["unique_processed"] += 1
                app_state["stats"]["topics"].add(event.topic)
            else:
                app_state["stats"]["duplicate_dropped"] += 1
                print(f"[DUPLICATE] Event duplikat terdeteksi dan dibuang: {event.topic}/{event.event_id}")
            
            queue.task_done()
            
        except Exception as e:
            print(f"Error di consumer: {e}")
            await asyncio.sleep(1)

# --- FastAPI Lifecycle (Startup & Shutdown) ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Aplikasi startup...")
    
    print("Me-reset statistik in-memory...")
    app_state["stats"]["received"] = 0
    app_state["stats"]["duplicate_dropped"] = 0
    app_state["start_time"] = datetime.now()
    app_state["event_queue"] = asyncio.Queue()
    
    init_db()
    
    load_initial_stats()
    
    asyncio.create_task(consumer())
    
    yield
    
    print("Aplikasi shutdown...")

# --- Inisialisasi Aplikasi FastAPI ---
app = FastAPI(lifespan=lifespan)

# --- API Endpoints ---

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.post("/publish")
async def publish_events(events: List[Event], request: Request):
    """
    Menerima satu atau batch event dan memasukkannya ke antrian internal.
    """
    queue = app_state["event_queue"]
    
    if not events:
        raise HTTPException(status_code=400, detail="Event list tidak boleh kosong")
        
    for event in events:
        await queue.put(event)
        app_state["stats"]["received"] += 1
        
    return {"status": "events queued", "count": len(events)}

@app.get("/events")
async def get_events(topic: str):
    """
    Mengembalikan daftar event unik yang telah diproses untuk topik tertentu.
    Data diambil dari DB persisten.
    """
    DATABASE_FILE = os.getenv("DATABASE_FILE", "aggregator.db")
    
    try:
        with sqlite3.connect(DATABASE_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute(
                "SELECT * FROM processed_events WHERE topic = ?",
                (topic,)
            )
            rows = cursor.fetchall()
            
            events_list = []
            for row in rows:
                event_data = dict(row)
                event_data["payload"] = json.loads(event_data["payload"])
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
    
    return {
        "received": app_state["stats"]["received"],
        "unique_processed": app_state["stats"]["unique_processed"],
        "duplicate_dropped": app_state["stats"]["duplicate_dropped"],
        "topics": list(app_state["stats"]["topics"]),
        "uptime": f"{uptime_delta.total_seconds():.2f}s"
    }

if __name__ == "__main__":
    import uvicorn
    init_db() 
    print("Menjalankan server Uvicorn di http://127.0.D0.1:8080")
    uvicorn.run(app, host="0.0.0.0", port=8080)

