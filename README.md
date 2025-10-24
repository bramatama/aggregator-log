# UTS Sistem Terdistribusi: Pub-Sub Log Aggregator

Proyek ini adalah implementasi layanan _Pub-Sub Log Aggregator_ untuk Ujian Tengah Semester Sistem Terdistribusi.

Tujuan utama layanan ini adalah menerima _event_ (log) dari _publisher_, memprosesnya secara asinkron, dan menjamin **idempotency** serta **deduplication** data. Sistem ini sepenuhnya berjalan di dalam container Docker dan menggunakan `sqlite3` sebagai _dedup store_ yang persisten.

## Fitur Utama

- **Pemrosesan Asinkron:** Menggunakan `FastAPI` + `asyncio.Queue` untuk menerima _event_ dengan cepat (`POST /publish`) dan memprosesnya di _background worker_ (`consumer`).
    
- **Idempotent Consumer (Poin B):** Satu _event_ dengan `(topic, event_id)` yang sama hanya akan diproses satu kali, bahkan jika diterima berkali-kali.
    
- **Deduplication (Poin B):** Duplikasi _event_ secara otomatis dideteksi menggunakan `PRIMARY KEY` di SQLite dan dibuang.
    
- **Persistensi & Toleransi Crash (Poin C):** _Dedup store_ (SQLite) tahan terhadap restart container. _Event_ yang sudah diproses tidak akan diproses ulang setelah sistem _crash_ atau _restart_.
    
- **Uji Skala (Poin D):** Sistem diuji menggunakan _service_ `publisher` terpisah di Docker Compose yang mengirim 5.000 _event_ (termasuk 20% duplikasi) untuk memastikan stabilitas dan responsivitas.
    
- **Unit Tested (Poin f):** Mencakup 7 _unit test_ (`pytest`) yang memverifikasi API, logika deduplikasi, persistensi, dan _stress_ kecil.
    

## Cara Menjalankan Proyek

Metode ini akan menjalankan **Poin (d) : Uji Responsivitas dengan pengujian skala besar**  secara otomatis menggunakan Docker Compose.   
Pada Docker Compose terdapat layanan Publisher yang berisi perintah untuk menjalankan load_test

### 1. Prasyarat

- Docker
    
- Docker Compose
    

### 2. Build dan Jalankan

Dari direktori _root_ proyek, jalankan perintah berikut:

```
docker-compose up --build
```

**Pada proses ini akan terjadi :**

1. **Build:** `docker-compose` akan membangun _image_ untuk `aggregator` (layanan utama) dan `publisher` (layanan penguji).
    
2. **Run `aggregator`:** Layanan `aggregator` akan dimulai, menginisialisasi database `aggregator-db-volume`, dan menunggu koneksi di port `8080`.
    
3. **Run `publisher`:** Layanan `publisher` akan menunggu hingga `aggregator` _healthy_, kemudian secara otomatis mengirimkan **5.000 event** (4.000 unik, 1.000 duplikat) untuk memenuhi **Poin (d) Skala Uji**.
    
4. **Verifikasi:** setelah event terkirim log dari `publisher` akan muncul memverifikasi bahwa statistik di `aggregator` sudah benar setelah 5.000 _event_ terkirim.
    

### 3. Mengakses Layanan

Setelah berjalan, layanan dapat diakses:

- **API Docs (Swagger):** `http://localhost:8080/docs`
    
- **Stats Endpoint:** `http://localhost:8080/stats`
    

### 4. Membersihkan

Untuk menghentikan dan menghapus container beserta _volume_ database (mereset total):

```
docker-compose down -v
```

## Cara Menjalankan Unit Tests

Anda juga dapat menjalankan 7 _unit test_ secara lokal (di luar Docker).

1. **Setup Lingkungan:**
    
    ```
    # Buat virtual environment
    python -m venv venv
    ./venv/Scripts/activate
    
    # Instal dependensi
    pip install -r requirements.txt
    ```
    
2. **Jalankan Pytest:** Dari direktori _root_ proyek, jalankan:
    
    ```
    pytest -v
    ```
    
    7 unit test akan berjalan dan `PASS`.
    

## Endpoint API

Dokumentasi lengkap tersedia di `http://localhost:8080/docs`.

- `POST /publish`: Menerima _batch_ (daftar) _event_ JSON dan memasukkannya ke antrian.
    
- `GET /events?topic={topic_name}`: Mengembalikan semua _event unik_ yang telah diproses untuk topik tersebut.
    
- `GET /stats`: Mengembalikan statistik operasional (total diterima, unik diproses, duplikat dibuang, dll).
    

## Video Demo