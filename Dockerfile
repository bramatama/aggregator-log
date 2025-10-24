# Gunakan base image yang disarankan di soal
FROM python:3.11-slim

# Set working directory di dalam container
WORKDIR /app

# Buat user non-root 'appuser' seperti yang disarankan
RUN adduser --disabled-password --gecos "" appuser

# Salin file dependensi
COPY requirements.txt .

# Install dependensi
RUN pip install --no-cache-dir -r requirements.txt

# Salin kode aplikasi (folder src) ke WORKDIR
COPY src/ ./src/

# Berikan kepemilikan file ke 'appuser'
RUN chown -R appuser:appuser /app

# Install curl untuk healthcheck
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Ganti ke user non-root
USER appuser

# Expose port yang digunakan aplikasi
EXPOSE 8080

# Perintah untuk menjalankan aplikasi
# Ini adalah cara standar production untuk Uvicorn
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080"]
