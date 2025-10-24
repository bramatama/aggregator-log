# Base image yang sama, kecil
FROM python:3.11-slim

WORKDIR /app

# Hanya perlu requests
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Salin skrip publisher
COPY publisher/publisher.py .

# Jalankan skrip publisher
# Skrip ini akan otomatis berjalan, mengirim event, dan exit.
CMD ["python", "publisher.py"]
