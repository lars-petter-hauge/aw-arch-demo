FROM python:3.11-slim

WORKDIR /app
COPY worker_c/requirements.txt .
RUN pip install -r requirements.txt

COPY messaging /app/messaging
RUN pip install --no-cache-dir /app/messaging
COPY base_worker /app/base_worker
COPY worker_c/worker.py .

CMD ["python", "worker.py"]
