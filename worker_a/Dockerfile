FROM python:3.12-slim

WORKDIR /app
COPY worker_a/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY base_worker /app/base_worker
COPY worker_a/worker.py .

CMD ["python", "worker.py"]
