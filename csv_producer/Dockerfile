FROM python:3.11-slim-bookworm
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY csv_producer.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "-u", "csv_producer.py"]
