#image du mastodon kafka
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY mastodon_producer.py .
CMD ["python", "-u", "mastodon_producer.py"]