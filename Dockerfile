FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY collectors ./collectors

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "collectors.ais_collector"]
