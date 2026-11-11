# Build context is the repo root (see docker-compose.yml), so `agent` stays an
# importable package and later steps can reach agent/memory/incidents.py.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY agent/requirements.txt agent/requirements.txt
RUN pip install --no-cache-dir -r agent/requirements.txt

COPY agent agent

CMD ["python", "-m", "agent.consumer"]
