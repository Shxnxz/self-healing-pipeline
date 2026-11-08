# Build context is the repo root (see docker-compose.yml) so the incident
# contract in agent/memory/incidents.py is shared, not copied.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY lookup/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY agent/__init__.py agent/__init__.py
COPY agent/memory agent/memory
COPY lookup/*.py ./

CMD ["python", "consumer.py"]
