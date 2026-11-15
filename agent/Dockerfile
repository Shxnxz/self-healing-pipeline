# Build context is the repo root (see docker-compose.yml), so `agent` stays an
# importable package and later steps can reach agent/memory/incidents.py.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

# Validate re-runs Silver 2's own standardization in a local Spark session, which
# needs a JVM. Same JDK as the Spark jobs' image (spark-jobs/Dockerfile).
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk-headless && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /app

COPY agent/requirements.txt agent/requirements.txt
RUN pip install --no-cache-dir -r agent/requirements.txt

# Fetch the Delta jars once, at build time, so the first validation does not wait
# for Maven (and works without it).
RUN python -c "from delta import configure_spark_with_delta_pip as c; from pyspark.sql import SparkSession as S; \
c(S.builder.master('local[1]').config('spark.ui.enabled', 'false')).getOrCreate().stop()"

# The pipeline modules Validate replays through: standardize_silver2 and the
# reference mappings, exactly as the Silver 2 job runs them (agent/replay.py).
COPY spark-jobs/common.py spark-jobs/sources.py spark-jobs/ref_mappings.py spark-jobs/silver2_job.py spark-jobs/
COPY agent agent

CMD ["python", "-m", "agent.consumer"]
