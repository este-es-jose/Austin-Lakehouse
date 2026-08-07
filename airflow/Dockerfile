FROM apache/airflow:3.3.0-python3.12


USER root

RUN apt-get update \
    && apt-get install \
        --yes \
        --no-install-recommends \
        openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"


USER airflow

COPY --chown=airflow:root \
    airflow/requirements.txt \
    /requirements.txt

RUN pip install \
    --no-cache-dir \
    "apache-airflow[statsd]==3.3.0" \
    -r /requirements.txt