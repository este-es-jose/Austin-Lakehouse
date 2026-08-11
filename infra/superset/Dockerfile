FROM apache/superset:6.0.0

USER root

RUN . /app/.venv/bin/activate && \
    uv pip install \
        "apache-superset[postgres,trino]==6.0.0"

USER superset