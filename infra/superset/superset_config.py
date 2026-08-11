import os

from urllib.parse import quote


postgres_user = quote(
    os.environ["POSTGRES_USER"],
    safe="",
)

postgres_password = quote(
    os.environ["POSTGRES_PASSWORD"],
    safe="",
)

postgres_host = os.getenv(
    "POSTGRES_HOST",
    "postgres",
)

postgres_port = os.getenv(
    "POSTGRES_PORT",
    "5432",
)

superset_database = os.getenv(
    "SUPERSET_DATABASE",
    "superset",
)


SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://"
    f"{postgres_user}:{postgres_password}"
    f"@{postgres_host}:{postgres_port}"
    f"/{superset_database}"
)

WTF_CSRF_ENABLED = True

# Local Docker uses HTTP instead of HTTPS.
TALISMAN_ENABLED = False