"""Connection setup shared by the independent DuckDB queries."""

from common import S3Config


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def configure_s3(con, config: S3Config, threads: int = 4) -> None:
    if threads < 1:
        raise ValueError("threads must be positive")
    con.execute("LOAD httpfs")
    con.execute(f"PRAGMA threads={threads}")
    settings = {
        "s3_endpoint": config.endpoint.removeprefix("http://").removeprefix("https://"),
        "s3_access_key_id": config.access_key,
        "s3_secret_access_key": config.secret_key,
        "s3_region": "us-east-1",
        "s3_url_style": "path",
    }
    for name, value in settings.items():
        con.execute(f"SET {name}={sql_string(value)}")
    con.execute(f"SET s3_use_ssl={'true' if config.endpoint.startswith('https://') else 'false'}")
