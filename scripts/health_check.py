# health_check.py
import socket
import psycopg2
import os

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "github_analytics")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")


def check_postgres():
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            connect_timeout=3
        )
        conn.close()
        return True
    except Exception as e:
        print(f"Postgres health check failed: {e}")
        return False


def main():
    checks = {
        "postgres": check_postgres(),
    }

    if all(checks.values()):
        print("OK")
        exit(0)
    else:
        print("UNHEALTHY", checks)
        exit(1)


if __name__ == "__main__":
    main()
