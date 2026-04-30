"""Fixtures for integration smoke tests.

These tests hand a built (sql, params) pair to a real DB driver and verify
the result roundtrips. They're skipped by default; opt in via `pytest -m integration`.
"""

import os
import sqlite3

import pytest


@pytest.fixture
def sqlite_conn():
    """In-memory SQLite connection seeded with a small employees table."""
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    cur.execute("CREATE TABLE employees (id INTEGER, name TEXT, salary INTEGER)")
    cur.executemany(
        "INSERT INTO employees VALUES (?, ?, ?)",
        [(1, "Alice", 100), (2, "Bob", 200), (3, "Carol", 300)],
    )
    cur.execute("CREATE TABLE events (id INTEGER, created_at TEXT)")
    cur.executemany(
        "INSERT INTO events VALUES (?, ?)",
        [
            (1, "2026-04-27 09:00:00"),
            (2, "2026-04-28 14:30:00"),
            (3, "2026-04-29 12:00:00"),
        ],
    )
    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def postgres_conn():
    """Postgres connection via psycopg, seeded with a small employees table.

    Skipped unless PYSQLSCRIBE_POSTGRES_DSN is set in the environment.
    """
    dsn = os.environ.get("PYSQLSCRIBE_POSTGRES_DSN")
    if not dsn:
        pytest.skip("PYSQLSCRIBE_POSTGRES_DSN not set")
    psycopg = pytest.importorskip("psycopg")
    conn = psycopg.connect(dsn)
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS employees")
        cur.execute("CREATE TABLE employees (id INTEGER, name TEXT, salary INTEGER)")
        cur.executemany(
            "INSERT INTO employees VALUES (%s, %s, %s)",
            [(1, "Alice", 100), (2, "Bob", 200), (3, "Carol", 300)],
        )
    conn.commit()
    yield conn
    with conn.cursor() as cur:
        cur.execute("DROP TABLE employees")
    conn.commit()
    conn.close()
