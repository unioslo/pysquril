"""
Internal connection and session management module.

INTERNAL MODULE - DO NOT IMPORT DIRECTLY.
This module is internal (underscore prefix) and should not be imported directly
by applications. Import from the top-level pysquril package instead:

    from pysquril import (
        sqlite_init, postgres_init, sqlite_session, postgres_session,
        async_sqlite_init, async_postgres_init, async_sqlite_session, async_postgres_session
    )

Contains:
- Synchronous connection initialization and session management for SQLite/PostgreSQL
- Asynchronous connection initialization and session management for SQLite/PostgreSQL
"""

import inspect
import sqlite3
from contextlib import contextmanager, asynccontextmanager
from typing import ContextManager, AsyncContextManager

import psycopg2
import psycopg2.extensions
import psycopg2.pool


# =============================================================================
# Synchronous Connection Management
# =============================================================================

def sqlite_init(path: str) -> sqlite3.Connection:
    """
    Initialize a synchronous SQLite connection.

    Args:
        path: Path to SQLite database file (or ":memory:" for in-memory)

    Returns:
        SQLite connection object
    """
    engine = sqlite3.connect(path)
    return engine


def postgres_init(
    dbconfig: dict,
    min_conn: int = 1,
    max_conn: int = 5
) -> psycopg2.pool.SimpleConnectionPool:
    """
    Initialize a synchronous PostgreSQL connection pool.

    Args:
        dbconfig: Dictionary with keys: dbname, user, pw, host, and optional port
        min_conn: Minimum number of connections in pool (default: 1)
        max_conn: Maximum number of connections in pool (default: 5)

    Returns:
        PostgreSQL connection pool
    """
    # Build connection parameters as kwargs (more reliable than DSN string for psycopg2)
    conn_params = {
        "dbname": dbconfig['dbname'],
        "user": dbconfig['user'],
        "password": dbconfig['pw'],
        "host": dbconfig['host'],
    }

    # Add port if specified
    if 'port' in dbconfig and dbconfig['port']:
        conn_params["port"] = dbconfig['port']

    pool = psycopg2.pool.SimpleConnectionPool(min_conn, max_conn, **conn_params)
    return pool


@contextmanager
def sqlite_session(
    engine: sqlite3.Connection,
) -> ContextManager[sqlite3.Cursor]:
    """
    Context manager for synchronous SQLite sessions.

    Handles cursor creation, commit/rollback, and cleanup.

    Args:
        engine: SQLite connection object

    Yields:
        SQLite cursor for executing queries
    """
    session = engine.cursor()
    try:
        yield session
        engine.commit()
    except Exception as e:
        engine.rollback()
        raise e
    finally:
        session.close()


@contextmanager
def postgres_session(
    pool: psycopg2.pool.SimpleConnectionPool,
) -> ContextManager[psycopg2.extensions.cursor]:
    """
    Context manager for synchronous PostgreSQL sessions.

    Handles connection pooling, cursor creation, commit/rollback, and cleanup.

    Args:
        pool: PostgreSQL connection pool

    Yields:
        PostgreSQL cursor for executing queries
    """
    engine = pool.getconn()
    session = engine.cursor()
    try:
        yield session
        engine.commit()
    except Exception as e:
        engine.rollback()
        raise e
    finally:
        session.close()
        pool.putconn(engine)


# =============================================================================
# Asynchronous Connection Management
# =============================================================================

async def async_sqlite_init(path: str):
    """
    Initialize an asynchronous SQLite connection.

    Args:
        path: Path to SQLite database file (or ":memory:" for in-memory)

    Returns:
        Async SQLite connection object (aiosqlite.Connection)
    """
    try:
        import aiosqlite
    except ImportError:
        raise ImportError(
            "aiosqlite is required for async SQLite support. "
            "Install with: pip install pysquril[async]"
        )

    engine = await aiosqlite.connect(path)
    return engine


async def async_postgres_init(
    dbconfig: dict,
    min_conn: int = 1,
    max_conn: int = 5
):
    """
    Initialize an asynchronous PostgreSQL connection pool.

    Args:
        dbconfig: Dictionary with keys: dbname, user, pw, host, and optional port
        min_conn: Minimum number of connections in pool (default: 1)
        max_conn: Maximum number of connections in pool (default: 5)

    Returns:
        Async PostgreSQL connection pool (psycopg_pool.AsyncConnectionPool)
    """
    try:
        from psycopg_pool import AsyncConnectionPool
    except ImportError:
        raise ImportError(
            "psycopg and psycopg-pool are required for async PostgreSQL support. "
            "Install with: pip install pysquril[async]"
        )

    # Build connection parameters as kwargs (consistent with sync version)
    conn_params = {
        "dbname": dbconfig['dbname'],
        "user": dbconfig['user'],
        "password": dbconfig['pw'],
        "host": dbconfig['host'],
    }

    # Add port if specified
    if 'port' in dbconfig and dbconfig['port']:
        conn_params["port"] = dbconfig['port']

    pool_kwargs = {"kwargs": conn_params, "min_size": min_conn, "max_size": max_conn}
    # open=False avoids the deprecation RuntimeWarning that AsyncConnectionPool
    # emits when the pool opens itself in the constructor. Only pass it if the
    # installed AsyncConnectionPool accepts the argument, as it could be removed
    # once opening the pool in the constructor is dropped.
    if "open" in inspect.signature(AsyncConnectionPool).parameters:
        pool_kwargs["open"] = False
    pool = AsyncConnectionPool(**pool_kwargs)
    await pool.open()
    return pool


@asynccontextmanager
async def async_sqlite_session(engine) -> AsyncContextManager:
    """
    Async context manager for SQLite sessions.

    Handles cursor creation, commit/rollback, and cleanup.

    Args:
        engine: Async SQLite connection object (aiosqlite.Connection)

    Yields:
        Async SQLite cursor for executing queries
    """
    session = await engine.cursor()
    try:
        yield session
        await engine.commit()
    except Exception as e:
        await engine.rollback()
        raise e
    finally:
        await session.close()


@asynccontextmanager
async def async_postgres_session(pool) -> AsyncContextManager:
    """
    Async context manager for PostgreSQL sessions.

    Handles connection pooling, cursor creation, commit/rollback, and cleanup.

    Args:
        pool: Async PostgreSQL connection pool (psycopg_pool.AsyncConnectionPool)

    Yields:
        Async PostgreSQL cursor for executing queries
    """
    async with pool.connection() as engine:
        async with engine.cursor() as session:
            try:
                yield session
                await engine.commit()
            except Exception as e:
                await engine.rollback()
                raise e
