"""
Internal asynchronous backend implementations.

INTERNAL MODULE - DO NOT IMPORT DIRECTLY.
This module is internal (underscore prefix) and should not be imported directly
by applications. Import from the top-level pysquril package instead:

    from pysquril import AsyncSqliteBackend, AsyncPostgresBackend, AsyncGenericBackend

Contains async implementations of:
- AsyncGenericBackend: Common async I/O operations
- AsyncSqliteBackend: Async SQLite-specific implementation
- AsyncPostgresBackend: Async PostgreSQL-specific implementation

Requires async extras: pip install pysquril[async]
"""

import json
import logging
import uuid

from collections.abc import AsyncIterable
from contextlib import asynccontextmanager
from typing import Any, Optional, Callable, Union

from pysquril._backends_core import BackendCore, AuditTransaction
from pysquril._connection import async_sqlite_session, async_postgres_session
from pysquril.exc import DataIntegrityError, OperationNotPermittedError
from pysquril.generator import SqliteQueryGenerator, PostgresQueryGenerator
from pysquril.utils import audit_table, audit_table_src, AUDIT_SEPARATOR, AUDIT_SUFFIX

import aiosqlite
import psycopg
import psycopg.errors


class AsyncGenericBackend(BackendCore):
    """
    Asynchronous backend implementation with async I/O operations.

    Inherits shared business logic from BackendCore and implements
    the async I/O operations using async/await patterns.
    """

    def _session_func(self) -> Callable:
        raise NotImplementedError

    # _diff_entries, _get_pk_value, _query_for_select, _query_for_select_many
    # are inherited from BackendCore

    async def _audit_source_exists(self, table_name: str) -> bool:
        """
        Check if the table from which audit records originate still exists.

        Deliberately runs on its own session: the probe may query a table
        that no longer exists, and on PostgreSQL a caught error would
        otherwise abort the caller's transaction.
        """
        exists = False
        try:
            current_data = []
            async for row in self.table_select(audit_table_src(table_name), ""):
                current_data.append(row)
                break  # Just need to know if any data exists
            exists = len(current_data) > 0
        except Exception:
            pass
        return exists

    async def table_restore(
        self, table_name: str, uri_query: str, session=None
    ) -> dict:
        """
        Restore rows to previous states, as recorded in the audit log.
        Async version of the restore operation.

        If a session is passed, the entire restore runs on that session
        and commits or rolls back as a single unit.
        """
        work_done = {"restores": [], "updates": []}

        query_parts = uri_query.split("&")
        if not query_parts or "restore" not in query_parts:
            return work_done

        sql = self.generator_class("", uri_query)
        message = sql.message
        primary_key = sql.parsed_uri_query.primary_key

        # Fetch the desired state and apply it in one session/transaction
        if "order" in query_parts:
            uri_query = uri_query.split("&order")[0]
        uri_query = f"{uri_query}&order=timestamp.asc"

        async with self._session_scope(session) as session:
            target_data = await self._select_all(
                audit_table(table_name), uri_query, session=session
            )
            if not target_data:
                return work_done
            await self._restore_entries(
                table_name,
                uri_query,
                primary_key,
                target_data,
                message,
                session,
                work_done,
            )

        return work_done

    async def _restore_entries(
        self,
        table_name: str,
        uri_query: str,
        primary_key: str,
        target_data: list,
        message: Optional[str],
        session,
        work_done: dict,
    ) -> None:
        """Apply restore entries onto a single, caller-owned session."""
        tsc = AuditTransaction(self.requestor, message, self.requestor_name)
        # ensure the table exists, may have been deleted
        await session.execute("SAVEPOINT pysquril_restore_create")
        try:
            await self.table_create(table_name, session)
        except Exception:
            await session.execute("ROLLBACK TO SAVEPOINT pysquril_restore_create")
        handled = []
        for entry in target_data:
            target_entry = entry.get("previous")
            pk_value = (
                self._get_pk_value(primary_key, target_entry) if target_entry else None
            )
            if pk_value in handled or entry.get("event") in [
                "restore",
                "create",
                "read",
            ]:
                continue
            target_entry = entry.get("previous")
            result = await self._select_all(
                table_name,
                f"where={primary_key}=eq.{pk_value}",
                session=session,
            )

            if len(result) > 1:
                raise DataIntegrityError(f"primary_key: {primary_key} is not unique")
            elif not result:
                await self.table_insert(table_name, target_entry, session)
                await self.table_insert(
                    audit_table(table_name),
                    tsc.event_restore(
                        diff=target_entry, previous=None, query=uri_query
                    ),
                    session,
                )
                work_done["restores"].append(entry)
            else:
                current_entry = result[0]
                to_change, to_remove, to_add = self._diff_entries(
                    current_entry, target_entry
                )
                if to_change or to_add:
                    to_change.update(to_add)
                    set_query = f"set={','.join(to_change.keys())}&where={primary_key}=eq.{pk_value}"
                    await self.table_update(
                        table_name,
                        set_query,
                        data=to_change,
                        tsc=tsc,
                        session=session,
                    )
                    work_done["updates"].append(entry)
                if to_remove:
                    keys = list(map(lambda x: f"-{x}", to_remove.keys()))
                    set_query = (
                        f"set={','.join(keys)}&where={primary_key}=eq.{pk_value}"
                    )
                    await self.table_update(
                        table_name,
                        set_query,
                        data=None,
                        tsc=tsc,
                        session=session,
                    )
                    work_done["updates"].append(entry)
            handled.append(pk_value)

    async def _define_all_view(self, table_name: str) -> None:
        """Create or update view for cross-schema queries."""
        tables = await self._tables_in_schemas(table_name)
        if not tables:
            return
        unions = " union all ".join([f"select * from {t}" for t in tables])
        view_name = self._fqtn(table_name, schema_name="all")
        session_func = self._session_func()
        async with session_func(self.engine) as session:
            await self._create_all_view(view_name, unions, session)

    async def _audit_insert(self, table_name: str, data, session=None) -> bool:
        """Record create events for inserted rows."""
        tsc = AuditTransaction(
            identity=self.requestor, identity_name=self.requestor_name
        )
        if isinstance(data, dict):
            data = [data]
        audit_data = [tsc.event_create(diff=entry) for entry in data]
        if session is not None:
            # ensure the audit table exists within the caller's transaction
            await self.table_create(audit_table(table_name), session)
        await self.table_insert(audit_table(table_name), audit_data, session=session)
        return True

    @asynccontextmanager
    async def _session_scope(self, session=None):
        """
        Yield the caller's session, or open (and commit/rollback) a fresh one.

        Lets methods share a single body whether or not a session was passed.
        """
        if session is not None:
            yield session
        else:
            async with self._session_func()(self.engine) as session:
                yield session

    async def _yield_results(self, query: str, session=None) -> AsyncIterable[tuple]:
        """Execute query and yield results asynchronously."""
        async with self._session_scope(session) as session:
            await session.execute(query)
            async for row in session:
                if row and row[0]:
                    yield self._decode_row(row)

    def _decode_row(self, row: Any) -> Any:
        raise NotImplementedError

    async def _select_all(self, table_name: str, uri_query: str, **kwargs) -> list:
        """Collect a table_select async iterator into a list."""
        return [row async for row in self.table_select(table_name, uri_query, **kwargs)]

    async def _is_audit_table(self, table_name: str) -> bool:
        """
        Determine whether a given table is an audit table.

        Deliberately runs on its own session: the probe may query a table
        that no longer exists, and on PostgreSQL a caught error would
        otherwise abort the caller's transaction.
        """
        sufficient = False
        necessary = table_name.endswith(AUDIT_SEPARATOR + AUDIT_SUFFIX)
        if not necessary:
            return necessary and sufficient
        try:
            dummy_event = AuditTransaction("").event_read(query="")
            result = None
            async for row in self._yield_results(
                f"select data from {self._fqtn(table_name)} limit 1"
            ):
                result = row
                break
            if result:
                sufficient = (
                    uuid.UUID(result.get("transaction_id"))
                    and uuid.UUID(result.get("event_id"))
                    and result.get("event") in ["update", "delete", "read", "create"]
                    and result.get("timestamp") is not None
                    and set(result.keys()).difference(dummy_event.keys()) == set()
                )
        except Exception:
            pass
        return necessary and sufficient

    async def table_select(
        self,
        table_name: str,
        uri_query: str,
        data: Optional[Union[dict, list]] = None,
        exclude_endswith: list = [],
        audit: bool = False,
        session=None,
    ) -> AsyncIterable[tuple]:
        """
        Yield results associated with a table_name and uri_query.

        If a session is passed, the query is executed on that session
        (allowing read-after-write within a single connection/transaction).
        Results are yielded lazily, so the caller must fully consume the
        returned async iterable before closing the session.
        """
        apply_cutoff = (
            await self._is_audit_table(table_name)
            and not await self._audit_source_exists(table_name)
            and self.backup_days is not None
        )
        if "*" in table_name:
            tables = await self.tables_list(
                exclude_endswith=exclude_endswith,
                table_like=table_name,
                session=session,
            )
            if not tables:
                return
            query = self._query_for_select_many(
                uri_query, tables, apply_cutoff=apply_cutoff
            )
        elif "," in table_name:
            tables = table_name.split(",")
            query = self._query_for_select_many(
                uri_query, tables, apply_cutoff=apply_cutoff
            )
        else:
            query = self._query_for_select(
                table_name, uri_query, data, apply_cutoff=apply_cutoff
            )

        if audit:
            tsc = AuditTransaction(
                identity=self.requestor, identity_name=self.requestor_name
            )
            if session is not None:
                # ensure the audit table exists within the caller's transaction
                await self.table_create(audit_table(table_name), session)
            await self.table_insert(
                audit_table(table_name),
                tsc.event_read(query=uri_query),
                session=session,
            )

        async for row in self._yield_results(query, session=session):
            yield row

    async def table_delete(
        self,
        table_name: str,
        uri_query: str,
        update_all_view: Optional[bool] = False,
        audit: bool = True,
        session=None,
    ) -> bool:
        """Delete data from table or drop table."""
        sql = self.generator_class(f"{self._fqtn(table_name)}", uri_query)
        is_audit_table = await self._is_audit_table(table_name)
        async with self._session_scope(session) as session:
            audit_data = []
            if audit:
                tsc = AuditTransaction(self.requestor, sql.message, self.requestor_name)
                async for row in self.table_select(
                    table_name, uri_query, session=session
                ):
                    audit_data.append(
                        tsc.event_delete(diff=None, previous=row, query=uri_query)
                    )
            await session.execute(sql.delete_query)
            if not is_audit_table and audit:
                await self.table_create(audit_table(table_name), session)
                await self.table_insert(audit_table(table_name), audit_data, session)

        if update_all_view:
            await self._define_all_view(table_name)
        return True

    async def _do_update(self, session, query: str) -> None:
        """Execute update query. Backend-specific implementation."""
        raise NotImplementedError

    async def table_update(
        self,
        table_name: str,
        uri_query: str,
        data: dict,
        tsc: Optional[AuditTransaction] = None,
        session=None,
    ) -> bool:
        """Update records in table."""
        if await self._is_audit_table(table_name):
            raise OperationNotPermittedError("audit tables cannot be altered directly")
        sql = self.generator_class(f"{self._fqtn(table_name)}", uri_query, data=data)
        async with self._session_scope(session) as session:
            if tsc is None:
                tsc = AuditTransaction(self.requestor, sql.message, self.requestor_name)
            # Build audit trail on the same session as the write
            audit_data = []
            async for val in self.table_select(
                table_name, uri_query, data=data, session=session
            ):
                audit_data.append(
                    tsc.event_update(diff=data, previous=val, query=uri_query)
                )
            await self._do_update(session, sql.update_query)
            await self.table_create(audit_table(table_name), session)
            await self.table_insert(audit_table(table_name), audit_data, session)

        return True

    async def table_alter(self, table_name: str, uri_query: str, session=None) -> dict:
        """
        Alter the name of a table, and its audit table (if it exists).

        If a session is passed, both alters run on that session so they
        commit or roll back together. A savepoint isolates the audit table
        alter, allowing a missing audit table to be tolerated without
        aborting the surrounding transaction.
        """

        # Protection: Cannot alter audit tables directly
        if await self._is_audit_table(table_name):
            raise OperationNotPermittedError("audit tables cannot be altered directly")

        sql = self.generator_class(
            f"{self._fqtn(table_name)}",
            uri_query,
            table_name_func=self._fqtn,
        )
        audit_table_name = audit_table(table_name)
        audit_sql = self.generator_class(
            f"{self._fqtn(audit_table_name)}",
            uri_query,
            table_name_func=self._fqtn,
            audit=True,
        )

        # Return structure matches sync version
        altered = {"tables": [table_name]}

        async with self._session_scope(session) as session:
            await session.execute(sql.alter_query)
            await session.execute("SAVEPOINT pysquril_alter_audit")
            try:
                await session.execute(audit_sql.alter_query)
                altered["tables"].append(audit_table_name)
            except (psycopg.errors.UndefinedTable, aiosqlite.OperationalError):
                await session.execute("ROLLBACK TO SAVEPOINT pysquril_alter_audit")

        return altered


class AsyncSqliteBackend(AsyncGenericBackend):
    """
    Async SQLite backend implementation using aiosqlite.

    Requires: pip install pysquril[async]
    """

    generator_class = SqliteQueryGenerator
    json_object_func = "json_object"

    def __init__(
        self,
        engine=None,
        verbose: bool = False,
        schema: str = None,
        requestor: str = None,
        backup_days: Optional[int] = None,
        schema_pattern: Optional[str] = None,
        requestor_name: Optional[str] = None,
    ) -> None:
        self.engine = engine
        self.verbose = verbose
        self.table_definition = "(data json unique not null)"
        self.schema = schema if schema else ""
        self.sep = "_" if self.schema else ""
        self.requestor = requestor
        self.requestor_name = requestor_name
        self.backup_days = backup_days
        self.schema_pattern = schema_pattern

    def _session_func(self) -> Callable:
        return async_sqlite_session

    def _fqtn(
        self,
        table_name: str,
        schema_name: Optional[str] = None,
        no_schema: bool = False,
    ) -> str:
        """Return fully qualified table name."""
        schema = schema_name or self.schema
        return f'"{schema}{self.sep}{table_name}"'

    async def _tables_in_schemas(self, table_name: str) -> list:
        """Return list of table instances across all schemas."""
        async with async_sqlite_session(self.engine) as session:
            await session.execute(
                f"""select name FROM sqlite_master where type = 'table'
                    and name like '{self.schema_pattern}%{table_name}'
                """
            )
            res = await session.fetchall()
        return [r[0] for r in res] if res else []

    async def _create_all_view(
        self,
        view_name: str,
        unions: str,
        session,
    ) -> None:
        """Create view for cross-schema queries."""
        await session.execute(f"drop view if exists {view_name}")
        await session.execute(f"create view {view_name} as {unions}")

    async def initialise(self) -> Optional[bool]:
        """Initialize the database connection."""
        return True

    async def tables_list(
        self,
        exclude_endswith: list = [],
        only_endswith: Optional[str] = None,
        remove_pattern: Optional[str] = None,
        table_like: Optional[str] = "",
        session=None,
    ) -> list:
        """List all tables in the database."""
        table_like_filter = ""
        if table_like:
            pattern = table_like.replace("*", "%")
            table_like_filter = f"and name like '{pattern}'"

        query = f"select name FROM sqlite_master where type = 'table' {table_like_filter} order by name asc"

        async with self._session_scope(session) as session:
            await session.execute(query)
            res = await session.fetchall()

        if not res:
            return []
        else:
            out = []
            for row in res:
                name = row[0]
                exclude = False

                # only_endswith filtering
                if only_endswith:
                    if not name.endswith(only_endswith):
                        exclude = True

                # exclude_endswith filtering
                for ends_with in exclude_endswith:
                    if name.endswith(ends_with):
                        exclude = True

                if not exclude:
                    # remove_pattern filtering
                    name = name.replace(remove_pattern, "") if remove_pattern else name
                    out.append(name)

            return out

    async def table_create(
        self,
        table_name: str,
        session: "aiosqlite.Cursor",
    ) -> bool:
        """Create a new table."""
        table_name = self._fqtn(table_name)
        await session.execute(
            f"create table if not exists {table_name} {self.table_definition}"
        )
        return True

    async def table_insert(
        self,
        table_name: str,
        data: Union[dict, list],
        session=None,
        update_all_view: Optional[bool] = False,
        audit: bool = False,
    ) -> bool:
        """Insert data into table."""

        try:
            data = [data] if isinstance(data, dict) else data
            table_name_fqtn = self._fqtn(table_name)

            if session is not None:
                # Session provided - caller handles exceptions
                for entry in data:
                    await session.execute(
                        f"insert into {table_name_fqtn} values (json(?))",
                        (json.dumps(entry),),
                    )
                if audit:
                    await self._audit_insert(table_name, data, session)
            else:
                # No session - handle table creation retry logic
                try:
                    async with self._session_scope() as session:
                        for entry in data:
                            await session.execute(
                                f"insert into {table_name_fqtn} values (json(?))",
                                (json.dumps(entry),),
                            )
                        if audit:
                            await self._audit_insert(table_name, data, session)
                except (aiosqlite.ProgrammingError, aiosqlite.OperationalError):
                    # Table doesn't exist - create and retry
                    async with self._session_scope() as session:
                        await self.table_create(table_name, session)
                        for entry in data:
                            await session.execute(
                                f"insert into {table_name_fqtn} values (json(?))",
                                (json.dumps(entry),),
                            )
                        if audit:
                            await self._audit_insert(table_name, data, session)
                    if update_all_view:
                        await self._define_all_view(table_name)

            return True
        except aiosqlite.IntegrityError:
            logging.info("Ignoring duplicate row")
            return True  # idempotent PUT
        except aiosqlite.ProgrammingError as e:
            logging.error("Syntax error?")
            raise e
        except aiosqlite.OperationalError as e:
            logging.error("Database issue")
            raise e
        except Exception as e:
            logging.error("Not sure what went wrong")
            raise e

    def _decode_row(self, row: Any) -> Any:
        return json.loads(row[0])

    async def _do_update(self, session, query: str) -> None:
        """Execute update query (may contain multiple statements)."""
        await session.executescript(query)


class AsyncPostgresBackend(AsyncGenericBackend):
    """
    Async PostgreSQL backend implementation using psycopg 3.

    Requires: pip install pysquril[async]
    """

    generator_class = PostgresQueryGenerator
    json_object_func = "jsonb_build_object"

    def __init__(
        self,
        engine=None,
        verbose: bool = False,
        schema: str = None,
        requestor: str = None,
        backup_days: Optional[int] = None,
        schema_pattern: Optional[str] = None,
        requestor_name: Optional[str] = None,
    ) -> None:
        self.engine = engine
        self.verbose = verbose
        self.table_definition = "(data jsonb not null, uniq text unique not null)"
        self.schema = schema if schema else "public"
        self.sep = "."
        self.requestor = requestor
        self.requestor_name = requestor_name
        self.backup_days = backup_days
        self.schema_pattern = schema_pattern

    def _session_func(self) -> Callable:
        return async_postgres_session

    def _fqtn(
        self,
        table_name: str,
        schema_name: Optional[str] = None,
        no_schema: bool = False,
    ) -> str:
        """Return fully qualified table name."""
        if no_schema:
            return f'"{table_name}"'
        schema = schema_name or self.schema
        schema = '"all"' if schema == "all" else schema  # all is a reserved word
        return f'{schema}{self.sep}"{table_name}"'

    async def _tables_in_schemas(self, table_name: str) -> list:
        """Return list of table instances across all schemas."""
        async with async_postgres_session(self.engine) as session:
            await session.execute(
                f"""select concat_ws('.', table_schema, concat('"', table_name, '"'))
                    from information_schema.tables where table_schema
                    like '{self.schema_pattern}%' and table_name = '{table_name}'
                """
            )
            res = await session.fetchall()
        return [r[0] for r in res] if res else []

    async def _create_all_view(
        self,
        view_name: str,
        unions: str,
        session,
    ) -> None:
        """Create view for cross-schema queries."""
        await session.execute('create schema if not exists "all"')
        await session.execute(f"create or replace view {view_name} as {unions}")

    async def initialise(self) -> Optional[bool]:
        """Initialize database by creating necessary functions and schema."""
        try:
            async with async_postgres_session(self.engine) as session:
                for stmt in self.generator_class.db_init_sql:
                    await session.execute(stmt)
        except Exception:
            pass  # throws a tuple concurrently updated when restarting many processes
        return True

    async def tables_list(
        self,
        exclude_endswith: list = [],
        only_endswith: Optional[str] = None,
        remove_pattern: Optional[str] = None,
        table_like: Optional[str] = "",
        session=None,
    ) -> list:
        """List all tables in the schema."""
        table_like_filter = ""
        if table_like:
            pattern = table_like.replace("*", "%")
            table_like_filter = f"and table_name like '{pattern}'"
        query = f"""select table_name from information_schema.tables
            where table_schema = '{self.schema}' {table_like_filter} order by table_name asc"""

        async with self._session_scope(session) as session:
            await session.execute(query)
            res = await session.fetchall()

        if not res:
            return []
        else:
            out = []
            for row in res:
                name = row[0]
                exclude = False
                if only_endswith:
                    if not name.endswith(only_endswith):
                        exclude = True
                for ends_with in exclude_endswith:
                    if name.endswith(ends_with):
                        exclude = True
                if not exclude:
                    name = name.replace(remove_pattern, "") if remove_pattern else name
                    out.append(name)
            return out

    async def table_create(
        self,
        table_name: str,
        session: "psycopg.AsyncCursor",
    ) -> bool:
        """Create a new table."""
        table_create = f"create table if not exists {self._fqtn(table_name)}{self.table_definition}"
        trigger_create = f"""
            create trigger ensure_unique_data before insert
            on {self.schema}{self.sep}"{table_name}"
            for each row execute procedure unique_data()
        """

        # Check if table exists
        await session.execute(
            f"select exists(select from pg_tables where schemaname = '{self.schema}' and tablename = '{table_name}')"
        )
        result = await session.fetchone()
        exists = result[0] if result else False

        if not exists:
            await session.execute(f"create schema if not exists {self.schema}")
            await session.execute(table_create)
            await session.execute(trigger_create)

        return True

    async def table_insert(
        self,
        table_name: str,
        data: Union[dict, list],
        session=None,
        update_all_view: Optional[bool] = False,
        audit: bool = False,
    ) -> bool:
        """Insert data into table."""
        try:
            data = [data] if isinstance(data, dict) else data
            table_name_fqtn = self._fqtn(table_name)

            if session is not None:
                # Session provided - caller handles exceptions
                for entry in data:
                    await session.execute(
                        f"insert into {table_name_fqtn} values (%s::jsonb)",
                        (json.dumps(entry),),
                    )
                if audit:
                    await self._audit_insert(table_name, data, session)
            else:
                # No session - handle table creation retry logic
                try:
                    async with self._session_scope() as session:
                        for entry in data:
                            await session.execute(
                                f"insert into {table_name_fqtn} values (%s::jsonb)",
                                (json.dumps(entry),),
                            )
                        if audit:
                            await self._audit_insert(table_name, data, session)
                except (psycopg.errors.UndefinedTable, psycopg.errors.OperationalError):
                    # Table doesn't exist - create and retry
                    async with self._session_scope() as session:
                        await self.table_create(table_name, session)
                        for entry in data:
                            await session.execute(
                                f"insert into {table_name_fqtn} values (%s::jsonb)",
                                (json.dumps(entry),),
                            )
                        if audit:
                            await self._audit_insert(table_name, data, session)
                    if update_all_view:
                        await self._define_all_view(table_name)

            return True
        except psycopg.errors.UniqueViolation:
            logging.info("Ignoring duplicate row")
            return True  # idempotent PUT
        except psycopg.errors.SyntaxError as e:
            logging.error("Syntax error?")
            raise e
        except psycopg.errors.OperationalError as e:
            logging.error("Database issue")
            raise e
        except Exception as e:
            logging.error("Not sure what went wrong")
            raise e

    def _decode_row(self, row: Any) -> Any:
        return row[0]

    async def _do_update(self, session, query: str) -> None:
        """Execute update query."""
        await session.execute(query)
