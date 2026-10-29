"""
Internal synchronous backend implementations.

INTERNAL MODULE - DO NOT IMPORT DIRECTLY.
This module is internal (underscore prefix) and should not be imported directly
by applications. Import from the top-level pysquril package instead:

    from pysquril import SqliteBackend, PostgresBackend, GenericBackend

Contains sync implementations of:
- GenericBackend: Common sync I/O operations
- SqliteBackend: SQLite-specific implementation
- PostgresBackend: PostgreSQL-specific implementation
"""

import datetime
import json
import logging
import sqlite3
import uuid

from contextlib import contextmanager
from datetime import timedelta
from typing import Union, Iterable, Optional, Any, Callable
from urllib.parse import unquote
from uuid import uuid4

import psycopg2
import psycopg2.extensions
import psycopg2.pool

from pysquril._backends_core import BackendCore, DatabaseBackend, AuditTransaction
from pysquril._connection import sqlite_session, postgres_session
from pysquril.exc import DataIntegrityError, OperationNotPermittedError
from pysquril.generator import SqliteQueryGenerator, PostgresQueryGenerator
from pysquril.utils import audit_table, audit_table_src, AUDIT_SEPARATOR, AUDIT_SUFFIX


# AuditTransaction and DatabaseBackend are imported from _backends_core
# BackendCore provides the shared business logic


class GenericBackend(BackendCore):
    """
    Synchronous backend implementation with I/O operations.

    Inherits shared business logic from BackendCore and implements
    the synchronous I/O operations using standard sync Python.
    """

    def _session_func(self) -> Callable:
        raise NotImplementedError

    # _diff_entries and _get_pk_value are now in BackendCore

    def _audit_source_exists(self, table_name: str) -> bool:
        """
        Check if the table from which audit records originate
        still exists.

        Deliberately runs on its own session: the probe may query a table
        that no longer exists, and on PostgreSQL a caught error would
        otherwise abort the caller's transaction.
        """
        exists = False
        try:
            current_data = list(self.table_select(audit_table_src(table_name), ""))
            exists = True
        except (sqlite3.OperationalError, psycopg2.errors.UndefinedTable):
            pass
        return exists

    def table_restore(
        self,
        table_name: str,
        uri_query: str,
        session: Optional[Union[sqlite3.Cursor, psycopg2.extensions.cursor]] = None,
    ) -> dict:
        """
        Restore rows to previous states, as recorded in the audit log.

        This can be either: undoing a delete, or updating a current
        record to a state prior to a specific update. The desired
        state is specified by means of a URI query. If the query
        yields multiple states of the same row, then the restore
        function will choose the oldest state, and ignore newer ones.

        Clients must provide the primary key which is used
        as the unique identifier, so we are able to identify the current
        row to which we need to apply changes.

        Examples:

        - restore one or more rows to a state prior to a specific
          update call:

            ?restore&where=transaction_id=eq.uuid&primary_key=key_name

        - restore a specific row to a prior state:

            ?restore&where=event_id=eq.uuid&primary_key=key_name

        - restore all rows to their first state after insert, before
          update or deletion:

            ?restore&primary_key=key_name

        When deleted rows are restored the event is recorded in the
        audit log as "restore" events. Such events are ignored by
        this function: that is, restore events are not restored,
        only updates and deletes.

        If a session is passed, the entire restore runs on that session
        and commits or rolls back as a single unit.

        """
        work_done = {"restores": [], "updates": []}

        query_parts = uri_query.split("&")
        if not query_parts or "restore" not in query_parts:
            return work_done

        # ensure we have enough information
        sql = self.generator_class("", uri_query)
        message = sql.message
        primary_key = sql.parsed_uri_query.primary_key

        # fetch the desired state
        if "order" in query_parts:
            uri_query = uri_query.split("&order")[0]
        uri_query = f"{uri_query}&order=timestamp.asc"  # sorted from old to new
        target_data = list(
            self.table_select(audit_table(table_name), uri_query, session=session)
        )
        if not target_data:
            return work_done  # nothing to do

        with self._session_scope(session) as session:
            self._restore_entries(
                table_name,
                uri_query,
                primary_key,
                target_data,
                message,
                session,
                work_done,
            )
        return work_done

    def _restore_entries(
        self,
        table_name: str,
        uri_query: str,
        primary_key: str,
        target_data: list,
        message: Optional[str],
        session: Union[sqlite3.Cursor, psycopg2.extensions.cursor],
        work_done: dict,
    ) -> None:
        """Apply restore entries onto a single, caller-owned session."""
        tsc = AuditTransaction(self.requestor, message, self.requestor_name)
        # ensure the table exists, may have been deleted
        session.execute("SAVEPOINT pysquril_restore_create")
        try:
            self.table_create(table_name, session)
        except psycopg2.errors.DuplicateObject:
            session.execute("ROLLBACK TO SAVEPOINT pysquril_restore_create")
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
            result = list(
                self.table_select(
                    table_name,
                    f"where={primary_key}=eq.{pk_value}",
                    session=session,
                )
            )
            if len(result) > 1:
                raise DataIntegrityError(f"primary_key: {primary_key} is not unique")
            elif not result:
                # then it is currently deleted
                self.table_insert(table_name, target_entry, session)
                self.table_insert(
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
                    # note: query construction depends on the constraint
                    # that only top-level keys are allowed in set operations
                    keys = set(to_change.keys()).union(to_add.keys())
                    to_change.update(to_add)
                    set_query = f"set={','.join(to_change.keys())}&where={primary_key}=eq.{pk_value}"
                    self.table_update(
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
                    self.table_update(
                        table_name,
                        set_query,
                        data=None,
                        tsc=tsc,
                        session=session,
                    )
                    work_done["updates"].append(entry)
            handled.append(pk_value)

    def _tables_in_schemas(self, table_name: str) -> list:
        """
        Return a list of all existing instances of the {table_name}
        across all schemas, including the schema name: {schema}.{table_name}.

        """
        raise NotImplementedError

    def _create_all_view(
        self,
        view_name: str,
        unions: str,
        session: Union[sqlite3.Cursor, psycopg2.extensions.cursor],
    ) -> None:
        """
        Backend specific implementation of view creation.

        """
        raise NotImplementedError

    def _fqtn(
        self,
        table_name: str,
        schema_name: Optional[str] = None,
        no_schema: bool = False,
    ) -> str:
        """
        Return a fully qualified table name - qualified with the schema.

        """
        raise NotImplementedError

    def _define_all_view(self, table_name: str) -> None:
        """
        To allow queries across all schemas, for a given table.

        The method is optionally called as part of table_insert,
        and table_delete - when a new table is created, and when
        an existing table is deleted.

        It works like this: suppose there are no tables at all.
        The first insert is done into table p11.A, and
        _define_all_view(A) is called. This creates a view named
        all.A - a view defined in the schema named 'all', with
        the name of the table which is being created. The view
        returns all data from tables named A in all schemas. At present
        it would be: create or replace view all.A as select * from p11.A;.

        Now the first insert is done into a new table in another schema p12 ->
        _define_all_view(A) is called. Now the view called
        all.A is updated to be the union of p11.A and p12.A:
        create or replace view all.A as select * from p11.A
        union all select * from p12.A;.

        Upon deletion of table p12.A, the view definition is
        updated to remove the reference to the deleted table.

        With the view in place, a caller can do table_select(all.{table_name})
        to query a given table across all schemas.

        """
        tables = self._tables_in_schemas(table_name)
        if not tables:
            return  # when the last one is deleted
        unions = " union all ".join([f"select * from {t}" for t in tables])
        view_name = self._fqtn(table_name, schema_name="all")
        session_func = self._session_func()
        with session_func(self.engine) as session:
            self._create_all_view(view_name, unions, session)

    # _query_for_select and _query_for_select_many are now in BackendCore

    @contextmanager
    def _session_scope(
        self,
        session: Optional[Union[sqlite3.Cursor, psycopg2.extensions.cursor]] = None,
    ) -> Iterable[Union[sqlite3.Cursor, psycopg2.extensions.cursor]]:
        """
        Yield the caller's session, or open (and commit/rollback) a fresh one.

        Lets methods share a single body whether or not a session was passed.
        """
        if session is not None:
            yield session
        else:
            with self._session_func()(self.engine) as session:
                yield session

    def _yield_results(
        self,
        query: str,
        session: Optional[Union[sqlite3.Cursor, psycopg2.extensions.cursor]] = None,
    ) -> Iterable[tuple]:
        with self._session_scope(session) as session:
            session.execute(query)
            for row in session:
                yield self._decode_row(row)

    def _decode_row(self, row: Any) -> Any:
        raise NotImplementedError

    def _is_audit_table(self, table_name: str) -> bool:
        """
        Determine whether a given table is an audit table.

        Deliberately runs on its own session: the probe may query a table
        that no longer exists, and on PostgreSQL a caught error would
        otherwise abort the caller's transaction.
        """
        sufficient = False
        neccesary = table_name.endswith(AUDIT_SEPARATOR + AUDIT_SUFFIX)
        if not neccesary:
            return neccesary and sufficient
        try:
            dummy_event = AuditTransaction("").event_read(query="")
            result = list(
                self._yield_results(
                    f"select data from {self._fqtn(table_name)} limit 1"
                )
            )[0]
            sufficient = (
                uuid.UUID(result.get("transaction_id"))
                and uuid.UUID(result.get("event_id"))
                and result.get("event") in ["update", "delete", "read", "create"]
                and result.get("timestamp") is not None
                and set(result.keys()).difference(dummy_event.keys()) == set()
            )

        except Exception as e:
            pass
        return neccesary and sufficient

    def table_select(
        self,
        table_name: str,
        uri_query: str,
        data: Optional[Union[dict, list]] = None,
        exclude_endswith: list = [],
        audit: bool = False,
        session: Optional[Union[sqlite3.Cursor, psycopg2.extensions.cursor]] = None,
    ) -> Iterable[tuple]:
        """
        Yield a resulset associated with a table_name, and a uri_query.

        The table_name can be either a reference to a specific table, or an
        asterisk expression intended to match a set of table names. In the
        latter case, a query is constructed to union the resulsets from
        all the relevant tables together.

        Optionally exclude tables that end with a specific pattern.

        If a session is passed, the query is executed on that session
        (allowing read-after-write within a single connection/transaction).
        The results are yielded lazily, so the caller must fully consume the
        returned iterable before closing the session.

        """
        apply_cutoff = (
            self._is_audit_table(table_name)
            and not self._audit_source_exists(table_name)
            and self.backup_days is not None
        )
        if "*" in table_name:
            tables = self.tables_list(
                exclude_endswith=exclude_endswith,
                table_like=table_name,
                session=session,
            )
            if not tables:
                return iter([])
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
                self.table_create(audit_table(table_name), session)
            self.table_insert(
                audit_table(table_name),
                tsc.event_read(query=uri_query),
                session=session,
            )
        return self._yield_results(query, session=session)

    def table_delete(
        self,
        table_name: str,
        uri_query: str,
        update_all_view: Optional[bool] = False,
        audit: bool = True,
        session: Optional[Union[sqlite3.Cursor, psycopg2.extensions.cursor]] = None,
    ) -> bool:
        """
        Delete the intended data from the table if a uri_query
        is present, otherwise drop the table. All deletions
        are recorded in the audit log by default, but this can
        be disabled by callers. This is useful for cases where
        data is being deleted completely for compliance purposes.

        """
        audit_data = []
        sql = self.generator_class(f"{self._fqtn(table_name)}", uri_query)
        is_audit_table = self._is_audit_table(table_name)
        if audit:
            tsc = AuditTransaction(self.requestor, sql.message, self.requestor_name)
            for row in self.table_select(table_name, uri_query, session=session):
                audit_data.append(
                    tsc.event_delete(diff=None, previous=row, query=uri_query)
                )
        with self._session_scope(session) as session:
            session.execute(sql.delete_query)
            if not is_audit_table and audit:
                self.table_create(audit_table(table_name), session)
                self.table_insert(audit_table(table_name), audit_data, session)
        if update_all_view:
            self._define_all_view(table_name)
        return True

    def _do_update(
        self,
        session: Union[sqlite3.Cursor, psycopg2.extensions.cursor],
        query: str,
    ) -> None:
        raise NotImplementedError

    def table_update(
        self,
        table_name: str,
        uri_query: str,
        data: dict,
        tsc: Optional[AuditTransaction] = None,
        session: Optional[Union[sqlite3.Cursor, psycopg2.extensions.cursor]] = None,
    ) -> bool:
        """
        Update one or more keys, recording changes in the audit log.

        """
        if self._is_audit_table(table_name):
            raise OperationNotPermittedError("audit tables cannot be altered directly")
        audit_data = []
        sql = self.generator_class(f"{self._fqtn(table_name)}", uri_query, data=data)
        tsc = (
            AuditTransaction(self.requestor, sql.message, self.requestor_name)
            if not tsc
            else tsc
        )
        for val in self.table_select(table_name, uri_query, data=data, session=session):
            audit_data.append(
                tsc.event_update(diff=data, previous=val, query=uri_query)
            )
        with self._session_scope(session) as session:
            self._do_update(session, sql.update_query)
            self.table_create(audit_table(table_name), session)
            self.table_insert(audit_table(table_name), audit_data, session)
        return True

    def table_alter(
        self,
        table_name: str,
        uri_query: str,
        session: Optional[Union[sqlite3.Cursor, psycopg2.extensions.cursor]] = None,
    ) -> dict:
        """
        Alter the name of a table, and its audit table (if it exists).
        Return information about which tables were altered.

        If a session is passed, both alters are executed on that session,
        so they commit or roll back together. A savepoint isolates the
        audit table alter, allowing a missing audit table to be tolerated
        without aborting the surrounding transaction.

        """
        if self._is_audit_table(table_name):
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
        altered = {"tables": [table_name]}
        with self._session_scope(session) as session:
            session.execute(sql.alter_query)
            session.execute("SAVEPOINT pysquril_alter_audit")
            try:
                session.execute(audit_sql.alter_query)
                altered["tables"].append(audit_table_name)
            except (psycopg2.errors.UndefinedTable, sqlite3.OperationalError):
                session.execute("ROLLBACK TO SAVEPOINT pysquril_alter_audit")
        return altered

    def _audit_insert(
        self,
        table_name: str,
        data: Union[str, list],
        session: Optional[Union[sqlite3.Cursor, psycopg2.extensions.cursor]] = None,
    ) -> bool:
        tsc = AuditTransaction(
            identity=self.requestor, identity_name=self.requestor_name
        )
        audit_data = []
        if type(data) is list:
            for row in data:
                audit_data.append(tsc.event_create(diff=row))
        else:
            audit_data = [tsc.event_create(diff=data)]
        self.table_insert(audit_table(table_name), audit_data, session=session)
        return True


class SqliteBackend(GenericBackend):
    """
    This backend works reliably, and offers decent read-write
    performance to API clients under the following conditions:

    a) using network storage, like NFS

        - using rollback-mode for transactions
        - where clients do not perform long-running read operations

    b) not using network storage

        - using WAL-mode for transactions
        - clients can perform long-running reads without
          blocking writers

    Briefly, WAL-mode (which offer non-blocking read/write) cannot
    be reliably used over NFS, because the locking primitives
    used by SQLite to prevent DB corruption are not implemented. So
    if you are using NFS, you must use rollback-mode. This however,
    means that writers require exclusive locks to write, which means
    that long-running reads by clients would block writers. This
    may be unfortunate, depending on the use case.

    For more background refer to:

        - https://www.sqlite.org/lockingv3.html
        - https://www.sqlite.org/wal.html
        - https://www.sqlite.org/threadsafe.html

    """

    generator_class = SqliteQueryGenerator
    json_object_func = "json_object"

    def __init__(
        self,
        engine: sqlite3.Connection,
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
        return sqlite_session

    def _fqtn(
        self,
        table_name: str,
        schema_name: Optional[str] = None,
        no_schema: bool = False,
    ) -> str:
        """
        Return a fully qualified table name - qualified with the schema.

        """
        schema = schema_name or self.schema
        return f'"{schema}{self.sep}{table_name}"'

    def _tables_in_schemas(self, table_name: str) -> list:
        """
        Return a list of all existing instances of the {table_name}
        across all schemas, including the schema name: {schema}.{table_name}.

        """
        with sqlite_session(self.engine) as session:
            res = session.execute(
                f"""select name FROM sqlite_master where type = 'table'
                    and name like '{self.schema_pattern}%{table_name}'
                """
            ).fetchall()
        return [r[0] for r in res] if res else []

    def _create_all_view(
        self, view_name: str, unions: str, session: sqlite3.Cursor
    ) -> None:
        session.execute(f"drop view if exists {view_name}")
        session.execute(f"create view {view_name} as {unions}")

    def initialise(self) -> Optional[bool]:
        pass

    def tables_list(
        self,
        exclude_endswith: list = [],
        only_endswith: Optional[str] = None,
        remove_pattern: Optional[str] = None,
        table_like: Optional[str] = "",
        session: Optional[sqlite3.Cursor] = None,
    ) -> list:
        table_like_filter = ""
        if table_like:
            pattern = table_like.replace("*", "%")
            table_like_filter = f"and name like '{pattern}'"
        query = f"select name FROM sqlite_master where type = 'table' {table_like_filter} order by name asc"
        with self._session_scope(session) as session:
            res = session.execute(query).fetchall()
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

    def table_create(
        self,
        table_name: str,
        session: sqlite3.Cursor,
    ) -> bool:
        session.execute(
            f"create table if not exists {self._fqtn(table_name)} {self.table_definition}"
        )
        return True

    def table_insert(
        self,
        table_name: str,
        data: Union[dict, list],
        session: Optional[sqlite3.Cursor] = None,
        update_all_view: Optional[bool] = False,
        audit: bool = False,
    ) -> bool:
        try:
            caller_session = session
            dtype = type(data)
            insert_stmt = f"insert into {self._fqtn(table_name)} (data) values (?)"
            target = []
            if dtype is list:
                for element in data:
                    target.append((json.dumps(element),))
            elif dtype is dict:
                target.append((json.dumps(data),))
            if session:
                # in this case we are re-using a session
                # from a context manager estabilshed by the caller
                # and if an exception is raised, the caller handles it
                session.executemany(insert_stmt, target)
            else:
                try:
                    with sqlite_session(self.engine) as session:
                        session.executemany(insert_stmt, target)
                except (sqlite3.ProgrammingError, sqlite3.OperationalError) as e:
                    with sqlite_session(self.engine) as session:
                        self.table_create(table_name, session)
                        session.executemany(insert_stmt, target)
                    if update_all_view:
                        self._define_all_view(table_name)
            if audit:
                self._audit_insert(table_name, data, caller_session)
            return True
        except sqlite3.IntegrityError as e:
            logging.info("Ignoring duplicate row")
            return True  # idempotent PUT
        except sqlite3.ProgrammingError as e:
            logging.error("Syntax error?")
            raise e
        except sqlite3.OperationalError as e:
            logging.error("Database issue")
            raise e
        except Exception as e:
            logging.error("Not sure what went wrong")
            raise e

    def _decode_row(self, row: Any) -> Any:
        return json.loads(row[0])

    def _do_update(self, session: sqlite3.Cursor, query: str) -> None:
        session.executescript(query)


class PostgresBackend(GenericBackend):
    """
    A PostgreSQL backend. PostgreSQL is a full-fledged
    client-server relational DB, implementing MVCC for
    concurrency control. This backend is therfore
    suitable for applications that have many simultaneous
    read and write operations, with full ACID compliance
    and no reader-writer blocking.

    For more info on MVCC, see:
    https://www.postgresql.org/docs/12/mvcc-intro.html

    """

    generator_class = PostgresQueryGenerator
    sep = "."
    json_object_func = "jsonb_build_object"

    def __init__(
        self,
        pool: psycopg2.pool.SimpleConnectionPool,
        verbose: bool = False,
        schema: str = None,
        requestor: str = None,
        backup_days: Optional[int] = None,
        schema_pattern: Optional[str] = None,
        requestor_name: Optional[str] = None,
    ) -> None:
        self.engine = pool
        self.verbose = verbose
        self.table_definition = "(data jsonb not null, uniq text unique not null)"
        self.schema = schema if schema else "public"
        self.requestor = requestor
        self.requestor_name = requestor_name
        self.backup_days = backup_days
        self.schema_pattern = schema_pattern

    def _session_func(self) -> Callable:
        return postgres_session

    def _fqtn(
        self,
        table_name: str,
        schema_name: Optional[str] = None,
        no_schema: bool = False,
    ) -> str:
        """
        Return a fully qualified table name - qualified with the schema.

        """
        if no_schema:
            return f'"{table_name}"'
        schema = schema_name or self.schema
        schema = '"all"' if schema == "all" else schema  # all is a reserved word
        return f'{schema}{self.sep}"{table_name}"'

    def _tables_in_schemas(self, table_name: str) -> list:
        """
        Return a list of all existing instances of the {table_name}
        across all schemas, including the schema name: {schema}.{table_name}.

        """
        with postgres_session(self.engine) as session:
            session.execute(
                f"""select concat_ws('.', table_schema, concat('"', table_name, '"'))
                    from information_schema.tables where table_schema
                    like '{self.schema_pattern}%' and table_name = '{table_name}'
                """
            )
            res = session.fetchall()
        return [r[0] for r in res] if res else []

    def _create_all_view(
        self, view_name: str, unions: str, session: psycopg2.extensions.cursor
    ) -> None:
        session.execute(f'create schema if not exists "all"')
        session.execute(f"create or replace view {view_name} as {unions}")

    def initialise(self) -> Optional[bool]:
        try:
            with postgres_session(self.engine) as session:
                for stmt in self.generator_class.db_init_sql:
                    session.execute(stmt)
        except psycopg2.InternalError as e:
            pass  # throws a tuple concurrently updated when restarting many processes
        return True

    def tables_list(
        self,
        exclude_endswith: list = [],
        only_endswith: Optional[str] = None,
        remove_pattern: Optional[str] = None,
        table_like: Optional[str] = "",
        session: Optional[psycopg2.extensions.cursor] = None,
    ) -> list:
        table_like_filter = ""
        if table_like:
            pattern = table_like.replace("*", "%")
            table_like_filter = f"and table_name like '{pattern}'"
        query = f"""select table_name from information_schema.tables
            where table_schema = '{self.schema}' {table_like_filter} order by table_name asc"""
        with self._session_scope(session) as session:
            session.execute(query)
            res = session.fetchall()
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

    def table_create(
        self,
        table_name: str,
        session: psycopg2.extensions.cursor,
    ) -> bool:
        table_create = f"create table if not exists {self._fqtn(table_name)}{self.table_definition}"
        trigger_create = f"""
            create trigger ensure_unique_data before insert
            on {self.schema}{self.sep}"{table_name}"
            for each row execute procedure unique_data()
        """  # change to create if not exists when pg ^v11
        session.execute(  # need to check if table exists
            f"select exists(select from pg_tables where schemaname = '{self.schema}' and tablename = '{table_name}')"
        )
        exists = session.fetchall()[0][0]
        if not exists:
            session.execute(f"create schema if not exists {self.schema}")
            session.execute(table_create)
            session.execute(trigger_create)

    def table_insert(
        self,
        table_name: str,
        data: Union[dict, list],
        session: Optional[psycopg2.extensions.cursor] = None,
        update_all_view: Optional[bool] = False,
        audit: bool = False,
    ) -> bool:
        try:
            caller_session = session
            dtype = type(data)
            insert_stmt = f"insert into {self._fqtn(table_name)} (data) values (%s)"
            target = []
            if dtype is list:
                for element in data:
                    target.append((json.dumps(element),))
            elif dtype is dict:
                target.append((json.dumps(data),))
            if session:
                # in this case we are re-using a session
                # from a context manager estabilshed by the caller
                # and if an exception is raised, the caller handles it
                session.executemany(insert_stmt, target)
            else:
                try:
                    with postgres_session(self.engine) as session:
                        session.executemany(insert_stmt, target)
                except (psycopg2.ProgrammingError, psycopg2.OperationalError) as e:
                    with postgres_session(self.engine) as session:
                        self.table_create(table_name, session)
                        session.executemany(insert_stmt, target)
                    if update_all_view:
                        self._define_all_view(table_name)
            if audit:
                self._audit_insert(table_name, data, caller_session)
            return True
        except psycopg2.IntegrityError as e:
            logging.info("Ignoring duplicate row")
            return True  # idempotent PUT
        except psycopg2.ProgrammingError as e:
            logging.error("Syntax error?")
            raise e
        except psycopg2.OperationalError as e:
            logging.error("Database issue")
            raise e
        except Exception as e:
            logging.error("Not sure what went wrong")
            raise e

    def _decode_row(self, row: Any) -> Any:
        return row[0]

    def _do_update(self, session: psycopg2.extensions.cursor, query: str) -> None:
        session.execute(query)
