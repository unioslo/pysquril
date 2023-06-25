
import datetime
import json
import logging
import sqlite3

from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Union, ContextManager, Iterable, Optional, Any, Callable
from uuid import uuid4

import psycopg2
import psycopg2.extensions
import psycopg2.pool

from pysquril.exc import DataIntegrityError, ParseError
from pysquril.generator import SqliteQueryGenerator, PostgresQueryGenerator

@contextmanager
def sqlite_session(
    engine: sqlite3.Connection,
) -> ContextManager[sqlite3.Cursor]:
    session = engine.cursor()
    try:
        yield session
        session.close()
    except Exception as e:
        session.close()
        engine.rollback()
        raise e
    finally:
        session.close()
        engine.commit()


@contextmanager
def postgres_session(
    pool: psycopg2.pool.SimpleConnectionPool,
) -> ContextManager[psycopg2.extensions.cursor]:
    engine = pool.getconn()
    session = engine.cursor()
    try:
        yield session
        session.close()
    except Exception as e:
        session.close()
        engine.rollback()
        raise e
    finally:
        session.close()
        engine.commit()
        pool.putconn(engine)


class AuditTransaction(object):

    """
    Container for generating audit events.
    Keeps state for transaction IDs, generates
    timestamps, and event IDs.

    """

    def __init__(self, identity: str) -> None:
        self.identity = identity
        self.timestamp = datetime.datetime.now().isoformat()
        self.transaction_id = self._id()

    def _id(self) -> str:
        return str(uuid4())

    def _event(self, diff: Any, previous: Any, event: str) -> dict:
        return {
            "diff": diff,
            "previous": previous,
            "event": event,
            "timestamp": self.timestamp,
            "identity": self.identity,
            "event_id": self._id(),
            "transaction_id": self.transaction_id,
        }

    def event_update(self, *, diff: Any, previous: Any) -> dict:
        return self._event(diff, previous, "update")

    def event_delete(self, *, diff: Any, previous: Any) -> dict:
        return self._event(diff, previous, "delete")

    def event_restore(self, *, diff: Any, previous: Any) -> dict:
        return self._event(diff, previous, "restore")


class DatabaseBackend(ABC):

    sep: str # schema separator character

    def __init__(
        self,
        engine: Union[
            sqlite3.Connection,
            psycopg2.pool.SimpleConnectionPool,
        ],
        verbose: bool = False,
        requestor: str = None,
    ) -> None:
        super(DatabaseBackend, self).__init__()
        self.engine = engine
        self.verbose = verbose
        self.requestor = requestor

    @abstractmethod
    def initialise(self) -> Optional[bool]:
        pass

    @abstractmethod
    def tables_list(self) -> list:
        pass

    @abstractmethod
    def table_create(
        self,
        table_name: str,
        session: Union[sqlite3.Cursor, psycopg2.extensions.cursor],
    ) -> bool:
        pass

    @abstractmethod
    def table_insert(
        self,
        table_name: str,
        data: Union[dict, list],
        session: Optional[Union[sqlite3.Cursor, psycopg2.extensions.cursor]] = None,
    ) -> bool:
        pass

    @abstractmethod
    def table_update(
        self,
        table_name: str,
        uri_query: str,
        data: dict,
        tsc: Optional[AuditTransaction] = None,
        session: Optional[Union[sqlite3.Cursor, psycopg2.extensions.cursor]] = None,
    ) -> bool:
        pass

    @abstractmethod
    def table_delete(self, table_name: str, uri_query: str) -> bool:
        pass

    @abstractmethod
    def table_select(self, table_name: str, uri_query: str, data: Optional[Union[dict, list]] = None) -> Iterable[tuple]:
        pass

    @abstractmethod
    def table_restore(self, table_name: str, uri_query: str) -> bool:
        pass


class GenericBackend(DatabaseBackend):

    """Implementation of common methods for specific backends."""

    def _session_func(self) -> Callable:
        raise NotImplementedError

    def _diff_entries(self, current_entry: dict, target_entry: dict) -> dict:
        """
        Calculate the difference between two dictionaries, show the difference
        between the previous relative to current entry. E.g.:

        _diff_entries({a: 3, b: 4}, {a: 3, b: 5}) -> {b: 5}

        The diff is recorded in the new audit log when moving the state
        from current to previous.

        """
        out = {}
        for k, v in target_entry.items():
            if current_entry.get(k) != v:
                out[k] = v
        return out

    def table_restore(self, table_name: str, uri_query: str) -> dict:
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

            ?rollback&where=transaction_id=eq.uuid&primary_key=key_name

        - restore a specific row to a prior state:

            ?rollback&where=event_id=eq.uuid&primary_key=key_name

        - restore all rows to their first state after insert, before
          update or deletion:

            ?rollback&primary_key=key_name

        When deleted rows are restored the event is recorded in the
        audit log as "restore" events. Such events are ignored by
        this function: that is, restore events are not restored,
        only updates and deletes.

        """
        # ensure we have enough information
        query_parts = uri_query.split("&")
        if not query_parts:
            raise ParseError("Missing query")
        if "rollback" not in query_parts:
            raise ParseError("Missing rollback directive")
        for part in query_parts:
            has_pk = False
            if part.startswith("primary_key"):
                primary_key = part.split("=")[-1]
                if primary_key == "":
                    raise ParseError("Missing primary_key value")
                else:
                    has_pk = True
                    break
        if not has_pk:
            raise ParseError("Missing primary_key")
        # fetch a copy of the current state, and all primay keys
        table_exists = False
        try:
            current_data = list(self.table_select(table_name, ""))
            current_pks = list(self.table_select(table_name, f"select={primary_key}"))
            table_exists = True
        except (sqlite3.OperationalError, psycopg2.errors.UndefinedTable):
            current_data = []
            current_pks = []
        # fetch the desired state
        if "order" in query_parts:
            uri_query = uri_query.split("&order")[0]
        uri_query = f"{uri_query}&order=timestamp.asc" # sorted from old to new
        target_data = list(self.table_select(f"{table_name}_audit", uri_query))
        if not target_data:
            return False # nothing to do
        tsc = AuditTransaction(self.requestor)
        session_func = self._session_func()
        try:
            # ensure the table exists, may have been deleted
            with session_func(self.engine) as session:
                self.table_create(table_name, session)
        except psycopg2.errors.DuplicateObject as e:
            pass # already exists
        handled = []
        work_done = {"restores": [], "updates": []}
        with session_func(self.engine) as session:
            for entry in target_data:
                target_entry = entry.get("previous")
                pk_value = target_entry.get(primary_key) if target_entry else None
                if pk_value in handled or entry.get("event") == "restore":
                    continue
                target_entry = entry.get("previous")
                pk_value = target_entry.get(primary_key)
                result = list(
                    self.table_select(
                        table_name,
                        f"where={primary_key}=eq.{pk_value}",
                    )
                )
                if len(result) > 1:
                    raise DataIntegrityError(f"primary_key: {primary_key} is not unique")
                elif not result:
                    # then it is currently deleted
                    self.table_insert(table_name, target_entry, session)
                    self.table_insert(
                        f"{table_name}_audit",
                        tsc.event_restore(diff=target_entry, previous=None),
                        session,
                    )
                    work_done["restores"].append(entry)
                else:
                    current_entry = result[0]
                    diff = self._diff_entries(current_entry, target_entry)
                    if diff:
                        self.table_update(
                            table_name,
                            f"set={','.join(diff.keys())}&where={primary_key}=eq.{pk_value}",
                            data=diff,
                            tsc=tsc,
                            session=session,
                        )
                    work_done["updates"].append(entry)
                handled.append(pk_value)
        return work_done


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

    def __init__(
        self,
        engine: sqlite3.Connection,
        verbose: bool = False,
        schema: str = None,
        requestor: str = None,
    ) -> None:
        self.engine = engine
        self.verbose = verbose
        self.table_definition = '(data json unique not null)'
        self.schema = schema if schema else ""
        self.sep = "_" if self.schema else ""
        self.requestor = requestor

    def _session_func(self) -> Callable:
        return sqlite_session

    def initialise(self) -> Optional[bool]:
        pass

    def tables_list(self, exclude_endswith: list = [], only_endswith: Optional[str] = None, remove_pattern: Optional[str] = None) -> list:
        query = "select name FROM sqlite_master where type = 'table'  order by name asc"
        with sqlite_session(self.engine) as session:
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
        session.execute(f'create table if not exists "{self.schema}{self.sep}{table_name}" {self.table_definition}')
        return True

    def table_insert(
        self,
        table_name: str,
        data: Union[dict, list],
        session: Optional[sqlite3.Cursor] = None,
    ) -> bool:
        try:
            dtype = type(data)
            insert_stmt = f'insert into "{self.schema}{self.sep}{table_name}" (data) values (?)'
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
                return True
            else:
                try:
                    with sqlite_session(self.engine) as session:
                        session.executemany(insert_stmt, target)
                    return True
                except (sqlite3.ProgrammingError, sqlite3.OperationalError) as e:
                    with sqlite_session(self.engine) as session:
                        self.table_create(table_name, session)
                        session.executemany(insert_stmt, target)
                    return True
        except sqlite3.IntegrityError as e:
            logging.info('Ignoring duplicate row')
            return True # idempotent PUT
        except sqlite3.ProgrammingError as e:
            logging.error('Syntax error?')
            raise e
        except sqlite3.OperationalError as e:
            logging.error('Database issue')
            raise e
        except Exception as e:
            logging.error('Not sure what went wrong')
            raise e

    def table_update(
        self,
        table_name: str,
        uri_query: str,
        data: dict,
        tsc: Optional[AuditTransaction] = None,
        session: Optional[sqlite3.Cursor] = None,
    ) -> bool:
        audit_data = []
        tsc = AuditTransaction(self.requestor) if not tsc else tsc
        for val in self.table_select(table_name, uri_query, data=data):
            audit_data.append(tsc.event_update(diff=data, previous=val))
        sql = self.generator_class(f'"{self.schema}{self.sep}{table_name}"', uri_query, data=data)
        if session:
            session.execute(sql.update_query)
            self.table_insert(f'{table_name}_audit', audit_data, session)
        else:
            with sqlite_session(self.engine) as session:
                session.execute(sql.update_query)
            self.table_insert(f'{table_name}_audit', audit_data)
        return True

    def table_delete(self, table_name: str, uri_query: str) -> bool:
        audit_data = []
        tsc = AuditTransaction(self.requestor)
        for row in self.table_select(table_name, uri_query):
            audit_data.append(tsc.event_delete(diff=None, previous=row))
        sql = self.generator_class(f'"{self.schema}{self.sep}{table_name}"', uri_query)
        with sqlite_session(self.engine) as session:
            session.execute(sql.delete_query)
            if not table_name.endswith("_audit"):
                self.table_insert(f'{table_name}_audit', audit_data, session)
        return True

    def _union_queries(self, uri_query: str, tables: list) -> str:
        queries = []
        for table_name in tables:
            sql = self.generator_class(f'"{self.schema}{self.sep}{table_name}"', uri_query)
            queries.append(f"select json_object('{self.schema}{self.sep}{table_name}', ({sql.select_query}))")
        return " union all ".join(queries)

    def table_select(self, table_name: str, uri_query: str, data: Optional[Union[dict, list]] = None, exclude_endswith: list = []) -> Iterable[tuple]:
        if table_name == "*":
            tables = self.tables_list(exclude_endswith = exclude_endswith)
            if not tables:
                return iter([])
            query = self._union_queries(uri_query, tables)
        else:
            sql = self.generator_class(f'"{self.schema}{self.sep}{table_name}"', uri_query, data=data)
            query = sql.select_query
        with sqlite_session(self.engine) as session:
            for row in session.execute(query):
                yield json.loads(row[0])


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

    def __init__(
        self,
        pool: psycopg2.pool.SimpleConnectionPool,
        verbose: bool = False,
        schema: str = None,
        requestor: str = None,
    ) -> None:
        self.engine = pool
        self.verbose = verbose
        self.table_definition = '(data jsonb not null, uniq text unique not null)'
        self.schema = schema if schema else 'public'
        self.requestor = requestor

    def _session_func(self) -> Callable:
        return postgres_session

    def initialise(self) -> Optional[bool]:
        try:
            with postgres_session(self.engine) as session:
                for stmt in self.generator_class.db_init_sql:
                        session.execute(stmt)
        except psycopg2.InternalError as e:
            pass # throws a tuple concurrently updated when restarting many processes
        return True

    def tables_list(self, exclude_endswith: list = [], only_endswith: Optional[str] = None, remove_pattern: Optional[str] = None) -> list:
        query = f"""select table_name from information_schema.tables
            where table_schema = '{self.schema}' order by table_name asc"""
        with postgres_session(self.engine) as session:
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
        table_create = f'create table if not exists {self.schema}{self.sep}"{table_name}"{self.table_definition}'
        trigger_create = f"""
            create trigger ensure_unique_data before insert
            on {self.schema}{self.sep}"{table_name}"
            for each row execute procedure unique_data()
        """ # change to create if not exists when pg ^v11
        session.execute(f'create schema if not exists {self.schema}')
        session.execute(table_create)
        session.execute(trigger_create)


    def table_insert(
        self,
        table_name: str,
        data: Union[dict, list],
        session: Optional[psycopg2.extensions.cursor] = None,
    ) -> bool:
        try:
            dtype = type(data)
            insert_stmt = f'insert into {self.schema}{self.sep}"{table_name}" (data) values (%s)'
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
                return True
            else:
                try:
                    with postgres_session(self.engine) as session:
                        session.executemany(insert_stmt, target)
                    return True
                except (psycopg2.ProgrammingError, psycopg2.OperationalError) as e:
                    with postgres_session(self.engine) as session:
                        self.table_create(table_name, session)
                        session.executemany(insert_stmt, target)
                    return True
        except psycopg2.IntegrityError as e:
            logging.info('Ignoring duplicate row')
            return True # idempotent PUT
        except psycopg2.ProgrammingError as e:
            logging.error('Syntax error?')
            raise e
        except psycopg2.OperationalError as e:
            logging.error('Database issue')
            raise e
        except Exception as e:
            logging.error('Not sure what went wrong')
            raise e

    def table_update(
        self,
        table_name: str,
        uri_query: str,
        data: dict,
        tsc: Optional[AuditTransaction] = None,
        session: Optional[psycopg2.extensions.cursor] = None,
    ) -> bool:
        audit_data = []
        tsc = AuditTransaction(self.requestor) if not tsc else tsc
        for val in self.table_select(table_name, uri_query, data=data):
            audit_data.append(tsc.event_update(diff=data, previous=val))
        sql = self.generator_class(f'{self.schema}{self.sep}"{table_name}"', uri_query, data=data)
        if session:
            session.execute(sql.update_query)
            self.table_insert(f'{table_name}_audit', audit_data, session)
        else:
            with postgres_session(self.engine) as session:
                session.execute(sql.update_query)
            self.table_insert(f'{table_name}_audit', audit_data)
        return True

    def table_delete(self, table_name: str, uri_query: str) -> bool:
        audit_data = []
        tsc = AuditTransaction(self.requestor)
        for row in self.table_select(table_name, uri_query):
            audit_data.append(tsc.event_delete(diff=None, previous=row))
        sql = self.generator_class(f'{self.schema}{self.sep}"{table_name}"', uri_query)
        with postgres_session(self.engine) as session:
            session.execute(sql.delete_query)
            if not table_name.endswith("_audit"):
                self.table_insert(f'{table_name}_audit', audit_data, session)
        return True

    def _union_queries(self, uri_query: str, tables: list) -> str:
        queries = []
        for table_name in tables:
            sql = self.generator_class(f'{self.schema}{self.sep}"{table_name}"', uri_query)
            queries.append(f"select jsonb_build_object('{table_name}', ({sql.select_query}))")
        return " union all ".join(queries)

    def table_select(self, table_name: str, uri_query: str, data: Optional[Union[dict, list]] = None, exclude_endswith: list = []) -> Iterable[tuple]:
        if table_name == "*":
            tables = self.tables_list(exclude_endswith = exclude_endswith)
            if not tables:
                return iter([])
            query = self._union_queries(uri_query, tables)
        else:
            sql = self.generator_class(f'{self.schema}{self.sep}"{table_name}"', uri_query, data=data)
            query = sql.select_query
        with postgres_session(self.engine) as session:
            session.execute(query)
            for row in session:
                yield row[0]
