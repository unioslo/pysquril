
import datetime
import json
import logging
import sqlite3

from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Union, ContextManager, Iterable, Optional

import psycopg2
import psycopg2.extensions
import psycopg2.pool

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
    def table_insert(self, table_name: str, data: Union[dict, list]) -> bool:
        pass

    @abstractmethod
    def table_update(self, table_name: str, uri: str, data: dict) -> bool:
        pass

    @abstractmethod
    def table_delete(self, table_name: str, uri: str) -> bool:
        pass

    @abstractmethod
    def table_select(self, table_name: str, uri: str, data: Optional[Union[dict, list]] = None) -> Iterable[tuple]:
        pass


class SqliteBackend(DatabaseBackend):

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
        verbose: bool =False,
        schema: str = None,
        requestor: str = None,
    ) -> None:
        self.engine = engine
        self.verbose = verbose
        self.table_definition = '(data json unique not null)'
        self.schema = schema if schema else ""
        self.sep = "_" if self.schema else ""
        self.requestor = requestor

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

    def table_insert(self, table_name: str, data: Union[dict, list]) -> bool:
        try:
            dtype = type(data)
            insert_stmt = f'insert into "{self.schema}{self.sep}{table_name}" (data) values (?)'
            target = []
            if dtype is list:
                for element in data:
                    target.append((json.dumps(element),))
            elif dtype is dict:
                target.append((json.dumps(data),))
            try:
                with sqlite_session(self.engine) as session:
                    session.executemany(insert_stmt, target)
                return True
            except (sqlite3.ProgrammingError, sqlite3.OperationalError) as e:
                with sqlite_session(self.engine) as session:
                    session.execute(f'create table if not exists "{self.schema}{self.sep}{table_name}" {self.table_definition}')
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

    def table_update(self, table_name: str, uri_query: str, data: dict) -> bool:
        old = list(self.table_select(table_name, uri_query, data=data))
        sql = self.generator_class(f'"{self.schema}{self.sep}{table_name}"', uri_query, data=data)
        with sqlite_session(self.engine) as session:
            session.execute(sql.update_query)
        audit_data = []
        for val in old:
            audit_data.append({
                'timestamp': datetime.datetime.now().isoformat(),
                'diff': data,
                'previous': val,
                'identity': self.requestor
            })
        self.table_insert(f'{table_name}_audit', audit_data)
        return True

    def table_delete(self, table_name: str, uri_query: str) -> bool:
        sql = self.generator_class(f'"{self.schema}{self.sep}{table_name}"', uri_query)
        with sqlite_session(self.engine) as session:
            try:
                session.execute(sql.delete_query)
            except sqlite3.OperationalError as e:
                logging.error(f'Syntax error?: {sql.delete_query}')
                raise e
        if not uri_query:
            sql = self.generator_class(f'"{self.schema}{self.sep}{table_name}_audit"', uri_query)
            try:
                with sqlite_session(self.engine) as session:
                    session.execute(sql.delete_query)
            except sqlite3.OperationalError:
                pass # alright if not exists
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


class PostgresBackend(object):

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
        self.pool = pool
        self.verbose = verbose
        self.table_definition = '(data jsonb not null, uniq text unique not null)'
        self.schema = schema if schema else 'public'
        self.requestor = requestor

    def initialise(self) -> Optional[bool]:
        try:
            with postgres_session(self.pool) as session:
                for stmt in self.generator_class.db_init_sql:
                        session.execute(stmt)
        except psycopg2.InternalError as e:
            pass # throws a tuple concurrently updated when restarting many processes
        return True

    def tables_list(self, exclude_endswith: list = [], only_endswith: Optional[str] = None, remove_pattern: Optional[str] = None) -> list:
        query = f"""select table_name from information_schema.tables
            where table_schema = '{self.schema}' order by table_name asc"""
        with postgres_session(self.pool) as session:
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

    def table_insert(self, table_name: str, data: Union[dict, list]) -> bool:
        try:
            dtype = type(data)
            insert_stmt = f'insert into {self.schema}{self.sep}"{table_name}" (data) values (%s)'
            target = []
            if dtype is list:
                for element in data:
                    target.append((json.dumps(element),))
            elif dtype is dict:
                target.append((json.dumps(data),))
            try:
                with postgres_session(self.pool) as session:
                    session.executemany(insert_stmt, target)
                return True
            except (psycopg2.ProgrammingError, psycopg2.OperationalError) as e:
                table_create = f'create table if not exists {self.schema}{self.sep}"{table_name}"{self.table_definition}'
                trigger_create = f"""
                    create trigger ensure_unique_data before insert on {self.schema}{self.sep}"{table_name}"
                    for each row execute procedure unique_data()"""
                with postgres_session(self.pool) as session:
                    session.execute(f'create schema if not exists {self.schema}')
                    session.execute(table_create)
                    session.execute(trigger_create)
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

    def table_update(self, table_name: str, uri_query: str, data: dict) -> bool:
        old = list(self.table_select(table_name, uri_query, data=data))
        sql = self.generator_class(f'{self.schema}{self.sep}"{table_name}"', uri_query, data=data)
        with postgres_session(self.pool) as session:
            session.execute(sql.update_query)
        audit_data = []
        for val in old:
            audit_data.append({
                'timestamp': datetime.datetime.now().isoformat(),
                'diff': data,
                'previous': val,
                'identity': self.requestor
            })
        self.table_insert(f'{table_name}_audit', audit_data)
        return True

    def table_delete(self, table_name: str, uri_query: str) -> bool:
        sql = self.generator_class(f'{self.schema}{self.sep}"{table_name}"', uri_query)
        with postgres_session(self.pool) as session:
            session.execute(sql.delete_query)
        if not uri_query:
            sql = self.generator_class(f'{self.schema}{self.sep}"{table_name}_audit"', uri_query)
            try:
                with postgres_session(self.pool) as session:
                    session.execute(sql.delete_query)
            except psycopg2.errors.UndefinedTable:
                pass # alright if not exists
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
        with postgres_session(self.pool) as session:
            session.execute(query)
            for row in session:
                yield row[0]
