
import json
import sqlite3

from typing import Callable, Union

import psycopg2
import psycopg2.extensions
import psycopg2.pool
import pytest

from termcolor import colored

from pysquril.backends import SqliteBackend, PostgresBackend, sqlite_session, postgres_session
from pysquril.generator import SqliteQueryGenerator, PostgresQueryGenerator
from pysquril.test_data import dataset

def sqlite_init(
    path: str,
    name: str = 'api-data.db',
) -> sqlite3.Connection:
    engine = sqlite3.connect(path + '/' + name)
    return engine


def postgres_init(dbconfig: dict) -> psycopg2.pool.SimpleConnectionPool:
    min_conn = 2
    max_conn = 5
    dsn = f"dbname={dbconfig['dbname']} user={dbconfig['user']} password={dbconfig['pw']} host={dbconfig['host']}"
    pool = psycopg2.pool.SimpleConnectionPool(
        min_conn, max_conn, dsn
    )
    return pool


class TestBackends(object):

    verbose = True

    data = dataset

    def run_backend_tests(
        self,
        data: list,
        engine: Union[sqlite3.Connection, psycopg2.pool.SimpleConnectionPool],
        session_func: Callable,
        SqlGeneratorCls: Union[SqliteQueryGenerator, PostgresQueryGenerator],
        DbBackendCls: Union[SqliteBackend, PostgresBackend],
        verbose: bool,
    ) -> None:

        def run_select_query(
            uri_query: str,
            table:  str = 'test_table',
            engine: Union[sqlite3.Connection, psycopg2.pool.SimpleConnectionPool] = engine,
            verbose: bool = verbose,
        ) -> list:
            out = []
            if verbose:
                print(colored(uri_query, 'magenta'))
            q = SqlGeneratorCls(table, uri_query)
            if verbose:
                print(colored(q.select_query, 'yellow'))
            with session_func(engine) as session:
                session.execute(q.select_query)
                resp = session.fetchall()
            for row in resp:
                target = row[0]
                if isinstance(target, dict) or isinstance(target, list):
                    _in = target
                else:
                    _in = json.loads(target)
                out.append(_in)
            if verbose:
                print(out)
            return out

        def run_update_query(
            uri_query: str,
            table: str = 'test_table',
            engine: Union[sqlite3.Connection, psycopg2.pool.SimpleConnectionPool] = engine,
            verbose: bool = verbose,
            data: list = data,
        ) -> list:
            q = SqlGeneratorCls(table, uri_query, data=data)
            if verbose:
                print(q.update_query)
            with session_func(engine) as session:
                session.execute(q.update_query)
            with session_func(engine) as session:
                session.execute(f'select * from {table}')
                resp = session.fetchall()
            for row in resp:
                target = row[0]
                if isinstance(target, dict):
                    _in = target
                else:
                    _in = json.loads(target)
                out.append(_in)
            return out

        def run_delete_query(
            uri_query: str,
            table: str = 'test_table',
            engine: Union[sqlite3.Connection, psycopg2.pool.SimpleConnectionPool] = engine,
            verbose: bool = verbose,
        ) -> bool:
            q = SqlGeneratorCls(table, uri_query)
            if verbose:
                print(q.delete_query)
            with session_func(engine) as session:
                session.execute(q.delete_query)
            return True

        db = DbBackendCls(engine)
        try:
            db.table_delete('test_table', '')
        except Exception as e:
            pass
        try:
            db.table_delete('another_table', '')
        except Exception as e:
            pass

        # test '*' without any tables
        out = list(db.table_select('*', 'select=count(1)', exclude_endswith = ['_audit', '_metadata']))
        assert list(out) == []

        # create tables
        db.table_insert('test_table', data)
        db.table_insert('another_table', data)

        # SELECT
        if verbose:
            print('\n===> SELECT\n')
        # simple key selection
        out = run_select_query('select=x')
        for entry in out:
            assert isinstance(entry, list)
        assert out[0][0] == 1900
        # more than one simple key
        out = run_select_query('select=x,z')
        assert len(out[0]) == 2
        # nested key
        out = run_select_query('select=a.k1')
        assert len(out[0]) == 1
        assert out[2] == [{'r1': [1, 2], 'r2': 2}]
        # simple array slice
        out = run_select_query('select=x,b[1]')
        assert out[0][1] == 2
        assert out[1][1] == 1
        # nested simple array slice
        out = run_select_query('select=x,a.k2[1]')
        assert out[2] == [88, 9]
        # selecting a key inside an array slice
        out = run_select_query('select=x,c[1|h]')
        assert out[0][1] is None
        assert out[1][1] == 32
        # selecting keys inside an array slice
        out = run_select_query('select=x,c[1|h,p]')
        assert out[0][1] is None
        assert out[1][1] == [32, 0]
        # broadcast key selection inside array - single key
        out = run_select_query('select=x,c[*|h]')
        assert len(out[1][1]) == 3
        assert out[1][1] == [3, 32, 0]
        # broadcast key selection inside array - mutliple keys
        out = run_select_query('select=x,c[*|h,p]')
        assert len(out[1][1]) == 3
        assert out[1][1][0] == [3, 99]
        # nested array selection
        out = run_select_query('select=a.k1.r1[0]')
        assert out[2] == [1]
        # nested keys
        # with single selection inside array, specific element
        out = run_select_query('select=a.k3[0|h]')
        assert out[3] == [0]
        # with single selection inside array, broadcast
        out = run_select_query('select=a.k3[*|h]')
        assert out[3] == [[0, 63]]
        # now multiple sub-selections
        out = run_select_query('select=a.k3[0|h,s]')
        assert out[3] == [[0, 521]]
        out = run_select_query('select=a.k3[*|h,s]')
        assert out[3] == [[[0, 521], [63, 333]]]
        # multiple sub-keys
        out = run_select_query('select=a.k1,a.k3')
        assert out[3] == [{'r1': [33, 200], 'r2': 90}, [{'h': 0, 'r': 77, 's': 521}, {'h': 63, 's': 333}]]

        # FUNCTIONS/AGGREGATIONS
        # supported: count, avg, sum, (max, min), min_ts, max_ts
        out = run_select_query('select=count(1)')
        assert out == [[5]]
        out = run_select_query('select=count(*)')
        assert out == [[5]]
        out = run_select_query('select=count(x)')
        assert out == [[4]]
        out = run_select_query('select=count(1),min(y)')
        assert out == [[5, 1]]
        out = run_select_query('select=count(1),avg(x),min(y),sum(x),max_ts(timestamp)')
        assert out == [[5, 526.2500000000000000, 1, 2105, '2020-10-14T20:20:34.388511']]
        # nested selections
        out = run_select_query('select=count(a.k1.r2),count(x),count(*)')
        assert out == [[2, 4, 5]]
        # array selections
        out = run_select_query('select=count(b[0])')
        assert out == [[2]]
        out = run_select_query('select=max(b[0])')
        assert out == [[1111]]
        out = run_select_query('select=min_ts(timestamps[0])')
        assert out == [['1984-10-13T10:15:26.388573']]
        # sub-selections
        out = run_select_query('select=count(a.k3[0|h])')
        assert out == [[1]]
        out = run_select_query('select=max(q.r[0|s])')
        assert out == [[77]]

        # broadcasting aggregations
        out = list(db.table_select('*', 'select=count(1)', exclude_endswith = ['_audit', '_metadata']))
        assert out == [{'another_table': [5]}, {'test_table': [5]}]

        # WHERE
        if verbose:
            print('\n===> WHERE\n')
        # simple key op
        out = run_select_query('where=x=gt.1000')
        assert out[0]['x'] == 1900
        # multipart simple key ops
        out = run_select_query('where=x=gt.1000,or:y=eq.11')
        assert len(out) == 2
        out = run_select_query('where=x=lt.1000,and:y=eq.11')
        assert out == []
        # groups (with a select)
        out = run_select_query('select=x&where=((x=lt.1000,and:y=eq.11),or:x=gt.1000)')
        assert out == [[1900]]
        # is, not, like, and null
        out = run_select_query('where=x=not.is.null')
        assert len(out) == 4
        out = run_select_query('select=d&where=d=not.like.*g3')
        assert len(out) == 2
        # in
        out = run_select_query('select=d&where=d=in.[string1,string2]')
        assert len(out) == 2
        assert out == [['string1'], ['string2']]
        # nested key ops
        out = run_select_query('where=a.k1.r2=eq.90')
        assert len(out) == 1
        # nested key ops with slicing
        out = run_select_query('select=x&where=a.k1.r1[0]=eq.1')
        assert out[0][0] == 88
        out = run_select_query('select=x&where=a.k3[0|h]=eq.0')
        assert out[0][0] == 107
        # timestamps
        out = run_select_query('select=x,timestamp&where=timestamp=gt.2020-10-14')
        assert len(out) == 3
        out = run_select_query('select=x,timestamp&where=timestamp=lt.2020-10-14')
        assert len(out) == 2
        # equality with strings made of digits, and with integers
        out = run_select_query('select=x&where=lol1=eq.123')
        assert out[0][0] == 1900
        out = run_select_query('select=x&where=lol2=eq.123')
        assert out[0][0] == 1900
        out = run_select_query('select=x&where=lol3.yeah=eq.123')
        assert out[0][0] == 1900
        out = run_select_query('select=x&where=lol4.yeah=eq.123')
        assert out[0][0] == 1900
        # same as ^, but with non-equality, neq
        out = run_select_query('select=y&where=lol1=neq.123,and:lol1=not.is.null')
        assert out[0][0] == 11
        out = run_select_query('select=y&where=lol2=neq.123,and:lol1=not.is.null')
        assert out[0][0] == 11
        out = run_select_query('select=y&where=lol3.yeah=neq.123,and:lol1=not.is.null')
        assert out[0][0] == 11
        out = run_select_query('select=y&where=lol4.yeah=neq.123,and:lol1=not.is.null')
        assert out[0][0] == 11

        # ORDER
        if verbose:
            print('\n===> ORDER\n')
        # Note: postgres and sqlite treat NULLs different in ordering
        # postgres puts them first, sqlite puts them last, so be it
        # simple key
        out = run_select_query('select=x&where=x=not.is.null&order=x.desc')
        x_array = [[1900], [107], [88], [10]]
        assert out == x_array
        x_array.reverse()
        out = run_select_query('select=x&where=x=not.is.null&order=x.asc')
        assert out == x_array
        # array selections
        out = run_select_query('select=x,a&where=a.k1.r1[0]=not.is.null&order=a.k1.r1[0].desc')
        assert out[0][0] == 107
        out = run_select_query('select=x,a&where=a.k3[0|h]=not.is.null&order=a.k3[0|h].desc')
        assert out[0][0] == 107
        # timestamps
        out = run_select_query('select=x,timestamp&order=timestamp.desc')
        assert out[0][1] == '2020-10-14T20:20:34.388511'
        out = run_select_query('select=x,timestamp&order=timestamp.asc')
        assert out[0][1] == '2020-10-13T10:15:26.388573'

        # RANGE
        if verbose:
            print('\n===> RANGE\n')
        out = run_select_query('select=x&where=x=not.is.null&order=x.desc&range=0.2')
        assert out == [[1900], [107]]
        out = run_select_query('select=x&where=x=not.is.null&order=x.desc&range=1.2')
        assert out == [[107], [88]]

        # UPDATE
        if verbose:
            print('\n===> UPDATE\n')
        out = run_update_query('set=x&where=x=lt.1000', data={'x': 999})
        out = run_select_query('select=x&where=x=eq.999')
        assert out[0][0] == 999
        assert len(out) == 3
        out = run_update_query('set=a&where=a.k1.r2=eq.90', data={'a': {'k1': {'r1': [33, 200], 'r2': 80 }}})
        out = run_select_query('where=a.k1.r2=eq.80')
        assert len(out) == 1
        assert out[0]['a']['k1']['r2'] == 80

        # DELETE
        if verbose:
            print('\n===> DELETE\n')
        out = run_delete_query('where=x=lt.1000')
        assert out is True
        out = run_select_query('select=x&where=x=lt.1000')
        assert out == []
        out = run_delete_query('')
        with pytest.raises(Exception):
            out = run_delete_query('')



    def test_sqlite(self):
        engine = sqlite_init('/tmp', name='api-test.db')
        self.run_backend_tests(
            self.data,
            engine,
            sqlite_session,
            SqliteQueryGenerator,
            SqliteBackend,
            self.verbose
        )

    def test_postgres(self) -> None:
        try:
            pool = postgres_init(
                {
                    "dbname": "pysquril_db",
                    "user": "pysquril_user",
                    "pw": "",
                    "host": "localhost",
                }
            )
            pg_backend = PostgresBackend(pool)
            pg_backend.initialise()
            self.run_backend_tests(
                self.data,
                pool,
                postgres_session,
                PostgresQueryGenerator,
                PostgresBackend,
                self.verbose
            )
        except psycopg2.OperationalError:
            print("missing postgres db, run:")
            print("$ createuser pysquril_user")
            print("$ createdb -O pysquril_user pysquril_db")
