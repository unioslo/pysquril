
import datetime
import json
import os
import sqlite3
import unittest
import tempfile

from datetime import timedelta
from typing import Callable, Union
from urllib.parse import quote

import psycopg2
import psycopg2.errors
import psycopg2.extensions
import psycopg2.pool
import pytest

from termcolor import colored

from pysquril.backends import SqliteBackend, PostgresBackend, sqlite_session, postgres_session
from pysquril.exc import ParseError, OperationNotPermittedError
from pysquril.generator import SqliteQueryGenerator, PostgresQueryGenerator
from pysquril.parser import (
    SelectClause,
    WhereClause,
    GroupByTerm,
    GroupByClause,
    AlterClause,
    UriQuery,
)
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

class TestParser(object):

    def test_select(self) -> None:
        c = SelectClause("x[*|a,b],y.z")
        assert len(c.split_clause()) == 2

    def test_where(self) -> None:
        # TODO: test x=gt.3,or:(y=gt.3,and:z=not.is.null)

        # basic where's

        c = WhereClause("a=eq.b,and:lol=not.is.null")
        assert len(c.split_clause()) == 2
        c = WhereClause("a[1|h]=eq.0,or:b[0]=gt.1")
        assert len(c.split_clause()) == 2
        c = WhereClause("((x=gt.3,or:y=gt.3),and:z=not.is.null)")
        assert len(c.split_clause()) == 3

        # quoting values

        c = WhereClause("lol=eq.'.'")
        assert len(c.split_clause()) == 1
        where_term = c.parsed[0]
        where_element = where_term.parsed[0]
        assert where_element.select_term.bare_term == "lol"
        assert where_element.op == "eq"
        assert where_element.val == "."

        c = WhereClause("a=gte.4,and:b=eq.'r,[,],',or:c=neq.0")
        assert len(c.split_clause()) == 3

        # ampersand

        c = WhereClause("a=eq.'&'")
        assert len(c.split_clause()) == 1

        # escaping quotes

        c = WhereClause("lol=eq.'\\'n kat loop oor die pad'")
        where_term = c.parsed[0]
        where_element = where_term.parsed[0]
        assert where_element.val == "''n kat loop oor die pad"


    def test_group_by(self) -> None:

        with pytest.raises(ParseError):
            GroupByTerm("max(key)")

        with pytest.raises(ParseError):
            GroupByTerm("a[1|b,c]")

        with pytest.raises(ParseError):
            GroupByTerm("a[*|b]")

        with pytest.raises(ParseError):
            GroupByTerm("a[*|b,c]")

        c = GroupByClause("a,b")
        assert len(c.split_clause()) == 2

        c = GroupByClause("a.b.c,d")
        assert len(c.split_clause()) == 2

    def test_alter(self) -> None:

        c = AlterClause("name=eq.new_name")
        term = c.parsed[0]
        element = term.parsed[0]
        assert element.val == "new_name"

        with pytest.raises(ParseError):
            AlterClause("num=eq.new_name")

        with pytest.raises(ParseError):
            AlterClause("name=neq.new_name")

    def test_uri_query(self) -> None:

        q = UriQuery("", "")

        indices = q._index_clauses("x&'&'&y")
        assert indices == [1, 5]

        sliced = q._slice(target="1&a&bc", positions=[1, 3])
        assert sliced == ["1", "a", "bc"]

        q = UriQuery("table", "where=a=eq.'g\\'n mooi dag buite'")
        assert q.where.original == "a=eq.'g\\'n mooi dag buite'"
        assert q.where.parsed[0].parsed[0].val == "g''n mooi dag buite"


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
            db = DbBackendCls(engine)
            out = list(db.table_select(table, uri_query))
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
                print(colored(q.update_query, 'cyan'))
            db = DbBackendCls(engine)
            db.table_update(table, uri_query, data)
            out = list(db.table_select(table, ""))
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
            db = DbBackendCls(engine)
            db.table_delete(table, uri_query)
            return True

        def run_alter_query(
            uri_query: str,
            table: str,
            engine: Union[sqlite3.Connection, psycopg2.pool.SimpleConnectionPool] = engine,
            verbose: bool = verbose,
        ) -> dict:
            db = DbBackendCls(engine)
            out = db.table_alter(table, uri_query)
            return out


        db = DbBackendCls(engine)
        try:
            db.table_delete('test_table', '')
        except Exception as e:
            pass
        try:
            db.table_delete('another_table', '')
        except Exception as e:
            pass
        try:
            db.table_delete('silly_table', '')
            db.table_delete('silly_table_audit', '')
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
        assert out[1][1] == [32, 0] or out[1][1] == '[32,0]' # sqlite
        # broadcast key selection inside array - single key
        out = run_select_query('select=x,c[*|h]')
        assert len(out[1][1]) == 3
        assert out[1][1] == [3, 32, 0]
        # broadcast key selection inside array - mutliple keys
        out = run_select_query('select=x,c[*|h,p]')
        assert len(out[1][1]) == 3
        assert out[1][1][0] == [3, 99] or out[1][1][0] == '[3,99]' # sqlite
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
        assert out[3] == [[0, 521]] or out[3] == ['[0,521]'] # sqlite
        out = run_select_query('select=a.k3[*|h,s]')
        assert out[3] == [[[0, 521], [63, 333]]] or out[3] == [['[0,521]', '[63,333]']] # sqlite
        # multiple sub-keys
        out = run_select_query('select=a.k1,a.k3')
        assert out[3] == [{'r1': [33, 200], 'r2': 90}, [{'h': 0, 'r': 77, 's': 521}, {'h': 63, 's': 333}]]

        # FUNCTIONS/AGGREGATIONS
        # supported: count, avg, sum, (max, min), min_ts, max_ts
        if verbose:
            print('\n===> FUNCTIONS\n')

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

        if verbose:
            print('\n===> BROADCASTING\n')

        # broadcasting aggregations
        out = list(db.table_select('*', 'select=count(1)', exclude_endswith = ['_audit', '_metadata']))
        assert out == [{'another_table': [5]}, {'test_table': [5]}]

        # fuzzy matching
        out = list(db.table_select('another*', 'select=count(1)', exclude_endswith = ['_audit', '_metadata']))
        assert out == [{'another_table': [5]}]

        out = list(db.table_select('*_table', 'select=count(1)', exclude_endswith = ['_audit', '_metadata']))
        assert out == [{'another_table': [5]}, {'test_table': [5]}]

        # broadcasting queries without aggregation
        out = list(db.table_select('*', 'select=x', exclude_endswith = ['_audit', '_metadata']))
        assert out is not None
        assert len(out) == 2
        assert len(out[0].get("another_table")) == 5
        assert len(out[1].get("test_table")) == 5

        out = list(db.table_select('*', 'select=x,y&where=z=not.is.null', exclude_endswith = ['_audit', '_metadata']))
        assert out is not None
        assert len(out) == 2
        assert len(out[0].get("another_table")) == 4
        assert len(out[1].get("test_table")) == 4

        # table lists
        out = list(
            db.table_select(
                'another_table,test_table',
                'select=x,y&where=z=not.is.null',
                exclude_endswith = ['_audit', '_metadata']
            )
        )
        assert out is not None
        assert len(out) == 2
        assert len(out[0].get("another_table")) == 4
        assert len(out[1].get("test_table")) == 4

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
        # Run the query with not as a string value.
        out=run_select_query('select=d&where=d=eq.not')
        assert len(out) == 0
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
        # floats
        out = run_select_query('select=z&where=float=eq.3.1')
        assert out[0][0] == 5
        out = run_select_query('select=z&where=float_str=eq.3.2')
        assert out[0][0] == 5
        out = run_select_query('select=z&where=float=gt.3.2')
        assert out[0][0] == 1
        # with quoting
        out = run_select_query("select=x&where=meh=eq.'.'")
        assert out[0][0] == 10
        out = run_select_query("select=x&where=lolly=eq.'()'")
        assert out[0][0] == 10
        out = run_select_query("select=x&where=wat=eq.'and:'")
        assert out[0][0] == 10
        out = run_select_query("select=x&where=meh2=eq.'()[],and:,or:. where=;'")
        assert out[0][0] == 107
        # ampersand
        out = run_select_query("select=x&where=being=eq.'arising&vanishing'")
        assert out[0][0] == 10
        # esacping single quotes
        out = run_select_query("select=x&where=loop=eq.'g\\'n kat oor die pad'")
        assert out[0][0] == 10

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

        # GROUP BY
        if verbose:
            print('\n===> GROUP BY\n')
        out = run_select_query('select=self,count(*)&group_by=self')
        assert len(out) == 2
        out = run_select_query('select=self,beneficial,count(*)&group_by=self,beneficial')
        assert len(out) == 4

        with pytest.raises(ParseError):
            run_select_query('select=self,x,count(*)&group_by=self,x&order=x.desc')

        with pytest.raises(ParseError):
            run_select_query('select=self,count(*)&group_by=self,x')

        # UPDATE
        if verbose:
            print('\n===> UPDATE\n')
        out = run_update_query('set=x&where=x=lt.1000', data={'x': 999})
        out = run_select_query('select=x&where=x=eq.999')
        assert out[0][0] == 999
        assert len(out) == 3
        out = run_update_query(
            'set=a&where=a.k1.r2=eq.90',
            data={'a': {'k1': {'r1': [33, 200], 'r2': 80 }}},
        )
        out = run_select_query('where=a.k1.r2=eq.80')
        assert len(out) == 1
        assert out[0]['a']['k1']['r2'] == 80
        with pytest.raises(ParseError):
            out = run_update_query('set=x&where=x=eq.1', data={})

        # multiple keys
        out = run_update_query(
            'set=x,y&where=float=eq.3.1',
            data={'x': 0, 'y': 1},
        )
        out = run_select_query('select=x,y&where=float=eq.3.1')
        assert len(out) == 1
        assert out[0][0] == 0
        assert out[0][1] == 1

        # setting to null
        out = run_update_query(
            'set=x&where=float=eq.3.1',
            data={'x': None},
        )
        out = run_select_query('select=x,y&where=float=eq.3.1')
        assert len(out) == 1
        assert out[0][0] == None

        # single quotes inside the payload
        out = run_update_query(
            "set=quotes_inside&where=wat=eq.'and:'",
            data={'quotes_inside': "this _has_ 'quotes'"},
        )
        out = run_select_query("select=quotes_inside&where=wat=eq.'and:'")
        assert len(out) == 1
        assert out[0][0] == 'this _has_ \'quotes\''

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

        # ALTER
        if verbose:
            print('\n===> ALTER\n')

        db.table_insert('some_table', data)

        # without an audit table
        out = run_alter_query("alter=name=eq.yet_another_table", "some_table")
        assert len(out["tables"]) == 1

        # with an audit table
        out = run_update_query(
            'set=x&where=float=eq.3.1',
            data={'x': None},
            table='yet_another_table'
        )
        out = run_alter_query("alter=name=eq.silly_table", "yet_another_table")
        assert len(out["tables"]) == 2

        # not permitted directly on an audit table
        with pytest.raises(OperationNotPermittedError):
            run_alter_query("alter=name=eq.new", "silly_table_audit")


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
                    "dbname": os.environ.get("PYSQURIL_POSTGRES_DB", "pysquril_db"),
                    "user": os.environ.get("PYSQURIL_POSTGRES_USER", "pysquril_user"),
                    "pw": os.environ.get("PYSQURIL_POSTGRES_PASSWORD", ""),
                    "host": os.environ.get("PYSQURIL_POSTGRES_HOST", "localhost"),
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
            raise

class TestSqlBackend(unittest.TestCase):
    __test__ = False
    
    backend: Union[SqliteBackend, PostgresBackend]
    engine: Union[sqlite3.Connection, psycopg2.pool.SimpleConnectionPool]

    def test_audit(self) -> bool:
        test_table = "just_an_average_audit_test_table"

        pkey = "id"

        key_to_update = "key1"
        original_value = 5

        data = {pkey: 1, key_to_update: original_value, "key2": "a"}
        more_data = {pkey: 2, key_to_update: original_value, "key3": {"moar": "things"}}

        self.backend.table_insert(table_name=test_table, data=data)
        self.backend.table_insert(table_name=test_table, data=more_data)

        # update the table with the new data
        new_data = {key_to_update: original_value+1}
        message = "all the messages"

        self.backend.table_update(
            table_name=test_table,
            uri_query=f"set={key_to_update}&where={key_to_update}=eq.{original_value}&message={quote(message)}",
            data=new_data,
        )
        result = list(self.backend.table_select(table_name=test_table, uri_query=""))
        self.assertTrue(result)
        retrieved_data = result[0]
        self.assertEqual(retrieved_data, {**data, **new_data})
        self.assertNotEqual(retrieved_data[key_to_update], original_value)
        self.assertEqual(retrieved_data[key_to_update], new_data[key_to_update])

        # view update audit data
        result = list(self.backend.table_select(
            table_name=f"{test_table}_audit", uri_query="order=timestamp.asc")
        )
        self.assertTrue(result)
        audit_event = result[0]
        self.assertEqual(audit_event["previous"], data)
        self.assertEqual(audit_event["diff"], new_data)
        self.assertEqual(audit_event["event"], "update")
        self.assertTrue(audit_event["transaction_id"] is not None)
        self.assertTrue(audit_event["event_id"] is not None)
        self.assertTrue(audit_event["timestamp"] is not None)
        self.assertTrue(audit_event["query"] is not None)
        self.assertEqual(audit_event["message"], message)

        # restore updates
        with self.assertRaises(ParseError): # missing restore directive
            self.backend.table_restore(table_name=test_table, uri_query="")
        with self.assertRaises(ParseError): # missing primary key
            self.backend.table_restore(table_name=test_table, uri_query="restore")
        with self.assertRaises(ParseError): # still missing primary key
            self.backend.table_restore(table_name=test_table, uri_query="restore&primary_key=")

        # restore to a specific state, for a specific row
        message = "undoing mistakes"
        result = self.backend.table_restore(
            table_name=test_table,
            uri_query=f"restore&primary_key={pkey}&where=event_id=eq.{audit_event.get('event_id')}&message={quote(message)}"
        )
        self.assertEqual(len(result.get("updates")), 1)
        self.assertEqual(len(result.get("restores")), 0)
        result = list(self.backend.table_select(table_name=test_table, uri_query="where=id=eq.1"))
        self.assertEqual(result[0].get(key_to_update), original_value)
        result = list(self.backend.table_select(
            table_name=f"{test_table}_audit", uri_query="order=timestamp.desc")
        )
        self.assertEqual(result[0].get("message"), message)

        # delete a specific entry
        message = "bad data: must delete, & never repeat (tm)"
        self.backend.table_delete(
            table_name=test_table,
            uri_query=f"where=key3=not.is.null&message={quote(message)}"
        )
        result = list(self.backend.table_select(table_name=f"{test_table}_audit", uri_query=""))
        self.assertEqual(len(result), 4)
        result = list(self.backend.table_select(
            table_name=f"{test_table}_audit", uri_query="order=timestamp.desc")
        )
        self.assertEqual(result[0].get("message"), message)

        # restore the deleted entry
        result = self.backend.table_restore(
            table_name=test_table,
            uri_query=f"restore&primary_key={pkey}&where=event=eq.delete"
        )
        self.assertEqual(len(result.get("updates")), 0)
        self.assertEqual(len(result.get("restores")), 1)
        result = list(self.backend.table_select(table_name=test_table, uri_query="where=id=eq.2"))
        self.assertTrue(result[0] is not None)

        # delete the table
        self.backend.table_delete(table_name=test_table, uri_query="")

        # check that the deletes are in the audit
        result = list(self.backend.table_select(table_name=f"{test_table}_audit", uri_query=""))
        self.assertEqual(len(result), 7)

        # restore everything
        result = self.backend.table_restore(table_name=test_table, uri_query=f"restore&primary_key={pkey}")
        self.assertTrue(result is not None)
        result = list(self.backend.table_select(table_name=test_table, uri_query="order=id.asc"))
        self.assertEqual(result[0], data)
        self.assertEqual(result[1], more_data)

        # delete the table (again)
        self.backend.table_delete(table_name=test_table, uri_query="")

        # try to retrieve deleted table
        select = self.backend.table_select(table_name=test_table, uri_query="")
        with self.assertRaises((sqlite3.OperationalError, psycopg2.errors.UndefinedTable)):
            next(select)

        # delete the audit table
        self.backend.table_delete(table_name=f"{test_table}_audit", uri_query="")
        
        # try to retrieve deleted table's audit table
        select = self.backend.table_select(table_name=f"{test_table}_audit", uri_query="")
        with self.assertRaises((sqlite3.OperationalError, psycopg2.errors.UndefinedTable)):
            next(select)

        # test deleting an entire table, without any updates
        # automatic audit table creation on delete
        # use a nested primary key, to test restores with such keys
        some_data = {"pk": {"id": 0}, "lol": None, "cat": [1, 2]}
        some_more_data = {"pk": {"id": 1}, "neither-lol-not-not-lol": None, "cat": []}
        some_table = "yay"
        self.backend.table_insert(table_name=some_table, data=some_data)
        self.backend.table_insert(table_name=some_table, data=some_more_data)
        self.backend.table_update(
            table_name=some_table,
            uri_query=f"set=lol&where=pk.id=eq.0",
            data={"lol": "wat"},
        )
        self.backend.table_delete(table_name=some_table, uri_query="")
        audit = list(self.backend.table_select(table_name=f"{some_table}_audit", uri_query=""))
        self.assertEqual(len(audit), 3)
        nested_result = self.backend.table_restore(
            table_name=some_table, uri_query=f"restore&primary_key=pk.id"
        )
        self.assertEqual(len(nested_result.get("restores")), 2)
        self.assertEqual(len(nested_result.get("updates")), 0)
        self.backend.table_delete(table_name=some_table, uri_query="")
        self.backend.table_delete(table_name=f"{some_table}_audit", uri_query="")


        # test backup retention enforcement
        backup_table = "backedup"
        self.backend.backup_days = 1
        self.backend.table_insert(table_name=backup_table, data={"breathe-in": "long", "id": 0})
        self.backend.table_insert(table_name=backup_table, data={"breathe-out": "long", "id": 1})
        self.backend.table_delete(table_name=backup_table, uri_query="")

        # within the retention period
        audit = list(self.backend.table_select(table_name=f"{backup_table}_audit", uri_query=""))
        self.assertEqual(len(audit), 2)
        result = self.backend.table_restore(
            table_name=backup_table, uri_query=f"restore&primary_key=id",
        )
        self.assertEqual(len(result.get("restores")), 2)
        original = list(self.backend.table_select(table_name=backup_table, uri_query=""))
        self.assertTrue(len(original), 2)

        # cleanup
        self.backend.table_delete(table_name=backup_table, uri_query="")
        self.backend.table_delete(table_name=f"{backup_table}_audit", uri_query="")


        # outside the retention period
        not_backup_table = "notbackedup"
        self.backend.backup_days = 1
        self.backend.table_insert(table_name=not_backup_table, data={"breathe-in": "short", "id": 0})
        self.backend.table_insert(table_name=not_backup_table, data={"breathe-out": "short", "id": 1})
        self.backend.table_delete(table_name=not_backup_table, uri_query="")

        # now adjust the audit timestamps to fall outside the retention period
        target = (datetime.datetime.now() - timedelta(days=2)).isoformat()
        if isinstance(self.backend, SqliteBackend):
            new = json.dumps({"timestamp": target})
            update_query = f"update {not_backup_table}_audit set data = json_patch(data, '{new}')"
        elif isinstance(self.backend, PostgresBackend):
            update_query = f"update {not_backup_table}_audit set data = jsonb_set(data, '{{timestamp}}', '\"{target}\"')"
        with self.session_func(self.engine) as session:
            session.execute(update_query)

        # should not be able to view audit or restore data
        audit = list(self.backend.table_select(table_name=f"{not_backup_table}_audit", uri_query=""))
        self.assertEqual(len(audit), 0)
        result = self.backend.table_restore(
            table_name=not_backup_table, uri_query=f"restore&primary_key=id",
        )
        self.assertEqual(len(result.get("restores")), 0)

        # cleanup
        self.backend.table_delete(table_name=f"{not_backup_table}_audit", uri_query="")


    def test_all_view(self) -> bool:
        tenant1 = "p11"
        tenant2 = "p12"
        tenant3 = "p13"
        table_name = "A"
        for idx, tenant in enumerate([tenant1, tenant2, tenant3]):
            view_backend = self.backend_class(
                self.engine, schema=tenant, schema_pattern="p"
            )
            view_backend.table_insert(
                table_name,
                data={"id": idx, "data": f"yes {str(idx)}"},
                update_all_view=True,
            )
        all_backend = self.backend_class(
            self.engine, schema="all", schema_pattern="p"
        )
        result = list(all_backend.table_select(table_name, ""))
        self.assertEqual(len(result), 3)

class TestSqliteBackend(TestSqlBackend):
    __test__ = True

    def setUp(self) -> None:
        self.directory = tempfile.gettempdir()
        self.file = f"{__package__}_test.db"
        self.engine = sqlite_init(self.directory, name=self.file)
        self.backend = SqliteBackend(self.engine)
        self.session_func = sqlite_session
        self.backend_class = SqliteBackend
    
    def tearDown(self) -> None:
        self.engine.close()
        if os.path.exists(f"{self.directory}/{self.file}"):
            os.remove(f"{self.directory}/{self.file}")

class TestPostgresBackend(TestSqlBackend):
    __test__ = True

    def setUp(self) -> None:
        self.engine = postgres_init(
            {
                "dbname": os.environ.get("PYSQURIL_POSTGRES_DB", "pysquril_db"),
                "user": os.environ.get("PYSQURIL_POSTGRES_USER", "pysquril_user"),
                "pw": os.environ.get("PYSQURIL_POSTGRES_PASSWORD", ""),
                "host": os.environ.get("PYSQURIL_POSTGRES_HOST", "localhost"),
            }
        )
        self.backend = PostgresBackend(self.engine)
        self.backend.initialise()
        self.session_func = postgres_session
        self.backend_class = PostgresBackend

    def tearDown(self) -> None:
        self.engine.closeall()
