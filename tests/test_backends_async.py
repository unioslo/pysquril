"""
Async backend tests for pysquril.

This test suite mirrors test_backends.py but with async/await patterns.
AsyncTestSqlBackend contains the same comprehensive tests as TestSqlBackend,
ensuring feature parity between sync and async implementations.
"""

import datetime
import json
import os
import pytest
import tempfile
import uuid
from datetime import timedelta
from urllib.parse import quote
from typing import Union

from pysquril import AsyncSqliteBackend, AsyncPostgresBackend
from pysquril._connection import async_sqlite_init, async_postgres_init
from pysquril.utils import audit_table, AUDIT_SEPARATOR, AUDIT_SUFFIX
from pysquril.exc import OperationNotPermittedError
from pysquril.generator import SqliteQueryGenerator, PostgresQueryGenerator
from test_data import dataset
from termcolor import colored


TEST_REQUESTOR = "p11-testasync"
TEST_REQUESTOR_NAME = "Kåre Kraft"
AUDIT_END = f"{AUDIT_SEPARATOR}{AUDIT_SUFFIX}"


class TestAsyncBackends:
    """
    Comprehensive async backend tests that mirror sync TestBackends.

    Tests all query operations: SELECT with nested keys/array slicing,
    functions/aggregations, WHERE clauses, GROUP BY, UPDATE, DELETE, ALTER.
    """

    verbose = True
    data = dataset

    async def run_async_backend_tests(
        self,
        data: list,
        backend: Union[AsyncSqliteBackend, AsyncPostgresBackend],
        SqlGeneratorCls: type,
        verbose: bool,
    ) -> None:
        """Async version of run_backend_tests from sync TestBackends"""

        async def run_select_query(
            uri_query: str,
            table: str = "test_table",
            verbose: bool = verbose,
        ) -> list:
            out = []
            if verbose:
                print(colored(uri_query, "magenta"))
            q = SqlGeneratorCls(table, uri_query)
            if verbose:
                print(colored(q.select_query, "yellow"))
            async for row in backend.table_select(table, uri_query):
                out.append(row)
            if verbose:
                print(out)
            return out

        async def run_update_query(
            uri_query: str,
            table: str = "test_table",
            verbose: bool = verbose,
            data: list = data,
        ) -> list:
            q = SqlGeneratorCls(table, uri_query, data=data)
            if verbose:
                print(colored(q.update_query, "cyan"))
            await backend.table_update(table, uri_query, data)
            out = []
            async for row in backend.table_select(table, ""):
                out.append(row)
            return out

        async def run_delete_query(
            uri_query: str,
            table: str = "test_table",
            verbose: bool = verbose,
        ) -> bool:
            q = SqlGeneratorCls(table, uri_query)
            if verbose:
                print(q.delete_query)
            await backend.table_delete(table, uri_query)
            return True

        async def run_alter_query(
            uri_query: str,
            table: str,
            verbose: bool = verbose,
        ) -> dict:
            out = await backend.table_alter(table, uri_query)
            return out

        # Cleanup existing tables
        try:
            await backend.table_delete("test_table", "")
        except Exception:
            pass
        try:
            await backend.table_delete("another_table", "")
        except Exception:
            pass
        try:
            await backend.table_delete("silly_table", "")
            await backend.table_delete(audit_table("silly_table"), "")
        except Exception:
            pass

        # test '*' without any tables
        out = []
        async for row in backend.table_select(
            "*", "select=count(1)", exclude_endswith=[AUDIT_END, "_metadata"]
        ):
            out.append(row)
        assert list(out) == []

        # create tables
        await backend.table_insert("test_table", data)
        await backend.table_insert("another_table", data)

        # SELECT
        if verbose:
            print("\n===> SELECT\n")
        # simple key selection
        out = await run_select_query("select=x")
        for entry in out:
            assert isinstance(entry, list)
        assert out[0][0] == 1900
        # more than one simple key
        out = await run_select_query("select=x,z")
        assert len(out[0]) == 2
        # nested key
        out = await run_select_query("select=a.k1")
        assert len(out[0]) == 1
        assert out[2] == [{"r1": [1, 2], "r2": 2}]
        # simple array slice
        out = await run_select_query("select=x,b[1]")
        assert out[0][1] == 2
        assert out[1][1] == 1
        # nested simple array slice
        out = await run_select_query("select=x,a.k2[1]")
        assert out[2] == [88, 9]
        # selecting a key inside an array slice
        out = await run_select_query("select=x,c[1|h]")
        assert out[0][1] is None
        assert out[1][1] == 32
        # selecting keys inside an array slice
        out = await run_select_query("select=x,c[1|h,p]")
        assert out[0][1] is None
        assert out[1][1] == [32, 0] or out[1][1] == "[32,0]"  # sqlite
        # broadcast key selection inside array - single key
        out = await run_select_query("select=x,c[*|h]")
        assert len(out[1][1]) == 3
        assert out[1][1] == [3, 32, 0]
        # broadcast key selection inside array - mutliple keys
        out = await run_select_query("select=x,c[*|h,p]")
        assert len(out[1][1]) == 3
        assert out[1][1][0] == [3, 99] or out[1][1][0] == "[3,99]"  # sqlite
        # nested array selection
        out = await run_select_query("select=a.k1.r1[0]")
        assert out[2] == [1]
        # nested keys
        # with single selection inside array, specific element
        out = await run_select_query("select=a.k3[0|h]")
        assert out[3] == [0]
        # with single selection inside array, broadcast
        out = await run_select_query("select=a.k3[*|h]")
        assert out[3] == [[0, 63]]
        # now multiple sub-selections
        out = await run_select_query("select=a.k3[0|h,s]")
        assert out[3] == [[0, 521]] or out[3] == ["[0,521]"]  # sqlite
        out = await run_select_query("select=a.k3[*|h,s]")
        assert out[3] == [[[0, 521], [63, 333]]] or out[3] == [
            ["[0,521]", "[63,333]"]
        ]  # sqlite
        # multiple sub-keys
        out = await run_select_query("select=a.k1,a.k3")
        assert out[3] == [
            {"r1": [33, 200], "r2": 90},
            [{"h": 0, "r": 77, "s": 521}, {"h": 63, "s": 333}],
        ]

        # FUNCTIONS/AGGREGATIONS
        # supported: count, avg, sum, (max, min), min_ts, max_ts
        if verbose:
            print("\n===> FUNCTIONS\n")

        out = await run_select_query("select=count(1)")
        assert out == [[len(dataset)]]
        out = await run_select_query("select=count(*)")
        assert out == [[len(dataset)]]
        out = await run_select_query("select=count(x)")
        assert out == [[4]]
        out = await run_select_query("select=count(1),min(y)")
        assert out == [[len(dataset), 1]]
        out = await run_select_query(
            "select=count(1),avg(x),min(y),sum(x),max_ts(timestamp)"
        )
        assert out == [
            [len(dataset), 526.2500000000000000, 1, 2105, "2020-10-14T20:20:34.388511"]
        ]
        # nested selections
        out = await run_select_query("select=count(a.k1.r2),count(x),count(*)")
        assert out == [[2, 4, len(dataset)]]
        # array selections
        out = await run_select_query("select=count(b[0])")
        assert out == [[2]]
        out = await run_select_query("select=max(b[0])")
        assert out == [[1111]]
        out = await run_select_query("select=min_ts(timestamps[0])")
        assert out == [["1984-10-13T10:15:26.388573"]]
        # sub-selections
        out = await run_select_query("select=count(a.k3[0|h])")
        assert out == [[1]]
        out = await run_select_query("select=max(q.r[0|s])")
        assert out == [[77]]

        if verbose:
            print("\n===> BROADCASTING\n")

        # broadcasting aggregations
        out = []
        async for row in backend.table_select(
            "*", "select=count(1)", exclude_endswith=[AUDIT_END, "_metadata"]
        ):
            out.append(row)
        assert out == [
            {"another_table": [len(dataset)]},
            {"test_table": [len(dataset)]},
        ]

        # fuzzy matching
        out = []
        async for row in backend.table_select(
            "another*", "select=count(1)", exclude_endswith=[AUDIT_END, "_metadata"]
        ):
            out.append(row)
        assert out == [{"another_table": [len(dataset)]}]

        out = []
        async for row in backend.table_select(
            "*_table", "select=count(1)", exclude_endswith=[AUDIT_END, "_metadata"]
        ):
            out.append(row)
        assert out == [
            {"another_table": [len(dataset)]},
            {"test_table": [len(dataset)]},
        ]

        # broadcasting queries without aggregation
        out = []
        async for row in backend.table_select(
            "*", "select=x", exclude_endswith=[AUDIT_END, "_metadata"]
        ):
            out.append(row)
        assert out is not None
        assert len(out) == 2
        assert len(out[0].get("another_table")) == len(dataset)
        assert len(out[1].get("test_table")) == len(dataset)

        out = []
        async for row in backend.table_select(
            "*",
            "select=x,y&where=z=not.is.null",
            exclude_endswith=[AUDIT_END, "_metadata"],
        ):
            out.append(row)
        assert out is not None
        assert len(out) == 2
        assert len(out[0].get("another_table")) == 4
        assert len(out[1].get("test_table")) == 4

        # table lists
        out = []
        async for row in backend.table_select(
            "another_table,test_table",
            "select=x,y&where=z=not.is.null",
            exclude_endswith=[AUDIT_END, "_metadata"],
        ):
            out.append(row)
        assert out is not None
        assert len(out) == 2
        assert len(out[0].get("another_table")) == 4
        assert len(out[1].get("test_table")) == 4

        # WHERE
        if verbose:
            print("\n===> WHERE\n")
        # simple key op
        out = await run_select_query("where=x=gt.1000")
        assert out[0]["x"] == 1900
        # multipart simple key ops
        out = await run_select_query("where=x=gt.1000,or:y=eq.11")
        assert len(out) == 2
        out = await run_select_query("where=x=lt.1000,and:y=eq.11")
        assert out == []
        # groups (with a select)
        out = await run_select_query(
            "select=x&where=((x=lt.1000,and:y=eq.11),or:x=gt.1000)"
        )
        assert out == [[1900]]
        # is, not, like, and null
        out = await run_select_query("where=x=not.is.null")
        assert len(out) == 4
        out = await run_select_query("select=d&where=d=not.like.*g3")
        assert len(out) == 2
        # Run the query with not as a string value.
        out = await run_select_query("select=d&where=d=eq.not")
        assert len(out) == 0
        # in
        out = await run_select_query("select=d&where=d=in.[string1,string2]")
        assert len(out) == 2
        out = await run_select_query(
            "select=contemplate&where=contemplate=in.['non arising','vanishing']"
        )
        assert len(out) == 2
        assert ["non arising"] in out
        assert ["vanishing"] in out
        out = await run_select_query(
            "select=contemplate&where=contemplate=in.['g\\'n niks nie','vanishing']"
        )
        assert len(out) == 2
        # nested key ops
        out = await run_select_query("where=a.k1.r2=eq.90")
        assert len(out) == 1
        # nested key ops with slicing
        out = await run_select_query("select=x&where=a.k1.r1[0]=eq.1")
        assert out[0][0] == 88
        out = await run_select_query("select=x&where=a.k3[0|h]=eq.0")
        assert out[0][0] == 107
        # timestamps
        out = await run_select_query("select=x,timestamp&where=timestamp=gt.2020-10-14")
        assert len(out) == 3
        out = await run_select_query("select=x,timestamp&where=timestamp=lt.2020-10-14")
        assert len(out) == 2
        # equality with strings made of digits, and with integers
        out = await run_select_query("select=x&where=lol1=eq.123")
        assert out[0][0] == 1900
        out = await run_select_query("select=x&where=lol2=eq.123")
        assert out[0][0] == 1900
        out = await run_select_query("select=x&where=lol3.yeah=eq.123")
        assert out[0][0] == 1900
        out = await run_select_query("select=x&where=lol4.yeah=eq.123")
        assert out[0][0] == 1900
        # same as ^, but with non-equality, neq
        out = await run_select_query("select=y&where=lol1=neq.123,and:lol1=not.is.null")
        assert out[0][0] == 11
        out = await run_select_query("select=y&where=lol2=neq.123,and:lol1=not.is.null")
        assert out[0][0] == 11
        out = await run_select_query(
            "select=y&where=lol3.yeah=neq.123,and:lol1=not.is.null"
        )
        assert out[0][0] == 11
        out = await run_select_query(
            "select=y&where=lol4.yeah=neq.123,and:lol1=not.is.null"
        )
        assert out[0][0] == 11
        # floats
        out = await run_select_query("select=z&where=float=eq.3.1")
        assert out[0][0] == 5
        out = await run_select_query("select=z&where=float_str=eq.3.2")
        assert out[0][0] == 5
        out = await run_select_query("select=z&where=float=gt.3.2")
        assert out[0][0] == 1
        # with quoting
        out = await run_select_query("select=x&where=meh=eq.'.'")
        assert out[0][0] == 10
        out = await run_select_query("select=x&where=lolly=eq.'()'")
        assert out[0][0] == 10
        out = await run_select_query("select=x&where=wat=eq.'and:'")
        assert out[0][0] == 10
        out = await run_select_query("select=x&where=meh2=eq.'()[],and:,or:. where=;'")
        assert out[0][0] == 107
        # ampersand
        out = await run_select_query("select=x&where=being=eq.'arising&vanishing'")
        assert out[0][0] == 10
        # esacping single quotes
        out = await run_select_query("select=x&where=loop=eq.'g\\'n kat oor die pad'")
        assert out[0][0] == 10
        # with a simple array
        out = await run_select_query("select=z&where=b[0]=eq.1")
        assert out[0][0] == 5

        # ORDER
        if verbose:
            print("\n===> ORDER\n")
        # Note: postgres and sqlite treat NULLs different in ordering
        # postgres puts them first, sqlite puts them last, so be it
        # simple key
        out = await run_select_query("select=x&where=x=not.is.null&order=x.desc")
        x_array = [[1900], [107], [88], [10]]
        assert out == x_array
        x_array.reverse()
        out = await run_select_query("select=x&where=x=not.is.null&order=x.asc")
        assert out == x_array
        # array selections
        out = await run_select_query(
            "select=x,a&where=a.k1.r1[0]=not.is.null&order=a.k1.r1[0].desc"
        )
        assert out[0][0] == 107
        out = await run_select_query(
            "select=x,a&where=a.k3[0|h]=not.is.null&order=a.k3[0|h].desc"
        )
        assert out[0][0] == 107
        # timestamps
        out = await run_select_query(
            "select=x,timestamp&order=timestamp.desc&where=timestamp=not.is.null"
        )
        assert out[0][1] == "2020-10-14T20:20:34.388511"
        out = await run_select_query(
            "select=x,timestamp&order=timestamp.asc&where=timestamp=not.is.null"
        )
        assert out[0][1] == "2020-10-13T10:15:26.388573"

        # RANGE
        if verbose:
            print("\n===> RANGE\n")
        out = await run_select_query(
            "select=x&where=x=not.is.null&order=x.desc&range=0.2"
        )
        assert out == [[1900], [107]]
        out = await run_select_query(
            "select=x&where=x=not.is.null&order=x.desc&range=1.2"
        )
        assert out == [[107], [88]]

        # GROUP BY
        if verbose:
            print("\n===> GROUP BY\n")
        out = await run_select_query(
            "select=self,count(*)&group_by=self&where=self=not.is.null"
        )
        assert len(out) == 2
        out = await run_select_query(
            "select=self,beneficial,count(*)&group_by=self,beneficial&where=self=not.is.null"
        )
        assert len(out) == 4

        # UPDATE
        if verbose:
            print("\n===> UPDATE\n")
        out = await run_update_query("set=x&where=x=lt.1000", data={"x": 999})
        out = await run_select_query("select=x&where=x=eq.999")
        assert out[0][0] == 999
        assert len(out) == 3

        new_entry = {"a": {"k1": {"r1": [33, 200], "r2": 80}}}
        out = await run_update_query(
            "set=a&where=a.k1.r2=eq.90",
            data=new_entry,
        )
        out = await run_select_query("where=a.k1.r2=eq.80")
        assert len(out) == 1
        assert out[0]["a"]["k1"]["r2"] == 80
        assert out[0]["a"] == new_entry["a"]  # ensure whole entry replaced

        # multiple keys
        out = await run_update_query(
            "set=x,y&where=float=eq.3.1",
            data={"x": 0, "y": 1},
        )
        out = await run_select_query("select=x,y&where=float=eq.3.1")
        assert len(out) == 1
        assert out[0][0] == 0
        assert out[0][1] == 1

        # setting to null
        out = await run_update_query(
            "set=x&where=float=eq.3.1",
            data={"x": None},
        )
        out = await run_select_query("select=x,y&where=float=eq.3.1")
        assert len(out) == 1
        assert out[0][0] == None

        # single quotes inside the payload
        out = await run_update_query(
            "set=quotes_inside&where=wat=eq.'and:'",
            data={"quotes_inside": "this _has_ 'quotes'"},
        )
        out = await run_select_query("select=quotes_inside&where=wat=eq.'and:'")
        assert len(out) == 1
        assert out[0][0] == "this _has_ 'quotes'"

        # Adding new top-level keys via update
        out = await run_update_query(
            "set=newkey,another&where=float=eq.3.1",
            data={"newkey": "a-lovely-value", "another": 1},
        )
        out = await run_select_query("select=newkey&where=float=eq.3.1")
        assert len(out) == 1
        assert out[0][0] == "a-lovely-value"

        out = await run_select_query("select=another&where=float=eq.3.1")
        assert len(out) == 1
        assert out[0][0] == 1

        # Removing top-level keys
        out = await run_update_query(
            "set=-newkey,-another&where=float=eq.3.1",
            data=None,
        )
        out = await run_select_query("where=float=eq.3.1")
        assert "newkey" not in out[0].keys()
        assert "another" not in out[0].keys()

        # replacing a whole entry
        fh = {"plant": "fiddlehead", "taste": "complex", "season": "april"}
        out = await run_update_query(
            "set=*&where=plant=like.'ground*'",
            data=fh,
        )
        out = await run_select_query("where=plant=not.is.null")
        assert out == [fh]  # 'note' key no longer present

        # Nested updates
        for val in [91, "a string", {"zzz": 0}, [1, 2]]:
            out = await run_update_query("set=a.k1.r2&where=z=eq.10", data=val)
            out = await run_select_query("where=z=eq.10")
            assert out[0]["a"]["k1"]["r2"] == val

            out = await run_update_query("set=b[0]&where=lol1=eq.456", data=val)
            out = await run_select_query("where=lol1=eq.456")
            assert out[0]["b"][0] == val

            out = await run_update_query("set=q.r[0|s]&where=z=eq.10", data=val)
            out = await run_select_query("where=z=eq.10")
            assert out[0]["q"]["r"][0]["s"] == val

        # DELETE
        if verbose:
            print("\n===> DELETE\n")
        out = await run_delete_query("where=x=lt.1000")
        assert out is True
        out = await run_select_query("select=x&where=x=lt.1000")
        assert out == []
        out = await run_delete_query("")
        with pytest.raises(Exception):
            out = await run_delete_query("")

        # ALTER
        if verbose:
            print("\n===> ALTER\n")

        await backend.table_insert("some_table", data)

        # without an audit table
        out = await run_alter_query("alter=name=eq.yet_another_table", "some_table")
        assert len(out["tables"]) == 1

        # with an audit table
        out = await run_update_query(
            "set=x&where=float=eq.3.1", data={"x": None}, table="yet_another_table"
        )
        out = await run_alter_query("alter=name=eq.silly_table", "yet_another_table")
        assert len(out["tables"]) == 2

        # not permitted directly on an audit table
        with pytest.raises(OperationNotPermittedError):
            await run_alter_query("alter=name=eq.new", audit_table("silly_table"))


class AsyncTestSqlBackend:
    """
    Base class for async backend tests.

    This mirrors TestSqlBackend from test_backends.py but with async/await.
    Subclasses must set __test__ = True and implement setUp.
    """

    __test__ = False

    backend: Union[AsyncSqliteBackend, AsyncPostgresBackend]
    backend_class: type

    @pytest.mark.asyncio
    async def test_audit(self):
        """Comprehensive async audit test - mirrors sync version"""
        test_table = "just_an_average_audit_test_table"
        pkey = "id"
        key_to_update = "key1"
        original_value = 5

        data = {pkey: 1, key_to_update: original_value, "key2": "a"}
        more_data = {pkey: 2, key_to_update: original_value, "key3": {"moar": "things"}}

        await self.backend.table_insert(table_name=test_table, data=data)
        await self.backend.table_insert(table_name=test_table, data=more_data)

        # Update the table with new data
        new_data = {key_to_update: original_value + 1}
        message = "all the messages"

        await self.backend.table_update(
            table_name=test_table,
            uri_query=f"set={key_to_update}&where={key_to_update}=eq.{original_value}&message={quote(message)}",
            data=new_data,
        )

        result = []
        async for row in self.backend.table_select(table_name=test_table, uri_query=""):
            result.append(row)

        assert result
        retrieved_data = result[0]
        assert retrieved_data == {**data, **new_data}
        assert retrieved_data[key_to_update] != original_value
        assert retrieved_data[key_to_update] == new_data[key_to_update]

        # View update audit data
        result = []
        async for row in self.backend.table_select(
            table_name=audit_table(test_table), uri_query="order=timestamp.asc"
        ):
            result.append(row)

        assert result
        audit_event = result[0]
        assert audit_event["previous"] == data
        assert audit_event["diff"] == new_data
        assert audit_event["event"] == "update"
        assert audit_event["transaction_id"] is not None
        assert audit_event["event_id"] is not None
        assert audit_event["timestamp"] is not None
        assert audit_event["query"] is not None
        assert audit_event["message"] == message
        assert audit_event["identity"] == TEST_REQUESTOR
        assert audit_event["identity_name"] == TEST_REQUESTOR_NAME

        # Restore to a specific state, for a specific row
        message = "undoing mistakes"
        result = await self.backend.table_restore(
            table_name=test_table,
            uri_query=f"restore&primary_key={pkey}&where=event_id=eq.{audit_event.get('event_id')}&message={quote(message)}",
        )
        assert len(result.get("updates")) == 1
        assert len(result.get("restores")) == 0

        result = []
        async for row in self.backend.table_select(
            table_name=test_table, uri_query="where=id=eq.1"
        ):
            result.append(row)
        assert result[0].get(key_to_update) == original_value

        result = []
        async for row in self.backend.table_select(
            table_name=audit_table(test_table), uri_query="order=timestamp.desc"
        ):
            result.append(row)
        assert result[0].get("message") == message

        # Delete a specific entry
        message = "bad data: must delete, & never repeat (tm)"
        await self.backend.table_delete(
            table_name=test_table,
            uri_query=f"where=key3=not.is.null&message={quote(message)}",
        )

        result = []
        async for row in self.backend.table_select(
            table_name=audit_table(test_table), uri_query=""
        ):
            result.append(row)
        assert len(result) == 4

        result = []
        async for row in self.backend.table_select(
            table_name=audit_table(test_table), uri_query="order=timestamp.desc"
        ):
            result.append(row)
        assert result[0].get("message") == message

        # Restore the deleted entry
        result = await self.backend.table_restore(
            table_name=test_table,
            uri_query=f"restore&primary_key={pkey}&where=event=eq.delete",
        )
        assert len(result.get("updates")) == 0
        assert len(result.get("restores")) == 1

        result = []
        async for row in self.backend.table_select(
            table_name=test_table, uri_query="where=id=eq.2"
        ):
            result.append(row)
        assert result[0] is not None

        # Delete the table
        await self.backend.table_delete(table_name=test_table, uri_query="")

        # Check that the deletes are in the audit
        result = []
        async for row in self.backend.table_select(
            table_name=audit_table(test_table), uri_query=""
        ):
            result.append(row)
        assert len(result) == 7

        # Restore everything
        result = await self.backend.table_restore(
            table_name=test_table, uri_query=f"restore&primary_key={pkey}"
        )
        assert result is not None

        result = []
        async for row in self.backend.table_select(
            table_name=test_table, uri_query="order=id.asc"
        ):
            result.append(row)
        assert result[0] == data
        assert result[1] == more_data

        # Delete the table (again)
        await self.backend.table_delete(table_name=test_table, uri_query="")

        # Try to retrieve deleted table
        select_gen = self.backend.table_select(table_name=test_table, uri_query="")
        with pytest.raises(Exception):  # Will raise OperationalError or UndefinedTable
            await select_gen.__anext__()

        # Delete the audit table
        await self.backend.table_delete(
            table_name=audit_table(test_table), uri_query=""
        )

        # Try to retrieve deleted table's audit table
        select_gen = self.backend.table_select(
            table_name=audit_table(test_table), uri_query=""
        )
        with pytest.raises(Exception):
            await select_gen.__anext__()

        # Test deleting an entire table, without any updates
        # Automatic audit table creation on delete
        # Use a nested primary key, to test restores with such keys
        some_data = {"pk": {"id": 0}, "lol": None, "cat": [1, 2]}
        some_more_data = {"pk": {"id": 1}, "neither-lol-not-not-lol": None, "cat": []}
        some_table = "yay"
        await self.backend.table_insert(table_name=some_table, data=some_data)
        await self.backend.table_insert(table_name=some_table, data=some_more_data)
        await self.backend.table_update(
            table_name=some_table,
            uri_query="set=lol&where=pk.id=eq.0",
            data={"lol": "wat"},
        )
        await self.backend.table_delete(table_name=some_table, uri_query="")

        audit = []
        async for row in self.backend.table_select(
            table_name=audit_table(some_table), uri_query=""
        ):
            audit.append(row)
        assert len(audit) == 3

        nested_result = await self.backend.table_restore(
            table_name=some_table, uri_query="restore&primary_key=pk.id"
        )
        assert len(nested_result.get("restores")) == 2
        assert len(nested_result.get("updates")) == 0
        await self.backend.table_delete(table_name=some_table, uri_query="")
        await self.backend.table_delete(
            table_name=audit_table(some_table), uri_query=""
        )

        # Test backup retention enforcement
        backup_table = "backedup"
        self.backend.backup_days = 1
        await self.backend.table_insert(
            table_name=backup_table, data={"breathe-in": "long", "id": 0}
        )
        await self.backend.table_insert(
            table_name=backup_table, data={"breathe-out": "long", "id": 1}
        )
        await self.backend.table_delete(table_name=backup_table, uri_query="")

        # Within the retention period
        audit = []
        async for row in self.backend.table_select(
            table_name=audit_table(backup_table), uri_query=""
        ):
            audit.append(row)
        assert len(audit) == 2

        result = await self.backend.table_restore(
            table_name=backup_table,
            uri_query="restore&primary_key=id",
        )
        assert len(result.get("restores")) == 2

        original = []
        async for row in self.backend.table_select(
            table_name=backup_table, uri_query=""
        ):
            original.append(row)
        assert len(original) == 2

        # Cleanup
        await self.backend.table_delete(table_name=backup_table, uri_query="")
        await self.backend.table_delete(
            table_name=audit_table(backup_table), uri_query=""
        )

        # Outside the retention period
        not_backup_table = "notbackedup"
        self.backend.backup_days = 1
        await self.backend.table_insert(
            table_name=not_backup_table, data={"breathe-in": "short", "id": 0}
        )
        await self.backend.table_insert(
            table_name=not_backup_table, data={"breathe-out": "short", "id": 1}
        )
        await self.backend.table_delete(table_name=not_backup_table, uri_query="")

        # Now adjust the audit timestamps to fall outside the retention period
        target = (datetime.datetime.now() - timedelta(days=2)).isoformat()
        if isinstance(self.backend, AsyncSqliteBackend):
            new = json.dumps({"timestamp": target})
            update_query = f"update {self.backend._fqtn(audit_table(not_backup_table))} set data = json_patch(data, '{new}')"
        elif isinstance(self.backend, AsyncPostgresBackend):
            update_query = f"update {self.backend._fqtn(audit_table(not_backup_table))} set data = jsonb_set(data, '{{timestamp}}', '\"{target}\"')"

        from pysquril._connection import async_sqlite_session, async_postgres_session

        session_func = (
            async_sqlite_session
            if isinstance(self.backend, AsyncSqliteBackend)
            else async_postgres_session
        )
        async with session_func(self.backend.engine) as session:
            await session.execute(update_query)

        # Should not be able to view audit or restore data
        audit = []
        async for row in self.backend.table_select(
            table_name=audit_table(not_backup_table), uri_query=""
        ):
            audit.append(row)
        assert len(audit) == 0

        result = await self.backend.table_restore(
            table_name=not_backup_table,
            uri_query="restore&primary_key=id",
        )
        assert len(result.get("restores")) == 0

        # Cleanup
        await self.backend.table_delete(
            table_name=audit_table(not_backup_table), uri_query=""
        )

        # Delete without audit
        table_without_audit = "without_audit"
        await self.backend.table_insert(
            table_name=table_without_audit, data={"breathe": "calming", "id": 0}
        )
        await self.backend.table_delete(
            table_name=table_without_audit, uri_query="", audit=False
        )

        with pytest.raises(Exception):
            audit = []
            async for row in self.backend.table_select(
                table_name=audit_table(table_without_audit),
                uri_query="",
            ):
                audit.append(row)

        # Audit for create and read
        verbose_table = "table_with_full_audit"
        try:
            await self.backend.table_delete(table_name=verbose_table, uri_query="")
            await self.backend.table_delete(
                table_name=audit_table(verbose_table), uri_query=""
            )
        except Exception:
            pass

        await self.backend.table_insert(
            table_name=verbose_table,
            data={"observing": "mind objects", "id": 0},
            audit=True,
        )

        # Consume the async generator for select with audit
        async for _ in self.backend.table_select(
            table_name=verbose_table,
            uri_query="select=observing",
            audit=True,
        ):
            pass

        audit = []
        async for row in self.backend.table_select(
            table_name=audit_table(verbose_table),
            uri_query="",
        ):
            audit.append(row)
        assert len(audit) == 2

        await self.backend.table_delete(table_name=verbose_table, uri_query="")
        await self.backend.table_delete(
            table_name=audit_table(verbose_table), uri_query=""
        )

        # Rolling back updates which add new keys
        test_add_key_table = "add_key"
        data_initial = {"id": 0, "a": 1}
        data_additional = {"b": 2}

        try:
            await self.backend.table_delete(table_name=test_add_key_table, uri_query="")
            await self.backend.table_delete(
                table_name=audit_table(test_add_key_table), uri_query=""
            )
        except Exception:
            pass

        await self.backend.table_insert(
            table_name=test_add_key_table, data=data_initial
        )

        await self.backend.table_update(
            table_name=test_add_key_table,
            uri_query="set=b&where=a=eq.1",
            data=data_additional,
        )

        # Check the audit
        result = await self.backend.table_restore(
            table_name=test_add_key_table,
            uri_query="restore&primary_key=id&where=event=eq.update",
        )
        assert len(result.get("updates")) == 1

        out = []
        async for row in self.backend.table_select(
            table_name=test_add_key_table, uri_query=""
        ):
            out.append(row)
        assert out == [data_initial]

        await self.backend.table_delete(table_name=test_add_key_table, uri_query="")
        await self.backend.table_delete(
            table_name=audit_table(test_add_key_table), uri_query=""
        )

        # Rolling back updates which remove existing keys
        test_remove_key_table = "remove_key"
        data_initial = {"id": 0, "a": 1, "b": 2}

        try:
            await self.backend.table_delete(
                table_name=test_remove_key_table, uri_query=""
            )
            await self.backend.table_delete(
                table_name=audit_table(test_remove_key_table), uri_query=""
            )
        except Exception:
            pass

        await self.backend.table_insert(
            table_name=test_remove_key_table, data=data_initial
        )

        await self.backend.table_update(
            table_name=test_remove_key_table,
            uri_query="set=-b&where=id=eq.0",
            data=None,
        )

        result = await self.backend.table_restore(
            table_name=test_remove_key_table,
            uri_query="restore&primary_key=id&where=event=eq.update",
        )
        assert len(result.get("updates")) == 1

        out = []
        async for row in self.backend.table_select(
            table_name=test_remove_key_table, uri_query=""
        ):
            out.append(row)
        assert out == [data_initial]

        await self.backend.table_delete(table_name=test_remove_key_table, uri_query="")
        await self.backend.table_delete(
            table_name=audit_table(test_remove_key_table), uri_query=""
        )

    @pytest.mark.asyncio
    async def test_all_view(self):
        """Test multi-tenant view functionality - mirrors sync version"""
        tenant1 = "p11"
        tenant2 = "p12"
        tenant3 = "p13"
        table_name = "A"

        for tenant in [tenant1, tenant2, tenant3]:
            view_backend = self.backend_class(
                self.backend.engine, schema=tenant, schema_pattern="p"
            )
            try:
                await view_backend.table_delete(table_name=table_name, uri_query="")
            except Exception:
                pass

            await view_backend.table_insert(
                table_name,
                data={"id": str(uuid.uuid4()), "data": "yes"},
                update_all_view=True,
            )

        all_backend = self.backend_class(
            self.backend.engine, schema="all", schema_pattern="p"
        )

        result = []
        async for row in all_backend.table_select(table_name, ""):
            result.append(row)
        assert len(result) == 3


class TestAsyncSqliteBackend(AsyncTestSqlBackend):
    """Async SQLite backend test suite - mirrors TestSqliteBackend"""

    __test__ = True

    @pytest.fixture(autouse=True)
    async def setup(self):
        """Setup async SQLite backend for testing"""
        self.directory = tempfile.gettempdir()
        self.file = "test_backends_async_test.db"
        path = f"{self.directory}/{self.file}"

        self.engine = await async_sqlite_init(path)
        self.backend = AsyncSqliteBackend(
            self.engine, requestor=TEST_REQUESTOR, requestor_name=TEST_REQUESTOR_NAME
        )
        self.backend_class = AsyncSqliteBackend

        yield

        # Cleanup
        await self.engine.close()
        if os.path.exists(path):
            os.remove(path)

    @pytest.mark.asyncio
    async def test_comprehensive(self):
        """Comprehensive async SQLite backend tests - mirrors sync TestBackends.test_sqlite"""
        test_runner = TestAsyncBackends()
        await test_runner.run_async_backend_tests(
            test_runner.data, self.backend, SqliteQueryGenerator, test_runner.verbose
        )


class TestAsyncPostgresBackend(AsyncTestSqlBackend):
    """Async PostgreSQL backend test suite - mirrors TestPostgresBackend"""

    __test__ = True

    @pytest.fixture(autouse=True)
    async def setup(self, async_postgres_config):
        """Setup async PostgreSQL backend for testing"""
        self.engine = await async_postgres_init(async_postgres_config)
        self.backend = AsyncPostgresBackend(
            self.engine, requestor=TEST_REQUESTOR, requestor_name=TEST_REQUESTOR_NAME
        )
        await self.backend.initialise()
        self.backend_class = AsyncPostgresBackend

        yield

        # Cleanup
        await self.engine.close()

    @pytest.mark.asyncio
    async def test_comprehensive(self):
        """Comprehensive async PostgreSQL backend tests - mirrors sync TestBackends.test_postgres"""
        test_runner = TestAsyncBackends()
        await test_runner.run_async_backend_tests(
            test_runner.data, self.backend, PostgresQueryGenerator, test_runner.verbose
        )


@pytest.mark.asyncio
async def test_async_postgres_init_no_deprecation_warning(async_postgres_config):
    """async_postgres_init should not open the pool implicitly."""
    import warnings

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        pool = await async_postgres_init(async_postgres_config)
        async with pool.connection() as conn:
            await conn.execute("SELECT 1")

    try:
        assert not any(
            issubclass(w.category, RuntimeWarning)
            and "constructor is deprecated" in str(w.message)
            for w in caught
        )
    finally:
        await pool.close()
