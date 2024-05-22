
from typing import Optional

from pysquril.backends import SqliteBackend, sqlite_init, PostgresBackend

class B(object):

    """
    A backend for running interactive pysquril select queries -
    a tool for understanding the query language and the library
    capabilities.

    To explore queries with ephemeral sqlite backend:

        B().D(
            [
                {"x": 0, "y": 1},
                {"x": 100, "y": 4, "z": [1,2]}
            ]
        ).Q("select=x,z[1]&order=x.desc")

    -> [[100, 2], [0, None]]

    Broadcasting queries across tables with asterisk matching on table
    names, with a persistent sqlite DB:

        B(sqlite_path="/tmp/lol.db").T("t1").D([{"a":4}])
        B(sqlite_path="/tmp/lol.db").T("t2").D([{"a":4}])

        B(sqlite_path="/tmp/lol.db").T("t*").Q("select=a")

        -> [{'t1': ['[4]']}, {'t2': ['[4]']}]

    Or with a postgres backend, using lists of tables:

        eninge = postgres_init(...)
        be = PostgresBackend(engine)
        B(backend=be).T("t1").D([{"x": 1}])
        B(backend=be).T("t2").D([{"x": 1}])

        B(backend=be).T("t1,t2").Q("select=x")

        -> [{'t1': [[1]]}, {'t2': [[1]]}]


    """

    def __init__(
        self,
        backend: Optional[PostgresBackend] = None,
        sqlite_path: Optional[str] = ":memory:",
        verbose: bool = False,
    ):
        if not backend:
            engine = sqlite_init(sqlite_path)
            self.backend = SqliteBackend(engine)
        else:
            self.backend = backend
        self.verbose = verbose
        self._table_name = "temp"

    @property
    def table_name(self) -> str:
        return self._table_name

    def T(self, table_name: str):
        """
        Define the table name.

        """
        self._table_name = table_name
        return self

    def D(self, data: list):
        """
        Pass data to the backend.

        """
        self.backend.table_insert(
            table_name=self.table_name, data=data,
        )
        return self

    def Q(self, query: str) -> tuple:
        """
        Run a select query, print the results,
        return the query and the results.

        """
        if self.verbose:
            print(query)
        result = list(
            self.backend.table_select(
                table_name=self.table_name, uri_query=query,
            )
        )
        print(result)
        return query, result
