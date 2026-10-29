"""
Internal core backend module containing shared business logic.

INTERNAL MODULE - DO NOT IMPORT DIRECTLY.
This module is internal (underscore prefix) and should not be imported directly
by applications. Import from the top-level pysquril package instead:

    from pysquril import DatabaseBackend, AuditTransaction

Contains:
- DatabaseBackend: Abstract base class for all backends
- BackendCore: Shared business logic for sync and async backends
- AuditTransaction: Audit event generation
"""

import datetime
import sqlite3

from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Union, Iterable, Optional, Any, Callable
from uuid import uuid4

import psycopg2
import psycopg2.extensions
import psycopg2.errors
import psycopg2.pool

from pysquril.generator import SqliteQueryGenerator, PostgresQueryGenerator


class AuditTransaction(object):
    """
    Container for generating audit events.
    Keeps state for transaction IDs, generates
    timestamps, and event IDs, propagates
    audit messages.

    There are five types of audit events:

    1. update - changes to existing data (default on)
    2. delete - deletions of existing data (default on)
    3. restore - rolling back update and/or delete events (default on)
    4. create - records of new data (default off)
    5. read - who accessed with a given query  (default off)

    Notes:

    If create and read events are used, one could calculate
    who looked at data for a given data subject, when on the
    basis of audit data alone. One would have to apply the
    query in each read event to the state of the data at the
    time of the query, and check if the data belonging to the
    subject (and/or the identfier of the data subject)
    is contained in the returned result.

    """

    def __init__(
        self,
        identity: str,
        message: Optional[str] = "",
        identity_name: Optional[str] = None,
    ) -> None:
        self.identity = identity
        self.identity_name = identity_name
        self.timestamp = datetime.datetime.now().isoformat()
        self.transaction_id = self._id()
        self.message = message

    def _id(self) -> str:
        return str(uuid4())

    def _event(self, diff: Any, previous: Any, event: str, query: str) -> dict:
        return {
            "diff": diff,
            "previous": previous,
            "event": event,
            "timestamp": self.timestamp,
            "identity": self.identity,
            "identity_name": self.identity_name,
            "event_id": self._id(),
            "transaction_id": self.transaction_id,
            "query": query,
            "message": self.message,
        }

    def event_update(self, *, diff: Any, previous: Any, query: str) -> dict:
        return self._event(diff, previous, "update", query)

    def event_delete(self, *, diff: Any, previous: Any, query: str) -> dict:
        return self._event(diff, previous, "delete", query)

    def event_restore(self, *, diff: Any, previous: Any, query: str) -> dict:
        return self._event(diff, previous, "restore", query)

    def event_create(self, *, diff: Any) -> dict:
        return self._event(diff, None, "create", None)

    def event_read(self, *, query: str) -> dict:
        return self._event(None, None, "read", query)


class DatabaseBackend(ABC):
    """
    Abstract base class defining the interface for all database backends.
    """

    sep: str  # schema separator character
    generator_class: Union[SqliteQueryGenerator, PostgresQueryGenerator]
    json_object_func: str

    def __init__(
        self,
        engine: Union[
            sqlite3.Connection,
            psycopg2.pool.SimpleConnectionPool,
        ],
        schema: str = None,
        verbose: bool = False,
        requestor: str = None,
        backup_days: Optional[int] = None,
        schema_pattern: Optional[str] = None,
        requestor_name: Optional[str] = None,
    ) -> None:
        super(DatabaseBackend, self).__init__()
        self.engine = engine
        self.verbose = verbose
        self.requestor = requestor
        self.requestor_name = requestor_name
        self.backup_days = backup_days
        self.schema_pattern = schema_pattern

    @abstractmethod
    def initialise(self) -> Optional[bool]:
        pass

    @abstractmethod
    def tables_list(self, session=None) -> list:
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
        update_all_view: bool = False,
        audit: bool = False,
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
    def table_delete(
        self,
        table_name: str,
        uri_query: str,
        update_all_view: bool = False,
        audit: bool = True,
        session: Optional[Union[sqlite3.Cursor, psycopg2.extensions.cursor]] = None,
    ) -> bool:
        pass

    @abstractmethod
    def table_select(
        self,
        table_name: str,
        uri_query: str,
        data: Optional[Union[dict, list]] = None,
        exclude_endswith: list = [],
        audit: bool = False,
        session: Optional[Union[sqlite3.Cursor, psycopg2.extensions.cursor]] = None,
    ) -> Iterable[tuple]:
        pass

    @abstractmethod
    def table_restore(
        self,
        table_name: str,
        uri_query: str,
        session: Optional[Union[sqlite3.Cursor, psycopg2.extensions.cursor]] = None,
    ) -> dict:
        pass

    @abstractmethod
    def table_alter(
        self,
        table_name: str,
        uri_query: str,
        session: Optional[Union[sqlite3.Cursor, psycopg2.extensions.cursor]] = None,
    ) -> dict:
        pass


class BackendCore(DatabaseBackend):
    """
    Core business logic shared by both sync and async backend implementations.

    This class contains pure business logic methods that don't perform I/O:
    - Diff calculations
    - Query string building
    - Audit event generation
    - Data transformation

    Subclasses (GenericBackend for sync, AsyncGenericBackend for async) provide
    the I/O-specific implementations.
    """

    @abstractmethod
    def _session_func(self) -> Callable:
        """Return the appropriate session context manager (sync or async)."""
        raise NotImplementedError

    @abstractmethod
    def _session_scope(self, session=None):
        """
        Yield the caller's session, or open (and commit/rollback) a fresh one.

        Implemented by the sync and async backend bases, since the context
        manager protocol differs between the two.
        """
        raise NotImplementedError

    @abstractmethod
    def _yield_results(self, query: str) -> Iterable[tuple]:
        """Execute query and yield results. Implemented by sync/async subclasses."""
        raise NotImplementedError

    @abstractmethod
    def _decode_row(self, row: Any) -> Any:
        """Decode a single raw database row into the stored document.

        Backend-specific: SQLite stores JSON as text and needs decoding,
        PostgreSQL returns jsonb already decoded.
        """
        raise NotImplementedError

    @abstractmethod
    def _tables_in_schemas(self, table_name: str) -> list:
        """Return list of table instances across schemas. Backend-specific."""
        raise NotImplementedError

    @abstractmethod
    def _create_all_view(
        self,
        view_name: str,
        unions: str,
        session: Union[sqlite3.Cursor, psycopg2.extensions.cursor],
    ) -> None:
        """Create view for cross-schema queries. Backend-specific."""
        raise NotImplementedError

    @abstractmethod
    def _fqtn(
        self,
        table_name: str,
        schema_name: Optional[str] = None,
        no_schema: bool = False,
    ) -> str:
        """Return fully qualified table name. Backend-specific."""
        raise NotImplementedError

    # Pure business logic methods (no I/O)

    def _diff_entries(self, current_entry: dict, target_entry: dict) -> tuple:
        """
        Calculate the difference between two dictionaries, show the difference
        between the current relative to target entry (desired state).

        _diff_entries(current, target) -> (to_change, to_remove, to_add)

        Returns keys/values to be:

        - kept and changed
        - removed
        - added

        ... in order to move from current to target.

        E.g.:

        _diff_entries({a: 3, b: 4}, {a: 3, b: 5}) -> ({b: 5}, _     , _     )
        _diff_entries({a: 3, b: 4}, {a: 3}      ) -> (_     , {b: 4}, _     )
        _diff_entries({a: 3}      , {a: 3, c: 9}) -> (_     , _     , {c: 9})

        """

        to_change = {}
        to_remove = {}
        to_add = {}

        # differences between keys that are present in both
        for k, v in target_entry.items():
            if k in current_entry and current_entry.get(k) != v:
                to_change[k] = v

        # keys present in current but not in target
        for k, v in current_entry.items():
            if k not in target_entry.keys():
                to_remove[k] = v

        # keys not present in current but in target
        for k, v in target_entry.items():
            if k not in current_entry.keys():
                to_add[k] = v

        return (to_change, to_remove, to_add)

    def _get_pk_value(self, primary_key: str, entry: dict) -> Any:
        """Extract primary key value from entry, supporting nested keys."""
        keys = primary_key.split(".")
        if len(keys) == 1:
            return entry.get(primary_key)
        else:
            for key in keys:
                nested_result = entry.get(key)
                entry = nested_result
            return nested_result

    def _query_for_select(
        self,
        table_name: str,
        uri_query: str,
        data: Optional[Union[dict, list]] = None,
        array_agg: bool = False,
        apply_cutoff: bool = False,
    ) -> str:
        """
        Return the appropriate select statement for a given
        table_name, and uri_query, calculating any backup
        cutoff for audit data if needed.

        """
        backup_cutoff = None
        if apply_cutoff:
            backup_cutoff = (
                datetime.date.today() - timedelta(days=self.backup_days)
            ).isoformat()
        sql = self.generator_class(
            f"{self._fqtn(table_name)}",
            uri_query,
            data=data,
            backup_cutoff=backup_cutoff,
            array_agg=array_agg,
        )
        return sql.select_query

    def _query_for_select_many(
        self, uri_query: str, tables: list, apply_cutoff: bool = False
    ) -> str:
        """Build a union query for selecting from multiple tables."""
        queries = []
        for table_name in tables:
            sql = self._query_for_select(
                table_name, uri_query, array_agg=True, apply_cutoff=apply_cutoff
            )
            queries.append(f"select {self.json_object_func}('{table_name}', ({sql}))")
        return " union all ".join(queries)
