
# pysquril

PYthon Structured URI Query Language. A library for implementing versioned, queryable, and auditable document oriented datastores. Features:

* Useful for implementing generic document oriented REST APIs
* Multi-tenancy
* Enforces uniqueness on records, otherwise schemaless
* SQL-like query language
  * features:
    * filtering (rows and columns/keys)
    * ordering
    * pagination
    * aggregation functions
    * group by
  * [EBNF](https://github.com/unioslo/pysquril/blob/master/docs/grammar.ebnf)
  * [Railroad diagrams](https://unioslo.github.io/pysquril/grammar.html)
* Audit (with rollback) - who, what, when, why
  * default events: update, delete
  * optional events: create, read
* PostgreSQL and sqlite database backends

## Example library usage

```python
import sqlite3

from pysquril.backends import SqliteBackend, sqlite_init

# get a connection pool
# most real world usage would use persistent storage
engine = sqlite_init(":memory:")

# instantiate a backend, for a given tenant, and identityå
tenant = "tenant1"
backend = SqliteBackend(
    engine,
    schema=tenant,
    requestor="some-user",
    requestor_name="Some Person Name",
)

# add some data
table = "mytable"
backend.table_insert(
    table_name=table,
    data={"saying": "good", "being": ["glad"], "id": 0},
)
backend.table_insert(
    table_name=table,
    data={"saying": "good", "being": ["content", "detached"], "id": 1},
)

# query the data
print(list(backend.table_select(table_name=table, uri_query="select=being")))

# change the second record, with a reason
reason_for_update = "a more accurate description"
backend.table_update(
    table_name=table,
    uri_query=f"set=saying&where=being[0]=eq.'content'&message='{reason_for_update}'",
    data={"saying": "excellent"},
)

# check the audit
print(list(backend.table_select(table_name=f"{table}_audit", uri_query="")))

# restore the data to its prior state
backend.table_restore(
    table_name=table,
    uri_query="restore&primary_key=id",
)

# check that the data is back to its original state
result = list(
    backend.table_select(
        table_name=table,
        uri_query="select=saying&where=being[0]=eq.'content'",
    )
)
assert result[0][0] == "good"
```

## Tests

```bash
poetry install
poetry run pytest -vs --durations=0 pysquril/tests.py
```

## Generating docs

```bash
npm install -g ebnf2railroad
ebnf2railroad pysquril/docs/grammar.ebnf -o pysquril/docs/grammar.html
```

## License

BSD.
