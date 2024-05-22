
# pysquril

PYthon Structured URI Query Language. A library for implementing versioned, queryable, and auditable document oriented datastores. Features:

* Useful for implementing generic document oriented REST APIs
* Multi-tenancy
* Enforces uniqueness on records, otherwise schemaless
* SQL-like query language
  * features:
    * selecting keys
    * aggregation functions
    * group by
    * filtering rows
    * ordering
    * pagination
  * [Examples](https://github.com/unioslo/pysquril/blob/master/docs/examples.md)
  * [EBNF](https://github.com/unioslo/pysquril/blob/master/docs/grammar.ebnf)
  * [Railroad diagrams](https://unioslo.github.io/pysquril/grammar.html)
  * [helper class to experiment with queries](https://github.com/unioslo/pysquril/blob/master/pysquril/interactive.py)
* Ability to apply queries to multiple tables (document sets) simultaneously
* Audit (with rollback) - who, what, when, why
  * default events: update, delete
  * optional events: create, read
* PostgreSQL and sqlite database backends

## Getting to know pysquril

Use the helper class to run interactive queries on your own input data:

```txt
pysquril % poetry run python
>>> from pysquril.interactive import B
>>> B().D([{"x": 0, "y": 1}, {"x": 100, "y": 4, "z": [1,2]}]).Q("select=x,z[1]")
[[0, None], [100, 2]]
>>> B().D([{"x": 0, "y": 1}, {"x": 100, "y": 4, "z": [1,2]}]).Q("select=x,z[1]&order=x.desc")
[[100, 2], [0, None]]
>>> B().D([{"x": 0, "y": 1}, {"x": 100, "y": 4, "z": [1,2]}]).Q("select=x,z[1]&where=y=gt.1")
[[100, 2]]
```

## Example library usage

```python
import sqlite3

from pysquril.backends import SqliteBackend, sqlite_init

# get a connection pool
# most real world usage would use persistent storage
engine = sqlite_init(":memory:")

# instantiate a backend, for a given tenant, and identity
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
