
import json

from sys import argv

from pysquril.interactive import B

data = [
    {"a": 1, "b": "yo", "c": [1, 2], "when": "2024-05-20T08:30:01.307111"},
    {"a": 11, "b": "man", "c": [3, 3, 9], "when": "2024-05-21T10:49:31.227735"},
    {"a": 9, "b": "yo", "d": {"e": 4}, "when": "2024-05-22T05:10:11.106601"},
    {"x": [{"a": 0, "b": 1, "c": "meh"}, {"a": 77, "b": 99}], "when": "2024-05-22T09:29:01.307735"},
    {"a": 0, "b": "y'all"},
]

selecting_keys = [
    "select=a",
    "select=a,c[0]",
    "select=d.e",
    "select=x[0|a]",
    "select=x[*|a]",
]

functions = [
    "select=avg(a)",
    "select=count(b)",
    "select=sum(a)",
    "select=min(c[0])",
    "select=max(c[1])",
    "select=max_ts(when),count(*)",
]

filtering_rows = [
    "where=a=eq.1",
    "where=a=gt.1",
    "where=b=eq.'y\\'all'",
    "where=b=like.'*all'",
    "where=b=in.[yo,man]",
    "where=x=not.is.null",
    "where=a=gte.0,and:b=eq.man",
    "where=a=eq.1,or:b=eq.'y\\'all'",
]

groupby = [
    "select=b,sum(a)&group_by=b",
    "select=b,count(*)&where=b=not.is.null&group_by=b",
]

ordering = [
    "order=a.desc",
]

pagination = [
    "range=0.1",
    "range=2.3",
]

together = [
    {
        "queries": selecting_keys,
        "title": "## Selecting keys",
    },
    {
        "queries": functions,
        "title": "## Functions",
    },
    {
        "queries": filtering_rows,
        "title": "## Filtering rows",
    },
    {
        "queries": groupby,
        "title": "## Group by",
    },
    {
        "queries": ordering,
        "title": "## Ordering",
    },
    {
        "queries": pagination,
        "title": "## Pagination",
    },
]

def generate_report(queries: dict, data: list, filename: str) -> None:
    with open(filename, "w") as f:
        f.write("# Query examples\n\n")
        f.write("Suppose one has the following data:\n")
        f.write("```json\n")
        f.write(json.dumps(data))
        f.write('\n```\n')
        for query_set in queries:
            f.write(f'\n{query_set.get("title")}\n\n')
            f.write("```txt\n")
            for query in query_set.get("queries"):
                query, result = B(verbose=True).D(data).Q(query)
                f.write(f"\n{query}\n")
                f.write(json.dumps(result))
                f.write("\n")
            f.write('```\n')

if __name__ == '__main__':
    generate_report(together, data, argv[1])
