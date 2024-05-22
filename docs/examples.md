# Query examples

Suppose one has the following data:
```json
[
    {"a": 1, "b": "yo", "c": [1, 2], "when": "2024-05-20T08:30:01.307111"},
    {"a": 11, "b": "man", "c": [3, 3, 9], "when": "2024-05-21T10:49:31.227735"},
    {"a": 9, "b": "yo", "d": {"e": 4}, "when": "2024-05-22T05:10:11.106601"},
    {"x": [{"a": 0, "b": 1, "c": "meh"}, {"a": 77, "b": 99}], "when": "2024-05-22T09:29:01.307735"},
    {"a": 0, "b": "y'all"}
]
```

## Selecting keys

```txt
select=a
[[1], [11], [9], [null], [0]]

select=a,c[0]
[[1, 1], [11, 3], [9, null], [null, null], [0, null]]

select=d.e
[[null], [null], [4], [null], [null]]

select=x[0|a]
[[null], [null], [null], [0], [null]]

select=x[*|a]
[[null], [null], [null], [[0, 77]], [null]]
```

## Functions

```txt
select=avg(a)
[[5.25]]

select=count(b)
[[4]]

select=sum(a)
[[21]]

select=min(c[0])
[[1]]

select=max(c[1])
[[3]]

select=max_ts(when),count(*)
[["2024-05-22T09:29:01.307735", 5]]
```

## Filtering rows

```txt
where=a=eq.1
[{"a": 1, "b": "yo", "c": [1, 2], "when": "2024-05-20T08:30:01.307111"}]

where=a=gt.1
[{"a": 11, "b": "man", "c": [3, 3, 9], "when": "2024-05-21T10:49:31.227735"}, {"a": 9, "b": "yo", "d": {"e": 4}, "when": "2024-05-22T05:10:11.106601"}]

where=b=eq.'y\\'all'
[{"a": 0, "b": "y'all"}]

where=b=like.'*all'
[{"a": 0, "b": "y'all"}]

where=b=in.[yo,man]
[{"a": 1, "b": "yo", "c": [1, 2], "when": "2024-05-20T08:30:01.307111"}, {"a": 11, "b": "man", "c": [3, 3, 9], "when": "2024-05-21T10:49:31.227735"}, {"a": 9, "b": "yo", "d": {"e": 4}, "when": "2024-05-22T05:10:11.106601"}]

where=x=not.is.null
[{"x": [{"a": 0, "b": 1, "c": "meh"}, {"a": 77, "b": 99}], "when": "2024-05-22T09:29:01.307735"}]

where=a=gte.0,and:b=eq.man
[{"a": 11, "b": "man", "c": [3, 3, 9], "when": "2024-05-21T10:49:31.227735"}]

where=a=eq.1,or:b=eq.'y\\'all'
[{"a": 1, "b": "yo", "c": [1, 2], "when": "2024-05-20T08:30:01.307111"}, {"a": 0, "b": "y'all"}]
```

## Group by
```txt

select=b,sum(a)&group_by=b
[[null, null], ["man", 11], ["y'all", 0], ["yo", 10]]

select=b,count(*)&where=b=not.is.null&group_by=b
[["man", 1], ["y'all", 1], ["yo", 2]]
```

## Ordering

```txt
order=a.desc
[{"a": 11, "b": "man", "c": [3, 3, 9], "when": "2024-05-21T10:49:31.227735"}, {"a": 9, "b": "yo", "d": {"e": 4}, "when": "2024-05-22T05:10:11.106601"}, {"a": 1, "b": "yo", "c": [1, 2], "when": "2024-05-20T08:30:01.307111"}, {"a": 0, "b": "y'all"}, {"x": [{"a": 0, "b": 1, "c": "meh"}, {"a": 77, "b": 99}], "when": "2024-05-22T09:29:01.307735"}]
```

## Pagination

```txt
range=0.1
[{"a": 1, "b": "yo", "c": [1, 2], "when": "2024-05-20T08:30:01.307111"}]

range=2.3
[{"a": 9, "b": "yo", "d": {"e": 4}, "when": "2024-05-22T05:10:11.106601"}, {"x": [{"a": 0, "b": 1, "c": "meh"}, {"a": 77, "b": 99}], "when": "2024-05-22T09:29:01.307735"}, {"a": 0, "b": "y'all"}]
```
