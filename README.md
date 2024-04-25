
# pysquril

Python implementation of structured URI query language.

## Query language

* [EBNF](https://github.com/unioslo/pysquril/blob/master/docs/grammar.ebnf)
* [Railroad diagrams](https://unioslo.github.io/pysquril/grammar.html)

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
