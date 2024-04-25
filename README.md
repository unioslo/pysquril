
# pysquril

Python implementation of structured URI query language.

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
