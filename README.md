# pure-suggest-data-service

Backend functions for PURE suggest to provide publications data
 
## Project seutp

```
npm install
gcloud init
```

## API

Single DOI lookup remains unchanged:

```
GET /pure-publications?doi=10.1234/example
```

Bulk lookup accepts repeated `doi` parameters, a comma/semicolon/whitespace-separated `dois` parameter, or a JSON POST body:

```
GET /pure-publications?dois=10.1234/a,10.1234/b
POST /pure-publications
{ "dois": ["10.1234/a", "10.1234/b"] }
```

Bulk responses are ordered arrays of the same per-DOI objects returned by the single lookup. Each DOI is read from and written to the existing per-DOI cache entry.
