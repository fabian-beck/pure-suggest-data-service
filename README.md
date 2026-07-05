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

Bulk responses are ordered arrays of the same per-DOI objects returned by the single lookup. The service starts per-DOI lookups concurrently while preserving response order. Each DOI is read from and written to the existing per-DOI cache entry.

## Prefetching

Normal user-facing requests record prefetch signals from both outgoing `reference` DOIs and incoming `citation` DOIs. A target DOI is counted once per distinct source publication, so repeated requests for the same source DOI do not inflate the signal. When the distinct-source count reaches `PREFETCH_SIGNAL_THRESHOLD`, the service enqueues a low-priority background fetch with `prefetch=true`.

Prefetch tasks write to the same `pure-publications` cache, but they do not record further prefetch signals. This prevents recursive citation/reference expansion. The deployment needs a Cloud Tasks queue named `pure-publications-prefetch` in addition to the existing refresh queue.

Tuning environment variables:

- `PREFETCH_SIGNAL_THRESHOLD` default `2`
- `PREFETCH_MAX_DOIS_PER_RELATION` default `50`
- `PREFETCH_MAX_SIGNALS_PER_REQUEST` default `200`
- `PREFETCH_MAX_ENQUEUES_PER_REQUEST` default `5`
- `PREFETCH_REQUEUE_COOLDOWN_MS` default `86400000`

## Log reports

Generate a self-contained interactive HTML report from the structured Cloud Logging output of `pure-publications`:

```
python scripts/log_report.py --project pure-suggest-data-service --since 24h --output reports/latest-log-report.html
```

If `gcloud` is installed but not on the Python subprocess path, pass it explicitly:

```
python scripts/log_report.py --project pure-suggest-data-service --since 7d --limit 10000 --gcloud "C:\Users\ba5tm7\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd" --save-raw reports/latest-log-raw.json --output reports/latest-log-report.html
```

The report focuses on this service's operational questions: cache tags, provider status and latency, Crossref backoff/rate limiting, fallback metadata sources inferred from provider successes, bulk-request summaries, and prefetch queue signals. It keeps `404` provider misses visible without counting them as operational failures.

Useful modes:

- `--input reports/latest-log-raw.json` rerenders a saved `gcloud logging read --format=json` result without querying Google Cloud again.
- `--demo` generates a small built-in report for a quick local smoke test.
- `--filter-extra 'jsonPayload.tag="cache-refresh"'` adds an extra Cloud Logging filter clause.
