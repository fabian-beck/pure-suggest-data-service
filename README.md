# pure-suggest-data-service

Backend functions for PURE suggest to provide publications data
 
## Project setup

```
npm install
gcloud init
```

## Deployment

```
npm run deploy
```

The function runs on Node.js 24 in `europe-west1` and needs:

- the Cloud Tasks queues `pure-publications-refresh` and `pure-publications-prefetch` in the same region; their dispatch rates limit how fast background lookups reach the providers
- the environment variables `OPENCITATIONS_ACCESS_TOKEN` and `OPENALEX_API_KEY` (both optional; without them OpenCitations is asked without a token and OpenAlex is skipped). Set them once, later deployments keep them:

```
gcloud functions deploy pure-publications --gen2 --region=europe-west1 --update-env-vars OPENCITATIONS_ACCESS_TOKEN=...
```

`npm start` runs the function locally with the Functions Framework.

## Tests

```
npm test
```

Provider requests and Firestore are stubbed (`test/helpers.js`). The tests in `test/store.integration.test.js` use the real Firestore access and only run against the Firestore emulator:

```
npx firebase-tools emulators:exec --only firestore --project demo-pure "npm test"
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

## Stored records

Each DOI has one record in the `pure-publications` Firestore collection (accessed through `store.js`). A refresh merges the provider results into the stored record instead of replacing it:

- A field that the fresh result lacks keeps its stored value, so a failed or partial provider response never removes data. The log entry lists these fields as `keptStoredFields`.
- Metadata sources are ranked Crossref, OpenCitations Meta, OpenAlex, DataCite. Fresh values replace stored ones only if they come from an equal or better source; a weaker source just fills gaps.
- While Crossref is unavailable, a stored record is kept as is (only its citations are updated) and Crossref is retried after 15 minutes.
- If the citation lookup fails, the stored citations are kept and the record is refreshed again after a day.

At most three Crossref requests are in flight per instance (`CROSSREF_MAX_CONCURRENT`); further lookups of a bulk request wait for a free slot, so many new DOIs at once do not run into Crossref's limit on concurrent requests.

`noCache=true` forces such a refresh for the requested DOIs, including the metadata; the response is the merged record.

### Refresh schedule

A record with metadata is never discarded. It is served as stored until its `refreshAfter` date; after that, the next request still gets the stored record and queues a background refresh. Each refresh updates the citations, while the metadata of a Crossref record is only fetched again at a longer interval (`getRefreshIntervals` in `index.js`):

| Publication age | Citations | Metadata |
| --- | --- | --- |
| up to 1 year | 7 days | 30 days |
| up to 5 years, or unknown | 30 days | 180 days |
| older | 90 days | 365 days |

Records from a fallback source ask Crossref again on every refresh. `metadataFetchedAt` and `citationsFetchedAt` record the last successful lookups.

Only entries without metadata (unknown DOIs) carry an `expireAt` date and are looked up again after a day. If the collection has a Firestore TTL policy on `expireAt`, it removes just these entries; records written before this schedule still carry `expireAt` until their next refresh, so the policy should be removed if stored data is to be kept (`gcloud firestore fields ttls list`).

## Prefetching

A publication that is requested for the first time is counted once as a signal for each of its outgoing `reference` DOIs and incoming `citation` DOIs. This happens in a background task; afterwards the stored record carries `prefetchSignalsAt`, and later requests for it cause no further bookkeeping. The counts are kept in the `prefetch-candidates` collection (one read and one write per target). When a target reaches `PREFETCH_SIGNAL_THRESHOLD` distinct sources and is not stored yet, the service enqueues a low-priority background fetch with `prefetch=true`.

The former `prefetch-observations` collection is no longer used and can be deleted.

Prefetch tasks write to the same `pure-publications` cache, but they do not record further prefetch signals. This prevents recursive citation/reference expansion. The deployment needs a Cloud Tasks queue named `pure-publications-prefetch` in addition to the existing refresh queue.

Tuning environment variables:

- `PREFETCH_SIGNAL_THRESHOLD` default `2`
- `PREFETCH_MAX_DOIS_PER_RELATION` default `50`
- `PREFETCH_MAX_SIGNALS_PER_REQUEST` default `200`
- `PREFETCH_MAX_ENQUEUES_PER_REQUEST` default `5`
- `PREFETCH_REQUEUE_COOLDOWN_MS` default `86400000`
- `PREFETCH_SIGNAL_TASK_DOIS_PER_REQUEST` default `10`

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
