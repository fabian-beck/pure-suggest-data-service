#!/usr/bin/env python3
"""Generate an interactive HTML report from pure-publications Cloud Logging data."""

from __future__ import annotations

import argparse
import collections
import datetime as dt
import json
import math
import os
import pathlib
import re
import statistics
import subprocess
import sys
import webbrowser
from typing import Any


DEFAULT_FUNCTION = "pure-publications"
DEFAULT_REGION = "europe-west1"
DEFAULT_OUTPUT = "reports/pure-publications-log-report.html"

PROVIDERS = [
    ("crossref", "Crossref", "status"),
    ("openCitationsMeta", "OpenCitations Meta", "status"),
    ("openAlex", "OpenAlex", "status"),
    ("dataCite", "DataCite", "status"),
    ("openCitations", "OpenCitations Citations", "statusCitations"),
]

CACHE_TAGS = [
    "cache-hit",
    "cache-stale",
    "cache-refresh",
    "cache-disabled",
    "cache-expired",
    "cache-miss",
]

PREFETCH_KEYS = [
    "signalCount",
    "queuedCount",
    "candidateCount",
    "cachedCount",
    "duplicateCount",
    "skippedCount",
    "errorCount",
    "enqueueErrorCount",
]


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def parse_datetime(value: str) -> dt.datetime:
    clean = value.strip()
    if clean.lower() == "now":
        return utc_now()
    if clean.endswith("Z"):
        clean = clean[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(clean)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Not a valid timestamp: {value}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def parse_duration(value: str) -> dt.timedelta:
    match = re.fullmatch(r"\s*(\d+)\s*([mhdw])\s*", value, flags=re.IGNORECASE)
    if not match:
        raise argparse.ArgumentTypeError(
            "Use a relative duration such as 30m, 24h, 7d, 2w or an ISO timestamp."
        )
    amount = int(match.group(1))
    unit = match.group(2).lower()
    factors = {
        "m": dt.timedelta(minutes=1),
        "h": dt.timedelta(hours=1),
        "d": dt.timedelta(days=1),
        "w": dt.timedelta(weeks=1),
    }
    return amount * factors[unit]


def parse_since(value: str, now: dt.datetime) -> dt.datetime:
    try:
        return now - parse_duration(value)
    except argparse.ArgumentTypeError:
        return parse_datetime(value)


def rfc3339(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def build_log_filter(args: argparse.Namespace, since: dt.datetime, until: dt.datetime | None) -> str:
    region_cloud_run = (
        f'resource.type="cloud_run_revision" '
        f'AND resource.labels.service_name="{args.function}" '
        f'AND resource.labels.location="{args.region}"'
    )
    region_cloud_function = (
        f'resource.type="cloud_function" '
        f'AND resource.labels.function_name="{args.function}" '
        f'AND resource.labels.region="{args.region}"'
    )
    payload_clause = '(jsonPayload.doi:* OR jsonPayload.tag="bulk" OR textPayload:"doi" OR textPayload:"bulk")'
    parts = [
        f"(({region_cloud_run}) OR ({region_cloud_function}))",
        payload_clause,
        f'timestamp>="{rfc3339(since)}"',
    ]
    if until:
        parts.append(f'timestamp<="{rfc3339(until)}"')
    if args.filter_extra:
        parts.append(f"({args.filter_extra})")
    return " AND ".join(parts)


def load_json_or_ndjson(path: pathlib.Path) -> list[dict[str, Any]]:
    text = path.read_text(encoding="utf-8")
    stripped = text.strip()
    if not stripped:
        return []
    try:
        data = json.loads(stripped)
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        if isinstance(data, dict):
            return [data]
    except json.JSONDecodeError:
        pass

    entries = []
    for line_no, line in enumerate(text.splitlines(), start=1):
        clean = line.strip()
        if not clean:
            continue
        try:
            item = json.loads(clean)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"{path}:{line_no}: expected JSON object or JSON array") from exc
        if isinstance(item, dict):
            entries.append(item)
    return entries


def run_gcloud(args: argparse.Namespace, log_filter: str) -> list[dict[str, Any]]:
    command = [
        args.gcloud,
        "logging",
        "read",
        log_filter,
        "--format=json",
        f"--limit={args.limit}",
        "--order=desc",
    ]
    if args.project:
        command.extend(["--project", args.project])

    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=args.timeout,
        )
    except FileNotFoundError as exc:
        raise SystemExit(
            f"Could not find '{args.gcloud}'. Install the Google Cloud CLI or pass --input."
        ) from exc
    except subprocess.CalledProcessError as exc:
        detail = exc.stderr.strip() or exc.stdout.strip()
        raise SystemExit(f"gcloud logging read failed:\n{detail}") from exc
    except subprocess.TimeoutExpired as exc:
        raise SystemExit(f"gcloud logging read timed out after {args.timeout} seconds") from exc

    if args.save_raw:
        raw_path = pathlib.Path(args.save_raw)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(result.stdout, encoding="utf-8")

    try:
        data = json.loads(result.stdout or "[]")
    except json.JSONDecodeError as exc:
        raise SystemExit("gcloud returned output that is not JSON") from exc
    return data if isinstance(data, list) else []


def parse_jsonish_text(text: str | None) -> dict[str, Any] | None:
    if not text:
        return None
    clean = text.strip()
    candidates = [clean]
    first = clean.find("{")
    last = clean.rfind("}")
    if first >= 0 and last > first:
        candidates.append(clean[first : last + 1])
    for candidate in candidates:
        if not candidate.startswith("{"):
            continue
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


def extract_payload(entry: dict[str, Any]) -> dict[str, Any] | None:
    payload = entry.get("jsonPayload")
    if isinstance(payload, dict):
        if set(payload) == {"message"} and isinstance(payload.get("message"), str):
            parsed = parse_jsonish_text(payload.get("message"))
            if parsed:
                payload = parsed
        else:
            payload = dict(payload)
    else:
        payload = parse_jsonish_text(entry.get("textPayload"))

    if not isinstance(payload, dict):
        return None

    looks_like_service_log = (
        "doi" in payload
        or payload.get("tag") == "bulk"
        or any(key in payload for key, _, _ in PROVIDERS)
        or "metadataSource" in payload
        or "prefetchTask" in payload
    )
    if not looks_like_service_log:
        return None

    payload["_timestamp"] = entry.get("timestamp") or entry.get("receiveTimestamp")
    payload["_insertId"] = entry.get("insertId")
    payload["_logName"] = entry.get("logName")
    return payload


def safe_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def status_text(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def status_is_issue(value: Any) -> bool:
    status = status_text(value)
    if not status:
        return False
    normalized = status.lower()
    if normalized in {"error", "timeout", "backoff", "enqueue-error", "mark-complete-error"}:
        return True
    if normalized in {"skipped", "missing-api-key"}:
        return False
    try:
        code = int(float(normalized))
    except ValueError:
        return normalized not in {"ok", "success", "fetched", "stored", "cached", "candidate", "queued"}
    return code in {401, 403, 408, 429} or code >= 500


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    sorted_values = sorted(values)
    index = (len(sorted_values) - 1) * pct
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return sorted_values[int(index)]
    lower_value = sorted_values[lower]
    upper_value = sorted_values[upper]
    return lower_value + (upper_value - lower_value) * (index - lower)


def iso_to_datetime(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        return parse_datetime(value)
    except argparse.ArgumentTypeError:
        return None


def bucket_label(value: dt.datetime, bucket: str) -> str:
    if bucket == "day":
        base = value.replace(hour=0, minute=0, second=0, microsecond=0)
        return base.strftime("%Y-%m-%d")
    if bucket == "hour":
        base = value.replace(minute=0, second=0, microsecond=0)
        return base.strftime("%Y-%m-%d %H:00")
    minute = (value.minute // 15) * 15
    base = value.replace(minute=minute, second=0, microsecond=0)
    return base.strftime("%Y-%m-%d %H:%M")


def choose_bucket(timestamps: list[dt.datetime]) -> str:
    if len(timestamps) < 2:
        return "hour"
    span = max(timestamps) - min(timestamps)
    if span <= dt.timedelta(hours=8):
        return "quarter"
    if span <= dt.timedelta(days=3):
        return "hour"
    return "day"


def provider_snapshot(event: dict[str, Any]) -> dict[str, dict[str, Any]]:
    snapshot: dict[str, dict[str, Any]] = {}
    for key, label, status_field in PROVIDERS:
        provider_data = event.get(key)
        if not isinstance(provider_data, dict):
            continue
        status = provider_data.get(status_field)
        if status is None and status_field != "status":
            status = provider_data.get("status")
        latency = safe_float(provider_data.get("processingTime"))
        snapshot[key] = {
            "label": label,
            "status": status_text(status),
            "latency": latency,
            "issue": status_is_issue(status),
            "error": provider_data.get("error"),
            "backoffUntil": provider_data.get("backoffUntil"),
        }
    return snapshot




def status_is_success(value: Any) -> bool:
    status = status_text(value)
    if status is None:
        return False
    try:
        return int(float(status)) == 200
    except ValueError:
        return status.lower() in {"ok", "success", "fetched"}


def infer_metadata_source(event: dict[str, Any]) -> str:
    explicit = event.get("metadataSource")
    if explicit:
        return str(explicit)

    providers = provider_snapshot(event)
    ordered_sources = [
        ("crossref", "Crossref"),
        ("openCitationsMeta", "OpenCitations Meta"),
        ("openAlex", "OpenAlex"),
        ("dataCite", "DataCite"),
    ]
    for key, label in ordered_sources:
        if status_is_success(providers.get(key, {}).get("status")):
            return label

    if event.get("tag") == "cache-hit" and event.get("title"):
        return "cache"
    if event.get("title"):
        return "unknown"
    return "none"
def summarize(entries: list[dict[str, Any]], args: argparse.Namespace, log_filter: str) -> dict[str, Any]:
    events = [payload for payload in (extract_payload(entry) for entry in entries) if payload]
    events.sort(key=lambda item: item.get("_timestamp") or "")

    request_events = [
        event for event in events if event.get("doi") and event.get("tag") != "bulk"
    ]
    bulk_events = [event for event in events if event.get("tag") == "bulk"]
    timestamps = [
        parsed for parsed in (iso_to_datetime(event.get("_timestamp")) for event in events) if parsed
    ]
    bucket = choose_bucket(timestamps)

    cache_counts = collections.Counter(event.get("tag") for event in request_events if event.get("tag"))
    metadata_counts = collections.Counter(
        infer_metadata_source(event) for event in request_events
    )
    cache_write_counts = collections.Counter(
        event.get("cacheWrite") or "none" for event in request_events
    )
    refresh_counts = collections.Counter(event.get("refresh") for event in request_events if event.get("refresh"))
    prefetch_task_counts = collections.Counter(
        event.get("prefetchTask") for event in request_events if event.get("prefetchTask")
    )

    provider_stats: dict[str, dict[str, Any]] = {}
    for key, label, _ in PROVIDERS:
        statuses: collections.Counter[str] = collections.Counter()
        latencies: list[float] = []
        issue_count = 0
        for event in request_events:
            snapshot = provider_snapshot(event).get(key)
            if not snapshot:
                continue
            if snapshot["status"]:
                statuses[snapshot["status"]] += 1
                if snapshot["issue"]:
                    issue_count += 1
            if snapshot["latency"] is not None:
                latencies.append(snapshot["latency"])
        provider_stats[key] = {
            "label": label,
            "count": sum(statuses.values()),
            "statuses": dict(statuses.most_common()),
            "issues": issue_count,
            "avg": statistics.fmean(latencies) if latencies else None,
            "p50": percentile(latencies, 0.5),
            "p95": percentile(latencies, 0.95),
            "max": max(latencies) if latencies else None,
        }

    request_latencies = [
        latency
        for latency in (safe_float(event.get("processingTime")) for event in request_events)
        if latency is not None
    ]
    time_series: dict[str, dict[str, Any]] = {}
    for event in request_events:
        parsed = iso_to_datetime(event.get("_timestamp"))
        if not parsed:
            continue
        label = bucket_label(parsed, bucket)
        point = time_series.setdefault(label, {"label": label, "count": 0, "issues": 0, "cacheHit": 0})
        point["count"] += 1
        if event.get("tag") == "cache-hit":
            point["cacheHit"] += 1
        if any(provider["issue"] for provider in provider_snapshot(event).values()):
            point["issues"] += 1

    prefetch_totals = dict.fromkeys(PREFETCH_KEYS, 0)
    prefetch_budget_exhausted = 0
    for event in request_events:
        prefetch = event.get("prefetch")
        if not isinstance(prefetch, dict):
            continue
        for key in PREFETCH_KEYS:
            prefetch_totals[key] += safe_int(prefetch.get(key)) or 0
        if prefetch.get("budgetExhausted"):
            prefetch_budget_exhausted += 1

    bulk_prefetch_signals = sum(safe_int(event.get("prefetchSignals")) or 0 for event in bulk_events)
    bulk_prefetch_enqueues = sum(
        safe_int(event.get("prefetchEnqueueAttempts")) or 0 for event in bulk_events
    )

    problem_events = []
    rows = []
    for index, event in enumerate(request_events):
        providers = provider_snapshot(event)
        provider_issues = [
            f"{provider['label']}: {provider['status']}"
            for provider in providers.values()
            if provider["issue"]
        ]
        own_issues = []
        if event.get("refresh") == "enqueue-error":
            own_issues.append("refresh enqueue-error")
        if event.get("prefetchTask") == "mark-complete-error":
            own_issues.append("prefetch mark-complete-error")
        prefetch = event.get("prefetch")
        if isinstance(prefetch, dict):
            if safe_int(prefetch.get("errorCount")):
                own_issues.append(f"prefetch errors: {prefetch.get('errorCount')}")
            if safe_int(prefetch.get("enqueueErrorCount")):
                own_issues.append(f"prefetch enqueue errors: {prefetch.get('enqueueErrorCount')}")
        issue_labels = provider_issues + own_issues

        provider_statuses = {
            key: {
                "status": provider["status"],
                "latency": provider["latency"],
                "issue": provider["issue"],
            }
            for key, provider in providers.items()
        }
        row = {
            "id": index,
            "timestamp": event.get("_timestamp"),
            "doi": event.get("doi"),
            "tag": event.get("tag") or "none",
            "processingTime": safe_float(event.get("processingTime")),
            "title": event.get("title"),
            "metadataSource": infer_metadata_source(event),
            "cacheWrite": event.get("cacheWrite"),
            "refresh": event.get("refresh"),
            "prefetchTask": event.get("prefetchTask"),
            "prefetch": prefetch if isinstance(prefetch, dict) else None,
            "providers": provider_statuses,
            "issues": issue_labels,
            "raw": event,
        }
        rows.append(row)
        if issue_labels:
            problem_events.append(row)

    cache_observed = sum(cache_counts[tag] for tag in CACHE_TAGS)
    cache_hits = cache_counts.get("cache-hit", 0)
    cache_hit_rate = cache_hits / cache_observed if cache_observed else None
    unique_dois = len({event.get("doi") for event in request_events if event.get("doi")})

    report = {
        "meta": {
            "generatedAt": rfc3339(utc_now()),
            "function": args.function,
            "project": args.project or os.environ.get("GOOGLE_CLOUD_PROJECT") or "",
            "region": args.region,
            "filter": log_filter,
            "sourceEntries": len(entries),
            "structuredEvents": len(events),
            "firstTimestamp": rfc3339(min(timestamps)) if timestamps else None,
            "lastTimestamp": rfc3339(max(timestamps)) if timestamps else None,
            "limit": args.limit,
            "limitReached": len(entries) >= args.limit and not args.input and not args.demo,
            "bucket": bucket,
        },
        "summary": {
            "requests": len(request_events),
            "bulkRuns": len(bulk_events),
            "uniqueDois": unique_dois,
            "cacheHitRate": cache_hit_rate,
            "avgProcessingTime": statistics.fmean(request_latencies) if request_latencies else None,
            "p95ProcessingTime": percentile(request_latencies, 0.95),
            "providerIssueEvents": len(problem_events),
            "prefetchSignals": prefetch_totals.get("signalCount", 0) + bulk_prefetch_signals,
            "prefetchQueued": prefetch_totals.get("queuedCount", 0) + bulk_prefetch_enqueues,
        },
        "cacheCounts": ordered_counts(cache_counts, CACHE_TAGS),
        "metadataCounts": dict(metadata_counts.most_common()),
        "cacheWriteCounts": dict(cache_write_counts.most_common()),
        "refreshCounts": dict(refresh_counts.most_common()),
        "prefetchTaskCounts": dict(prefetch_task_counts.most_common()),
        "prefetchTotals": prefetch_totals,
        "prefetchBudgetExhausted": prefetch_budget_exhausted,
        "providerStats": provider_stats,
        "timeSeries": [time_series[key] for key in sorted(time_series)],
        "problemRows": problem_events[:50],
        "slowRows": sorted(
            rows,
            key=lambda row: row["processingTime"] if row["processingTime"] is not None else -1,
            reverse=True,
        )[:25],
        "rows": rows,
    }
    return report


def ordered_counts(counter: collections.Counter[str], preferred_order: list[str]) -> dict[str, int]:
    result: dict[str, int] = {}
    for key in preferred_order:
        if counter.get(key):
            result[key] = counter[key]
    for key, value in counter.most_common():
        if key not in result:
            result[str(key)] = value
    return result


def make_demo_entries() -> list[dict[str, Any]]:
    now = utc_now().replace(minute=0, second=0, microsecond=0)
    samples = [
        {
            "doi": "10.1145/3544548.3581099",
            "tag": "cache-hit",
            "metadataSource": "Crossref",
            "title": "A cached publication with prefetch signals",
            "processingTime": 42,
            "prefetch": {
                "signalCount": 8,
                "queuedCount": 2,
                "candidateCount": 4,
                "cachedCount": 1,
                "duplicateCount": 1,
                "skippedCount": 0,
                "errorCount": 0,
                "enqueueErrorCount": 0,
            },
        },
        {
            "doi": "10.1000/missing-crossref",
            "tag": "cache-miss",
            "metadataSource": "OpenCitations Meta",
            "title": "Fallback metadata example",
            "processingTime": 1280,
            "crossref": {"status": 404, "processingTime": 310},
            "openCitationsMeta": {"status": 200, "processingTime": 240},
            "openCitations": {"statusCitations": 200, "processingTime": 680},
            "cacheWrite": "stored",
        },
        {
            "doi": "10.1000/openalex-timeout",
            "tag": "cache-expired",
            "metadataSource": "none",
            "title": None,
            "processingTime": 10250,
            "crossref": {"status": "timeout", "processingTime": 10000},
            "openAlex": {"status": "skipped", "reason": "missing-api-key"},
            "cacheWrite": "skipped-transient-crossref-failure",
        },
        {
            "doi": "10.1000/stale",
            "tag": "cache-stale",
            "metadataSource": "Crossref",
            "title": "Stale cache refreshed in background",
            "processingTime": 68,
            "refresh": "queued",
            "refreshTask": "projects/demo/locations/europe-west1/queues/pure-publications-refresh/tasks/1",
        },
        {
            "doi": "10.1000/prefetched",
            "tag": "cache-refresh",
            "metadataSource": "Crossref",
            "title": "Prefetch task completed",
            "processingTime": 950,
            "crossref": {"status": 200, "processingTime": 400},
            "openCitations": {"statusCitations": 200, "processingTime": 330},
            "cacheWrite": "stored",
            "prefetchTask": "fetched",
        },
        {
            "tag": "bulk",
            "doiCount": 4,
            "prefetchSignals": 14,
            "prefetchEnqueueAttempts": 3,
            "processingTime": 1880,
        },
    ]
    entries = []
    for index, payload in enumerate(samples):
        timestamp = now - dt.timedelta(hours=len(samples) - index)
        entries.append(
            {
                "timestamp": rfc3339(timestamp),
                "insertId": f"demo-{index}",
                "jsonPayload": {"severity": "INFO", **payload},
            }
        )
    return entries


def safe_json_for_script(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")


def render_html(report: dict[str, Any]) -> str:
    report_json = safe_json_for_script(report)
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Pure Publications Log Report</title>
<style>
:root {{
  color-scheme: light;
  --ink: #172033;
  --muted: #667085;
  --line: #d9dee8;
  --page: #f5f7fb;
  --panel: #ffffff;
  --accent: #0f766e;
  --blue: #2563eb;
  --warn: #b45309;
  --bad: #b91c1c;
  --ok: #15803d;
  --violet: #7c3aed;
  --shadow: 0 12px 36px rgba(23, 32, 51, 0.09);
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}}
* {{ box-sizing: border-box; }}
body {{
  margin: 0;
  background: var(--page);
  color: var(--ink);
  font-size: 15px;
  line-height: 1.45;
}}
header {{
  background: #ffffff;
  border-bottom: 1px solid var(--line);
}}
.wrap {{
  width: min(1380px, calc(100vw - 32px));
  margin: 0 auto;
}}
.hero {{
  padding: 28px 0 20px;
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: 20px;
  align-items: end;
}}
h1 {{
  margin: 0;
  font-size: clamp(28px, 4vw, 44px);
  line-height: 1.05;
  letter-spacing: 0;
}}
.subline {{
  margin: 10px 0 0;
  color: var(--muted);
  max-width: 900px;
}}
.pillrow {{
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  justify-content: flex-end;
}}
.pill {{
  border: 1px solid var(--line);
  border-radius: 999px;
  padding: 6px 10px;
  background: #fff;
  color: var(--muted);
  white-space: nowrap;
}}
main {{
  padding: 22px 0 48px;
}}
.kpis {{
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 12px;
  margin-bottom: 18px;
}}
.kpi, .panel {{
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: 8px;
  box-shadow: var(--shadow);
}}
.kpi {{
  padding: 16px;
  min-height: 112px;
}}
.kpi .label {{
  color: var(--muted);
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: 0.08em;
}}
.kpi .value {{
  margin-top: 8px;
  font-size: clamp(26px, 5vw, 40px);
  font-weight: 800;
  line-height: 1;
}}
.kpi .note {{
  margin-top: 10px;
  color: var(--muted);
  font-size: 13px;
}}
.grid {{
  display: grid;
  grid-template-columns: minmax(0, 1.35fr) minmax(320px, 0.65fr);
  gap: 14px;
  align-items: start;
}}
.panel {{
  padding: 16px;
  margin-bottom: 14px;
}}
.panel h2 {{
  margin: 0 0 12px;
  font-size: 18px;
  letter-spacing: 0;
}}
.panel p {{
  margin: 0 0 10px;
  color: var(--muted);
}}
.chart {{
  min-height: 260px;
}}
.bar-list {{
  display: grid;
  gap: 9px;
}}
.bar-row {{
  display: grid;
  grid-template-columns: minmax(118px, 170px) minmax(0, 1fr) 52px;
  gap: 10px;
  align-items: center;
}}
.bar-label {{
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}}
.bar-track {{
  height: 12px;
  background: #eef2f7;
  border-radius: 999px;
  overflow: hidden;
}}
.bar-fill {{
  height: 100%;
  width: 0%;
  background: var(--accent);
}}
.bar-value {{
  text-align: right;
  color: var(--muted);
  font-variant-numeric: tabular-nums;
}}
.provider-table, .data-table {{
  width: 100%;
  border-collapse: collapse;
}}
th, td {{
  border-bottom: 1px solid var(--line);
  padding: 9px 8px;
  text-align: left;
  vertical-align: top;
}}
th {{
  color: var(--muted);
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  background: #fbfcff;
  position: sticky;
  top: 0;
  z-index: 1;
}}
tbody tr:hover {{
  background: #f8fafc;
}}
.status {{
  display: inline-flex;
  align-items: center;
  gap: 6px;
  min-height: 24px;
  border-radius: 999px;
  padding: 2px 8px;
  background: #eef2f7;
  color: var(--ink);
  font-size: 12px;
  white-space: nowrap;
}}
.status.ok {{ background: #dcfce7; color: #166534; }}
.status.warn {{ background: #fef3c7; color: #92400e; }}
.status.bad {{ background: #fee2e2; color: #991b1b; }}
.status.neutral {{ background: #eef2ff; color: #3730a3; }}
.controls {{
  display: grid;
  grid-template-columns: minmax(220px, 1fr) repeat(3, minmax(150px, 190px)) auto;
  gap: 10px;
  margin-bottom: 12px;
  align-items: end;
}}
label {{
  display: grid;
  gap: 5px;
  color: var(--muted);
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: 0.06em;
}}
input, select, button {{
  min-height: 38px;
  border: 1px solid var(--line);
  border-radius: 6px;
  background: #fff;
  color: var(--ink);
  font: inherit;
  letter-spacing: 0;
}}
input, select {{
  padding: 7px 10px;
}}
button {{
  cursor: pointer;
  padding: 7px 12px;
  font-weight: 650;
}}
button.primary {{
  background: var(--ink);
  color: #fff;
  border-color: var(--ink);
}}
.table-wrap {{
  max-height: 640px;
  overflow: auto;
  border: 1px solid var(--line);
  border-radius: 8px;
  background: #fff;
}}
.doi {{
  font-family: ui-monospace, SFMono-Regular, Consolas, "Liberation Mono", monospace;
  font-size: 12px;
}}
.title-cell {{
  max-width: 340px;
}}
.muted {{
  color: var(--muted);
}}
.raw {{
  margin: 0;
  white-space: pre-wrap;
  overflow: auto;
  max-height: 280px;
  background: #101828;
  color: #e6edf7;
  border-radius: 6px;
  padding: 12px;
  font-size: 12px;
}}
.spark {{
  width: 100%;
  height: 260px;
  display: block;
}}
.legend {{
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  color: var(--muted);
  font-size: 13px;
}}
.legend span::before {{
  content: "";
  display: inline-block;
  width: 10px;
  height: 10px;
  border-radius: 2px;
  margin-right: 6px;
  vertical-align: -1px;
  background: var(--blue);
}}
.legend .issues::before {{ background: var(--bad); }}
.legend .cache::before {{ background: var(--ok); }}
.empty {{
  padding: 22px;
  color: var(--muted);
  text-align: center;
}}
.small {{
  font-size: 12px;
}}
.split {{
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
  gap: 14px;
}}
@media (max-width: 980px) {{
  .hero, .grid, .split {{
    grid-template-columns: 1fr;
  }}
  .pillrow {{
    justify-content: flex-start;
  }}
  .kpis {{
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }}
  .controls {{
    grid-template-columns: 1fr 1fr;
  }}
}}
@media (max-width: 620px) {{
  .wrap {{
    width: min(100vw - 20px, 1380px);
  }}
  .kpis, .controls {{
    grid-template-columns: 1fr;
  }}
  .bar-row {{
    grid-template-columns: 1fr;
    gap: 5px;
  }}
}}
</style>
</head>
<body>
<header>
  <div class="wrap hero">
    <div>
      <h1>Pure Publications Log Report</h1>
      <p class="subline" id="subtitle"></p>
    </div>
    <div class="pillrow" id="metaPills"></div>
  </div>
</header>
<main class="wrap">
  <section class="kpis" id="kpis"></section>
  <section class="grid">
    <div>
      <section class="panel">
        <h2>Traffic And Health Over Time</h2>
        <div id="timeChart" class="chart"></div>
        <div class="legend">
          <span>requests</span>
          <span class="cache">cache hits</span>
          <span class="issues">provider issues</span>
        </div>
      </section>
      <section class="panel">
        <h2>DOI Request Explorer</h2>
        <div class="controls">
          <label>Search
            <input id="search" type="search" placeholder="DOI, title, source, status">
          </label>
          <label>Cache
            <select id="tagFilter"></select>
          </label>
          <label>Provider
            <select id="providerFilter"></select>
          </label>
          <label>Issues
            <select id="issueFilter">
              <option value="all">All requests</option>
              <option value="issues">With issues</option>
              <option value="clean">Without issues</option>
            </select>
          </label>
          <button class="primary" id="downloadCsv">CSV</button>
        </div>
        <div class="table-wrap">
          <table class="data-table">
            <thead>
              <tr>
                <th data-sort="timestamp">Time</th>
                <th data-sort="doi">DOI</th>
                <th data-sort="tag">Cache</th>
                <th data-sort="metadataSource">Source</th>
                <th data-sort="processingTime">Total</th>
                <th>Providers</th>
                <th>Notes</th>
              </tr>
            </thead>
            <tbody id="requestRows"></tbody>
          </table>
        </div>
        <p class="small muted" id="rowCount"></p>
      </section>
    </div>
    <aside>
      <section class="panel">
        <h2>Cache Behavior</h2>
        <div id="cacheBars" class="bar-list"></div>
      </section>
      <section class="panel">
        <h2>Metadata Sources</h2>
        <div id="metadataBars" class="bar-list"></div>
      </section>
      <section class="panel">
        <h2>Prefetch Pipeline</h2>
        <div id="prefetchBars" class="bar-list"></div>
      </section>
    </aside>
  </section>
  <section class="panel">
    <h2>Provider Status And Latency</h2>
    <div class="table-wrap">
      <table class="provider-table">
        <thead>
          <tr>
            <th>Provider</th>
            <th>Calls</th>
            <th>Issues</th>
            <th>Statuses</th>
            <th>Average</th>
            <th>p95</th>
            <th>Max</th>
          </tr>
        </thead>
        <tbody id="providerRows"></tbody>
      </table>
    </div>
  </section>
  <section class="split">
    <section class="panel">
      <h2>Recent Provider And Queue Issues</h2>
      <div id="problemList"></div>
    </section>
    <section class="panel">
      <h2>Slowest Requests</h2>
      <div id="slowList"></div>
    </section>
  </section>
</main>
<script>
const REPORT = {report_json};

const PROVIDERS = [
  ["crossref", "Crossref"],
  ["openCitationsMeta", "OpenCitations Meta"],
  ["openAlex", "OpenAlex"],
  ["dataCite", "DataCite"],
  ["openCitations", "OpenCitations Citations"],
];

const state = {{
  search: "",
  tag: "all",
  provider: "all",
  issue: "all",
  sortKey: "timestamp",
  sortDir: -1,
  expanded: null,
}};

function fmtNumber(value, digits = 0) {{
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "n/a";
  return Number(value).toLocaleString(undefined, {{ maximumFractionDigits: digits }});
}}

function fmtMs(value) {{
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "n/a";
  const number = Number(value);
  if (number >= 1000) return (number / 1000).toLocaleString(undefined, {{ maximumFractionDigits: 2 }}) + " s";
  return Math.round(number).toLocaleString() + " ms";
}}

function fmtPct(value) {{
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "n/a";
  return (Number(value) * 100).toLocaleString(undefined, {{ maximumFractionDigits: 1 }}) + "%";
}}

function statusClass(status, issue) {{
  if (issue) return "bad";
  if (status === null || status === undefined) return "neutral";
  const text = String(status).toLowerCase();
  if (text === "skipped") return "neutral";
  const code = Number(text);
  if (!Number.isNaN(code) && code >= 300) {{
    if (code === 404) return "warn";
    if (code === 429 || code >= 500) return "bad";
    return "warn";
  }}
  return "ok";
}}

function escapeHtml(value) {{
  return String(value ?? "").replace(/[&<>"']/g, char => ({{
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  }}[char]));
}}

function statusPill(status, issue) {{
  if (status === null || status === undefined || status === "") return "";
  return `<span class="status ${{statusClass(status, issue)}}">${{escapeHtml(status)}}</span>`;
}}

function renderMeta() {{
  document.getElementById("subtitle").textContent =
    `${{REPORT.meta.function}} in ${{REPORT.meta.region}}. Generated ${{REPORT.meta.generatedAt}} from ${{fmtNumber(REPORT.meta.sourceEntries)}} Cloud Logging entries.${{REPORT.meta.firstTimestamp ? ` Observed ${{REPORT.meta.firstTimestamp}} to ${{REPORT.meta.lastTimestamp}}.` : ""}}`;
  const pills = [
    REPORT.meta.project ? ["Project", REPORT.meta.project] : null,
    ["Structured", fmtNumber(REPORT.meta.structuredEvents)],
    ["Bucket", REPORT.meta.bucket],
    REPORT.meta.limitReached ? ["Limit", `reached ${{fmtNumber(REPORT.meta.limit)}}`] : null,
  ].filter(Boolean);
  document.getElementById("metaPills").innerHTML = pills
    .map(([label, value]) => `<span class="pill">${{escapeHtml(label)}}: ${{escapeHtml(value)}}</span>`)
    .join("");
}}

function renderKpis() {{
  const s = REPORT.summary;
  const kpis = [
    ["DOI requests", fmtNumber(s.requests), `${{fmtNumber(s.uniqueDois)}} unique DOIs`],
    ["Cache hit rate", fmtPct(s.cacheHitRate), "fresh cache responses among cache-tagged requests"],
    ["p95 total time", fmtMs(s.p95ProcessingTime), `average ${{fmtMs(s.avgProcessingTime)}}`],
    ["Prefetch queued", fmtNumber(s.prefetchQueued), `${{fmtNumber(s.prefetchSignals)}} observed relation signals`],
  ];
  document.getElementById("kpis").innerHTML = kpis.map(([label, value, note]) => `
    <article class="kpi">
      <div class="label">${{escapeHtml(label)}}</div>
      <div class="value">${{escapeHtml(value)}}</div>
      <div class="note">${{escapeHtml(note)}}</div>
    </article>
  `).join("");
}}

function renderBars(id, counts, palette = ["#0f766e", "#2563eb", "#b45309", "#7c3aed", "#15803d", "#b91c1c"]) {{
  const entries = Object.entries(counts || {{}}).filter(([, value]) => Number(value) > 0);
  const max = Math.max(1, ...entries.map(([, value]) => Number(value)));
  const target = document.getElementById(id);
  if (!entries.length) {{
    target.innerHTML = '<div class="empty">No matching events in this window.</div>';
    return;
  }}
  target.innerHTML = entries.map(([label, value], index) => `
    <div class="bar-row">
      <div class="bar-label" title="${{escapeHtml(label)}}">${{escapeHtml(label)}}</div>
      <div class="bar-track"><div class="bar-fill" style="width:${{Math.max(2, Number(value) / max * 100)}}%;background:${{palette[index % palette.length]}}"></div></div>
      <div class="bar-value">${{fmtNumber(value)}}</div>
    </div>
  `).join("");
}}

function renderTimeChart() {{
  const data = REPORT.timeSeries || [];
  const target = document.getElementById("timeChart");
  if (!data.length) {{
    target.innerHTML = '<div class="empty">No request timestamps available.</div>';
    return;
  }}
  const width = 980;
  const height = 260;
  const pad = 32;
  const max = Math.max(1, ...data.flatMap(point => [point.count, point.cacheHit, point.issues]));
  const slot = (width - pad * 2) / data.length;
  const barWidth = Math.max(3, Math.min(28, slot * 0.62));
  const y = value => height - pad - (Number(value) / max) * (height - pad * 2);
  const bars = data.map((point, index) => {{
    const x = pad + index * slot + (slot - barWidth) / 2;
    const requestH = height - pad - y(point.count);
    const cacheH = height - pad - y(point.cacheHit);
    const issueH = height - pad - y(point.issues);
    return `
      <g>
        <title>${{escapeHtml(point.label)}}: ${{point.count}} requests, ${{point.cacheHit}} cache hits, ${{point.issues}} issues</title>
        <rect x="${{x.toFixed(2)}}" y="${{y(point.count).toFixed(2)}}" width="${{barWidth.toFixed(2)}}" height="${{Math.max(1, requestH).toFixed(2)}}" fill="#2563eb" rx="2"></rect>
        <rect x="${{(x + barWidth * 0.2).toFixed(2)}}" y="${{y(point.cacheHit).toFixed(2)}}" width="${{(barWidth * 0.28).toFixed(2)}}" height="${{Math.max(0, cacheH).toFixed(2)}}" fill="#15803d" rx="2"></rect>
        <rect x="${{(x + barWidth * 0.58).toFixed(2)}}" y="${{y(point.issues).toFixed(2)}}" width="${{(barWidth * 0.28).toFixed(2)}}" height="${{Math.max(0, issueH).toFixed(2)}}" fill="#b91c1c" rx="2"></rect>
      </g>
    `;
  }}).join("");
  const axis = [0, 0.5, 1].map(factor => {{
    const value = Math.round(max * factor);
    const yy = y(value);
    return `<g><line x1="${{pad}}" x2="${{width - pad}}" y1="${{yy}}" y2="${{yy}}" stroke="#d9dee8"></line><text x="4" y="${{yy + 4}}" font-size="11" fill="#667085">${{value}}</text></g>`;
  }}).join("");
  const first = escapeHtml(data[0].label);
  const last = escapeHtml(data[data.length - 1].label);
  target.innerHTML = `
    <svg class="spark" viewBox="0 0 ${{width}} ${{height}}" role="img" aria-label="Request volume over time">
      ${{axis}}
      ${{bars}}
      <text x="${{pad}}" y="${{height - 6}}" font-size="11" fill="#667085">${{first}}</text>
      <text x="${{width - pad}}" y="${{height - 6}}" font-size="11" text-anchor="end" fill="#667085">${{last}}</text>
    </svg>
  `;
}}

function renderProviders() {{
  const rows = Object.entries(REPORT.providerStats || {{}}).map(([key, stats]) => {{
    const statuses = Object.entries(stats.statuses || {{}})
      .map(([status, count]) => `${{statusPill(status, statusClass(status, false) === "bad")}} <span class="muted">${{fmtNumber(count)}}</span>`)
      .join(" ");
    return `
      <tr>
        <td>${{escapeHtml(stats.label)}}</td>
        <td>${{fmtNumber(stats.count)}}</td>
        <td>${{stats.issues ? statusPill(stats.issues + " issue" + (stats.issues === 1 ? "" : "s"), true) : statusPill("clean", false)}}</td>
        <td>${{statuses || '<span class="muted">not called</span>'}}</td>
        <td>${{fmtMs(stats.avg)}}</td>
        <td>${{fmtMs(stats.p95)}}</td>
        <td>${{fmtMs(stats.max)}}</td>
      </tr>
    `;
  }}).join("");
  document.getElementById("providerRows").innerHTML = rows || '<tr><td colspan="7" class="empty">No provider calls found.</td></tr>';
}}

function renderPrefetch() {{
  const totals = REPORT.prefetchTotals || {{}};
  const useful = {{
    signals: (totals.signalCount || 0),
    queued: (totals.queuedCount || 0),
    candidates: (totals.candidateCount || 0),
    cached: (totals.cachedCount || 0),
    duplicates: (totals.duplicateCount || 0),
    errors: (totals.errorCount || 0) + (totals.enqueueErrorCount || 0),
  }};
  renderBars("prefetchBars", useful, ["#2563eb", "#0f766e", "#7c3aed", "#15803d", "#b45309", "#b91c1c"]);
}}

function fillFilters() {{
  const tags = ["all", ...new Set(REPORT.rows.map(row => row.tag).filter(Boolean))];
  document.getElementById("tagFilter").innerHTML = tags.map(tag => `<option value="${{escapeHtml(tag)}}">${{tag === "all" ? "All cache tags" : escapeHtml(tag)}}</option>`).join("");
  document.getElementById("providerFilter").innerHTML =
    '<option value="all">All providers</option>' +
    PROVIDERS.map(([key, label]) => `<option value="${{key}}">${{escapeHtml(label)}}</option>`).join("");
}}

function filteredRows() {{
  const query = state.search.trim().toLowerCase();
  return REPORT.rows.filter(row => {{
    if (state.tag !== "all" && row.tag !== state.tag) return false;
    if (state.provider !== "all" && !row.providers[state.provider]) return false;
    if (state.issue === "issues" && !row.issues.length) return false;
    if (state.issue === "clean" && row.issues.length) return false;
    if (!query) return true;
    const haystack = [
      row.timestamp, row.doi, row.tag, row.title, row.metadataSource,
      row.cacheWrite, row.refresh, row.prefetchTask, row.issues.join(" "),
      ...Object.values(row.providers).map(provider => provider.status),
    ].join(" ").toLowerCase();
    return haystack.includes(query);
  }}).sort((a, b) => {{
    const av = a[state.sortKey];
    const bv = b[state.sortKey];
    if (state.sortKey === "processingTime") {{
      return ((Number(av) || -1) - (Number(bv) || -1)) * state.sortDir;
    }}
    return String(av ?? "").localeCompare(String(bv ?? "")) * state.sortDir;
  }});
}}

function providerBadges(row) {{
  return PROVIDERS.map(([key, label]) => {{
    const provider = row.providers[key];
    if (!provider || provider.status === null || provider.status === undefined) return "";
    return `<span title="${{escapeHtml(label)}} ${{provider.latency !== null && provider.latency !== undefined ? fmtMs(provider.latency) : ""}}">${{statusPill(label.replace(/ .*$/, "") + " " + provider.status, provider.issue)}}</span>`;
  }}).filter(Boolean).join(" ");
}}

function renderRows() {{
  const rows = filteredRows();
  const body = document.getElementById("requestRows");
  if (!rows.length) {{
    body.innerHTML = '<tr><td colspan="7" class="empty">No rows match the current filters.</td></tr>';
  }} else {{
    body.innerHTML = rows.slice(0, 500).map(row => {{
      const expanded = state.expanded === row.id;
      const notes = [
        row.cacheWrite ? `write: ${{row.cacheWrite}}` : "",
        row.refresh ? `refresh: ${{row.refresh}}` : "",
        row.prefetchTask ? `prefetch: ${{row.prefetchTask}}` : "",
        ...row.issues,
      ].filter(Boolean).join("; ");
      return `
        <tr data-id="${{row.id}}">
          <td class="small">${{escapeHtml(row.timestamp || "")}}</td>
          <td class="doi">${{escapeHtml(row.doi || "")}}</td>
          <td>${{statusPill(row.tag, row.tag === "cache-miss" || row.tag === "cache-expired")}}</td>
          <td>${{escapeHtml(row.metadataSource || "")}}</td>
          <td>${{fmtMs(row.processingTime)}}</td>
          <td>${{providerBadges(row) || '<span class="muted">cache only</span>'}}</td>
          <td class="title-cell">
            <button data-expand="${{row.id}}">${{expanded ? "Hide" : "Details"}}</button>
            <span class="${{row.issues.length ? "" : "muted"}}">${{escapeHtml(notes || row.title || "")}}</span>
          </td>
        </tr>
        ${{expanded ? `<tr><td colspan="7"><pre class="raw">${{escapeHtml(JSON.stringify(row.raw, null, 2))}}</pre></td></tr>` : ""}}
      `;
    }}).join("");
  }}
  document.getElementById("rowCount").textContent =
    `${{fmtNumber(rows.length)}} matching request rows` + (rows.length > 500 ? " (showing first 500)" : "");
  document.querySelectorAll("[data-expand]").forEach(button => {{
    button.addEventListener("click", event => {{
      event.stopPropagation();
      const id = Number(button.getAttribute("data-expand"));
      state.expanded = state.expanded === id ? null : id;
      renderRows();
    }});
  }});
}}

function listRows(id, rows, emptyText) {{
  const target = document.getElementById(id);
  if (!rows.length) {{
    target.innerHTML = `<div class="empty">${{escapeHtml(emptyText)}}</div>`;
    return;
  }}
  target.innerHTML = `<div class="bar-list">${{rows.map(row => `
    <div>
      <div><span class="doi">${{escapeHtml(row.doi || "")}}</span> ${{row.processingTime !== null && row.processingTime !== undefined ? `<span class="muted">${{fmtMs(row.processingTime)}}</span>` : ""}}</div>
      <div class="small muted">${{escapeHtml(row.timestamp || "")}}</div>
      <div>${{row.issues.length ? row.issues.map(issue => statusPill(issue, true)).join(" ") : escapeHtml(row.title || row.tag || "")}}</div>
    </div>
  `).join("")}}</div>`;
}}

function downloadCsv() {{
  const headers = ["timestamp", "doi", "tag", "metadataSource", "processingTime", "cacheWrite", "refresh", "prefetchTask", "issues"];
  const rows = filteredRows().map(row => headers.map(header => {{
    const value = header === "issues" ? row.issues.join("; ") : row[header];
    return `"${{String(value ?? "").replaceAll('"', '""')}}"`;
  }}).join(","));
  const blob = new Blob([headers.join(",") + "\\n" + rows.join("\\n")], {{ type: "text/csv;charset=utf-8" }});
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = "pure-publications-log-report.csv";
  anchor.click();
  URL.revokeObjectURL(url);
}}

function wireEvents() {{
  document.getElementById("search").addEventListener("input", event => {{
    state.search = event.target.value;
    renderRows();
  }});
  document.getElementById("tagFilter").addEventListener("change", event => {{
    state.tag = event.target.value;
    renderRows();
  }});
  document.getElementById("providerFilter").addEventListener("change", event => {{
    state.provider = event.target.value;
    renderRows();
  }});
  document.getElementById("issueFilter").addEventListener("change", event => {{
    state.issue = event.target.value;
    renderRows();
  }});
  document.getElementById("downloadCsv").addEventListener("click", downloadCsv);
  document.querySelectorAll("th[data-sort]").forEach(header => {{
    header.addEventListener("click", () => {{
      const key = header.getAttribute("data-sort");
      if (state.sortKey === key) state.sortDir *= -1;
      else {{
        state.sortKey = key;
        state.sortDir = key === "processingTime" ? -1 : 1;
      }}
      renderRows();
    }});
  }});
}}

function init() {{
  renderMeta();
  renderKpis();
  renderTimeChart();
  renderBars("cacheBars", REPORT.cacheCounts);
  renderBars("metadataBars", REPORT.metadataCounts, ["#7c3aed", "#0f766e", "#2563eb", "#b45309", "#15803d"]);
  renderPrefetch();
  renderProviders();
  fillFilters();
  renderRows();
  listRows("problemList", REPORT.problemRows || [], "No provider or queue issues found.");
  listRows("slowList", REPORT.slowRows || [], "No timed requests found.");
  wireEvents();
}}

init();
</script>
</body>
</html>
"""


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build an interactive HTML report from pure-publications gcloud logs."
    )
    parser.add_argument("--project", default=os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT"))
    parser.add_argument("--function", default=DEFAULT_FUNCTION)
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--since", default="24h", help="Relative window such as 24h/7d or an ISO timestamp.")
    parser.add_argument("--until", help="Optional ISO timestamp or 'now'.")
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--input", help="Read saved gcloud JSON or NDJSON instead of calling gcloud.")
    parser.add_argument("--save-raw", help="Save raw gcloud JSON to this path.")
    parser.add_argument("--filter-extra", help="Additional Cloud Logging filter expression.")
    parser.add_argument("--gcloud", default="gcloud")
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--open", action="store_true", help="Open the generated report in the default browser.")
    parser.add_argument("--demo", action="store_true", help="Render a built-in demo dataset instead of calling gcloud.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    now = utc_now()
    since = parse_since(args.since, now)
    until = parse_datetime(args.until) if args.until else None
    log_filter = build_log_filter(args, since, until)

    if args.demo:
        entries = make_demo_entries()
    elif args.input:
        entries = load_json_or_ndjson(pathlib.Path(args.input))
    else:
        entries = run_gcloud(args, log_filter)

    report = summarize(entries, args, log_filter)
    output_path = pathlib.Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_html(report), encoding="utf-8")
    print(f"Wrote {output_path.resolve()}")
    print(
        f"Included {report['summary']['requests']} DOI request logs, "
        f"{report['summary']['bulkRuns']} bulk summaries, "
        f"{report['summary']['providerIssueEvents']} issue rows."
    )
    if report["meta"].get("limitReached"):
        limit = report["meta"]["limit"]
        first = report["meta"].get("firstTimestamp")
        last = report["meta"].get("lastTimestamp")
        print(f"Limit reached at {limit} entries; observed {first} to {last}.")
    if args.open:
        webbrowser.open(output_path.resolve().as_uri())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
