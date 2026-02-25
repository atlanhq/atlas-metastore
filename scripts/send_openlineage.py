#!/usr/bin/env python3
import json
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

TOPIC = os.getenv("OL_TOPIC", "openlineage-topic")
ERROR_TOPIC = os.getenv("OL_ERROR_TOPIC", "openlineage-topic-errors")
BROKER = os.getenv("OL_BROKER", "localhost:9092")
KAFKA_CONTAINER = os.getenv("OL_KAFKA_CONTAINER", "redpanda")
ATLAS_URL = os.getenv("OL_ATLAS_URL", "http://localhost:21000/api/atlas/v2/openlineage")
REST_TIMEOUT_SECONDS = int(os.getenv("OL_REST_TIMEOUT_SECONDS", "30"))
REST_POLL_INTERVAL_SECONDS = float(os.getenv("OL_REST_POLL_INTERVAL_SECONDS", "1"))
ATLAS_AUTH = os.getenv("OL_ATLAS_AUTH", "admin:admin")


def now_utc_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def produce_event(event, topic=TOPIC):
    payload = json.dumps(event, separators=(",", ":")) + "\n"
    cmd = [
        "docker",
        "exec",
        "-i",
        KAFKA_CONTAINER,
        "rpk",
        "topic",
        "produce",
        topic,
        "--brokers",
        BROKER,
        "--format",
        "%v\\n",
    ]
    subprocess.run(cmd, input=payload.encode("utf-8"), check=True)


def produce_raw(raw, topic=TOPIC):
    payload = raw + "\n"
    cmd = [
        "docker",
        "exec",
        "-i",
        KAFKA_CONTAINER,
        "rpk",
        "topic",
        "produce",
        topic,
        "--brokers",
        BROKER,
        "--format",
        "%v\\n",
    ]
    subprocess.run(cmd, input=payload.encode("utf-8"), check=True)


def query_rest_once(url):
    cmd = [
        "curl",
        "-s",
        "-u",
        ATLAS_AUTH,
        "-H",
        "Accept: application/json",
        url,
    ]
    output = subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT)
    return output


def query_rest(url, timeout_seconds=REST_TIMEOUT_SECONDS, poll_interval=REST_POLL_INTERVAL_SECONDS):
    start = time.time()
    last_error = None
    first = True
    while time.time() - start < timeout_seconds:
        if not first:
            time.sleep(poll_interval)
        first = False
        try:
            return query_rest_once(url)
        except subprocess.CalledProcessError as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    raise RuntimeError("REST query failed without an error")


def consume_last(topic):
    cmd = [
        "docker",
        "exec",
        KAFKA_CONTAINER,
        "rpk",
        "topic",
        "consume",
        topic,
        "--brokers",
        BROKER,
        "-o",
        "-1",
        "-n",
        "1",
        "--format",
        "%v\\n",
    ]
    return subprocess.check_output(cmd, text=True)


def print_header(title):
    print("\n" + title)
    print("-" * len(title))


def summarize_rest(raw, max_events=5):
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return f"REST response (non-JSON): {raw.strip()}"

    run_id = data.get("runId")
    count = data.get("eventCount")
    events = data.get("events", [])

    lines = [f"runId={run_id}", f"eventCount={count}"]
    if events:
        lines.append("events:")
        for event in events[:max_events]:
            lines.append(
                f"- id={event.get('eventId')} time={event.get('eventTime')} status={event.get('status')}"
            )
        if len(events) > max_events:
            lines.append(f"- ... ({len(events) - max_events} more)")
        if data.get("nextPagingState"):
            lines.append("nextPagingState=(present)")
    return "\n".join(lines)


def summarize_event(raw):
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return f"Event response (non-JSON): {raw.strip()}"

    return (
        f"eventId={data.get('eventId')}\n"
        f"eventTime={data.get('eventTime')}\n"
        f"status={data.get('status')}\n"
        f"runId={data.get('runId')}"
    )


def summarize_error(raw):
    text = raw.strip()
    if not text:
        return "(empty)"
    try:
        data = json.loads(text)
        keys = ["error", "message", "event", "payload", "details"]
        summary = {k: data.get(k) for k in keys if k in data}
        if summary:
            return json.dumps(summary, indent=2, sort_keys=True)
    except json.JSONDecodeError:
        pass
    return text[:240] + ("..." if len(text) > 240 else "")


try:
    print_header("Single event -> Kafka -> REST")
    run_id = str(uuid.uuid4())
    event_time = now_utc_iso()

    single_event = {
        "eventType": "START",
        "eventTime": event_time,
        "run": {"runId": run_id},
        "job": {"namespace": "local", "name": "sample-job"},
        "producer": "local://openlineage-test",
    }

    print(f"produce: topic={TOPIC} runId={run_id} eventTime={event_time}")
    produce_event(single_event)

    print("REST summary:")
    print(summarize_rest(query_rest(f"{ATLAS_URL}/runs/{run_id}")))

    print_header("Two events same runId -> REST list + get-by-id")
    run_id_dupe = str(uuid.uuid4())
    base_time = datetime.now(timezone.utc).replace(microsecond=0)

    for i, event_type in enumerate(["START", "COMPLETE"], start=0):
        event = {
            "eventType": event_type,
            "eventTime": (base_time + timedelta(seconds=i)).isoformat().replace("+00:00", "Z"),
            "run": {"runId": run_id_dupe},
            "job": {"namespace": "local", "name": "sample-job"},
            "producer": "local://openlineage-test",
        }
        produce_event(event)

    print(f"produce: topic={TOPIC} runId={run_id_dupe} events=2")

    list_response = query_rest(f"{ATLAS_URL}/runs/{run_id_dupe}")
    print("REST list summary:")
    print(summarize_rest(list_response))

    list_data = json.loads(list_response)
    if list_data.get("events"):
        print("\nREST get-by-id (first event):")
        event_id = list_data["events"][0].get("eventId")
        if event_id:
            event_response = query_rest(f"{ATLAS_URL}/runs/{run_id_dupe}/events/{event_id}")
            print(summarize_event(event_response))
        else:
            print("(missing eventId in list response)")
    else:
        print("(no events returned to fetch by id)")

    print_header("Bad event -> error queue")
    bad_event = "}"
    produce_raw(bad_event)

    print(f"produce: topic={TOPIC} payload='}}'")
    print("error-topic last message:")
    print(summarize_error(consume_last(ERROR_TOPIC)))
except subprocess.CalledProcessError as exc:
    print("Command failed:")
    print(" ".join(exc.cmd))
    if exc.output:
        print(exc.output.strip())
    sys.exit(exc.returncode)
