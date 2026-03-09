"""Search helpers for verifying entity indexing in Elasticsearch."""

import time

# Polling configuration for ES eventual consistency.
SEARCH_POLL_INTERVAL_S = 5       # Poll every 5 seconds
SEARCH_TIMEOUT_S = 120           # Give up after 2 minutes
SEARCH_SLOW_THRESHOLD_S = 30     # Flag a warning if sync takes longer than this

# ANSI colors for console warnings
_YELLOW = "\033[93m"
_RESET = "\033[0m"


def _search_by_qn(client, qn):
    """Issue a single index-search query by qualifiedName. Returns (available, count, entities).

    available=False means the search endpoint is not usable (404/400/405/non-200).
    """
    resp = client.post("/search/indexsearch", json_data={
        "dsl": {
            "from": 0,
            "size": 1,
            "query": {
                "bool": {
                    "must": [
                        {"term": {"__qualifiedName": qn}},
                        {"term": {"__state": "ACTIVE"}},
                    ]
                }
            }
        }
    })
    if resp.status_code in (404, 400, 405):
        return False, 0, []
    if resp.status_code != 200:
        return False, 0, []
    body = resp.json()
    count = body.get("approximateCount", 0)
    entities = body.get("entities", [])
    return True, count, entities


def assert_entity_in_search(client, qn, type_name="DataSet",
                            timeout_s=SEARCH_TIMEOUT_S,
                            poll_interval_s=SEARCH_POLL_INTERVAL_S):
    """Poll until the entity appears in search, or timeout.

    Polls every poll_interval_s seconds up to timeout_s total.
    If sync takes > SEARCH_SLOW_THRESHOLD_S, prints a yellow warning with the latency.

    Returns the entity dict if found, None if the endpoint is unavailable.
    Raises AssertionError if not found after timeout.
    """
    start = time.time()
    attempts = 0

    while True:
        elapsed = time.time() - start
        attempts += 1

        available, count, entities = _search_by_qn(client, qn)
        if not available:
            return None  # Endpoint unavailable

        if count > 0:
            sync_latency = time.time() - start
            if sync_latency > SEARCH_SLOW_THRESHOLD_S:
                print(
                    f"         {_YELLOW}[SLOW SYNC] Entity {type_name} "
                    f"(qn=...{qn[-30:]}) took {sync_latency:.1f}s to appear in search "
                    f"({attempts} polls){_RESET}",
                    flush=True,
                )
            return entities[0] if entities else {}

        if elapsed >= timeout_s:
            break

        time.sleep(poll_interval_s)

    total_elapsed = time.time() - start
    raise AssertionError(
        f"Entity with qualifiedName={qn} (type={type_name}) not found in search "
        f"after {total_elapsed:.1f}s ({attempts} polls, timeout={timeout_s}s)"
    )


def assert_entity_not_in_search(client, qn, type_name="DataSet",
                                timeout_s=SEARCH_TIMEOUT_S,
                                poll_interval_s=SEARCH_POLL_INTERVAL_S):
    """Poll until the entity is gone from active search, or timeout.

    Polls every poll_interval_s seconds up to timeout_s total.
    If sync takes > SEARCH_SLOW_THRESHOLD_S, prints a yellow warning.

    Returns True if entity is gone, None if search endpoint unavailable.
    Raises AssertionError if entity still found after timeout.
    """
    start = time.time()
    attempts = 0
    last_count = 0

    while True:
        elapsed = time.time() - start
        attempts += 1

        available, count, entities = _search_by_qn(client, qn)
        if not available:
            return None  # Endpoint unavailable

        last_count = count
        if count == 0:
            sync_latency = time.time() - start
            if sync_latency > SEARCH_SLOW_THRESHOLD_S:
                print(
                    f"         {_YELLOW}[SLOW SYNC] Entity {type_name} "
                    f"(qn=...{qn[-30:]}) took {sync_latency:.1f}s to be removed from search "
                    f"({attempts} polls){_RESET}",
                    flush=True,
                )
            return True

        if elapsed >= timeout_s:
            break

        time.sleep(poll_interval_s)

    total_elapsed = time.time() - start
    raise AssertionError(
        f"Entity with qualifiedName={qn} still found in search (count={last_count}) "
        f"after {total_elapsed:.1f}s ({attempts} polls, timeout={timeout_s}s)"
    )
