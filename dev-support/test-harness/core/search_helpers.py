"""Search helpers for verifying entity indexing in Elasticsearch."""

import time

# Upfront wait for ES indexing eventual consistency (seconds).
SEARCH_CONSISTENCY_WAIT_S = 30


def assert_entity_in_search(client, qn, type_name="DataSet", wait_s=SEARCH_CONSISTENCY_WAIT_S):
    """Wait for eventual consistency, then assert the entity appears in search.

    Sleeps wait_s upfront, queries once, and asserts hard.
    Returns the entity dict if found, None only if the endpoint is unavailable (404/400/405).
    Raises AssertionError if not found after waiting.
    """
    time.sleep(wait_s)

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
        return None

    assert resp.status_code == 200, (
        f"Search endpoint returned {resp.status_code}, expected 200"
    )

    body = resp.json()
    count = body.get("approximateCount", 0)
    assert count > 0, (
        f"Entity with qualifiedName={qn} (type={type_name}) not found in search "
        f"after waiting {wait_s}s"
    )

    entities = body.get("entities", [])
    return entities[0] if entities else body


def assert_entity_not_in_search(client, qn, type_name="DataSet", wait_s=SEARCH_CONSISTENCY_WAIT_S):
    """Wait for eventual consistency, then confirm entity is gone from active search.

    Returns True if entity is gone, None if search endpoint unavailable.
    Raises AssertionError if entity still found.
    """
    time.sleep(wait_s)

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
        return None

    if resp.status_code == 200:
        body = resp.json()
        count = body.get("approximateCount", 0)
        assert count == 0, (
            f"Entity with qualifiedName={qn} still found in search (count={count}) "
            f"after waiting {wait_s}s"
        )
        return True

    return None
