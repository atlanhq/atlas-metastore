"""Search helpers for verifying entity indexing in Elasticsearch."""

import time


def assert_entity_in_search(client, qn, type_name="DataSet", max_retries=5, retry_delay_s=2):
    """Poll index search until the entity with the given qualifiedName appears.

    Returns the entity dict if found, None if search endpoint unavailable.
    Raises AssertionError if not found after retries.
    """
    for attempt in range(max_retries):
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
            if count > 0:
                entities = body.get("entities", [])
                return entities[0] if entities else body

        if attempt < max_retries - 1:
            time.sleep(retry_delay_s)

    raise AssertionError(
        f"Entity with qualifiedName={qn} (type={type_name}) not found in search "
        f"after {max_retries} retries (~{max_retries * retry_delay_s}s)"
    )


def assert_entity_not_in_search(client, qn, type_name="DataSet", wait_s=5):
    """Wait then confirm the entity no longer appears in active search results.

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
        if count == 0:
            return True
        raise AssertionError(
            f"Entity with qualifiedName={qn} still found in search (count={count}) "
            f"after waiting {wait_s}s"
        )

    return None
