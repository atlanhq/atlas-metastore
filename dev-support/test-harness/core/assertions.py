"""Assertion helpers for API response validation."""


class AssertionError(Exception):
    """Test assertion failure with context."""

    def __init__(self, message, expected=None, actual=None, response=None):
        self.expected = expected
        self.actual = actual
        self.response = response
        detail = message
        if expected is not None:
            detail += f" (expected={expected!r}, actual={actual!r})"
        if response is not None:
            detail += f" [HTTP {response.status_code} {response.request_method} {response.request_path}]"
            # Include truncated response body for debugging
            body = response.body if hasattr(response, "body") else None
            if body and response.status_code >= 400:
                body_str = str(body)[:300]
                detail += f"\n    Response: {body_str}"
        super().__init__(detail)


def assert_status(resp, expected):
    """Assert HTTP status code matches exactly."""
    if resp.status_code != expected:
        raise AssertionError(
            f"Expected status {expected}, got {resp.status_code}",
            expected=expected, actual=resp.status_code, response=resp,
        )


def assert_status_in(resp, expected_list):
    """Assert HTTP status code is one of the expected values."""
    if resp.status_code not in expected_list:
        raise AssertionError(
            f"Expected status in {expected_list}, got {resp.status_code}",
            expected=expected_list, actual=resp.status_code, response=resp,
        )


def _resolve_path(obj, dot_path):
    """Resolve a dot-separated path like 'entity.guid' against a dict/object."""
    parts = dot_path.split(".")
    current = obj
    for part in parts:
        if isinstance(current, dict):
            if part not in current:
                return None, False
            current = current[part]
        elif isinstance(current, list):
            try:
                idx = int(part)
                current = current[idx]
            except (ValueError, IndexError):
                return None, False
        else:
            return None, False
    return current, True


def assert_field_present(resp, *dot_path_parts):
    """Assert that a nested field exists in the response body.

    Usage: assert_field_present(resp, "entity", "guid")
           or assert_field_present(resp, "entity.guid")
    """
    dot_path = ".".join(dot_path_parts)
    body = resp.body if hasattr(resp, "body") else resp
    val, found = _resolve_path(body, dot_path)
    if not found:
        raise AssertionError(
            f"Field '{dot_path}' not found in response",
            expected=f"field '{dot_path}' present", actual="missing", response=resp if hasattr(resp, "status_code") else None,
        )


def assert_field_equals(resp, dot_path, expected_value):
    """Assert that a nested field equals the expected value."""
    body = resp.body if hasattr(resp, "body") else resp
    val, found = _resolve_path(body, dot_path)
    if not found:
        raise AssertionError(
            f"Field '{dot_path}' not found",
            expected=expected_value, actual="<missing>", response=resp if hasattr(resp, "status_code") else None,
        )
    if val != expected_value:
        raise AssertionError(
            f"Field '{dot_path}' mismatch",
            expected=expected_value, actual=val, response=resp if hasattr(resp, "status_code") else None,
        )


def assert_field_not_empty(resp, dot_path):
    """Assert that a nested field exists and is non-empty (truthy)."""
    body = resp.body if hasattr(resp, "body") else resp
    val, found = _resolve_path(body, dot_path)
    if not found:
        raise AssertionError(
            f"Field '{dot_path}' not found",
            expected="non-empty value", actual="<missing>", response=resp if hasattr(resp, "status_code") else None,
        )
    if not val:
        raise AssertionError(
            f"Field '{dot_path}' is empty",
            expected="non-empty value", actual=val, response=resp if hasattr(resp, "status_code") else None,
        )


def assert_field_contains(resp, dot_path, substring):
    """Assert that a field's string value contains a substring."""
    body = resp.body if hasattr(resp, "body") else resp
    val, found = _resolve_path(body, dot_path)
    if not found:
        raise AssertionError(
            f"Field '{dot_path}' not found",
            expected=f"contains '{substring}'", actual="<missing>",
            response=resp if hasattr(resp, "status_code") else None,
        )
    if substring not in str(val):
        raise AssertionError(
            f"Field '{dot_path}' does not contain '{substring}'",
            expected=f"contains '{substring}'", actual=val,
            response=resp if hasattr(resp, "status_code") else None,
        )


def assert_list_min_length(resp, dot_path, min_len):
    """Assert that a list field has at least min_len items."""
    body = resp.body if hasattr(resp, "body") else resp
    val, found = _resolve_path(body, dot_path)
    if not found:
        raise AssertionError(
            f"Field '{dot_path}' not found",
            expected=f"list with >= {min_len} items", actual="<missing>",
            response=resp if hasattr(resp, "status_code") else None,
        )
    if not isinstance(val, list):
        raise AssertionError(
            f"Field '{dot_path}' is not a list",
            expected=f"list with >= {min_len} items", actual=type(val).__name__,
            response=resp if hasattr(resp, "status_code") else None,
        )
    if len(val) < min_len:
        raise AssertionError(
            f"Field '{dot_path}' has {len(val)} items, expected >= {min_len}",
            expected=f">= {min_len}", actual=len(val),
            response=resp if hasattr(resp, "status_code") else None,
        )
