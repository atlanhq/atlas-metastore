"""HTTP client with latency tracking for Atlas REST API."""

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import requests


@dataclass
class ApiResponse:
    status_code: int
    body: Any
    headers: Dict[str, str]
    latency_ms: float
    request_method: str
    request_path: str

    @property
    def ok(self):
        return 200 <= self.status_code < 300

    def json(self):
        return self.body


class AtlasClient:
    """HTTP client wrapping requests with auth, latency tracking, and 401 retry."""

    def __init__(self, api_base, admin_base, auth_provider, timeout=30, request_logger=None):
        self.api_base = api_base.rstrip("/")
        self.admin_base = admin_base.rstrip("/")
        self.auth = auth_provider
        self.timeout = timeout
        self.latency_log: List[Dict] = []
        self.request_logger = request_logger

    def get(self, path, params=None, admin=False) -> ApiResponse:
        return self._do("GET", path, params=params, admin=admin)

    def post(self, path, json_data=None, params=None, admin=False) -> ApiResponse:
        return self._do("POST", path, json_data=json_data, params=params, admin=admin)

    def put(self, path, json_data=None, params=None, admin=False) -> ApiResponse:
        return self._do("PUT", path, json_data=json_data, params=params, admin=admin)

    def delete(self, path, json_data=None, params=None, admin=False) -> ApiResponse:
        return self._do("DELETE", path, json_data=json_data, params=params, admin=admin)

    def _do(self, method, path, json_data=None, params=None, admin=False) -> ApiResponse:
        base = self.admin_base if admin else self.api_base
        url = f"{base}{path}"

        headers = self.auth.get_headers()
        auth_obj = self.auth.get_requests_auth()

        start = time.perf_counter()
        try:
            resp = requests.request(
                method, url,
                headers=headers,
                auth=auth_obj,
                json=json_data,
                params=params,
                timeout=self.timeout,
            )
        except requests.exceptions.Timeout:
            latency_ms = (time.perf_counter() - start) * 1000
            api_resp = ApiResponse(
                status_code=408,
                body={"error": "Request timed out"},
                headers={},
                latency_ms=latency_ms,
                request_method=method,
                request_path=path,
            )
            self.latency_log.append({
                "method": method, "path": path,
                "status": 408, "latency_ms": latency_ms,
            })
            if self.request_logger:
                self.request_logger.log(method, path, params, json_data, 408, api_resp.body, latency_ms)
            return api_resp

        # 401 retry
        if resp.status_code == 401 and self.auth.handle_401():
            headers = self.auth.get_headers()
            resp = requests.request(
                method, url,
                headers=headers,
                auth=auth_obj,
                json=json_data,
                params=params,
                timeout=self.timeout,
            )

        latency_ms = (time.perf_counter() - start) * 1000

        # Parse body
        try:
            body = resp.json()
        except (ValueError, requests.exceptions.JSONDecodeError):
            body = resp.text

        api_resp = ApiResponse(
            status_code=resp.status_code,
            body=body,
            headers=dict(resp.headers),
            latency_ms=latency_ms,
            request_method=method,
            request_path=path,
        )

        self.latency_log.append({
            "method": method,
            "path": path,
            "status": resp.status_code,
            "latency_ms": latency_ms,
        })

        if self.request_logger:
            self.request_logger.log(method, path, params, json_data, resp.status_code, body, latency_ms)

        return api_resp
