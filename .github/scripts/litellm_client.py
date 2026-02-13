#!/usr/bin/env python3
"""
Shared LiteLLM proxy client for PR review and test analysis scripts.
"""

import time
import requests
from typing import Dict, List


class LiteLLMClient:
    """Client for LiteLLM proxy API"""

    def __init__(self, base_url: str, api_key: str, model: str = "claude"):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.model = model
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

    def chat(self, messages: List[Dict[str, str]], max_tokens: int = 4000,
             temperature: float = 0.1, retries: int = 2) -> str:
        """Send chat completion request to LiteLLM proxy with retry on transient errors."""
        payload = {
            "model": self.model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature
        }

        last_error = None
        for attempt in range(1 + retries):
            try:
                response = requests.post(
                    f"{self.base_url}/chat/completions",
                    headers=self.headers,
                    json=payload,
                    timeout=120
                )

                if response.status_code == 200:
                    result = response.json()
                    return result["choices"][0]["message"]["content"]

                # Retry on 5xx (transient server errors)
                if response.status_code >= 500 and attempt < retries:
                    wait = 2 ** attempt
                    print(f"LiteLLM returned {response.status_code}, retrying in {wait}s...")
                    time.sleep(wait)
                    last_error = f"LiteLLM API error: {response.status_code} - {response.text}"
                    continue

                raise Exception(f"LiteLLM API error: {response.status_code} - {response.text}")

            except requests.exceptions.Timeout:
                if attempt < retries:
                    wait = 2 ** attempt
                    print(f"Request timed out, retrying in {wait}s...")
                    time.sleep(wait)
                    last_error = "Request timed out"
                    continue
                raise Exception("LiteLLM request timed out after retries")

        raise Exception(last_error or "LiteLLM request failed")
