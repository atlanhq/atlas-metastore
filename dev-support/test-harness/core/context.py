"""Shared test state and cleanup registry."""

import threading
from collections import OrderedDict
from typing import Any, Callable, Dict, List, Optional


class TestContext:
    """Thread-safe key-value store for sharing data between tests.

    Also maintains a LIFO cleanup stack so children are deleted before parents.
    """

    def __init__(self):
        self._data: Dict[str, Any] = {}
        self._entities: OrderedDict = OrderedDict()   # name -> {guid, type}
        self._cleanup_stack: List[Callable] = []
        self._lock = threading.Lock()

    # ---- generic k/v ----

    def set(self, key, value):
        with self._lock:
            self._data[key] = value

    def get(self, key, default=None):
        with self._lock:
            return self._data.get(key, default)

    # ---- entity registry ----

    def register_entity(self, name, guid, type_name=None, qualifiedName=None):
        with self._lock:
            self._entities[name] = {
                "guid": guid, "typeName": type_name,
                "qualifiedName": qualifiedName,
            }

    def get_entity_guid(self, name) -> Optional[str]:
        with self._lock:
            entry = self._entities.get(name)
            return entry["guid"] if entry else None

    def get_entity_qn(self, name) -> Optional[str]:
        with self._lock:
            entry = self._entities.get(name)
            return entry.get("qualifiedName") if entry else None

    def get_entity(self, name) -> Optional[Dict]:
        with self._lock:
            return self._entities.get(name)

    def list_entities(self) -> Dict[str, Dict]:
        with self._lock:
            return dict(self._entities)

    # ---- cleanup ----

    def register_cleanup(self, fn: Callable):
        """Register a no-arg callable to run during cleanup (LIFO order)."""
        with self._lock:
            self._cleanup_stack.append(fn)

    def run_cleanup(self):
        """Execute all cleanup functions in reverse registration order."""
        errors = []
        with self._lock:
            stack = list(reversed(self._cleanup_stack))
            self._cleanup_stack.clear()
        for fn in stack:
            try:
                fn()
            except Exception as e:
                errors.append(e)
        return errors
