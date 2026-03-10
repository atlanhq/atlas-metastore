"""Test discovery, ordering, and execution."""

import importlib
import os
import pkgutil
import sys
import time
import traceback

from core.assertions import AssertionError
from core.decorators import get_suite_registry, register_suite_tests
from core.reporter import TestResult


def discover_suites(suites_package="suites"):
    """Import all suites/test_*.py modules to trigger @suite decorators."""
    pkg_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), suites_package)
    for importer, modname, ispkg in pkgutil.iter_modules([pkg_dir]):
        if modname.startswith("test_"):
            importlib.import_module(f"{suites_package}.{modname}")

    # Register tests within each suite
    registry = get_suite_registry()
    for suite_name, meta in registry.items():
        register_suite_tests(suite_name, meta["cls"])


def resolve_order(suite_filter=None):
    """Topological sort of suites by depends_on_suites. Returns ordered list of suite names."""
    registry = get_suite_registry()

    # Filter suites if specified
    if suite_filter:
        # Normalize names: allow "admin" to match "test_admin" or "admin"
        normalized = set()
        for s in suite_filter:
            normalized.add(s)
            normalized.add(f"test_{s}")
            # Strip test_ prefix for matching
            if s.startswith("test_"):
                normalized.add(s[5:])

        available = {name for name in registry if name in normalized or
                     f"test_{name}" in normalized or
                     name.replace("test_", "") in normalized}

        # Include dependencies
        def add_deps(name):
            if name in available:
                return
            available.add(name)
            for dep in registry.get(name, {}).get("depends_on", []):
                add_deps(dep)

        for name in list(available):
            for dep in registry.get(name, {}).get("depends_on", []):
                add_deps(dep)
    else:
        available = set(registry.keys())

    # Topological sort (Kahn's algorithm)
    in_degree = {name: 0 for name in available}
    for name in available:
        for dep in registry.get(name, {}).get("depends_on", []):
            if dep in available:
                in_degree[name] = in_degree.get(name, 0) + 1

    queue = sorted([n for n in available if in_degree.get(n, 0) == 0])
    ordered = []
    while queue:
        node = queue.pop(0)
        ordered.append(node)
        for name in available:
            deps = registry.get(name, {}).get("depends_on", [])
            if node in deps:
                in_degree[name] -= 1
                if in_degree[name] == 0:
                    queue.append(name)
        queue.sort()

    # Check for cycles
    if len(ordered) < len(available):
        missing = available - set(ordered)
        print(f"WARNING: Circular dependencies detected for suites: {missing}")
        ordered.extend(sorted(missing))

    return ordered


def _should_run_test(test_meta, tags, exclude_tags):
    """Check if a test should run based on tag filters."""
    test_tags = set(test_meta.get("tags", []))

    if exclude_tags:
        if test_tags & set(exclude_tags):
            return False

    if tags:
        if not (test_tags & set(tags)):
            return False

    return True


def run(client, ctx, reporter, config):
    """Main test execution loop."""
    discover_suites()
    ordered_suites = resolve_order(config.suites if config.suites else None)

    registry = get_suite_registry()

    if config.verbose:
        print(f"Suite execution order: {ordered_suites}")

    for suite_name in ordered_suites:
        meta = registry.get(suite_name)
        if not meta:
            continue

        suite_cls = meta["cls"]
        tests = meta.get("tests", [])

        if not tests:
            continue

        # Filter tests by tags
        runnable_tests = [
            t for t in tests
            if _should_run_test(t, config.tags, config.exclude_tags)
        ]

        if not runnable_tests:
            continue

        reporter.suite_start(suite_name, meta.get("description", ""))

        # Instantiate suite if it's a class with setup/teardown
        suite_instance = None
        if isinstance(suite_cls, type):
            suite_instance = suite_cls()

        # Run setup if available
        if suite_instance and hasattr(suite_instance, "setup"):
            try:
                suite_instance.setup(client, ctx)
            except Exception as e:
                reporter.record(TestResult(
                    suite=suite_name,
                    test_name="<setup>",
                    status="ERROR",
                    error_message=str(e),
                    error_type=type(e).__name__,
                ))
                reporter.suite_end(suite_name)
                continue

        # Run tests
        blocked_tests = set()  # Tests that failed, errored, or were skipped
        for test_meta in runnable_tests:
            test_name = test_meta["name"]
            test_fn = test_meta["fn"]

            # Check depends_on
            deps = test_meta.get("depends_on", [])
            if any(d in blocked_tests for d in deps):
                blocked_tests.add(test_name)
                reporter.record(TestResult(
                    suite=suite_name,
                    test_name=test_name,
                    status="SKIP",
                    error_message=f"Dependency failed: {deps}",
                ))
                continue

            request_logger = ctx.get("request_logger")
            if request_logger:
                request_logger.set_context(suite_name, test_name)

            start = time.perf_counter()
            try:
                if suite_instance:
                    test_fn(suite_instance, client, ctx)
                else:
                    test_fn(client, ctx)
                latency = (time.perf_counter() - start) * 1000
                reporter.record(TestResult(
                    suite=suite_name,
                    test_name=test_name,
                    status="PASS",
                    latency_ms=latency,
                ))
            except AssertionError as e:
                latency = (time.perf_counter() - start) * 1000
                blocked_tests.add(test_name)
                reporter.record(TestResult(
                    suite=suite_name,
                    test_name=test_name,
                    status="FAIL",
                    latency_ms=latency,
                    error_message=str(e),
                    error_type="AssertionError",
                ))
            except Exception as e:
                latency = (time.perf_counter() - start) * 1000
                blocked_tests.add(test_name)
                tb = traceback.format_exc()
                reporter.record(TestResult(
                    suite=suite_name,
                    test_name=test_name,
                    status="ERROR",
                    latency_ms=latency,
                    error_message=f"{type(e).__name__}: {e}\n{tb}",
                    error_type=type(e).__name__,
                ))

        # Run teardown if available
        if suite_instance and hasattr(suite_instance, "teardown"):
            try:
                suite_instance.teardown(client, ctx)
            except Exception as e:
                if config.verbose:
                    print(f"  Teardown error in {suite_name}: {e}")

        reporter.suite_end(suite_name)
