"""Helpers for loading plugin modules offline.

The plugins import `requests`; when it is not installed (e.g. a bare CI or
dev box) a minimal stub is registered so the pure logic can still be tested.
No test ever performs network I/O.
"""
import importlib.util
import os
import sys
import types

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _ensure_requests_stub():
    try:
        import requests  # noqa: F401
        return
    except ImportError:
        pass
    stub = types.ModuleType("requests")

    class RequestException(Exception):
        pass

    class HTTPError(RequestException):
        pass

    class Timeout(RequestException):
        pass

    class ConnectionError_(RequestException):
        pass

    exceptions = types.SimpleNamespace(
        RequestException=RequestException, HTTPError=HTTPError,
        Timeout=Timeout, ConnectionError=ConnectionError_,
    )

    def _no_network(*_a, **_k):
        raise AssertionError("network access attempted in an offline test")

    class Session:
        def __init__(self):
            self.headers = {}
            self.cookies = types.SimpleNamespace(set=lambda *a, **k: None)

        post = get = _no_network

    stub.exceptions = exceptions
    stub.HTTPError = HTTPError
    stub.RequestException = RequestException
    stub.Session = Session
    stub.post = stub.get = _no_network
    sys.modules["requests"] = stub


def load_plugin(folder, module_name=None):
    _ensure_requests_stub()
    module_name = module_name or folder
    path = os.path.join(REPO_ROOT, folder, f"{module_name}.py")
    spec = importlib.util.spec_from_file_location(f"plugin_{module_name}", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class RecordingStash:
    """Mock for a StashInterface-like object: answers call_GQL from a list of
    (substring, response) rules and records every call. Any query containing
    'mutation' is recorded in .mutations so tests can assert no writes."""

    def __init__(self, rules=None):
        self.rules = list(rules or [])
        self.calls = []
        self.mutations = []

    def call_GQL(self, query, variables=None):
        self.calls.append((query, variables))
        if query.lstrip().startswith("mutation"):
            self.mutations.append((query, variables))
        for needle, response in self.rules:
            if needle in query:
                return response(variables) if callable(response) else response
        return {}
