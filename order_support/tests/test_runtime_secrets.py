# ABOUTME: Tests for loading the worker's API keys from Secrets Manager into the environment.
# Uses a stub Secrets Manager client; no AWS access needed.

import json
import os

import pytest

from runtime_secrets import MissingSecretKeyError, load_secrets_into_env


class _StubSecretsManager:
    def __init__(self, secret: dict[str, str]) -> None:
        self._secret = secret
        self.requested_ids: list[str] = []

    def get_secret_value(self, *, SecretId: str) -> dict[str, str]:
        self.requested_ids.append(SecretId)
        return {"SecretString": json.dumps(self._secret)}


def test_sets_each_required_key_from_the_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TEMPORAL_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    stub = _StubSecretsManager({"TEMPORAL_API_KEY": "tk", "OPENAI_API_KEY": "ok"})

    load_secrets_into_env(
        "order-support/keys", ["TEMPORAL_API_KEY", "OPENAI_API_KEY"], client=stub
    )

    assert stub.requested_ids == ["order-support/keys"]
    assert os.environ["TEMPORAL_API_KEY"] == "tk"
    assert os.environ["OPENAI_API_KEY"] == "ok"


def test_fails_loudly_when_the_secret_lacks_a_required_key() -> None:
    stub = _StubSecretsManager({"TEMPORAL_API_KEY": "tk"})

    with pytest.raises(MissingSecretKeyError, match="OPENAI_API_KEY"):
        load_secrets_into_env(
            "order-support/keys", ["TEMPORAL_API_KEY", "OPENAI_API_KEY"], client=stub
        )
