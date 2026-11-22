# ABOUTME: Loads the worker's API keys from one AWS Secrets Manager secret into os.environ.
# Keeps TEMPORAL_API_KEY / OPENAI_API_KEY out of agentcore.json (and so out of git).

import json
import os
from collections.abc import Sequence
from typing import Any, Protocol


class MissingSecretKeyError(KeyError):
    """The secret exists but does not hold a key the worker requires."""


class SecretsManagerClient(Protocol):
    def get_secret_value(self, *, SecretId: str) -> dict[str, Any]: ...


def load_secrets_into_env(
    secret_id: str,
    required_keys: Sequence[str],
    *,
    client: SecretsManagerClient,
) -> None:
    """Fetch a JSON secret and export each of ``required_keys`` from it as an env var."""
    secret = json.loads(client.get_secret_value(SecretId=secret_id)["SecretString"])
    missing = [key for key in required_keys if key not in secret]
    if missing:
        raise MissingSecretKeyError(
            f"secret {secret_id!r} is missing required key(s): {', '.join(missing)}"
        )
    for key in required_keys:
        os.environ[key] = secret[key]
