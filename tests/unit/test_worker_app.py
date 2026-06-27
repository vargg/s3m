"""Tests for worker startup validation."""

import pytest

from s3mer.config.settings import Settings, set_settings_override
from s3mer.worker.app import create_worker_app


def test_worker_rejects_kafka_disabled() -> None:
    settings = Settings.model_validate(
        {
            "backends": {
                "primary": {
                    "endpoint_url": "http://primary:9000",
                    "access_key": "a",
                    "secret_key": "s",
                    "is_primary": True,
                },
            },
            "write_strategy": "multi_sync_simple",
            "kafka": {"enabled": False},
        },
    )
    set_settings_override(settings)
    try:
        with pytest.raises(ValueError, match=r"kafka\.enabled must be true"):
            create_worker_app()
    finally:
        set_settings_override(None)
