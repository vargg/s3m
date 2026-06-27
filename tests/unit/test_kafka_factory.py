"""Tests for Kafka replication stack factory."""

from unittest.mock import MagicMock, patch

from s3mer.common.metrics import NullMetricsTracker
from s3mer.config.settings import Settings, WriteStrategyType
from s3mer.kafka.factory import build_replication_stack
from s3mer.kafka.manager import BatchReplicationManager, PerBackendReplicationManager
from s3mer.kafka.null import NullReplicationManager


def _base_settings() -> dict:
    return {
        "backends": {
            "primary": {
                "endpoint_url": "http://primary:9000",
                "access_key": "a",
                "secret_key": "s",
                "is_primary": True,
            },
            "secondary": {
                "endpoint_url": "http://secondary:9000",
                "access_key": "a",
                "secret_key": "s",
                "is_primary": False,
            },
        },
    }


class TestBuildReplicationStack:
    def test_kafka_disabled_returns_null_manager(self) -> None:
        settings = Settings.model_validate(
            {
                **_base_settings(),
                "write_strategy": WriteStrategyType.MULTI_SYNC_SIMPLE,
                "kafka": {"enabled": False},
            },
        )
        stack = build_replication_stack(settings, NullMetricsTracker())
        assert isinstance(stack.manager, NullReplicationManager)
        assert stack.broker is None

    @patch("s3mer.kafka.factory.create_broker")
    def test_kafka_enabled_batch_mode(self, mock_create_broker: MagicMock) -> None:
        mock_broker = object()
        mock_create_broker.return_value = mock_broker
        settings = Settings.model_validate({**_base_settings(), "replication_mode": "batch"})
        stack = build_replication_stack(settings, NullMetricsTracker())
        assert isinstance(stack.manager, BatchReplicationManager)
        assert stack.broker is mock_broker

    @patch("s3mer.kafka.factory.create_broker")
    def test_kafka_enabled_per_backend_mode(self, mock_create_broker: MagicMock) -> None:
        mock_broker = object()
        mock_create_broker.return_value = mock_broker
        settings = Settings.model_validate({**_base_settings(), "replication_mode": "per_backend"})
        stack = build_replication_stack(settings, NullMetricsTracker())
        assert isinstance(stack.manager, PerBackendReplicationManager)
        assert stack.broker is mock_broker
