"""No-op replication components used when Kafka is disabled."""

from typing import Any

from s3mer.common.metrics import MetricsTracker
from s3mer.kafka.manager import BaseReplicationManager
from s3mer.kafka.messages import ReplicationMessage
from s3mer.routing.operations import S3Operation


class NullReplicationPublisher:
    """No-op publisher for deployments that do not use async Kafka replication."""

    @property
    def topic(self) -> str:
        return ""

    async def publish(self, message: ReplicationMessage, topic: str | None = None) -> None:
        del message, topic


class NullReplicationManager(BaseReplicationManager):
    """Replication manager that discards schedule requests (Kafka disabled)."""

    def __init__(self, metrics: MetricsTracker) -> None:
        super().__init__(NullReplicationPublisher(), metrics)

    async def schedule_replication(
        self,
        operation: S3Operation,
        params: dict[str, Any],
        response: dict[str, Any],
        source_backend_name: str,
        target_backend_names: list[str],
    ) -> None:
        del operation, params, response, source_backend_name, target_backend_names
