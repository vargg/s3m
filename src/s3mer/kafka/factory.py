"""Factory for Kafka replication wiring in the proxy."""

from dataclasses import dataclass

from faststream.kafka import KafkaBroker

from s3mer.common.metrics import MetricsTracker
from s3mer.config.settings import ReplicationMode, Settings
from s3mer.kafka.broker import create_broker
from s3mer.kafka.manager import (
    BaseReplicationManager,
    BatchReplicationManager,
    PerBackendReplicationManager,
)
from s3mer.kafka.null import NullReplicationManager
from s3mer.kafka.publisher import ReplicationPublisher


@dataclass(frozen=True)
class ReplicationStack:
    """Replication manager and optional Kafka broker for the proxy lifecycle."""

    manager: BaseReplicationManager
    broker: KafkaBroker | None


def build_replication_stack(settings: Settings, metrics: MetricsTracker) -> ReplicationStack:
    """Build replication manager and broker according to kafka.enabled and replication_mode."""
    if not settings.kafka.enabled:
        return ReplicationStack(NullReplicationManager(metrics), broker=None)

    broker = create_broker(settings.kafka)
    publisher = ReplicationPublisher(broker, settings.kafka.topic)
    if settings.replication_mode == ReplicationMode.PER_BACKEND:
        manager: BaseReplicationManager = PerBackendReplicationManager(publisher, metrics)
    else:
        manager = BatchReplicationManager(publisher, metrics)
    return ReplicationStack(manager, broker)
