"""Tests for no-op replication components."""

from s3mer.common.metrics import NullMetricsTracker
from s3mer.kafka.messages import ReplicationMessage
from s3mer.kafka.null import NullReplicationManager, NullReplicationPublisher
from s3mer.routing.operations import S3Operation


async def test_null_publisher_noop() -> None:
    publisher = NullReplicationPublisher()
    assert publisher.topic == ""
    message = ReplicationMessage(
        operation="put_object",
        bucket="b",
        key="k",
        source_backend="primary",
        target_backends=["secondary"],
    )
    await publisher.publish(message)


async def test_null_manager_noop() -> None:
    manager = NullReplicationManager(NullMetricsTracker())
    await manager.schedule_replication(
        operation=S3Operation.PUT_OBJECT,
        params={"Bucket": "b", "Key": "k"},
        response={"ETag": '"1"'},
        source_backend_name="primary",
        target_backend_names=["secondary"],
    )
