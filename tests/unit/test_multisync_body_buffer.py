from collections.abc import AsyncIterator
from pathlib import Path

from s3mer.common.streaming import ConcurrentFileStream, MultiSyncBodyBuffer, StreamConfig


async def test_buffers_small_body_in_memory() -> None:
    config = StreamConfig(chunk_size=1024, max_memory_size=10_485_760, buffer_dir=None)
    buffer = await MultiSyncBodyBuffer.from_body(b"hello", config)
    assert buffer is not None

    body_a = buffer.body_for_backend()
    body_b = buffer.body_for_backend()
    assert body_a == b"hello"
    assert body_b == b"hello"
    await buffer.close()


async def test_memory_bodies_share_one_immutable_buffer() -> None:
    """Backends fan out over the same bytes object — no copy, no shared cursor to rewind."""
    config = StreamConfig(chunk_size=1024, max_memory_size=10_485_760, buffer_dir=None)
    buffer = await MultiSyncBodyBuffer.from_body(b"hello", config)
    assert buffer is not None

    assert buffer.body_for_backend() is buffer.body_for_backend()
    await buffer.close()


async def test_buffers_stream_in_memory() -> None:
    config = StreamConfig(chunk_size=4, max_memory_size=1024, buffer_dir=None)

    async def stream() -> AsyncIterator[bytes]:
        yield b"ab"
        yield b"cd"

    buffer = await MultiSyncBodyBuffer.from_body(stream(), config)
    assert buffer is not None
    assert buffer.body_for_backend() == b"abcd"
    await buffer.close()


async def test_spills_large_stream_to_disk(tmp_path: Path) -> None:
    config = StreamConfig(chunk_size=4, max_memory_size=8, buffer_dir=str(tmp_path))

    async def stream() -> AsyncIterator[bytes]:
        yield b"123456789"

    buffer = await MultiSyncBodyBuffer.from_body(stream(), config)
    assert buffer is not None
    reader = buffer.body_for_backend()
    assert isinstance(reader, ConcurrentFileStream)
    assert await reader.read() == b"123456789"
    await buffer.close()


async def test_spilled_bodies_get_independent_streams(tmp_path: Path) -> None:
    """Each backend needs its own handle and offset; botocore rewinds these via seek(0)."""
    config = StreamConfig(chunk_size=4, max_memory_size=8, buffer_dir=str(tmp_path))

    async def stream() -> AsyncIterator[bytes]:
        yield b"123456789"

    buffer = await MultiSyncBodyBuffer.from_body(stream(), config)
    assert buffer is not None

    reader_a = buffer.body_for_backend()
    reader_b = buffer.body_for_backend()
    assert isinstance(reader_a, ConcurrentFileStream)
    assert isinstance(reader_b, ConcurrentFileStream)
    assert reader_a is not reader_b

    assert await reader_a.read() == b"123456789"
    reader_a.seek(0)
    assert await reader_a.read() == b"123456789"
    assert await reader_b.read() == b"123456789"
    await buffer.close()
