"""Regression tests for session isolation during failed or interleaved CONNECTs.

A rejected CONNECT (bad credentials or an authentication plugin raising) must never
delete or mutate the session of a working client using the same client id. These
tests drive the broker with in-memory stream adapters so they need no network socket.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from amqtt.adapters import BufferReader, BufferWriter, ReaderAdapter, WriterAdapter
from amqtt.broker import Broker, Server
from amqtt.mqtt.connack import ConnackPacket, NOT_AUTHORIZED
from amqtt.mqtt.connect import ConnectPacket, ConnectPayload, ConnectVariableHeader
from amqtt.mqtt.constants import QOS_0, QOS_1
from amqtt.mqtt.disconnect import DisconnectPacket
from amqtt.mqtt.protocol.broker_handler import BrokerProtocolHandler
from amqtt.session import ApplicationMessage, Session


class MemoryStream:
    """One-way in-memory byte stream backed by an asyncio queue.

    Honors exact-size reads: MQTT packet decoding repeatedly calls ``read(1)``.
    """

    def __init__(self) -> None:
        self._chunks: asyncio.Queue[bytes | None] = asyncio.Queue()
        self._buffer = b""
        self._closed = False

    def write(self, data: bytes) -> None:
        if not self._closed:
            self._chunks.put_nowait(bytes(data))

    async def read(self, n: int = -1) -> bytes:
        if n is None or n < 0:
            parts = [self._buffer]
            self._buffer = b""
            while not self._closed:
                chunk = await self._chunks.get()
                if chunk is None:
                    self._closed = True
                    break
                parts.append(chunk)
            return b"".join(parts)

        while not self._closed and len(self._buffer) < n:
            chunk = await self._chunks.get()
            if chunk is None:
                self._closed = True
                break
            self._buffer += chunk
        data = self._buffer[:n]
        self._buffer = self._buffer[n:]
        return data

    def feed_eof(self) -> None:
        if not self._closed:
            self._chunks.put_nowait(None)


class MemoryPipe:
    """A pair of connected one-way streams forming a full-duplex channel."""

    def __init__(self) -> None:
        # Bytes written by the client and read by the broker.
        self.client_to_broker = MemoryStream()
        # Bytes written by the broker and read by the client.
        self.broker_to_client = MemoryStream()


class MemoryReader(ReaderAdapter):
    def __init__(self, stream: MemoryStream) -> None:
        self._stream = stream

    async def read(self, n: int = -1) -> bytes:
        return await self._stream.read(n)

    def feed_eof(self) -> None:
        self._stream.feed_eof()


class MemoryWriter(WriterAdapter):
    def __init__(self, stream: MemoryStream) -> None:
        self._stream = stream
        self.closed = False

    def write(self, data: bytes) -> None:
        if not self.closed:
            self._stream.write(data)

    async def drain(self) -> None:
        pass

    def get_peer_info(self) -> tuple[str, int]:
        return "127.0.0.1", 43210

    def get_ssl_info(self):
        return None

    async def close(self) -> None:
        if not self.closed:
            self.closed = True
            self._stream.feed_eof()


async def build_connect_packet(
    client_id: str,
    *,
    clean_session: bool,
    username: str | None = None,
    password: str | None = None,
    will_topic: str | None = None,
    will_message: bytes | None = None,
) -> bytes:
    vh = ConnectVariableHeader()
    payload = ConnectPayload()
    vh.clean_session_flag = clean_session
    vh.keep_alive = 30
    payload.client_id = client_id
    if username is not None:
        vh.username_flag = True
        payload.username = username
    if password is not None:
        vh.password_flag = True
        payload.password = password
    if will_topic is not None:
        vh.will_flag = True
        vh.will_retain_flag = False
        vh.will_qos = QOS_0
        payload.will_topic = will_topic
        payload.will_message = will_message
    packet = ConnectPacket(variable_header=vh, payload=payload)
    buffer_writer = BufferWriter()
    await packet.to_stream(buffer_writer)
    return buffer_writer.get_buffer()


async def build_disconnect_packet() -> bytes:
    buffer_writer = BufferWriter()
    await DisconnectPacket().to_stream(buffer_writer)
    return buffer_writer.get_buffer()


@pytest.fixture
async def broker(mock_plugin_manager) -> Broker:
    assert mock_plugin_manager is not None
    broker = Broker()
    broker._servers["default"] = Server("default", MagicMock())
    # The mock plugin manager replaces fire_event with a plain mock; make it awaitable.
    broker.plugins_manager.fire_event = AsyncMock()
    broker.plugins_manager.is_topic_filtering_enabled.return_value = False
    return broker


def make_stream_pair() -> tuple[MemoryReader, MemoryWriter, MemoryPipe]:
    """Create a broker reader/writer and the pipe carrying both directions."""
    pipe = MemoryPipe()
    reader = MemoryReader(pipe.client_to_broker)
    writer = MemoryWriter(pipe.broker_to_client)
    return reader, writer, pipe


def seed_working_session(broker: Broker, client_id: str) -> tuple[Session, BrokerProtocolHandler]:
    """Create a fully populated, connected session representing a working client."""
    session = Session()
    session.client_id = client_id
    session.clean_session = False
    session.username = "legit-user"
    session.password = "legit-password"
    session.will_flag = True
    session.will_topic = "/old/will"
    session.will_message = b"old-will-payload"
    session.will_qos = QOS_1
    session.will_retain = True
    session.remote_address = "10.0.0.1"
    session.remote_port = 1111
    session.transitions.connect()

    # QoS 0 retained messages are delivered on reconnect without waiting on a PUBACK,
    # which keeps these in-memory tests free of a running acknowledgement client.
    queued = ApplicationMessage(None, "/offline/topic", QOS_0, b"offline-payload", retain=False)
    session.retained_messages.put_nowait(queued)
    session.delivered_message_queue.put_nowait(
        ApplicationMessage(None, "/inflight/topic", QOS_0, b"inflight-payload", retain=False),
    )

    handler = BrokerProtocolHandler(broker.plugins_manager, session)
    broker._sessions[client_id] = (session, handler)
    broker._subscriptions["/old/topic"] = [(session, QOS_1)]
    return session, handler


def set_auth(broker: Broker, outcome) -> None:
    if isinstance(outcome, BaseException):
        broker.plugins_manager.map_plugin_auth = AsyncMock(side_effect=outcome)
    else:
        broker.plugins_manager.map_plugin_auth = AsyncMock(return_value={MagicMock(): outcome})


async def drive_connection(broker: Broker, connect_bytes: bytes):
    reader, writer, pipe = make_stream_pair()
    pipe.client_to_broker.write(connect_bytes)
    await broker._client_connected("default", reader, writer)
    return reader, writer, pipe


async def start_connection(broker: Broker, connect_bytes: bytes):
    """Start a connection as a background task and hand back its in-memory pipe."""
    reader, writer, pipe = make_stream_pair()
    pipe.client_to_broker.write(connect_bytes)
    task = asyncio.ensure_future(broker._client_connected("default", reader, writer))
    return reader, writer, pipe, task


async def read_connack(pipe: MemoryPipe) -> ConnackPacket:
    stream = pipe.broker_to_client
    fixed = await stream.read(2)
    remaining_length = fixed[1]
    rest = await stream.read(remaining_length)
    return await ConnackPacket.from_stream(BufferReader(fixed + rest))

def assert_old_session_untouched(broker: Broker, session: Session, handler: BrokerProtocolHandler) -> None:
    client_id = session.client_id
    # Same session object and handler remain registered.
    assert broker._sessions[client_id] == (session, handler)

    # Credentials and will are unchanged.
    assert session.username == "legit-user"
    assert session.password == "legit-password"
    assert session.will_flag is True
    assert session.will_topic == "/old/will"
    assert session.will_message == b"old-will-payload"
    assert session.will_qos == QOS_1
    assert session.will_retain is True

    # Subscriptions survive.
    assert broker._subscriptions["/old/topic"] == [(session, QOS_1)]

    # Offline/inflight queues survive.
    assert session.retained_messages.qsize() == 1
    assert session.delivered_message_queue.qsize() == 1
    assert session.retained_messages.get_nowait().data == b"offline-payload"
    assert session.delivered_message_queue.get_nowait().data == b"inflight-payload"

    # The old connection remains in the connected state.
    assert session.transitions.is_connected()


@pytest.mark.asyncio
@pytest.mark.parametrize("clean_session", [True, False])
async def test_rejected_connect_preserves_working_session(broker, clean_session):
    client_id = f"client-clean-{clean_session}"
    old_session, old_handler = seed_working_session(broker, client_id)
    set_auth(broker, False)

    connect_bytes = await build_connect_packet(
        client_id,
        clean_session=clean_session,
        username="attacker",
        password="wrong-password",
        will_topic="/evil/will",
        will_message=b"evil",
    )
    _, writer, pipe = await drive_connection(broker, connect_bytes)

    connack = await read_connack(pipe)
    assert connack.return_code == NOT_AUTHORIZED
    assert writer.closed is True

    assert_old_session_untouched(broker, old_session, old_handler)


@pytest.mark.asyncio
@pytest.mark.parametrize("clean_session", [True, False])
async def test_auth_plugin_exception_preserves_working_session(broker, clean_session, caplog):
    client_id = f"client-raise-{clean_session}"
    old_session, old_handler = seed_working_session(broker, client_id)
    set_auth(broker, RuntimeError("plugin exploded"))

    connect_bytes = await build_connect_packet(
        client_id,
        clean_session=clean_session,
        username="attacker",
        password="wrong-password",
        will_topic="/evil/will",
        will_message=b"evil",
    )
    _, writer, pipe = await drive_connection(broker, connect_bytes)

    connack = await read_connack(pipe)
    assert connack.return_code == NOT_AUTHORIZED
    assert writer.closed is True
    assert "Authentication plugin raised" in caplog.text

    assert_old_session_untouched(broker, old_session, old_handler)


@pytest.mark.asyncio
async def test_rejected_connect_runs_before_any_session_adoption(broker):
    client_id = "client-order-check"
    old_session, old_handler = seed_working_session(broker, client_id)

    async def failing_auth(*, session: Session) -> dict:
        # While authentication runs, no shared state may have changed yet.
        assert broker._sessions[client_id] == (old_session, old_handler)
        assert session is not old_session
        assert broker._subscriptions["/old/topic"] == [(old_session, QOS_1)]
        return {MagicMock(): False}

    broker.plugins_manager.map_plugin_auth = failing_auth

    connect_bytes = await build_connect_packet(client_id, clean_session=True, username="attacker")
    await drive_connection(broker, connect_bytes)

    assert_old_session_untouched(broker, old_session, old_handler)


@pytest.mark.asyncio
async def test_anonymous_clean_session_gets_generated_id_before_auth(broker):
    set_auth(broker, True)
    connect_bytes = await build_connect_packet("", clean_session=True)

    _reader, writer, pipe, task = await start_connection(broker, connect_bytes)

    connack = await read_connack(pipe)
    assert connack.return_code == 0
    registered_ids = list(broker._sessions)
    assert len(registered_ids) == 1
    assert registered_ids[0]
    assert broker._sessions[registered_ids[0]][0].transitions.is_connected()

    pipe.client_to_broker.write(await build_disconnect_packet())
    await asyncio.wait_for(task, timeout=2)


@pytest.mark.asyncio
async def test_legitimate_reconnect_takes_over_existing_session(broker):
    client_id = "client-legit-takeover"
    old_session, old_handler = seed_working_session(broker, client_id)
    old_writer = MagicMock()
    old_writer.close = AsyncMock()
    old_handler.writer = old_writer
    set_auth(broker, True)

    connect_bytes = await build_connect_packet(
        client_id,
        clean_session=False,
        username="legit-user",
        password="legit-password",
        will_topic="/new/will",
        will_message=b"new-will",
    )
    _reader, writer, pipe, task = await start_connection(broker, connect_bytes)

    connack = await read_connack(pipe)
    assert connack.return_code == 0
    assert connack.session_parent == 1

    # The old connection's handler is stopped (which closes its writer).
    old_writer.close.assert_awaited()

    session, handler = broker._sessions[client_id]
    assert session is old_session
    assert handler is not old_handler
    assert handler.writer is writer
    assert session.will_topic == "/new/will"
    assert session.will_message == b"new-will"
    # Queued messages and subscriptions carry over to the taken-over session.
    assert session.retained_messages.qsize() == 1
    assert broker._subscriptions["/old/topic"] == [(session, QOS_1)]
    assert session.transitions.is_connected()

    # End the new connection with a normal DISCONNECT so the message loop exits cleanly.
    pipe.client_to_broker.write(await build_disconnect_packet())
    await asyncio.wait_for(task, timeout=2)


@pytest.mark.asyncio
async def test_legitimate_clean_session_reconnect_replaces_old_state(broker):
    client_id = "client-clean-replace"
    old_session, old_handler = seed_working_session(broker, client_id)
    old_writer = MagicMock()
    old_writer.close = AsyncMock()
    old_handler.writer = old_writer
    set_auth(broker, True)

    connect_bytes = await build_connect_packet(client_id, clean_session=True, username="legit-user")
    _reader, writer, pipe, task = await start_connection(broker, connect_bytes)

    connack = await read_connack(pipe)
    assert connack.return_code == 0
    assert connack.session_parent == 0

    session, _handler = broker._sessions[client_id]
    assert session is not old_session
    assert "/old/topic" not in broker._subscriptions
    assert session.retained_messages.qsize() == 0
    old_writer.close.assert_awaited()

    pipe.client_to_broker.write(await build_disconnect_packet())
    await asyncio.wait_for(task, timeout=2)


@pytest.mark.asyncio
async def test_concurrent_failed_connect_cannot_overwrite_successful_one(broker):
    client_id = "client-interleave"
    old_session, _old_handler = seed_working_session(broker, client_id)

    auth_started = asyncio.Event()
    release_failing_auth = asyncio.Event()

    async def auth_gate(*, session: Session) -> dict:
        if session.username == "attacker":
            auth_started.set()
            await release_failing_auth.wait()
            return {MagicMock(): False}
        return {MagicMock(): True}

    broker.plugins_manager.map_plugin_auth = auth_gate

    failing_connect = await build_connect_packet(
        client_id, clean_session=False, username="attacker", will_topic="/evil", will_message=b"evil",
    )
    winning_connect = await build_connect_packet(
        client_id,
        clean_session=False,
        username="legit-user",
        password="legit-password",
        will_topic="/new/will",
        will_message=b"new-will",
    )

    failing_reader, failing_writer, failing_pipe = make_stream_pair()
    failing_pipe.client_to_broker.write(failing_connect)
    failing_task = asyncio.ensure_future(
        broker._client_connected("default", failing_reader, failing_writer),
    )
    await auth_started.wait()

    # The winner authenticates and fully adopts the session while the loser is still
    # blocked inside authentication.
    winning_reader, winning_writer, winning_pipe = make_stream_pair()
    winning_pipe.client_to_broker.write(winning_connect)
    winning_task = asyncio.ensure_future(
        broker._client_connected("default", winning_reader, winning_writer),
    )

    connack = await read_connack(winning_pipe)
    assert connack.return_code == 0

    winning_session, winning_handler = broker._sessions[client_id]
    assert winning_session.will_topic == "/new/will"

    # Now let the blocked, rejected connection finish.
    release_failing_auth.set()
    await asyncio.wait_for(failing_task, timeout=2)
    assert failing_writer.closed is True

    # The loser must not have replaced the winner's state.
    session, handler = broker._sessions[client_id]
    assert session is winning_session
    assert handler is winning_handler
    assert session.will_topic == "/new/will"
    assert session.will_message == b"new-will"
    assert session.username == "legit-user"
    assert session.transitions.is_connected()

    # The winner's successful reconnect delivered the queued offline message, while
    # the untouched in-flight queue and the original subscriptions survive.
    assert session.retained_messages.qsize() == 0
    assert session.delivered_message_queue.qsize() == 1
    assert session.delivered_message_queue.get_nowait().data == b"inflight-payload"
    assert broker._subscriptions["/old/topic"] == [(session, QOS_1)]
    assert old_session is session

    winning_pipe.client_to_broker.write(await build_disconnect_packet())
    await asyncio.wait_for(winning_task, timeout=2)
