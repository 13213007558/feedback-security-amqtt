"""Authorization and lifecycle tests for MQTT last-will messages.

These tests run the broker over an in-memory transport (no TCP socket needed) so they
can exercise the broker's full connection, will and take-over paths deterministically.
"""

import asyncio

import pytest

from amqtt.adapters import ReaderAdapter, WriterAdapter
from amqtt.broker import Broker
from amqtt.contexts import BrokerConfig, ListenerConfig, ListenerType
from amqtt.errors import NoDataError
from amqtt.mqtt import packet_class
from amqtt.mqtt.connack import CONNECTION_ACCEPTED, NOT_AUTHORIZED
from amqtt.mqtt.connect import ConnectPacket, ConnectPayload, ConnectVariableHeader
from amqtt.mqtt.constants import QOS_0
from amqtt.mqtt.disconnect import DisconnectPacket
from amqtt.mqtt.packet import MQTTFixedHeader
from amqtt.mqtt.subscribe import SubscribePacket
from tests.support.will_acl_plugin import DynamicTopicAclPlugin


class _Channel:
    """One-way in-memory byte channel supporting EOF, mimicking an asyncio stream."""

    def __init__(self):
        self._queue: asyncio.Queue = asyncio.Queue()
        self._buffer = b""
        self._eof = False

    async def read(self, n=-1):
        if n == -1:
            while not self._buffer:
                if self._eof:
                    return b""
                item = await self._queue.get()
                if item is None:
                    self._eof = True
                    return b""
                self._buffer += item
            data, self._buffer = self._buffer, b""
            return data
        while len(self._buffer) < n:
            if self._eof:
                return b""
            item = await self._queue.get()
            if item is None:
                self._eof = True
                return b""
            self._buffer += item
        data, self._buffer = self._buffer[:n], self._buffer[n:]
        return data

    def write(self, data):
        if not self._eof:
            self._queue.put_nowait(data)

    def close(self):
        if not self._eof:
            self._eof = True
            self._queue.put_nowait(None)


class _ChannelReader(ReaderAdapter):
    def __init__(self, channel):
        self._channel = channel

    async def read(self, n=-1):
        return await self._channel.read(n)

    def feed_eof(self):
        self._channel.close()


class _ChannelWriter(WriterAdapter):
    def __init__(self, channel):
        self._channel = channel
        self.is_closed = False

    def write(self, data):
        self._channel.write(data)

    async def drain(self):
        await asyncio.sleep(0)

    def get_peer_info(self):
        return ("127.0.0.1", 40000)

    def get_ssl_info(self):
        return None

    async def close(self):
        self.is_closed = True


def _memory_pair():
    client_to_server, server_to_client = _Channel(), _Channel()
    server_reader, server_writer = _ChannelReader(client_to_server), _ChannelWriter(server_to_client)
    client_reader, client_writer = _ChannelReader(server_to_client), _ChannelWriter(client_to_server)
    return server_reader, server_writer, client_reader, client_writer


def _connect_packet(client_id, *, will_topic=None, will_message=None, will_retain=False, clean_session=True):
    variable_header = ConnectVariableHeader()
    payload = ConnectPayload()
    variable_header.keep_alive = 0
    variable_header.clean_session_flag = clean_session
    variable_header.will_flag = will_topic is not None
    variable_header.will_retain_flag = will_retain
    variable_header.will_qos = QOS_0
    payload.client_id = client_id
    payload.will_topic = will_topic
    payload.will_message = will_message
    return ConnectPacket(variable_header=variable_header, payload=payload)


async def _read_packet(reader):
    fixed = await MQTTFixedHeader.from_stream(reader)
    if fixed is None:
        return None
    return await packet_class(fixed).from_stream(reader, fixed_header=fixed)


async def _read_packet_wait(reader, timeout=0.5):
    try:
        return await asyncio.wait_for(_read_packet(reader), timeout=timeout)
    except (asyncio.TimeoutError, NoDataError):
        return None


class _Connection:
    def __init__(self, task, server_reader, server_writer, client_reader, client_writer):
        self.task = task
        self.server_reader = server_reader
        self.server_writer = server_writer
        self.client_reader = client_reader
        self.client_writer = client_writer


def _broker_config():
    return BrokerConfig(
        listeners={"default": ListenerConfig(type=ListenerType.EXTERNAL)},
        plugins={
            "amqtt.plugins.authentication.AnonymousAuthPlugin": {"allow_anonymous": True},
            "tests.support.will_acl_plugin.DynamicTopicAclPlugin": {},
        },
    )


@pytest.fixture
async def will_broker():
    # Mutate the shared policy mapping in place via the stable support module: the plugin
    # instance constructed by the broker references this same class object and mapping.
    DynamicTopicAclPlugin.reset({"allowed/will"})
    broker = Broker(_broker_config())
    await broker.start()
    yield broker
    if not broker.transitions.is_stopped():
        await broker.shutdown()


async def _open(broker, client_id, *, wait_connected=True, **kwargs):
    server_reader, server_writer, client_reader, client_writer = _memory_pair()
    task = asyncio.create_task(broker.external_connected(server_reader, server_writer, "default"))
    await _connect_packet(client_id, **kwargs).to_stream(client_writer)
    connack = await _read_packet_wait(client_reader)
    # The CONNACK is sent before handler.start() schedules the reader task. Wait until the
    # connection is fully established so an immediate abrupt close is actually observed.
    if wait_connected and connack.variable_header.return_code == CONNECTION_ACCEPTED:
        def fully_connected():
            entry = broker.sessions.get(client_id)
            return (
                entry is not None
                and entry[0].transitions.state == "connected"
                and entry[1]._reader_task is not None
            )

        await _wait_until(fully_connected)
    connection = _Connection(task, server_reader, server_writer, client_reader, client_writer)
    return connection, connack


def _abrupt_close(connection):
    # Simulate an abnormal TCP drop (no DISCONNECT packet): EOF the broker's reader.
    connection.server_reader.feed_eof()


async def _settle():
    # Let the reader task and the broadcast loop drain pending work.
    for _ in range(25):
        await asyncio.sleep(0.02)


async def _wait_until(predicate, timeout=2.0):
    """Poll until predicate() is true or the timeout elapses (then raise)."""
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            msg = "condition was not satisfied before timeout"
            raise AssertionError(msg)
        await asyncio.sleep(0.01)


async def _wait_disconnected(broker, client_id):
    """Wait until the broker has finished processing that client's disconnection."""
    def disconnected():
        entry = broker.sessions.get(client_id)
        return entry is None or entry[0].transitions.state == "disconnected"

    await _wait_until(disconnected)


async def _drain(reader, delay=0.4):
    packets = []
    deadline = asyncio.get_running_loop().time() + delay
    while True:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            break
        try:
            packet = await asyncio.wait_for(_read_packet(reader), timeout=remaining)
        except (asyncio.TimeoutError, NoDataError):
            break
        if packet is None:
            break
        packets.append(packet)
    return packets


@pytest.mark.asyncio
async def test_unauthorized_will_rejected_at_connect(will_broker):
    connection, connack = await _open(
        will_broker, "rejected", wait_connected=False, will_topic="forbidden/will", will_message=b"bye")

    assert connack.variable_header.return_code == NOT_AUTHORIZED
    await _settle()
    # A rejected clean session must not linger.
    assert "rejected" not in will_broker.sessions
    connection.task.cancel()
    await asyncio.gather(connection.task, return_exceptions=True)


@pytest.mark.asyncio
async def test_authorized_will_delivered_on_abnormal_disconnect(will_broker):
    subscriber, _ = await _open(will_broker, "subscriber")
    await SubscribePacket.build([("allowed/will", QOS_0)], 1).to_stream(subscriber.client_writer)
    await _read_packet_wait(subscriber.client_reader)

    willer, connack = await _open(
        will_broker, "willer", will_topic="allowed/will", will_message=b"bye")
    assert connack.variable_header.return_code == CONNECTION_ACCEPTED

    _abrupt_close(willer)
    await _wait_disconnected(will_broker, "willer")
    await _settle()
    packets = await _drain(subscriber.client_reader, 0.5)

    assert len(packets) == 1
    assert packets[0].topic_name == "allowed/will"
    assert packets[0].data == b"bye"


@pytest.mark.asyncio
async def test_graceful_disconnect_does_not_publish_will(will_broker):
    subscriber, _ = await _open(will_broker, "subscriber")
    await SubscribePacket.build([("allowed/will", QOS_0)], 1).to_stream(subscriber.client_writer)
    await _read_packet_wait(subscriber.client_reader)

    willer, _ = await _open(
        will_broker, "graceful", will_topic="allowed/will", will_message=b"bye")
    await DisconnectPacket().to_stream(willer.client_writer)
    await _settle()

    assert await _drain(subscriber.client_reader, 0.4) == []


@pytest.mark.asyncio
async def test_revoked_permission_blocks_will_broadcast_and_retain(will_broker):
    subscriber, _ = await _open(will_broker, "subscriber")
    await SubscribePacket.build([("allowed/will", QOS_0)], 1).to_stream(subscriber.client_writer)
    await _read_packet_wait(subscriber.client_reader)

    willer, connack = await _open(
        will_broker,
        "revoked",
        will_topic="allowed/will",
        will_message=b"retained-bye",
        will_retain=True,
    )
    assert connack.variable_header.return_code == CONNECTION_ACCEPTED

    # Revoke publish permission before the connection drops.
    DynamicTopicAclPlugin.policy["publish"].discard("allowed/will")
    _abrupt_close(willer)
    await _wait_disconnected(will_broker, "revoked")
    await _settle()

    assert await _drain(subscriber.client_reader, 0.4) == []
    assert "allowed/will" not in will_broker.retained_messages


@pytest.mark.asyncio
async def test_takeover_prevents_stale_connection_will_and_uses_new_session(will_broker):
    subscriber, _ = await _open(will_broker, "subscriber")
    await SubscribePacket.build([("allowed/will", QOS_0)], 1).to_stream(subscriber.client_writer)
    await _read_packet_wait(subscriber.client_reader)

    old, connack = await _open(
        will_broker, "shared-id", will_topic="allowed/will", will_message=b"OLD-WILL", clean_session=False)
    assert connack.variable_header.return_code == CONNECTION_ACCEPTED
    await _settle()
    old_epoch = will_broker.sessions["shared-id"][0].connection_epoch

    new, connack = await _open(
        will_broker, "shared-id", will_topic="allowed/will", will_message=b"NEW-WILL", clean_session=False)
    assert connack.variable_header.return_code == CONNECTION_ACCEPTED
    await _settle()

    session = will_broker.sessions["shared-id"][0]
    # The surviving session belongs to the new connection and carries its will config.
    assert session.connection_epoch == old_epoch + 1
    assert session.will_message == b"NEW-WILL"

    # Delayed cleanup of the superseded old connection must not publish its will.
    _abrupt_close(old)
    await _wait_until(lambda: old.task.done())
    await _settle()
    stale_packets = await _drain(subscriber.client_reader, 0.5)
    assert all(getattr(packet, "data", None) != b"OLD-WILL" for packet in stale_packets)

    # The new connection's own abnormal disconnect still fires its own (new) will.
    _abrupt_close(new)
    await _wait_disconnected(will_broker, "shared-id")
    await _settle()
    new_packets = await _drain(subscriber.client_reader, 0.6)
    assert any(packet.data == b"NEW-WILL" for packet in new_packets)
