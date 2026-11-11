"""RECEIVE authorization must apply to every delivery path.

A client may be allowed to subscribe to a broad filter while only being
authorized to receive a subset of the concrete topics matching it. Messages
retained before the receive permission is revoked must not be replayed later:

* real-time broadcasts go through ``Broker._run_broadcast``,
* retained messages matching a new subscription go through
  ``Broker._publish_retained_messages_for_subscription``,
* messages queued for a disconnected persistent session go through
  ``Broker._publish_session_retained_messages``.

These tests drive the real broker call chains (real ``TopicAccessControlListPlugin``
and real session queues) without binding a network socket; the protocol handler is
mocked only where a packet would be written to the wire.
"""

import asyncio
from collections import deque
from unittest.mock import AsyncMock, MagicMock

import pytest

from amqtt.broker import Broker, RetainedApplicationMessage
from amqtt.contexts import Action
from amqtt.mqtt.constants import QOS_0, QOS_1, QOS_2
from amqtt.session import Session

WIDE_FILTER = "data/#"
ALLOWED_TOPIC = "data/allowed"
DENIED_TOPIC = "data/secret"


def _broker_config(topic_check):
    return {
        "listeners": {"default": {"type": "tcp", "bind": "127.0.0.1:1883"}},
        "sys_interval": 0,
        # the legacy "auth" section is required for entry-point plugin loading
        "auth": {"allow-anonymous": True},
        "topic-check": topic_check,
    }


def _topic_check(receive_topics):
    return {
        "enabled": True,
        "plugins": ["topic_acl"],
        "subscribe-acl": {"restricted": [WIDE_FILTER]},
        "receive-acl": {"restricted": receive_topics},
    }


@pytest.fixture
async def receive_acl_broker():
    topic_check = _topic_check([ALLOWED_TOPIC])
    broker = Broker(
        _broker_config(topic_check),
        plugin_namespace="amqtt.broker.plugins",
    )
    assert broker.plugins_manager.is_topic_filtering_enabled()
    yield broker, topic_check


def _client_session():
    session = Session()
    session.client_id = "restricted-client"
    session.username = "restricted"
    session.transitions.connect()
    return session


def _attach_handler(broker, session):
    handler = MagicMock()
    handler.mqtt_publish = AsyncMock()
    broker._sessions[session.client_id] = (session, handler)
    return handler


@pytest.mark.asyncio
async def test_subscribe_filter_permitted_but_concrete_topic_receive_denied(receive_acl_broker):
    broker, _ = receive_acl_broker
    session = _client_session()

    assert await broker._topic_filtering(session, WIDE_FILTER, Action.SUBSCRIBE) is True
    assert await broker._topic_filtering(session, ALLOWED_TOPIC, Action.RECEIVE) is True
    assert await broker._topic_filtering(session, DENIED_TOPIC, Action.RECEIVE) is False


@pytest.mark.asyncio
async def test_retained_message_replay_on_subscription_checks_receive(receive_acl_broker):
    broker, _ = receive_acl_broker
    session = _client_session()
    handler = _attach_handler(broker, session)

    # Retain both an authorized and an unauthorized concrete topic first; the
    # broad subscription filter matches both.
    broker._retained_messages[ALLOWED_TOPIC] = RetainedApplicationMessage(
        None,
        ALLOWED_TOPIC,
        b"allowed",
        QOS_1,
    )
    broker._retained_messages[DENIED_TOPIC] = RetainedApplicationMessage(
        None,
        DENIED_TOPIC,
        b"secret",
        QOS_1,
    )

    await broker._publish_retained_messages_for_subscription((WIDE_FILTER, QOS_1), session)

    delivered = {call.args[0]: call for call in handler.mqtt_publish.call_args_list}
    assert list(delivered) == [ALLOWED_TOPIC]
    # retained replay preserves the retained flag and negotiated QoS (min of subscription and retained)
    assert delivered[ALLOWED_TOPIC].args == (ALLOWED_TOPIC, b"allowed", QOS_1)
    assert delivered[ALLOWED_TOPIC].kwargs == {"retain": True}


@pytest.mark.asyncio
async def test_retained_message_replay_after_permission_revocation_is_blocked(receive_acl_broker):
    broker, topic_check = receive_acl_broker
    session = _client_session()
    handler = _attach_handler(broker, session)

    # Message retained while the topic was still receivable...
    topic_check["receive-acl"]["restricted"] = [ALLOWED_TOPIC, DENIED_TOPIC]
    broker._retained_messages[DENIED_TOPIC] = RetainedApplicationMessage(
        None,
        DENIED_TOPIC,
        b"secret",
        QOS_2,
    )
    # ...permission changes before the client subscribes.
    topic_check["receive-acl"]["restricted"] = [ALLOWED_TOPIC]

    await broker._publish_retained_messages_for_subscription((WIDE_FILTER, QOS_2), session)

    handler.mqtt_publish.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("qos", [QOS_0, QOS_1, QOS_2])
async def test_retained_replay_denied_for_every_qos(receive_acl_broker, qos):
    broker, _ = receive_acl_broker
    session = _client_session()
    handler = _attach_handler(broker, session)

    broker._retained_messages[DENIED_TOPIC] = RetainedApplicationMessage(
        None,
        DENIED_TOPIC,
        b"secret",
        qos,
    )

    await broker._publish_retained_messages_for_subscription((WIDE_FILTER, QOS_2), session)

    handler.mqtt_publish.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("qos", [QOS_1, QOS_2])
async def test_disconnected_backlog_denied_for_every_qos(receive_acl_broker, qos):
    broker, _ = receive_acl_broker
    session = _client_session()
    session.transitions.disconnect()
    handler = _attach_handler(broker, session)

    await broker._retain_broadcast_message(
        {"session": None, "topic": DENIED_TOPIC, "data": b"secret"},
        qos,
        session,
    )

    session.transitions.connect()
    await broker._publish_session_retained_messages(session)

    handler.mqtt_publish.assert_not_called()
    assert session.retained_messages.empty()


@pytest.mark.asyncio
async def test_disconnected_session_backlog_checks_receive_on_reconnect(receive_acl_broker):
    broker, _ = receive_acl_broker
    session = _client_session()
    session.transitions.disconnect()
    handler = _attach_handler(broker, session)

    # QoS 1/2 messages queued (via the real offline branch) while disconnected.
    await broker._retain_broadcast_message(
        {"session": None, "topic": ALLOWED_TOPIC, "data": b"allowed-backlog"},
        QOS_1,
        session,
    )
    await broker._retain_broadcast_message(
        {"session": None, "topic": DENIED_TOPIC, "data": b"secret-backlog"},
        QOS_2,
        session,
    )
    assert session.retained_messages.qsize() == 2

    session.transitions.connect()
    await broker._publish_session_retained_messages(session)

    delivered = {call.args[0]: call for call in handler.mqtt_publish.call_args_list}
    assert list(delivered) == [ALLOWED_TOPIC]
    # the authorized backlog message keeps its original QoS and the retain flag
    assert delivered[ALLOWED_TOPIC].args == (ALLOWED_TOPIC, b"allowed-backlog", QOS_1)
    assert delivered[ALLOWED_TOPIC].kwargs == {"retain": True}
    # denied messages are dropped instead of left to loop forever
    assert session.retained_messages.empty()


@pytest.mark.asyncio
async def test_backlog_qos0_message_is_never_queued(receive_acl_broker):
    broker, _ = receive_acl_broker
    session = _client_session()
    session.transitions.disconnect()
    handler = _attach_handler(broker, session)
    broker._subscriptions[WIDE_FILTER] = [(session, QOS_1)]

    # [MQTT 3.1.2.4] QoS 0 messages are not queued for offline sessions in the first place
    await broker._broadcast_queue.put(
        {"session": None, "topic": ALLOWED_TOPIC, "data": b"qos0", "qos": QOS_0},
    )
    await broker._run_broadcast(deque())

    handler.mqtt_publish.assert_not_called()
    assert session.retained_messages.empty()


@pytest.mark.asyncio
async def test_live_broadcast_checks_receive_for_each_matching_subscription(receive_acl_broker):
    broker, _ = receive_acl_broker
    session = _client_session()
    handler = _attach_handler(broker, session)
    broker._subscriptions[WIDE_FILTER] = [(session, QOS_1)]

    await broker._broadcast_queue.put(
        {"session": None, "topic": DENIED_TOPIC, "data": b"secret", "qos": QOS_2},
    )
    await broker._broadcast_queue.put(
        {"session": None, "topic": ALLOWED_TOPIC, "data": b"allowed", "qos": QOS_2},
    )
    running_tasks = deque()
    await broker._run_broadcast(running_tasks)
    await broker._run_broadcast(running_tasks)
    await asyncio.gather(*running_tasks, return_exceptions=True)

    delivered = {call.args[0]: call for call in handler.mqtt_publish.call_args_list}
    assert list(delivered) == [ALLOWED_TOPIC]
    # live delivery keeps the published QoS and never sets the retain flag
    assert delivered[ALLOWED_TOPIC].args == (ALLOWED_TOPIC, b"allowed", QOS_2)
    assert delivered[ALLOWED_TOPIC].kwargs == {"retain": False}


@pytest.mark.asyncio
async def test_live_broadcast_queues_only_authorized_messages_while_disconnected(receive_acl_broker):
    broker, _ = receive_acl_broker
    session = _client_session()
    session.transitions.disconnect()
    handler = _attach_handler(broker, session)
    broker._subscriptions[WIDE_FILTER] = [(session, QOS_1)]

    await broker._broadcast_queue.put(
        {"session": None, "topic": DENIED_TOPIC, "data": b"secret", "qos": QOS_1},
    )
    await broker._broadcast_queue.put(
        {"session": None, "topic": ALLOWED_TOPIC, "data": b"allowed", "qos": QOS_1},
    )
    await broker._run_broadcast(deque())
    await broker._run_broadcast(deque())

    handler.mqtt_publish.assert_not_called()
    assert session.retained_messages.qsize() == 1
    queued = session.retained_messages.get_nowait()
    assert queued.topic == ALLOWED_TOPIC
    assert queued.data == b"allowed"
    assert queued.qos == QOS_1
    assert queued.retain is True


@pytest.mark.asyncio
async def test_backlog_preserves_order_of_authorized_messages(receive_acl_broker):
    broker, _ = receive_acl_broker
    session = _client_session()
    session.transitions.disconnect()
    handler = _attach_handler(broker, session)

    for index, (topic, qos) in enumerate(
        [
            (DENIED_TOPIC, QOS_1),
            (ALLOWED_TOPIC, QOS_1),
            (DENIED_TOPIC, QOS_2),
            (ALLOWED_TOPIC, QOS_2),
        ],
    ):
        await broker._retain_broadcast_message(
            {"session": None, "topic": topic, "data": f"msg-{index}".encode()},
            qos,
            session,
        )

    session.transitions.connect()
    await broker._publish_session_retained_messages(session)

    delivered = handler.mqtt_publish.call_args_list
    assert [call.args[0] for call in delivered] == [ALLOWED_TOPIC, ALLOWED_TOPIC]
    assert [call.args[2] for call in delivered] == [QOS_1, QOS_2]
    assert [call.args[1] for call in delivered] == [b"msg-1", b"msg-3"]
    assert all(call.kwargs == {"retain": True} for call in delivered)
