"""End-to-end strict subscribe ACL tests through the broker internals.

These tests drive the real broker subscription and broadcast machinery without
binding a network socket (the sandbox and CI do not require one): a rejected
subscription produces a concrete witness topic, and that same witness is run
through the broker's filter matching and message delivery path to prove it is a
genuine over-reach rather than a theoretical example.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from amqtt.broker import Broker
from amqtt.mqtt.constants import QOS_0
from amqtt.mqtt.protocol.broker_handler import BrokerProtocolHandler
from amqtt.session import Session


def _build_broker(strict_rules):
    broker = Broker(
        {
            "listeners": {"default": {"type": "tcp", "bind": "127.0.0.1:1"}},
            "plugins": {
                "amqtt.plugins.topic_checking.TopicAccessControlListPlugin": {
                    "strict_subscribe_acl": strict_rules
                }
            },
        }
    )
    return broker


def _register_session(broker, client_id, username):
    session = Session()
    session.client_id = client_id
    session.username = username
    session.transitions.set_state("connected")
    # A lightweight handler is enough: mqtt_publish is patched while delivery
    # only needs _get_handler() to return a non-None object.
    handler = BrokerProtocolHandler(broker.plugins_manager, session)
    handler._writer = MagicMock()
    broker._sessions[client_id] = (session, handler)
    return session, handler


@pytest.mark.asyncio
async def test_witness_rejected_at_subscription_and_undeliverable():
    broker = _build_broker({"user": ["tenant/+/public/#"]})
    broker._broadcast_task = asyncio.ensure_future(broker._broadcast_loop())
    try:
        session, _handler = _register_session(broker, "client-1", "user")

        # The over-broad request is refused at the real subscription entrypoint.
        return_code = await broker.add_subscription(("tenant/#", QOS_0), session)
        assert return_code == 0x80
        assert "tenant/#" not in broker._subscriptions

        # The plugin produces a concrete witness for the same request.
        plugin = broker.plugins_manager._topic_plugins[0]
        result = plugin.check_subscription("tenant/#", ["tenant/+/public/#"])
        assert result.allowed is False
        witness = result.witness
        assert witness is not None

        # The witness is independently validated by the broker's own matcher:
        # the requested filter would receive it, the allowed filter would not.
        assert broker._matches(witness, "tenant/#") is True
        assert broker._matches(witness, "tenant/+/public/#") is False

        # Through the real delivery path, a message published to the witness
        # topic reaches nobody because the refused filter was never registered.
        with patch(
            "amqtt.broker.BrokerProtocolHandler.mqtt_publish",
            new_callable=AsyncMock,
        ) as delivered:
            await broker.internal_message_broadcast(witness, b"secret", QOS_0)
            await asyncio.sleep(0.05)
            delivered.assert_not_called()
    finally:
        broker._broadcast_shutdown_waiter.set_result(True)
        await asyncio.sleep(0.05)
        broker._broadcast_task.cancel()


@pytest.mark.asyncio
async def test_authorized_filter_subscribes_and_receives_delivery():
    broker = _build_broker({"user": ["tenant/+/public/#"]})
    broker._broadcast_task = asyncio.ensure_future(broker._broadcast_loop())
    try:
        session, _handler = _register_session(broker, "client-2", "user")

        # A request fully inside the allowed scope is accepted and registered.
        return_code = await broker.add_subscription(
            ("tenant/+/public/#", QOS_0), session
        )
        assert return_code == QOS_0
        assert "tenant/+/public/#" in broker._subscriptions

        # The witness that defeats the broad request is still out of scope for
        # the narrow, authorised filter.
        assert broker._matches("tenant", "tenant/+/public/#") is False

        with patch(
            "amqtt.broker.BrokerProtocolHandler.mqtt_publish",
            new_callable=AsyncMock,
        ) as delivered:
            # In-scope message is delivered through the real broadcast loop.
            await broker.internal_message_broadcast(
                "tenant/acme/public/metrics", b"ok", QOS_0
            )
            await asyncio.sleep(0.05)
            delivered.assert_awaited_once()
            args, _ = delivered.await_args
            assert args[0] == "tenant/acme/public/metrics"

            # The over-reach witness is matched by no registered filter.
            delivered.reset_mock()
            await broker.internal_message_broadcast("tenant", b"secret", QOS_0)
            await asyncio.sleep(0.05)
            delivered.assert_not_called()
    finally:
        broker._broadcast_shutdown_waiter.set_result(True)
        await asyncio.sleep(0.05)
        broker._broadcast_task.cancel()


@pytest.mark.asyncio
async def test_dollar_topic_witness_uses_broker_dollar_rule():
    broker = _build_broker({"user": ["#"]})
    session, _handler = _register_session(broker, "client-3", "user")
    # A '$' request against a single '#' allowance must be refused.
    return_code = await broker.add_subscription(("$SYS/#", QOS_0), session)
    assert return_code == 0x80

    plugin = broker.plugins_manager._topic_plugins[0]
    result = plugin.check_subscription("$SYS/#", ["#"])
    assert result.allowed is False
    witness = result.witness
    # The broker's matcher honours [MQTT-4.7.2-1]: '#' never matches '$'.
    assert broker._matches(witness, "#") is False
    assert broker._matches(witness, "$SYS/#") is True
