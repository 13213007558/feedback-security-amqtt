"""Regression coverage for combining multiple auth plugin verdicts into a CONNECT decision.

Verdict contract (see ``amqtt.broker.Broker._authenticate``):
- ``True``: plugin allows the connection
- ``False``: plugin denies the connection
- ``None``: plugin abstains; its verdict is ignored
- a raised exception or a timeout: the plugin is faulty; the connection is denied and an
  attributable log line (never containing the password) is emitted

The final decision is a logical AND over every boolean verdict, and connections without any
boolean verdict are denied.
"""

import asyncio
from contextlib import suppress
import logging

import pytest

from amqtt.broker import Broker
from amqtt.client import MQTTClient
from amqtt.contexts import BaseContext
from amqtt.errors import ConnectError, PluginImportError, PluginLoadError
from amqtt.plugins.manager import PluginManager
from amqtt.session import Session

BROKER_LOGGER = "amqtt.broker"


def _manager(plugins_config: dict[str, dict]) -> PluginManager:
    context = BaseContext()
    context.logger = logging.getLogger("testlog")
    context.config = {"plugins": plugins_config}
    return PluginManager("amqtt.test.plugins", context=context)


def _broker_config(bind: str, plugins: list, auth_timeout: float | None = 10) -> dict:
    return {
        "listeners": {
            "default": {"type": "tcp", "bind": bind, "max_connections": 10},
        },
        "auth_timeout": auth_timeout,
        "plugins": plugins,
    }


async def _close_manager(broker: Broker) -> None:
    await broker.plugins_manager.close()


@pytest.mark.asyncio
async def test_all_plugins_abstain_is_treated_as_denial() -> None:
    """No boolean verdict (all None) must not authenticate the connection."""
    manager = _manager({"tests.plugins.mocks.TestNeutralAuthPlugin": {}})

    returns = await manager.map_plugin_auth(session=Session())
    assert list(returns.values()) == [None]

    broker = Broker(config=_broker_config("127.0.0.1:0", [
        {"tests.plugins.mocks.TestNeutralAuthPlugin": {}},
    ]))
    try:
        session = Session()
        session.client_id = "neutral-client"
        assert await broker._authenticate(session, broker.listeners_config["default"]) is False
    finally:
        await _close_manager(broker)


@pytest.mark.asyncio
async def test_single_denier_overrules_multiple_allowers() -> None:
    """One False denies the connection even when other plugins return True or None."""
    manager = _manager({
        "tests.plugins.mocks.TestAuthPlugin": {},
        "tests.plugins.mocks.TestDenyAuthPlugin": {},
        "tests.plugins.mocks.TestNeutralAuthPlugin": {},
    })

    verdicts = list((await manager.map_plugin_auth(session=Session())).values())
    assert set(verdicts) == {True, False, None}

    broker = Broker(config=_broker_config("127.0.0.1:0", [
        {"tests.plugins.mocks.TestAuthPlugin": {}},
        {"tests.plugins.mocks.TestDenyAuthPlugin": {}},
        {"tests.plugins.mocks.TestNeutralAuthPlugin": {}},
    ]))
    try:
        session = Session()
        session.client_id = "mixed-verdict-client"
        assert await broker._authenticate(session, broker.listeners_config["default"]) is False
    finally:
        await _close_manager(broker)


@pytest.mark.asyncio
async def test_all_boolean_allowers_authenticate() -> None:
    """Every boolean verdict being True (an abstaining plugin alongside) authenticates."""
    broker = Broker(config=_broker_config("127.0.0.1:0", [
        {"tests.plugins.mocks.TestAuthPlugin": {}},
        {"tests.plugins.mocks.TestNeutralAuthPlugin": {}},
    ]))
    try:
        session = Session()
        session.client_id = "allowed-client"
        assert await broker._authenticate(session, broker.listeners_config["default"]) is True
    finally:
        await _close_manager(broker)


@pytest.mark.asyncio
async def test_misspelled_plugin_config_option_fails_at_startup() -> None:
    """Unknown config keys are rejected while loading, before the broker serves connections."""
    config = _broker_config("127.0.0.1:0", [
        {"tests.plugins.mocks.TestMisconfiguredAuthPlugin": {"knwon_option": "typo"}},
    ])

    with pytest.raises(PluginLoadError):
        Broker(config=config)


@pytest.mark.asyncio
async def test_misspelled_plugin_path_fails_at_startup() -> None:
    """A dotted path that cannot be imported is reported as a startup import error."""
    config = _broker_config("127.0.0.1:0", [
        {"tests.plugins.mocks.DoesNotExistAuthPlugin": {}},
    ])

    with pytest.raises(PluginImportError):
        Broker(config=config)


@pytest.mark.asyncio
async def test_plugin_exception_is_isolated_and_denies_with_log(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A raising plugin returns its exception instead of breaking the auth call, and denies."""
    secret = "super-secret-password"
    manager = _manager({
        "tests.plugins.mocks.TestRaisingAuthPlugin": {"error_message": "ldap down"},
        "tests.plugins.mocks.TestAuthPlugin": {},
    })

    returns = await manager.map_plugin_auth(session=Session())
    exceptions = [result for result in returns.values() if isinstance(result, BaseException)]
    assert len(exceptions) == 1
    assert isinstance(exceptions[0], RuntimeError)
    assert str(exceptions[0]) == "ldap down"

    broker = Broker(config=_broker_config("127.0.0.1:0", [
        {"tests.plugins.mocks.TestRaisingAuthPlugin": {"error_message": "ldap down"}},
        {"tests.plugins.mocks.TestAuthPlugin": {}},
    ]))
    try:
        session = Session()
        session.client_id = "faulty-plugin-client"
        session.username = "alice"
        session.password = secret
        with caplog.at_level(logging.ERROR, logger=BROKER_LOGGER):
            verdict = await broker._authenticate(session, broker.listeners_config["default"])

        assert verdict is False
        error_text = "\n".join(
            record.getMessage() for record in caplog.records if record.levelno >= logging.ERROR
        )
        assert "TestRaisingAuthPlugin" in error_text
        assert "ldap down" in error_text
        assert "alice" in error_text
        assert secret not in error_text
        assert "password" not in error_text.lower()
    finally:
        await _close_manager(broker)

@pytest.mark.asyncio
async def test_plugin_timeout_is_reported_as_timeout_error() -> None:
    """A hung plugin is cancelled after the timeout and reported as TimeoutError."""
    manager = _manager({"tests.plugins.mocks.TestSlowAuthPlugin": {"delay": 30}})

    results = list((await manager.map_plugin_auth(session=Session(), timeout=0.1)).values())

    assert len(results) == 1
    assert isinstance(results[0], asyncio.TimeoutError)


@pytest.mark.asyncio
async def test_broker_denies_connection_when_plugin_times_out(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Broker-level timeout: a hung plugin denies the connection and logs the plugin name."""
    broker = Broker(config=_broker_config(
        "127.0.0.1:0",
        [{"tests.plugins.mocks.TestSlowAuthPlugin": {"delay": 30}}],
        auth_timeout=0.1,
    ))
    try:
        session = Session()
        session.client_id = "slow-client"
        session.username = "bob"
        with caplog.at_level(logging.ERROR, logger=BROKER_LOGGER):
            verdict = await broker._authenticate(session, broker.listeners_config["default"])

        assert verdict is False
        error_text = "\n".join(
            record.getMessage() for record in caplog.records if record.levelno >= logging.ERROR
        )
        assert "TestSlowAuthPlugin" in error_text
        assert "timed out" in error_text
    finally:
        await _close_manager(broker)


@pytest.mark.asyncio
async def test_authentication_failure_denies_and_logs_without_password(
    unused_tcp_port: int,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """End-to-end: a raising auth plugin yields CONNACK 0x05 and a password-free error log."""
    bind = f"127.0.0.1:{unused_tcp_port}"
    secret = "super-secret-password"
    config = _broker_config(bind, [
        {"tests.plugins.mocks.TestRaisingAuthPlugin": {"error_message": "backend unavailable"}},
    ])

    broker = Broker(config=config)
    client = MQTTClient(config={"auto_reconnect": False})

    await broker.start()
    await asyncio.sleep(0.1)
    try:
        with caplog.at_level(logging.ERROR, logger=BROKER_LOGGER):
            with pytest.raises(ConnectError) as exc:
                await client.connect(f"mqtt://alice:{secret}@{bind}/")

        assert exc.value.return_code == 0x05
        error_text = "\n".join(
            record.getMessage() for record in caplog.records if record.levelno >= logging.ERROR
        )
        assert "TestRaisingAuthPlugin" in error_text
        assert "backend unavailable" in error_text
        assert secret not in error_text
        assert "password" not in error_text.lower()
    finally:
        with suppress(Exception):
            await client.disconnect()
        await broker.shutdown()
        await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_authentication_timeout_denies_connection(
    unused_tcp_port: int,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """End-to-end: a plugin exceeding auth_timeout causes CONNACK 0x05, not a hung connection."""
    bind = f"127.0.0.1:{unused_tcp_port}"
    secret = "another-secret"
    config = _broker_config(
        bind,
        [{"tests.plugins.mocks.TestSlowAuthPlugin": {"delay": 30}}],
        auth_timeout=0.2,
    )

    broker = Broker(config=config)
    client = MQTTClient(config={"auto_reconnect": False})

    await broker.start()
    await asyncio.sleep(0.1)
    try:
        with caplog.at_level(logging.ERROR, logger=BROKER_LOGGER):
            with pytest.raises(ConnectError) as exc:
                await asyncio.wait_for(
                    client.connect(f"mqtt://bob:{secret}@{bind}/"),
                    timeout=5,
                )

        assert exc.value.return_code == 0x05
        error_text = "\n".join(
            record.getMessage() for record in caplog.records if record.levelno >= logging.ERROR
        )
        assert "TestSlowAuthPlugin" in error_text
        assert "timed out" in error_text
        assert secret not in error_text
    finally:
        with suppress(Exception):
            await client.disconnect()
        await broker.shutdown()
        await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_successful_multi_plugin_authentication_allows_connect(
    unused_tcp_port: int,
) -> None:
    """End-to-end: all-True boolean verdicts result in an accepted CONNECT."""
    bind = f"127.0.0.1:{unused_tcp_port}"
    config = _broker_config(bind, [
        {"amqtt.plugins.authentication.AnonymousAuthPlugin": {"allow_anonymous": True}},
        {"tests.plugins.mocks.TestAuthPlugin": {}},
    ])

    broker = Broker(config=config)
    client = MQTTClient(config={"auto_reconnect": False})

    await broker.start()
    await asyncio.sleep(0.1)
    try:
        await client.connect(f"mqtt://{bind}/")
    finally:
        with suppress(Exception):
            await client.disconnect()
        await broker.shutdown()
        await asyncio.sleep(0.1)
