import logging

import pytest

from amqtt.broker import BrokerContext, Broker

from amqtt.contexts import BaseContext, Action
from amqtt.errors import PluginInitError
from amqtt.plugins.topic_checking import TopicAccessControlListPlugin, TopicTabooPlugin
from amqtt.plugins.base import BaseTopicPlugin
from amqtt.session import Session

logger = logging.getLogger(__name__)


def _testlog_records(caplog):
    return [record for record in caplog.records if record.name == "testlog"]


# Base plug-in object


@pytest.mark.asyncio
async def test_base_no_config(caplog):
    """Check BaseTopicPlugin returns false if no topic-check is present."""
    with caplog.at_level(logging.DEBUG, logger="testlog"):
        context = BaseContext()
        context.logger = logging.getLogger("testlog")
        context.config = {}

        plugin = BaseTopicPlugin(context)
        authorised = await plugin.topic_filtering()
        assert authorised is False

    # Warning messages are only generated if using deprecated plugin configuration on initial load
    log_records = _testlog_records(caplog)
    assert len(log_records) == 1
    assert log_records[0].levelno == logging.WARNING
    assert log_records[0].message == "'topic-check' section not found in context configuration"


@pytest.mark.asyncio
async def test_base_empty_config(caplog):
    """Check BaseTopicPlugin returns false if topic-check is empty."""
    with caplog.at_level(logging.DEBUG, logger="testlog"):
        broker = Broker()
        context = BrokerContext(broker)
        context.logger = logging.getLogger("testlog")
        context.config = {"topic-check": {}}

        plugin = BaseTopicPlugin(context)
        authorised = await plugin.topic_filtering()
        assert authorised is False

    # Warning messages are only generated if using deprecated plugin configuration on initial load
    log_records = _testlog_records(caplog)
    assert len(log_records) == 1
    assert log_records[0].levelno == logging.WARNING
    assert log_records[0].message == "'topic-check' section not found in context configuration"


@pytest.mark.asyncio
async def test_base_disabled_config(caplog):
    """Check BaseTopicPlugin returns true if disabled. (it doesn't actually check)."""
    with caplog.at_level(logging.DEBUG, logger="testlog"):
        context = BaseContext()
        context.logger = logging.getLogger("testlog")
        context.config = {"topic-check": {"enabled": False}}

        plugin = BaseTopicPlugin(context)
        authorised = await plugin.topic_filtering()
        assert authorised is True

    # Should NOT have printed warnings
    assert len(_testlog_records(caplog)) == 0


@pytest.mark.asyncio
async def test_base_enabled_config(caplog):
    """Check BaseTopicPlugin returns true if enabled."""
    with caplog.at_level(logging.DEBUG, logger="testlog"):
        context = BaseContext()
        context.logger = logging.getLogger("testlog")
        context.config = {"topic-check": {"enabled": True}}

        plugin = BaseTopicPlugin(context)
        authorised = await plugin.topic_filtering()
        assert authorised is True

    # Should NOT have printed warnings
    assert len(_testlog_records(caplog)) == 0


# Taboo plug-in


@pytest.mark.asyncio
async def test_taboo_empty_config(caplog):
    """Check TopicTabooPlugin returns false if topic-check absent."""
    with caplog.at_level(logging.DEBUG, logger="testlog"):
        context = BaseContext()
        context.logger = logging.getLogger("testlog")
        context.config = {}

        plugin = TopicTabooPlugin(context)
        assert (await plugin.topic_filtering()) is False

    # Warning messages are only generated if using deprecated plugin configuration on initial load
    log_records = _testlog_records(caplog)
    assert len(log_records) == 1
    assert log_records[0].levelno == logging.WARNING
    assert log_records[0].message == "'topic-check' section not found in context configuration"


@pytest.mark.asyncio
async def test_taboo_disabled(caplog):
    """Check TopicTabooPlugin returns true if checking disabled."""
    with caplog.at_level(logging.DEBUG, logger="testlog"):
        context = BaseContext()
        context.logger = logging.getLogger("testlog")
        context.config = {"topic-check": {"enabled": False}}

        session = Session()
        session.username = "anybody"

        plugin = TopicTabooPlugin(context)
        assert (await plugin.topic_filtering(session=session, topic="not/prohibited")) is True

    # Should NOT have printed warnings
    assert len(_testlog_records(caplog)) == 0


@pytest.mark.parametrize("test_config", [
    ({"topic-check": {"enabled": True}}),
    (TopicTabooPlugin.Config())
])
@pytest.mark.asyncio
async def test_taboo_not_taboo_topic(caplog, test_config):
    """Check TopicTabooPlugin returns true if topic not taboo."""
    with caplog.at_level(logging.DEBUG, logger="testlog"):
        context = BaseContext()
        context.logger = logging.getLogger("testlog")
        context.config = test_config

        session = Session()
        session.username = "anybody"

        plugin = TopicTabooPlugin(context)
        assert (await plugin.topic_filtering(session=session, topic="not/prohibited")) is True

    # Should NOT have printed warnings
    assert len(_testlog_records(caplog)) == 0


@pytest.mark.parametrize("test_config", [
    ({"topic-check": {"enabled": True}}),
    (TopicTabooPlugin.Config())
])
@pytest.mark.asyncio
async def test_taboo_anon_taboo_topic(caplog, test_config):
    """Check TopicTabooPlugin returns false if topic is taboo and session is anonymous."""
    with caplog.at_level(logging.DEBUG, logger="testlog"):
        context = BaseContext()
        context.logger = logging.getLogger("testlog")
        context.config = test_config

        session = Session()
        session.username = ""

        plugin = TopicTabooPlugin(context)
        assert (await plugin.topic_filtering(session=session, topic="prohibited")) is False

    # Should NOT have printed warnings
    assert len(_testlog_records(caplog)) == 0


@pytest.mark.parametrize("test_config", [
    ({"topic-check": {"enabled": True}}),
    (TopicTabooPlugin.Config())
])
@pytest.mark.asyncio
async def test_taboo_notadmin_taboo_topic(caplog, test_config):
    """Check TopicTabooPlugin returns false if topic is taboo and user is not "admin"."""
    with caplog.at_level(logging.DEBUG, logger="testlog"):
        context = BaseContext()
        context.logger = logging.getLogger("testlog")
        context.config = test_config

        session = Session()
        session.username = "notadmin"

        plugin = TopicTabooPlugin(context)
        assert (await plugin.topic_filtering(session=session, topic="prohibited")) is False

    # Should NOT have printed warnings
    assert len(_testlog_records(caplog)) == 0

@pytest.mark.parametrize("test_config", [
    ({"topic-check": {"enabled": True}}),
    (TopicTabooPlugin.Config())
])
@pytest.mark.asyncio
async def test_taboo_admin_taboo_topic(caplog, test_config):
    """Check TopicTabooPlugin returns true if topic is taboo and user is "admin"."""
    with caplog.at_level(logging.DEBUG, logger="testlog"):
        context = BaseContext()
        context.logger = logging.getLogger("testlog")
        context.config = test_config

        session = Session()
        session.username = "admin"

        plugin = TopicTabooPlugin(context)
        assert (await plugin.topic_filtering(session=session, topic="prohibited")) is True

    # Should NOT have printed warnings
    assert len(_testlog_records(caplog)) == 0


# TopicAccessControlListPlugin tests


def test_topic_ac_not_match():
    """Test TopicAccessControlListPlugin.topic_ac returns false if topics do not match."""
    assert TopicAccessControlListPlugin.topic_ac("a/topic/to/match", "a/topic/to/notmatch") is False


def test_topic_ac_not_match_longer_acl():
    """Test TopicAccessControlListPlugin.topic_ac returns false if topics do not match and ACL topic is longer."""
    assert TopicAccessControlListPlugin.topic_ac("topic", "topic/is/longer") is False


def test_topic_ac_not_match_longer_rq():
    """Test TopicAccessControlListPlugin.topic_ac returns false if topics do not match and RQ topic is longer."""
    assert TopicAccessControlListPlugin.topic_ac("topic/is/longer", "topic") is False


def test_topic_ac_match_exact():
    """Test TopicAccessControlListPlugin.topic_ac returns true if topics match exactly."""
    assert TopicAccessControlListPlugin.topic_ac("exact/topic", "exact/topic") is True


def test_topic_ac_match_plus():
    """Test TopicAccessControlListPlugin.topic_ac correctly handles '+' wildcard."""
    assert (
        TopicAccessControlListPlugin.topic_ac(
            "a/topic/anything/value",
            "a/topic/+/value",
        )
        is True
    )


def test_topic_ac_match_hash():
    """Test TopicAccessControlListPlugin.topic_ac correctly handles '#' wildcard."""
    assert (
        TopicAccessControlListPlugin.topic_ac(
            "topic/prefix/and/suffix",
            "topic/prefix/#",
        )
        is True
    )


@pytest.mark.asyncio
async def test_taclp_empty_config(caplog):
    """Check TopicAccessControlListPlugin returns false if topic-check absent."""
    with caplog.at_level(logging.DEBUG, logger="testlog"):
        context = BaseContext()
        context.logger = logging.getLogger("testlog")
        context.config = {}

        plugin = TopicAccessControlListPlugin(context)
        assert (await plugin.topic_filtering()) is False

    # Warning messages are only generated if using deprecated plugin configuration on initial load
    log_records = _testlog_records(caplog)
    assert len(log_records) == 1
    assert log_records[0].levelno == logging.WARNING
    assert log_records[0].message == "'topic-check' section not found in context configuration"


@pytest.mark.asyncio
async def test_taclp_true_disabled():
    """Check TopicAccessControlListPlugin returns true if topic checking is disabled."""
    context = BaseContext()
    context.logger = logging.getLogger("testlog")
    context.config = {"topic-check": {"enabled": False}}

    session = Session()
    session.username = "user"

    plugin = TopicAccessControlListPlugin(context)
    authorised = await plugin.topic_filtering(
        action=Action.PUBLISH,
        session=session,
        topic="a/topic",
    )
    assert authorised is True


@pytest.mark.parametrize("test_config", [
    ({"topic-check": {"enabled": True}}),
    (TopicAccessControlListPlugin.Config())
])
@pytest.mark.asyncio
async def test_taclp_true_no_pub_acl(test_config):
    """Check TopicAccessControlListPlugin returns true if action=publish and no publish-acl given.

    (This is for backward-compatibility with existing installations.).
    """
    context = BaseContext()
    context.logger = logging.getLogger("testlog")
    context.config = test_config

    session = Session()
    session.username = "user"

    plugin = TopicAccessControlListPlugin(context)
    authorised = await plugin.topic_filtering(
        action=Action.PUBLISH,
        session=session,
        topic="a/topic",
    )
    assert authorised is True


@pytest.mark.parametrize("test_config", [
    ({
        "topic-check": {
            "enabled": True,
            "acl": {"anotheruser": ["allowed/topic", "another/allowed/topic/#"]},
        },
    }),
    (TopicAccessControlListPlugin.Config(
        acl={"anotheruser": ["allowed/topic", "another/allowed/topic/#"]}
    ))
])
@pytest.mark.asyncio
async def test_taclp_false_sub_no_topic(test_config):
    """Check TopicAccessControlListPlugin returns false user there is no topic."""
    context = BaseContext()
    context.logger = logging.getLogger("testlog")
    context.config = test_config

    session = Session()
    session.username = "user"

    plugin = TopicAccessControlListPlugin(context)
    authorised = await plugin.topic_filtering(
        action=Action.SUBSCRIBE,
        session=session,
        topic="",
    )
    assert authorised is False


@pytest.mark.parametrize("test_config", [
    ({
        "topic-check": {
            "enabled": True,
            "acl": {"anotheruser": ["allowed/topic", "another/allowed/topic/#"]},
        },
    }),
    (TopicAccessControlListPlugin.Config(
        acl={"anotheruser": ["allowed/topic", "another/allowed/topic/#"]}
    ))
])
@pytest.mark.asyncio
async def test_taclp_false_sub_unknown_user(test_config):
    """Check TopicAccessControlListPlugin returns false user is not listed in ACL."""
    context = BaseContext()
    context.logger = logging.getLogger("testlog")
    context.config = test_config

    session = Session()
    session.username = "user"

    plugin = TopicAccessControlListPlugin(context)
    authorised = await plugin.topic_filtering(
        action=Action.SUBSCRIBE,
        session=session,
        topic="allowed/topic",
    )
    assert authorised is False


@pytest.mark.parametrize("test_config", [
    ({
        "topic-check": {
            "enabled": True,
            "acl": {"user": ["allowed/topic", "another/allowed/topic/#"]},
        },
    }),
    (TopicAccessControlListPlugin.Config(
        acl={"user": ["allowed/topic", "another/allowed/topic/#"]}
    ))
])
@pytest.mark.asyncio
async def test_taclp_false_sub_no_permission(test_config):
    """Check TopicAccessControlListPlugin returns false if "acl" does not list allowed topic."""
    context = BaseContext()
    context.logger = logging.getLogger("testlog")
    context.config = test_config

    session = Session()
    session.username = "user"

    plugin = TopicAccessControlListPlugin(context)
    authorised = await plugin.topic_filtering(
        action=Action.SUBSCRIBE,
        session=session,
        topic="forbidden/topic",
    )
    assert authorised is False

@pytest.mark.parametrize("test_config", [
    ({
        "topic-check": {
            "enabled": True,
            "acl": {"user": ["allowed/topic", "another/allowed/topic/#"]},
        },
    }),
    (TopicAccessControlListPlugin.Config(
        acl={"user": ["allowed/topic", "another/allowed/topic/#"]}
    ))
])
@pytest.mark.asyncio
async def test_taclp_true_sub_permission(test_config):
    """Check TopicAccessControlListPlugin returns true if "acl" lists allowed topic."""
    context = BaseContext()
    context.logger = logging.getLogger("testlog")
    context.config = test_config

    session = Session()
    session.username = "user"

    plugin = TopicAccessControlListPlugin(context)
    authorised = await plugin.topic_filtering(
        action=Action.SUBSCRIBE,
        session=session,
        topic="allowed/topic",
    )
    assert authorised is True


@pytest.mark.parametrize("test_config", [
    ({
        "topic-check": {
            "enabled": True,
            "publish-acl": {"user": ["allowed/topic", "another/allowed/topic/#"]},
        },
    }),
    (TopicAccessControlListPlugin.Config(
        publish_acl={"user": ["allowed/topic", "another/allowed/topic/#"]}
    ))
])
@pytest.mark.asyncio
async def test_taclp_true_pub_permission(test_config):
    """Check TopicAccessControlListPlugin returns true if "publish-acl" lists allowed topic for publish action."""
    context = BaseContext()
    context.logger = logging.getLogger("testlog")
    context.config = test_config

    session = Session()
    session.username = "user"

    plugin = TopicAccessControlListPlugin(context)
    authorised = await plugin.topic_filtering(
        action=Action.PUBLISH,
        session=session,
        topic="allowed/topic",
    )
    assert authorised is True


@pytest.mark.parametrize("test_config", [
    ({
        "topic-check": {
            "enabled": True,
            "acl": {"anonymous": ["allowed/topic", "another/allowed/topic/#"]},
        },
    }),
    (TopicAccessControlListPlugin.Config(
        acl={"anonymous": ["allowed/topic", "another/allowed/topic/#"]}
    ))
])
@pytest.mark.asyncio
async def test_taclp_true_anon_sub_permission(test_config):
    """Check TopicAccessControlListPlugin handles anonymous users."""
    context = BaseContext()
    context.logger = logging.getLogger("testlog")
    context.config = test_config

    session = Session()
    session.username = None

    plugin = TopicAccessControlListPlugin(context)
    authorised = await plugin.topic_filtering(
        action=Action.SUBSCRIBE,
        session=session,
        topic="allowed/topic",
    )
    assert authorised is True


# Strict subscribe ACL tests


STRICT_DATACLASS_CONFIG = TopicAccessControlListPlugin.Config(
    strict_subscribe_acl={"user": ["tenant/+/public/#", "tenant/#"]}
)
STRICT_ENTRYPOINT_CONFIG = {
    "topic-check": {
        "enabled": True,
        "strict-subscribe-acl": {"user": ["tenant/+/public/#", "tenant/#"]},
    }
}


def _strict_plugin(test_config):
    context = BaseContext()
    context.logger = logging.getLogger("testlog")
    context.config = test_config
    session = Session()
    session.username = "user"
    return TopicAccessControlListPlugin(context), session


def test_strict_check_returns_witness():
    """The strict check exposes a concrete over-reach witness."""
    plugin, _ = _strict_plugin(STRICT_DATACLASS_CONFIG)
    result = plugin.check_subscription("tenant/#", ["tenant/+/public/#"])
    assert result.allowed is False
    assert result.witness == "tenant"


@pytest.mark.parametrize("test_config", [STRICT_DATACLASS_CONFIG, STRICT_ENTRYPOINT_CONFIG])
@pytest.mark.asyncio
async def test_strict_allows_fully_covered_filter(test_config):
    """A request whose every topic is authorised is accepted."""
    plugin, session = _strict_plugin(test_config)
    authorised = await plugin.topic_filtering(
        action=Action.SUBSCRIBE,
        session=session,
        topic="tenant/+/public/#",
    )
    assert authorised is True


@pytest.mark.parametrize("test_config", [STRICT_DATACLASS_CONFIG, STRICT_ENTRYPOINT_CONFIG])
@pytest.mark.asyncio
async def test_strict_denies_broad_filter_with_witness_log(test_config, caplog):
    """A broader request than any allowed rule is denied and logged with a witness."""
    plugin, session = _strict_plugin(
        TopicAccessControlListPlugin.Config(strict_subscribe_acl={"user": ["tenant/+/public/#"]})
        if isinstance(test_config, TopicAccessControlListPlugin.Config)
        else {"topic-check": {"enabled": True,
                              "strict-subscribe-acl": {"user": ["tenant/+/public/#"]}}}
    )
    with caplog.at_level(logging.WARNING, logger="testlog"):
        authorised = await plugin.topic_filtering(
            action=Action.SUBSCRIBE,
            session=session,
            topic="tenant/#",
        )
    assert authorised is False
    assert any("witness topic 'tenant'" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_strict_union_of_rules_covers_wildcard():
    """Multiple allowed rules jointly cover a request only via real containment."""
    config = TopicAccessControlListPlugin.Config(
        strict_subscribe_acl={"user": ["a/b", "a/c"]}
    )
    plugin, session = _strict_plugin(config)
    # 'a/+' can match values beyond b and c, so the union does not cover it
    denied = await plugin.topic_filtering(
        action=Action.SUBSCRIBE, session=session, topic="a/+"
    )
    assert denied is False

    config_cover = TopicAccessControlListPlugin.Config(
        strict_subscribe_acl={"user": ["a/+"]}
    )
    plugin_cover, session_cover = _strict_plugin(config_cover)
    allowed = await plugin_cover.topic_filtering(
        action=Action.SUBSCRIBE, session=session_cover, topic="a/+"
    )
    assert allowed is True


@pytest.mark.asyncio
async def test_strict_dollar_topic_requires_explicit_rule():
    """A leading '#' allowed rule never grants '$' topics in strict mode."""
    config = TopicAccessControlListPlugin.Config(
        strict_subscribe_acl={"user": ["#"]}
    )
    plugin, session = _strict_plugin(config)
    denied = await plugin.topic_filtering(
        action=Action.SUBSCRIBE, session=session, topic="$SYS/#"
    )
    assert denied is False

    config_explicit = TopicAccessControlListPlugin.Config(
        strict_subscribe_acl={"user": ["#", "$SYS/#"]}
    )
    plugin_explicit, session_explicit = _strict_plugin(config_explicit)
    allowed = await plugin_explicit.topic_filtering(
        action=Action.SUBSCRIBE, session=session_explicit, topic="$SYS/#"
    )
    assert allowed is True


@pytest.mark.asyncio
async def test_strict_flag_upgrades_subscribe_acl():
    """strict-subscribe: true applies containment checks to subscribe-acl rules."""
    config = TopicAccessControlListPlugin.Config(
        subscribe_acl={"user": ["tenant/+/public/#"]},
        strict_subscribe=True,
    )
    plugin, session = _strict_plugin(config)
    denied = await plugin.topic_filtering(
        action=Action.SUBSCRIBE, session=session, topic="tenant/#"
    )
    assert denied is False


@pytest.mark.asyncio
async def test_legacy_acl_keeps_legacy_semantics():
    """The deprecated 'acl' option retains the original topic_ac comparison."""
    config = TopicAccessControlListPlugin.Config(
        acl={"user": ["tenant/+/public/#"]}
    )
    plugin, session = _strict_plugin(config)
    # Legacy filter-to-filter behaviour: a request literally matching the
    # allowed filter is accepted even though it is itself a wildcard filter.
    authorised = await plugin.topic_filtering(
        action=Action.SUBSCRIBE, session=session, topic="tenant/+/public/#"
    )
    assert authorised is True


def test_strict_and_legacy_options_are_mutually_exclusive():
    """Configuring both strict and legacy subscription ACL fails fast."""
    context = BaseContext()
    context.logger = logging.getLogger("testlog")
    context.config = TopicAccessControlListPlugin.Config(
        subscribe_acl={"user": ["a"]},
        strict_subscribe_acl={"user": ["b"]},
    )
    with pytest.raises(PluginInitError):
        TopicAccessControlListPlugin(context)
