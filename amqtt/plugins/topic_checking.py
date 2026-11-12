from dataclasses import dataclass
from typing import Any
import warnings

from amqtt.contexts import Action, BaseContext
from amqtt.errors import PluginInitError
from amqtt.plugins.base import BaseTopicPlugin
from amqtt.plugins.topic_matching import (
    ContainmentResult,
    subscriptions_cover_filter,
)
from amqtt.session import Session


class TopicTabooPlugin(BaseTopicPlugin):
    def __init__(self, context: BaseContext) -> None:
        super().__init__(context)
        self._taboo: list[str] = ["prohibited", "top-secret", "data/classified"]

    async def topic_filtering(
        self, *, session: Session | None = None, topic: str | None = None, action: Action | None = None
    ) -> bool | None:
        filter_result = await super().topic_filtering(session=session, topic=topic, action=action)
        if filter_result:
            if session and session.username == "admin":
                return True
            return not (topic and topic in self._taboo)
        return bool(filter_result)


class TopicAccessControlListPlugin(BaseTopicPlugin):
    """Topic authorisation based on per-user allowed topic filters.

    Two subscription modes exist:

    * **Legacy** (the deprecated ``acl`` option, or ``subscribe-acl`` without
      strict mode): the requested filter is compared against each allowed
      filter with :meth:`topic_ac`.  This is a filter-to-filter comparison and
      can wrongly accept a request that contains wildcards, because it never
      proves that *every* topic the request can match is authorised.
    * **Strict** (``strict-subscribe-acl``, or ``subscribe-acl`` together with
      ``strict-subscribe: true``): a subscription is accepted only when every
      concrete topic matched by the requested filter is covered by the union of
      the user's allowed filters.  A rejection carries a concrete *witness*
      topic that the request matches but no allowed rule accepts.
    """

    def __init__(self, context: BaseContext) -> None:
        super().__init__(context)

        if self._get_config_option("acl", None):
            warnings.warn("The 'acl' option is deprecated, please use 'subscribe-acl' instead. "
                          "Support will dropped in future versions.",
                          DeprecationWarning, stacklevel=1)

        if self._get_config_option("acl", None) and self._get_config_option("subscribe-acl", None):
            msg = "'acl' has been replaced with 'subscribe-acl'; only one may be included"
            raise PluginInitError(msg)

        if self._get_config_option("strict-subscribe-acl", None) and self._get_config_option(
            "subscribe-acl", None
        ):
            msg = (
                "'strict-subscribe-acl' is the strict replacement for 'subscribe-acl'; "
                "only one of them may be included"
            )
            raise PluginInitError(msg)

        if self._get_config_option("strict-subscribe-acl", None) and self._get_config_option("acl", None):
            msg = "'strict-subscribe-acl' cannot be combined with the deprecated 'acl'"
            raise PluginInitError(msg)

    @staticmethod
    def topic_ac(topic_requested: str, topic_allowed: str) -> bool:
        """Legacy filter-to-filter comparison (retained for backward compatibility)."""
        req_split = topic_requested.split("/")
        allowed_split = topic_allowed.split("/")
        ret = True
        for i in range(max(len(req_split), len(allowed_split))):
            try:
                a_aux = req_split[i]
                b_aux = allowed_split[i]
            except IndexError:
                ret = False
                break
            if b_aux == "#":
                break
            if b_aux in ("+", a_aux):
                continue
            ret = False
            break
        return ret

    def check_subscription(self, requested_filter: str, allowed_topics: list[str]) -> ContainmentResult:
        """Strictly decide whether a subscription filter is fully authorised.

        Returns a :class:`ContainmentResult` whose ``witness`` is a concrete
        topic matched by ``requested_filter`` but by none of the
        ``allowed_topics`` whenever the request must be rejected.
        """
        return subscriptions_cover_filter(requested_filter, allowed_topics)

    def _strict_subscribe_allowed(
        self, requested_filter: str, allowed_topics: list[str], username: str
    ) -> bool:
        """Run the strict containment check and log any rejection witness."""
        result = self.check_subscription(requested_filter, allowed_topics)
        if not result.allowed:
            self.context.logger.warning(
                "Strict subscribe ACL denied filter %r for user %r; "
                "witness topic %r is matched by the request but by no allowed rule",
                requested_filter,
                username,
                result.witness,
            )
        return result.allowed

    async def topic_filtering(
        self, *, session: Session | None = None, topic: str | None = None, action: Action | None = None
    ) -> bool | None:
        filter_result = await super().topic_filtering(session=session, topic=topic, action=action)
        if not filter_result:
            return False

        # hbmqtt and older amqtt do not support publish filtering
        if action == Action.PUBLISH and not self._get_config_option("publish-acl", {}):
            # maintain backward compatibility, assume permitted
            return True

        req_topic = topic
        if not req_topic:
            return False

        username = session.username if session else None
        if username is None:
            username = "anonymous"

        acl: dict[str, Any] | None = None
        strict_acl: dict[str, Any] | None = None
        match action:
            case Action.PUBLISH:
                acl = self._get_config_option("publish-acl", None)
            case Action.SUBSCRIBE:
                strict_acl = self._get_config_option("strict-subscribe-acl", None)
                # The dataclass exposes an explicit None for an unset field, so
                # fall back to the deprecated 'acl' option when 'subscribe-acl'
                # was not configured.
                subscribe_acl = self._get_config_option("subscribe-acl", None)
                acl = subscribe_acl if subscribe_acl is not None else self._get_config_option("acl", None)
            case Action.RECEIVE:
                acl = self._get_config_option("receive-acl", None)
            case _:
                msg = "Received an invalid action type."
                raise ValueError(msg)

        if action == Action.SUBSCRIBE and strict_acl is not None:
            allowed_topics = strict_acl.get(username, [])
            return self._strict_subscribe_allowed(req_topic, allowed_topics, username)

        # In dataclass configuration an unset mapping is None; in the legacy
        # entrypoint style an absent option is also None.  Both mean "no rule
        # configured", which legacy behaviour treats as permitted.
        if acl is None:
            return True

        allowed_topics = acl.get(username, [])
        if not allowed_topics:
            return False

        if action == Action.SUBSCRIBE and self._get_config_option("strict-subscribe", False):
            return self._strict_subscribe_allowed(req_topic, allowed_topics, username)

        return any(self.topic_ac(req_topic, allowed_topic) for allowed_topic in allowed_topics)

    @dataclass
    class Config:
        """Mappings of username and list of approved topics.

        The mapping options default to ``None`` so that "option absent" is
        distinguishable from "option present with no rules" (an explicit empty
        mapping blocks every non-listed user).
        """

        publish_acl: dict[str, list[str]] | None = None
        acl: dict[str, list[str]] | None = None
        subscribe_acl: dict[str, list[str]] | None = None
        strict_subscribe_acl: dict[str, list[str]] | None = None
        strict_subscribe: bool = False
