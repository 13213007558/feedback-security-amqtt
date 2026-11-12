"""In-memory topic authorization plugin used by the will-authorization tests.

Kept in a stable, importable module (rather than defined inside the test module) so the
plugin class object loaded by the broker's PluginManager is the same one whose ``policy``
the tests mutate, regardless of pytest's test-module import mode.
"""

from amqtt.contexts import Action
from amqtt.plugins.base import BaseTopicPlugin
from amqtt.session import Session


class DynamicTopicAclPlugin(BaseTopicPlugin):
    """Topic plugin whose per-action policy can be mutated at runtime."""

    policy: dict[str, set[str]] = {"publish": set(), "subscribe": set(), "receive": set()}

    async def topic_filtering(
        self, *, session: Session | None = None, topic: str | None = None, action: Action | None = None
    ) -> bool | None:
        if not topic or action is None:
            return False
        return topic in self.policy[action.value]

    @classmethod
    def reset(cls, allowed_topics: set[str]) -> None:
        for topics in cls.policy.values():
            topics.clear()
        for action in cls.policy:
            cls.policy[action].update(allowed_topics)
