import asyncio
import logging

from dataclasses import dataclass

from amqtt.plugins.base import BasePlugin, BaseAuthPlugin, BaseTopicPlugin
from amqtt.contexts import BaseContext, Action

from amqtt.session import Session

logger = logging.getLogger(__name__)


class TestSimplePlugin(BasePlugin):

    def __init__(self, context: BaseContext):
        super().__init__(context)


class TestConfigPlugin(BasePlugin):

    def __init__(self, context: BaseContext):
        super().__init__(context)

    @dataclass
    class Config:
        option1: int
        option2: str
        option3: int = 20


class TestCoroErrorPlugin(BaseAuthPlugin):

    def authenticate(self, *, session: Session) -> bool | None:
        return True


class TestAuthPlugin(BaseAuthPlugin):

    async def authenticate(self, *, session: Session) -> bool | None:
        return True


class TestNeutralAuthPlugin(BaseAuthPlugin):
    """Auth plugin that abstains (returns None) for every connection."""

    async def authenticate(self, *, session: Session) -> bool | None:
        return None


class TestDenyAuthPlugin(BaseAuthPlugin):
    """Auth plugin that rejects (returns False) for every connection."""

    async def authenticate(self, *, session: Session) -> bool | None:
        return False


class TestRaisingAuthPlugin(BaseAuthPlugin):
    """Auth plugin that always fails at runtime with an exception."""

    @dataclass
    class Config:
        error_message: str = "boom"

    async def authenticate(self, *, session: Session) -> bool | None:
        raise RuntimeError(self.config.error_message)


class TestSlowAuthPlugin(BaseAuthPlugin):
    """Auth plugin whose verdict never arrives within a normal auth timeout."""

    @dataclass
    class Config:
        delay: float = 30.0

    async def authenticate(self, *, session: Session) -> bool | None:
        await asyncio.sleep(self.config.delay)
        return True


class TestMisconfiguredAuthPlugin(BaseAuthPlugin):
    """Auth plugin with a strict Config, used to prove unknown keys fail at startup."""

    @dataclass
    class Config:
        known_option: str = "default"


class TestNoAuthPlugin(BaseAuthPlugin):


    async def authenticate(self, *, session: Session) -> bool | None:
        return False


class TestAllowTopicPlugin(BaseTopicPlugin):

    def __init__(self, context: BaseContext):
        super().__init__(context)

    async def topic_filtering(
        self, *, session: Session | None = None, topic: str | None = None, action: Action | None = None
    ) -> bool:
        return True


class TestBlockTopicPlugin(BaseTopicPlugin):

    def __init__(self, context: BaseContext):
        super().__init__(context)

    async def topic_filtering(
        self, *, session: Session | None = None, topic: str | None = None, action: Action | None = None
    ) -> bool:
        logger.debug("topic filtering plugin is returning false")
        return False
