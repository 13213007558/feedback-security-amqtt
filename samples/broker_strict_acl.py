import asyncio
import logging

from amqtt.broker import Broker

"""
Run `samples/client_subscribe_acl.py` against this broker to observe strict
subscription authorisation. Unlike the legacy `acl` mode, a SUBSCRIBE filter is
accepted only when every topic it can match is covered by the user's allowed
rules; a rejection is logged together with a concrete witness topic.
"""

logger = logging.getLogger(__name__)

config = {
    "listeners": {
        "default": {
            "type": "tcp",
            "bind": "0.0.0.0:1883",
        },
    },
    "plugins": {
        "amqtt.plugins.authentication.AnonymousAuthPlugin": {"allow_anonymous": True},
        "amqtt.plugins.sys.broker.BrokerSysPlugin": {"sys_interval": 10},
        "amqtt.plugins.topic_checking.TopicAccessControlListPlugin": {
            # Strict mode: each filter must be fully contained in the union of
            # the user's allowed filters.
            "strict_subscribe_acl": {
                # username: [list of allowed topic filters]
                "test": [
                    "repositories/+/master",
                    "calendar/#",
                    "data/memes",
                ],
                # an explicit empty list blocks every subscription
                "anonymous": [],
            },
        },
    },
}


async def main_loop():
    broker = Broker(config)
    try:
        await broker.start()
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        await broker.shutdown()


async def main() -> None:
    task = asyncio.create_task(main_loop())
    try:
        await task
    except asyncio.CancelledError:
        pass


def __main__():
    formatter = "[%(asctime)s] :: %(levelname)s :: %(name)s :: %(message)s"
    logging.basicConfig(level=logging.INFO, format=formatter)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(main())
    try:
        loop.run_until_complete(task)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Stopping server...")
        task.cancel()
        loop.run_until_complete(task)
    finally:
        loop.close()


if __name__ == "__main__":
    __main__()
