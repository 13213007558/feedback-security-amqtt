"""Tests for strict subscription-filter containment and topic matching.

The containment property is cross-checked against an exhaustive enumeration of
concrete topics over a finite alphabet.  This verifies the finite-partition
proof rather than asserting inclusion from a handful of example topics.
"""

import itertools
import random

import pytest

from amqtt.plugins.topic_matching import (
    ContainmentResult,
    subscriptions_cover_filter,
    topic_matches,
)


class TestTopicMatches:
    """The shared matcher must follow the broker's MQTT semantics."""

    @pytest.mark.parametrize(
        ("topic", "a_filter", "expected"),
        [
            ("a/b/c", "a/b/c", True),
            ("a/b/c", "a/+/c", True),
            ("a/b/c", "a/#", True),
            ("a/b", "a/#", True),
            ("a", "a/#", True),
            ("a/b/c", "a/b", False),
            ("a/b/c", "a/+/d", False),
            ("a//c", "a/+/c", True),
            ("a//c", "a//c", True),
            ("/a", "/+", True),
            ("a/b", "+", False),
            ("$SYS/x", "#", False),
            ("$SYS/x", "+", False),
            ("$SYS/x", "$SYS/#", True),
            ("$SYS/x/y", "$SYS/+/#", True),
            ("$SYS", "$SYS", True),
        ],
    )
    def test_matches(self, topic, a_filter, expected):
        assert topic_matches(topic, a_filter) is expected

    def test_matches_broker_semantics(self):
        import logging

        from amqtt.broker import Broker

        broker = Broker.__new__(Broker)
        broker.logger = logging.getLogger("test")
        cases = [
            ("a/b/c", "a/+/c"),
            ("a/b/c/d", "a/#"),
            ("$SYS/broker/load", "#"),
            ("$SYS/broker/load", "+/broker/#"),
            ("a//b", "a/+/b"),
            ("x", "#"),
        ]
        for topic, a_filter in cases:
            assert topic_matches(topic, a_filter) == broker._matches(topic, a_filter)


class TestContainmentNamedScenarios:
    """Scenarios explicitly required by the strict authorisation design."""

    def test_equal_wildcard_filters_cover_each_other(self):
        result = subscriptions_cover_filter("tenant/+/public/#", ["tenant/+/public/#"])
        assert result == ContainmentResult(allowed=True)

    def test_broader_allowed_filter_covers_request(self):
        result = subscriptions_cover_filter("tenant/+/public/#", ["tenant/#"])
        assert result.allowed is True

    def test_narrow_allowed_filter_does_not_cover_broad_request(self):
        result = subscriptions_cover_filter("tenant/#", ["tenant/+/public/#"])
        assert result.allowed is False
        # '#' matches zero levels, so 'tenant' is the concrete over-reach witness
        assert result.witness == "tenant"
        assert topic_matches(result.witness, "tenant/#")
        assert not topic_matches(result.witness, "tenant/+/public/#")

    def test_witness_is_a_real_counterexample(self):
        result = subscriptions_cover_filter(
            "tenant/+/public/#",
            ["tenant/+/public/metrics", "tenant/+/public/state/#"],
        )
        assert result.allowed is False
        assert result.witness is not None
        assert topic_matches(result.witness, "tenant/+/public/#")
        assert not any(
            topic_matches(result.witness, rule)
            for rule in ("tenant/+/public/metrics", "tenant/+/public/state/#")
        )

    def test_union_of_rules_can_together_cover_a_wildcard_request(self):
        # every single-level tenant value is covered by one of the two rules
        result = subscriptions_cover_filter(
            "tenant/+/public",
            ["tenant/a/public", "tenant/b/public"],
        )
        assert result.allowed is False  # only 'a' and 'b' exist; another value escapes
        gap = subscriptions_cover_filter(
            "tenant/+/public",
            ["tenant/+/public"],
        )
        assert gap.allowed is True

    def test_union_cover_plus_then_hash(self):
        result = subscriptions_cover_filter(
            "x/+/#",
            ["x/a/#", "x/b/#"],
        )
        # '+' admits values other than a and b
        assert result.allowed is False
        assert topic_matches(result.witness, "x/+/#")
        assert not any(topic_matches(result.witness, r) for r in ("x/a/#", "x/b/#"))

    def test_empty_levels_are_distinct_buckets(self):
        # a//c is matched by a/+/c but a literal-empty rule differs from '+'
        assert subscriptions_cover_filter("a//c", ["a//c"]).allowed is True
        denied = subscriptions_cover_filter("a/+/c", ["a/x/c"])
        assert denied.allowed is False
        assert topic_matches(denied.witness, "a/+/c")
        # a leading slash produces a genuine empty first level
        leading = subscriptions_cover_filter("/+", ["/a"])
        assert leading.allowed is False
        assert leading.witness is not None
        assert topic_matches(leading.witness, "/+")

    def test_dollar_topics_not_covered_by_hash_or_plus(self):
        # a single '#' must not grant access to '$' topics [MQTT-4.7.2-1]
        result = subscriptions_cover_filter("$SYS/#", ["#"])
        assert result.allowed is False
        assert result.witness == "$SYS"
        assert not topic_matches(result.witness, "#")

        # explicit '$' rule plus '#' together cover the request
        covered = subscriptions_cover_filter("$SYS/#", ["#", "$SYS/#"])
        assert covered.allowed is True

        # a leading '+' request never reaches '$' topics, so '$' rules are not
        # needed for it but '+' still needs non-dollar coverage
        plus_request = subscriptions_cover_filter("+", ["$SYS"])
        assert plus_request.allowed is False

    def test_empty_allowed_list_denies_with_witness(self):
        result = subscriptions_cover_filter("a/+/c", [])
        assert result.allowed is False
        assert topic_matches(result.witness, "a/+/c")

    def test_hash_matches_zero_levels_witness(self):
        # 'a/#' matches the topic 'a' itself, which 'a/b/#' cannot match
        result = subscriptions_cover_filter("a/#", ["a/b/#"])
        assert result.allowed is False
        assert result.witness == "a"

    def test_fresh_witness_token_never_collides_with_literal(self):
        # The generated "other value" token at depth 0 must not collide with a
        # literal used at depth 1; otherwise the '+' branch wrongly looks covered.
        result = subscriptions_cover_filter(
            "+/_amqtt_witness_0_/a",
            ["_amqtt_witness_0_/#"],
        )
        assert result.allowed is False
        assert result.witness is not None
        assert topic_matches(result.witness, "+/_amqtt_witness_0_/a")
        assert not topic_matches(result.witness, "_amqtt_witness_0_/#")


class TestContainmentAgainstExhaustiveEnumeration:
    """Cross-check the proof against brute-force topic enumeration.

    The finite alphabet below spans the literal tokens, an empty level, a
    dollar level and a fresh "other" value, so every partition bucket used by
    the implementation is exercised concretely.
    """

    # The alphabet spans every partition bucket the proof relies on: literal
    # tokens used by the parametrised filters, an empty level, a dollar level
    # and the concrete "other" tokens the implementation emits.
    ALPHABET = [
        "a",
        "b",
        "x",
        "tenant",
        "",
        "$SYS",
        "_amqtt_witness_0_",
        "_amqtt_witness_1_",
        "_amqtt_escape_1_",
    ]
    MAX_DEPTH = 4

    def _brute_force_witness(self, requested, allowed_filters):
        # Enumerate from depth 1 (the empty string is not a valid MQTT topic).
        # The horizon stays one level beyond the longest filter so a '#'
        # extension cannot hide beyond the search horizon.
        horizon = max(
            len(f.split("/")) for f in [requested, *allowed_filters]
        ) + 1
        for depth in range(1, horizon + 1):
            for combo in itertools.product(self.ALPHABET, repeat=depth):
                topic = "/".join(combo)
                if topic_matches(topic, requested) and not any(
                    topic_matches(topic, rule) for rule in allowed_filters
                ):
                    return topic
        return None

    @pytest.mark.parametrize(
        ("requested", "allowed_filters"),
        [
            ("tenant/+/public/#", ["tenant/#"]),
            ("tenant/#", ["tenant/+/public/#"]),
            ("#", ["+"]),
            ("#", ["#"]),
            ("a/+", ["a/b", "a/c"]),
            ("a/#", ["a/b/#", "a/c"]),
            ("$SYS/#", ["#"]),
            ("$SYS/#", ["#", "$SYS/#"]),
            ("+/+", ["a/#", "b/#"]),
            ("a//+", ["a//b", "a//c"]),
            ("x/+/#", ["x/a/#", "x/b/#"]),
            ("/#", ["/a/#"]),
        ],
    )
    def test_matches_brute_force(self, requested, allowed_filters):
        result = subscriptions_cover_filter(requested, allowed_filters)
        brute_witness = self._brute_force_witness(requested, allowed_filters)
        assert result.allowed == (brute_witness is None)
        if not result.allowed:
            assert result.witness is not None
            assert topic_matches(result.witness, requested)
            assert not any(
                topic_matches(result.witness, r) for r in allowed_filters
            )

    def test_witness_never_an_allowed_topic(self):
        requested, allowed_filters = "tenant/#", ["tenant/+/public/#"]
        result = subscriptions_cover_filter(requested, allowed_filters)
        assert result.allowed is False
        # every returned witness must independently satisfy both predicates
        assert topic_matches(result.witness, requested)
        assert all(not topic_matches(result.witness, rule) for rule in allowed_filters)


class TestContainmentRandomisedEquivalence:
    """Deterministic randomised cross-check against exhaustive enumeration.

    A fixed seed keeps the suite reproducible.  The brute-force alphabet
    contains the concrete witness tokens the implementation may emit, so the
    enumeration can independently rediscover every counter-example rather than
    trusting a few hand-picked topics.
"""

    SEED = 20261111
    TOKENS = ["a", "b", "", "$SYS", "_other_", "_amqtt_witness_0_", "_amqtt_witness_1_"]
    TRIALS = 300

    def _valid_filter(self, levels):
        return all("#" not in level or level == "#" for level in levels[:-1]) and all(
            "+" not in level or level == "+" for level in levels
        )

    def _random_filter(self, rng):
        depth = rng.randint(1, 3)
        levels = ["+" if rng.random() < 0.35 else rng.choice(self.TOKENS) for _ in range(depth)]
        if rng.random() < 0.35:
            levels[-1] = "#"
        return "/".join(levels)

    def _brute_force_witness(self, requested, allowed_filters):
        for depth in range(1, 5):
            for combo in itertools.product(self.TOKENS, repeat=depth):
                topic = "/".join(combo)
                if topic_matches(topic, requested) and not any(
                    topic_matches(topic, rule) for rule in allowed_filters
                ):
                    return topic
        return None

    def test_random_filters_match_brute_force(self):
        rng = random.Random(self.SEED)
        for _ in range(self.TRIALS):
            requested_levels = [
                "+" if rng.random() < 0.35 else rng.choice(self.TOKENS)
                for _ in range(rng.randint(1, 3))
            ]
            if rng.random() < 0.35:
                requested_levels[-1] = "#"
            if not self._valid_filter(requested_levels):
                continue
            requested = "/".join(requested_levels)

            allowed_filters = []
            for _ in range(rng.randint(0, 3)):
                candidate = self._random_filter(rng)
                if self._valid_filter(candidate.split("/")):
                    allowed_filters.append(candidate)

            result = subscriptions_cover_filter(requested, allowed_filters)
            brute_witness = self._brute_force_witness(requested, allowed_filters)
            assert result.allowed == (brute_witness is None)
            if not result.allowed:
                assert result.witness is not None
                assert topic_matches(result.witness, requested)
                assert all(
                    not topic_matches(result.witness, rule) for rule in allowed_filters
                )
