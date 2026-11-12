"""Strict MQTT topic matching and subscription-filter containment.

The helpers in this module implement the MQTT 3.1.1 topic semantics used by the
broker (see :meth:`amqtt.broker.Broker._matches`):

* ``+`` matches exactly one topic level;
* ``#`` matches the remainder of a topic (zero or more levels) and may only be
  the final level of a filter;
* a filter whose first level is ``+`` or ``#`` never matches a topic that starts
  with ``$`` (``[MQTT-4.7.2-1]``).

The :func:`subscriptions_cover_filter` function answers the strict
authorisation question: does *every* concrete topic that a requested filter can
match belong to the union of the allowed filters?  The proof does not sample a
few hand-picked example topics.  At every depth the infinite alphabet of
possible level values is partitioned into a finite set of buckets (the literal
tokens of the allowed filters at that depth, plus one bucket for every other
value), and each bucket is explored through the filter trie.  A concrete
counter-example topic is returned as soon as a bucket escapes every allowed
filter.
"""

from dataclasses import dataclass

WILDCARD_SINGLE = "+"
WILDCARD_MULTI = "#"


def topic_matches(topic: str, a_filter: str) -> bool:
    """Return whether a concrete ``topic`` is matched by the filter ``a_filter``.

    Mirrors :meth:`amqtt.broker.Broker._matches`, including the rule that
    ``$``-prefixed topics are never matched by filters beginning with ``+`` or
    ``#``.
    """
    if topic.startswith("$") and a_filter.startswith((WILDCARD_SINGLE, WILDCARD_MULTI)):
        return False

    if WILDCARD_MULTI not in a_filter and WILDCARD_SINGLE not in a_filter:
        return a_filter == topic

    filter_levels = a_filter.split("/")
    topic_levels = topic.split("/")

    for index, level in enumerate(filter_levels):
        if (WILDCARD_SINGLE in level and level != WILDCARD_SINGLE) or (
            WILDCARD_MULTI in level and level != WILDCARD_MULTI
        ):
            return False

        if level == WILDCARD_MULTI:
            return index == len(filter_levels) - 1

        if index >= len(topic_levels) or level not in (WILDCARD_SINGLE, topic_levels[index]):
            return False

    return len(filter_levels) == len(topic_levels)


@dataclass(frozen=True)
class ContainmentResult:
    """Outcome of a strict subscription authorisation check.

    Attributes
    ----------
    allowed:
        ``True`` only when every topic matched by the requested filter is
        covered by the union of the allowed filters.
    witness:
        When ``allowed`` is ``False``, a concrete topic that the requested
        filter matches but none of the allowed filters accept.  ``None`` when
        the request is fully covered (or the request filter is invalid).
    """

    allowed: bool
    witness: str | None = None


def _literals_by_depth(allowed_filters: list[str]) -> list[set[str]]:
    """Collect the non-wildcard level tokens used at every depth."""
    literals: list[set[str]] = []
    for a_filter in allowed_filters:
        for index, level in enumerate(a_filter.split("/")):
            if index == len(literals):
                literals.append(set())
            if level not in (WILDCARD_SINGLE, WILDCARD_MULTI):
                literals[index].add(level)
    return literals


def _suffix_covers_prefix(suffix: tuple[str, ...]) -> bool:
    """Return whether an active suffix already matches the topic built so far.

    An empty suffix matched every level exactly; a ``#`` suffix matches the
    current prefix because ``#`` also covers zero additional levels.
    """
    return len(suffix) == 0 or suffix[0] == WILDCARD_MULTI


def _fresh_token(used: set[str], depth: int) -> str:
    """Return a level value guaranteed not to appear among ``used`` literals."""
    candidate = f"_amqtt_witness_{depth}_"
    suffix = 0
    while candidate in used:
        suffix += 1
        candidate = f"_amqtt_witness_{depth}_{suffix}_"
    used.add(candidate)
    return candidate


def _complete_witness(prefix: list[str], request_levels: list[str], next_depth: int) -> str:
    """Extend a prefix that escaped every allowed filter into a request topic.

    ``next_depth`` is the depth at which the allowed set became empty.  The
    remaining fixed request levels are copied verbatim and any ``+`` is given a
    concrete value; a trailing ``#`` simply terminates because the prefix is
    already a complete topic it can match.
    """
    used: set[str] = set()
    levels = list(prefix)
    for index in range(next_depth, len(request_levels)):
        level = request_levels[index]
        if level == WILDCARD_MULTI:
            break
        levels.append(_fresh_token(used, index) if level == WILDCARD_SINGLE else level)
    return "/".join(levels)


def _next_active(active: set[tuple[str, ...]], level: str) -> set[tuple[str, ...]]:
    """Advance the allowed-filter suffixes by consuming one topic level."""
    next_active: set[tuple[str, ...]] = set()
    for suffix in active:
        if len(suffix) == 0:
            # An exact match cannot cover any level beyond the current prefix.
            continue
        head, tail = suffix[0], suffix[1:]
        if head == WILDCARD_MULTI:
            # '#' keeps matching further levels and also stays valid as-is
            next_active.add(suffix)
            continue
        if head == WILDCARD_SINGLE or head == level:
            next_active.add(tail)
    return next_active


def _extend_until_escape(
    prefix: list[str],
    active: set[tuple[str, ...]],
    literals: list[set[str]],
    max_depth: int,
    depth: int,
) -> str | None:
    """Extend a prefix past a requested '#' until it escapes every suffix.

    A ``#`` suffix whose fixed/``+`` prefix is not yet consumed does not cover
    shorter topics, so the walk keeps extending with values that appear as no
    literal until every fixed-length, single-level ``+`` and not-yet-reached
    ``#`` rule is exhausted.  A directly applicable ``#`` (already at its
    boundary) would cover every continuation and makes the request allowed.
    """
    if any(len(suffix) > 0 and suffix[0] == WILDCARD_MULTI for suffix in active):
        return None
    used: set[str] = set().union(*literals) if literals else set()
    levels = list(prefix)
    current = set(active)
    while current and depth <= max_depth:
        # A concrete prefix is covered only by an exact match (empty suffix) or
        # a '#' that has reached its own boundary.  A '#' still guarded by '+'
        # prefix levels does not match this shorter prefix, so the prefix is a
        # witness even though its suffix state keeps advancing.
        covered = any(
            len(suffix) == 0 or (len(suffix) == 1 and suffix[0] == WILDCARD_MULTI)
            for suffix in current
        )
        if not covered:
            return "/".join(levels)
        value = _fresh_token(used, depth)
        levels.append(value)
        current = _next_active(current, value)
        if any(len(suffix) > 0 and suffix[0] == WILDCARD_MULTI for suffix in current):
            # A '#' reached its boundary: every longer continuation is covered.
            return None
        depth += 1
    if not current:
        return "/".join(levels)
    return None


def subscriptions_cover_filter(
    requested_filter: str,
    allowed_filters: list[str],
) -> ContainmentResult:
    """Strictly check that ``requested_filter`` is contained in ``allowed_filters``.

    The request is allowed only if the set of concrete topics it can match is a
    subset of the union of the topics matched by the allowed filters.  The
    finite-partition walk over the filter trie examines every possible level
    value rather than a few examples and returns a concrete violating topic
    whenever containment fails.
    """
    if not allowed_filters:
        return ContainmentResult(False, _witness_topic(requested_filter))

    request_levels = requested_filter.split("/")
    allowed_levels = [a_filter.split("/") for a_filter in allowed_filters]

    first_request_level = request_levels[0]
    if first_request_level.startswith("$"):
        # [MQTT-4.7.2-1] filters beginning with '+' or '#' never match
        # '$'-prefixed topics, so they cannot cover any requested '$' topic.
        allowed_levels = [
            levels
            for levels in allowed_levels
            if levels[0] not in (WILDCARD_SINGLE, WILDCARD_MULTI)
        ]
        if not allowed_levels:
            return ContainmentResult(False, _witness_topic(requested_filter))

    literals = _literals_by_depth(["/".join(levels) for levels in allowed_levels])
    initial_active: set[tuple[str, ...]] = {tuple(levels) for levels in allowed_levels}
    hash_index = len(request_levels) - 1
    request_ends_hash = request_levels[hash_index] == WILDCARD_MULTI
    # Past this depth only '#' suffixes can survive, which cover everything.
    depth_limit = max(len(levels) for levels in allowed_levels) + len(request_levels) + 1

    # Every literal token appearing anywhere in the allowed rules, plus any
    # generated witness token, so an "other value" bucket can never collide with
    # a literal at another depth or a token already emitted for this request.
    used_tokens: set[str] = set().union(*literals) if literals else set()

    def fresh_token(depth: int) -> str:
        return _fresh_token(used_tokens, depth)

    def alphabet(depth: int, dollar_blocked: bool) -> list[str]:
        tokens = literals[depth] if depth < len(literals) else set()
        values = sorted(tokens)
        if depth > 0 and "" not in tokens:
            # The empty level is a distinct bucket (e.g. filters like 'a//b');
            # the empty string itself is not a valid topic.
            values.append("")
        values.append(fresh_token(depth))
        if depth == 0 and not dollar_blocked:
            values.append("$SYS/amqtt/witness")
        if dollar_blocked:
            values = [value for value in values if not value.startswith("$")]
        return values

    def walk(
        depth: int,
        active: set[tuple[str, ...]],
        prefix: list[str],
        dollar_blocked: bool,
    ) -> str | None:
        covered_here = any(_suffix_covers_prefix(suffix) for suffix in active)
        request_level = request_levels[depth] if depth < len(request_levels) else None

        # Reachable through a trailing '#' extending to another level: every
        # non-empty prefix is a concrete topic the request matches.  Search for
        # a continuation that escapes the surviving rules; a directly
        # applicable '#' covers every continuation and makes the request valid.
        if request_ends_hash and depth > hash_index:
            witness = _extend_until_escape(
                prefix, active, literals, depth_limit, depth,
            )
            if witness is not None:
                return witness
            if request_level is None:
                return None if covered_here or not prefix else "/".join(prefix)

        if request_level is None:
            return None if covered_here else "/".join(prefix)

        if request_level == WILDCARD_MULTI:
            # The '#' boundary makes the current (non-empty) prefix a topic.
            if not covered_here and prefix:
                return "/".join(prefix)
            candidates = alphabet(depth, dollar_blocked)
        elif request_level == WILDCARD_SINGLE:
            candidates = alphabet(depth, dollar_blocked)
        else:
            candidates = [request_level]

        if depth >= depth_limit:
            if active and all(suffix and suffix[0] == WILDCARD_MULTI for suffix in active):
                return None
            return "/".join(prefix + [candidates[0]])

        for level in candidates:
            next_prefix = prefix + [level]
            next_active = _next_active(active, level)
            extended_depth = depth + 1

            if not next_active:
                if request_ends_hash and extended_depth > hash_index:
                    return "/".join(next_prefix)
                return _complete_witness(next_prefix, request_levels, extended_depth)

            witness = walk(extended_depth, next_active, next_prefix, dollar_blocked)
            if witness is not None:
                return witness
        return None

    dollar_blocked = first_request_level in (WILDCARD_SINGLE, WILDCARD_MULTI)
    witness = walk(0, initial_active, [], dollar_blocked)
    return ContainmentResult(witness is None, witness)


def _witness_topic(requested_filter: str) -> str:
    """Build a concrete topic matched by a filter when no rule is available."""
    used: set[str] = set()
    levels: list[str] = []
    for index, level in enumerate(requested_filter.split("/")):
        if level == WILDCARD_MULTI:
            # '#' may match zero trailing levels, so stop here.
            break
        if level == WILDCARD_SINGLE:
            levels.append(_fresh_token(used, index))
        else:
            levels.append(level)
    return "/".join(levels)
