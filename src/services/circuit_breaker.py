"""
PostCallCircuitBreaker — Tries to protect the LLM API from overload.

The idea was sound: if we're sending too many LLM requests, slow down new
calls before the provider starts returning 429s. The execution is too blunt.

Current behaviour:
  - Checks RPM usage against LLM_REQUESTS_PER_MINUTE
  - If usage >= 90%: freeze the dialler for the agent for 1800 seconds
  - The dialler checks this before dispatching new calls

Problems:
  1. Binary response. 89% capacity: full speed. 90%: complete stop for 30 minutes.
     There's no middle gear — no "slow down a bit", no "pause for 5 seconds".

  2. Wrong granularity. Freezes at agent_id level. If one campaign is consuming
     all the LLM quota, every agent — across all customers — hits the freeze.

  3. Measuring the wrong thing. This tracks in-flight Celery tasks via an RPM
     counter. But LLM providers rate-limit on tokens/minute, not requests/minute.
     A 10-turn conversation uses 3× the tokens of a 3-turn one. The circuit
     breaker doesn't know that.

  4. The counter is written by record_postcall_start(), which is called after
     deciding to fire the LLM request — not before. By the time the breaker
     could trip, the requests are already in flight.

  5. No visibility. The dialler just sees "circuit open". It doesn't know if it's
     because of a genuine quota issue or a transient Redis blip that made the
     counter stale.

Consider: what would a system look like where the dialler doesn't freeze at all,
but instead naturally dispatches fewer calls when LLM capacity is constrained?
The capacity signal already exists — it just needs to be used differently.
"""

"""
PostCallCircuitBreaker — Implements proportional backpressure to protect the LLM API.

Instead of a binary freeze (which halts all agents cross-customer for 30 minutes),
this service monitors current RPM and provides a gradual throttle multiplier (0.0 to 1.0)
which the dialler can use to naturally slow down or speed up outbound calling.
"""

import logging
from src.config import settings
from src.utils.redis_client import redis_client

logger = logging.getLogger(__name__)


class PostCallCircuitBreaker:
    """
    Tracks and checks LLM capacity usage to recommend a dialling rate.
    """

    async def check_capacity(self, agent_id: str) -> float:
        """
        Returns a capacity multiplier float (0.0 to 1.0) indicating recommended dialling speed:
        - usage < 50%  -> 1.0 (Full speed)
        - usage 50-70% -> 0.75 (75% speed)
        - usage 70-85% -> 0.50 (50% speed)
        - usage 85-95% -> 0.25 (25% speed)
        - usage >= 95% -> 0.05 (5% speed, never completely freezes)
        """
        try:
            current_rpm = int(await redis_client.get("llm:postcall:rpm") or 0)
        except Exception as e:
            logger.warning(f"Failed to fetch postcall RPM from Redis: {e}")
            return 1.0

        max_rpm = settings.LLM_REQUESTS_PER_MINUTE
        usage_ratio = current_rpm / max_rpm if max_rpm > 0 else 0

        if usage_ratio < 0.50:
            return 1.0
        elif usage_ratio < 0.70:
            return 0.75
        elif usage_ratio < 0.85:
            return 0.50
        elif usage_ratio < 0.95:
            return 0.25
        else:
            logger.warning(
                "ALERT: Extreme LLM capacity constraint. Proportional backpressure active.",
                extra={
                    "agent_id": agent_id,
                    "usage_ratio": round(usage_ratio, 2),
                    "current_rpm": current_rpm,
                }
            )
            return 0.05

    async def record_postcall_start(self) -> None:
        """
        Increment the RPM counter when a post-call LLM request starts.
        """
        try:
            await redis_client.incr("llm:postcall:rpm")
            await redis_client.expire("llm:postcall:rpm", 60)
        except Exception as e:
            logger.warning(f"Failed to increment postcall RPM in Redis: {e}")

    async def record_postcall_end(self) -> None:
        """
        Decrement the RPM counter when the LLM request completes.
        """
        try:
            await redis_client.decr("llm:postcall:rpm")
        except Exception as e:
            logger.warning(f"Failed to decrement postcall RPM in Redis: {e}")


circuit_breaker = PostCallCircuitBreaker()

