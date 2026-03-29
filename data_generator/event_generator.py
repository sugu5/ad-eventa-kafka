import uuid
import random
from datetime import datetime, timezone
from typing import Optional
from faker import Faker

# Weighted event types: views are most common, purchases rarest
EVENT_TYPES = ["view", "view", "view", "view", "view",
               "click", "click", "click",
               "purchase"]

DEVICES = ["mobile", "desktop", "tablet"]


class EventGenerator:
    """Generates realistic synthetic ad-interaction events."""

    def __init__(self, user_pool_size: int = 1000, seed: Optional[int] = None):
        self.faker = Faker()
        self._rng = random.Random(seed)

        if seed is not None:
            self.faker.seed_instance(seed)

        # Pre-generate a fixed pool of users for session continuity
        self.user_ids = [self.faker.uuid4() for _ in range(user_pool_size)]
        self.countries = [self.faker.country_code() for _ in range(50)]

    def generate_event(self) -> dict:
        """Return a single ad event as a plain dict (Avro-serializable)."""
        event = {
            "event_id": str(uuid.uuid4()),
            "event_time": datetime.now(timezone.utc).isoformat(),
            "user_id": self._rng.choice(self.user_ids),
            "campaign_id": self._rng.randint(1, 100),
            "ad_id": self._rng.randint(1, 500),
            "device": self._rng.choice(DEVICES),
            "country": self._rng.choice(self.countries),
            "event_type": self._rng.choice(EVENT_TYPES),
            "ad_creative_id": str(uuid.uuid4()) if self._rng.random() > 0.3 else None,
        }
        return event