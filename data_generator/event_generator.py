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

# User segments for v3
USER_SEGMENTS = ["premium", "basic", "free"]


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
        
        # Pre-assign segments to users (v3) - stays consistent per user
        self.user_segments = {
            user_id: self._rng.choice(USER_SEGMENTS)
            for user_id in self.user_ids
        }

    def generate_event(self) -> dict:
        """Return a single ad event as a plain dict (Avro-serializable).
        
        Schema v3: Includes new fields:
        - user_segment: User tier (premium/basic/free)
        - conversion_value: Monetary value for purchases
        - geo_latitude: Geographic latitude
        
        Backward compatible: v1 and v2 producers omit these fields (get null)
        """
        user_id = self._rng.choice(self.user_ids)
        event_type = self._rng.choice(EVENT_TYPES)
        
        # Calculate engagement score based on event type (from v2)
        if event_type == "purchase":
            engagement_score = self._rng.uniform(0.7, 1.0)
        elif event_type == "click":
            engagement_score = self._rng.uniform(0.4, 0.8)
        else:  # view
            engagement_score = self._rng.uniform(0.1, 0.5)
        
        # Randomly omit engagement_score in ~10% of events
        if self._rng.random() < 0.1:
            engagement_score = None
        
        # v3: User segment (consistent per user)
        user_segment = self.user_segments.get(user_id)
        
        # v3: Conversion value (only for purchases, higher for premium users)
        conversion_value = None
        if event_type == "purchase":
            base_value = self._rng.uniform(10, 500)
            # Premium users have 20% higher conversion values
            if user_segment == "premium":
                conversion_value = base_value * 1.2
            elif user_segment == "basic":
                conversion_value = base_value
            else:  # free
                conversion_value = base_value * 0.7
            conversion_value = round(conversion_value, 2)
        
        # v3: Geographic data (realistic latitude bounds)
        # Omitted in ~15% of events to test null handling
        if self._rng.random() > 0.15:
            geo_latitude = self._rng.uniform(-90, 90)  # Valid latitude range
        else:
            geo_latitude = None

        event = {
            "event_id": str(uuid.uuid4()),
            "event_time": datetime.now(timezone.utc).isoformat(),
            "user_id": user_id,
            "campaign_id": self._rng.randint(1, 100),
            "ad_id": self._rng.randint(1, 500),
            "device": self._rng.choice(DEVICES),
            "country": self._rng.choice(self.countries),
            "event_type": event_type,
            "ad_creative_id": str(uuid.uuid4()) if self._rng.random() > 0.3 else None,
            "engagement_score": engagement_score,  # v2
            "user_segment": user_segment,  # v3 NEW
            "conversion_value": conversion_value,  # v3 NEW
            "geo_latitude": geo_latitude,  # v3 NEW
        }
        return event