"""Tests for the EventGenerator."""
import pytest
from data_generator.event_generator import EventGenerator
from ad_stream_producer.schema import AdEvent


@pytest.fixture
def generator():
    return EventGenerator(user_pool_size=50, seed=42)


class TestEventGenerator:
    """Verify generated events match the expected schema."""

    def test_returns_dict(self, generator):
        event = generator.generate_event()
        assert isinstance(event, dict)

    def test_required_fields_present(self, generator):
        event = generator.generate_event()
        required = [
            "event_id", "event_time", "user_id",
            "campaign_id", "ad_id", "device",
            "country", "event_type",
        ]
        for field in required:
            assert field in event, f"Missing required field: {field}"

    def test_validates_against_pydantic_model(self, generator):
        """Every generated event must pass Pydantic validation."""
        for _ in range(100):
            event = generator.generate_event()
            validated = AdEvent(**event)
            assert validated.event_id == event["event_id"]

    def test_user_pool_reuse(self, generator):
        """Users should be drawn from a fixed pool, not unique each time."""
        user_ids = {generator.generate_event()["user_id"] for _ in range(200)}
        # With a pool of 50 and 200 draws, we should see repeats
        assert len(user_ids) <= 50

    def test_event_type_distribution(self, generator):
        """Views should be more common than purchases."""
        types = [generator.generate_event()["event_type"] for _ in range(1000)]
        view_count = types.count("view")
        purchase_count = types.count("purchase")
        assert view_count > purchase_count

    def test_ad_creative_id_nullable(self, generator):
        """ad_creative_id should sometimes be None."""
        values = [generator.generate_event()["ad_creative_id"] for _ in range(200)]
        assert None in values
        assert any(v is not None for v in values)

    def test_campaign_id_range(self, generator):
        event = generator.generate_event()
        assert 1 <= event["campaign_id"] <= 100

    def test_ad_id_range(self, generator):
        event = generator.generate_event()
        assert 1 <= event["ad_id"] <= 500

    def test_device_values(self, generator):
        event = generator.generate_event()
        assert event["device"] in {"mobile", "desktop", "tablet"}

    def test_seed_reproducibility(self):
        """Two generators with the same seed produce deterministic choices."""
        g1 = EventGenerator(user_pool_size=10, seed=99)
        g2 = EventGenerator(user_pool_size=10, seed=99)
        # uuid4 and event_time are non-deterministic; verify seeded fields
        deterministic_fields = ["user_id", "campaign_id", "ad_id", "device",
                                "country", "event_type"]
        for _ in range(10):
            e1 = g1.generate_event()
            e2 = g2.generate_event()
            for field in deterministic_fields:
                assert e1[field] == e2[field], f"Mismatch on {field}"


