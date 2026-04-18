"""Tests for ProducerService (Kafka mocked)."""
import pytest
from unittest.mock import MagicMock, patch
from ad_stream_producer.producer_service import ProducerService


@pytest.fixture
def mock_serializer():
    """Returns a fake Avro serializer that just returns bytes."""
    return MagicMock(return_value=b"\x00\x00\x00\x00\x01fake-avro")


@pytest.fixture
def service(mock_serializer):
    """ProducerService with the Kafka producer mocked out."""
    with patch("ad_stream_producer.producer_service.AdKafkaProducer") as MockProducer:
        instance = MockProducer.return_value
        instance.send_event = MagicMock()
        instance.close = MagicMock()

        svc = ProducerService(
            topic="test-topic",
            bootstrap_servers=["localhost:9092"],
            avro_serializer=mock_serializer,
        )
        svc.start_time = 1.0  # avoid NoneType errors in rate calc
        yield svc


class TestProducerService:

    def test_produce_event_calls_serializer(self, service, mock_serializer):
        service.produce_event()
        mock_serializer.assert_called_once()

    def test_produce_event_calls_send(self, service):
        service.produce_event()
        service.producer.send_event.assert_called_once()
        call_kwargs = service.producer.send_event.call_args
        assert call_kwargs[1]["topic"] == "test-topic"
        assert call_kwargs[1]["value"] == b"\x00\x00\x00\x00\x01fake-avro"

    def test_events_count_increments(self, service):
        assert service.events_count == 0
        service.produce_event()
        assert service.events_count == 1
        service.produce_event()
        assert service.events_count == 2

    def test_shutdown_calls_close(self, service):
        service.shutdown()
        service.producer.close.assert_called_once()

    def test_produce_event_handles_serializer_error(self, service, mock_serializer):
        mock_serializer.side_effect = Exception("schema mismatch")
        # Should not raise — errors are caught and logged
        service.produce_event()
        assert service.events_count == 0  # event was not counted

