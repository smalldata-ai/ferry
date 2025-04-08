import pytest
from unittest.mock import MagicMock, patch
from ferry.src.sources.confluent_kafka_source import KafkaSource


def test_parse_kafka_uri_valid_uri():
    source = KafkaSource()
    uri = "kafka://localhost:9092?group_id=test_group&use_avro=true&schema_registry=http://registry:8081"
    broker, config = source._parse_kafka_uri(uri)

    assert broker == "localhost:9092"
    assert config["group_id"] == "test_group"
    assert config["use_avro"] is True
    assert config["schema_registry"] == "http://registry:8081"


def test_parse_kafka_uri_missing_broker():
    source = KafkaSource()
    uri = "kafka://?group_id=test"

    with pytest.raises(ValueError, match="Missing Kafka broker."):
        source._parse_kafka_uri(uri)


@patch("ferry.src.sources.confluent_kafka_source.Consumer")
def test_create_kafka_consumer_plaintext(mock_consumer_class):
    source = KafkaSource()

    consumer_mock = MagicMock()
    mock_consumer_class.return_value = consumer_mock

    consumer_mock.list_topics.return_value.topics = {
        "test_topic": MagicMock(partitions={0: {}, 1: {}})
    }
    consumer_mock.get_watermark_offsets.return_value = (0, 10)

    broker = "localhost:9092"
    config = {"security_protocol": "PLAINTEXT", "group_id": "test_group", "use_avro": False}
    consumer, avro = source._create_kafka_consumer(
        broker, "test_topic", config, start_from="earliest"
    )

    assert avro is None
    assert consumer is not None


@patch("ferry.src.sources.confluent_kafka_source.AvroDeserializer")
@patch("ferry.src.sources.confluent_kafka_source.SchemaRegistryClient")
@patch("ferry.src.sources.confluent_kafka_source.Consumer")
def test_create_kafka_consumer_with_avro(
    mock_consumer_class, mock_registry_client_class, mock_deserializer_class
):
    source = KafkaSource()
    consumer_mock = MagicMock()
    mock_consumer_class.return_value = consumer_mock
    consumer_mock.list_topics.return_value.topics = {"topic": MagicMock(partitions={0: {}})}
    consumer_mock.get_watermark_offsets.return_value = (0, 10)

    config = {
        "group_id": "group",
        "security_protocol": "PLAINTEXT",
        "use_avro": True,
        "schema_registry": "http://localhost:8081",
    }

    consumer, deserializer = source._create_kafka_consumer(
        "broker", "topic", config, start_from="earliest"
    )

    assert consumer is not None
    assert deserializer is not None


@patch("time.time", side_effect=[0, 1, 2, 3, 4, 5, 6])
def test_consume_messages_json(mock_time):
    source = KafkaSource()
    consumer = MagicMock()
    msg = MagicMock()
    msg.value.return_value = b'{"name": "test"}'
    msg.key.return_value = b"key"
    msg.offset.return_value = 10
    msg.partition.return_value = 1
    msg.timestamp.return_value = (0, 999999)
    msg.error.return_value = None
    consumer.poll.side_effect = [msg, None, None, None, None, None, None]

    result = list(
        source._consume_messages(consumer, "topic", None, batch_size=1, batch_timeout=1.0)
    )
    assert result[0]["name"] == "test"
    assert result[0]["key"] == "key"
    assert result[0]["offset"] == 10


def test_consume_messages_fallback_csv():
    source = KafkaSource()
    consumer = MagicMock()
    msg = MagicMock()
    msg.value.return_value = b"val1,val2"
    msg.key.return_value = b"header:col1,col2"
    msg.offset.return_value = 42
    msg.partition.return_value = 2
    msg.timestamp.return_value = (0, 888888)
    msg.error.return_value = None

    consumer.poll.side_effect = [msg, None, None, None, None, None, None]

    result = list(
        source._consume_messages(consumer, "topic", None, batch_size=1, batch_timeout=1.0)
    )
    assert result[0]["col1"] == "val1"
    assert result[0]["col2"] == "val2"


def test_create_consumer_invalid_start_from():
    source = KafkaSource()

    with pytest.raises(ValueError, match="Failed to interpret timestamp"):
        config = {"security_protocol": "PLAINTEXT", "use_avro": False, "group_id": "test"}
        consumer_mock = MagicMock()
        consumer_mock.list_topics.return_value.topics = {"topic": MagicMock(partitions={0: {}})}
        consumer_mock.offsets_for_times.side_effect = Exception("invalid timestamp")

        with patch("ferry.src.sources.confluent_kafka_source.Consumer", return_value=consumer_mock):
            source._create_kafka_consumer("broker", "topic", config, start_from="timestamp:invalid")


@patch("ferry.src.sources.confluent_kafka_source.Consumer")
def test_create_kafka_consumer_topic_not_found(mock_consumer_class):
    source = KafkaSource()
    consumer_mock = MagicMock()
    consumer_mock.list_topics.return_value.topics = {}

    mock_consumer_class.return_value = consumer_mock

    with pytest.raises(ValueError, match="Topic 'missing_topic' not found in Kafka."):
        source._create_kafka_consumer(
            "broker",
            "missing_topic",
            {"group_id": "x", "use_avro": False, "security_protocol": "PLAINTEXT"},
            "earliest",
        )
