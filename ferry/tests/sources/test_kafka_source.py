import unittest
from unittest.mock import patch, MagicMock
from ferry.src.sources.kafka_source import KafkaSource
from ferry.src.data_models.ingest_model import ResourceConfig, IncrementalConfig
from kafka import KafkaConsumer
import dlt


class TestKafkaSource(unittest.TestCase):
    def setUp(self):
        self.kafka_source = KafkaSource()
        self.valid_uri = "kafka://localhost:9092?group_id=test_group&security_protocol=PLAINTEXT"
        self.invalid_uri = "kafka://localhost:9092"

    @patch("ferry.src.sources.kafka_source.KafkaSource._create_kafka_consumer")
    def test_dlt_source_system(self, mock_create_consumer):
        mock_consumer = MagicMock(spec=KafkaConsumer)
        mock_create_consumer.return_value = mock_consumer

        resources = [
            ResourceConfig(
                source_table_name="test_topic",
                destination_table_name="test_table",
                incremental_config=IncrementalConfig(incremental_key="timestamp"),
            )
        ]

        source = self.kafka_source.dlt_source_system(self.valid_uri, resources, "test_identity")

        self.assertIsInstance(source, dlt.extract.source.DltSource)
        self.assertEqual(source.schema.name, "test_identity")
        self.assertEqual(len(source.resources), 1)

    def test_parse_kafka_uri_valid(self):
        broker, config = self.kafka_source._parse_kafka_uri(self.valid_uri)
        self.assertEqual(broker, "localhost:9092")
        self.assertEqual(config["group_id"], "test_group")
        self.assertEqual(config["security_protocol"], "PLAINTEXT")

    def test_parse_kafka_uri_invalid(self):
        with self.assertRaises(ValueError) as context:
            self.kafka_source._parse_kafka_uri(self.invalid_uri)
        self.assertIn("Missing required parameters", str(context.exception))

    @patch("kafka.KafkaConsumer")
    def test_create_kafka_consumer(self, mock_kafka_consumer):
        mock_kafka_consumer.return_value = MagicMock()
        broker = "localhost:9092"
        topic = "test_topic"
        config = {"security_protocol": "PLAINTEXT", "group_id": "test_group"}

        consumer = self.kafka_source._create_kafka_consumer(broker, topic, config)
        self.assertIsInstance(consumer, KafkaConsumer)
        mock_kafka_consumer.assert_called()

    @patch("ferry.src.sources.kafka_source.KafkaConsumer.poll")
    def test_consume_messages(self, mock_poll):
        mock_poll.side_effect = [
            {
                0: [
                    MagicMock(
                        key=b"key1", value=b"value1", offset=1, partition=0, timestamp=1234567890
                    )
                ]
            },
            {},
        ]
        mock_consumer = MagicMock()
        records = list(self.kafka_source._consume_messages(mock_consumer, "test_topic"))

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["key"], "key1")
        self.assertEqual(records[0]["value"], "value1")
        self.assertEqual(records[0]["offset"], 1)


if __name__ == "__main__":
    unittest.main()
