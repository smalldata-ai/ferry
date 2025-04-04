import dlt
import logging
from typing import Iterator, List, Dict, Any
from dlt.extract.source import DltSource
from ferry.src.sources.source_base import SourceBase
from ferry.src.data_models.ingest_model import ResourceConfig
from urllib.parse import urlparse, parse_qs
from kafka import KafkaConsumer, TopicPartition
from dlt.extract.incremental import Incremental

logger = logging.getLogger(__name__)


class KafkaSource(SourceBase):
    def __init__(self):
        """Initialize Kafka source without needing the URI at instantiation."""
        super().__init__()

    def dlt_source_system(
        self, uri: str, resources: List[ResourceConfig], identity: str
    ) -> DltSource:
        """Processes Kafka messages as a DLT source system."""
        kafka_broker, kafka_config = self._parse_kafka_uri(uri)
        resources_list = []

        for resource_config in resources:
            topic_name = resource_config.source_table_name  # Topic comes from ResourceConfig
            logger.info(f"Processing Kafka topic: {topic_name}")

            consumer = self._create_kafka_consumer(kafka_broker, topic_name, kafka_config)

            # Ensure Incremental object instead of string
            incremental_key = (
                Incremental(resource_config.incremental_config.incremental_key)
                if resource_config.incremental_config
                else None
            )

            kafka_resource = dlt.resource(
                self._consume_messages(consumer, topic_name),
                name=resource_config.source_table_name,
                write_disposition="append",
                primary_key="offset",
                incremental=incremental_key,
            )

            resources_list.append(kafka_resource)

        return DltSource(
            schema=dlt.Schema(identity),
            section="kafka_source",
            resources=resources_list,
        )

    def _parse_kafka_uri(self, uri: str):
        """Parses Kafka connection URI and extracts required configurations."""
        parsed_uri = urlparse(uri)
        query_params = parse_qs(parsed_uri.query)

        broker = parsed_uri.netloc  # e.g., "localhost:9092"
        if not broker:
            raise ValueError(f"Invalid Kafka URI: Missing bootstrap_servers in {uri}")

        kafka_config = {
            "group_id": query_params.get("group_id", [None])[0],
            "security_protocol": query_params.get("security_protocol", [None])[0],
            "sasl_mechanisms": query_params.get("sasl_mechanisms", [None])[0],
            "sasl_username": query_params.get("sasl_username", [None])[0],
            "sasl_password": query_params.get("sasl_password", [None])[0],
        }

        # Validate required parameters
        missing_params = [k for k, v in kafka_config.items() if v is None]
        if missing_params:
            raise ValueError(f"Invalid Kafka URI: Missing required parameters {missing_params}")

        # Validate security_protocol
        allowed_protocols = {"PLAINTEXT", "SASL_PLAINTEXT", "SASL_SSL"}
        if kafka_config["security_protocol"] not in allowed_protocols:
            raise ValueError(
                f"Invalid security_protocol: {kafka_config['security_protocol']} (Allowed: {allowed_protocols})"
            )

        # Validate sasl_mechanisms
        allowed_mechanisms = {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}
        if kafka_config["sasl_mechanisms"] not in allowed_mechanisms:
            raise ValueError(
                f"Invalid sasl_mechanisms: {kafka_config['sasl_mechanisms']} (Allowed: {allowed_mechanisms})"
            )

        return broker, kafka_config

    def _create_kafka_consumer(
        self, broker: str, topic: str, config: Dict[str, Any]
    ) -> KafkaConsumer:
        """Creates a Kafka consumer to read messages."""
        consumer_config = {
            "bootstrap_servers": broker,
            "auto_offset_reset": "earliest",
            "enable_auto_commit": False,  # Manual offset tracking
            "value_deserializer": lambda x: x.decode("utf-8"),
        }

        if config["security_protocol"] and config["security_protocol"] != "PLAINTEXT":
            consumer_config["security_protocol"] = config["security_protocol"]
            consumer_config["sasl_mechanism"] = config["sasl_mechanisms"]
            consumer_config["sasl_plain_username"] = config["sasl_username"]
            consumer_config["sasl_plain_password"] = config["sasl_password"]

        if config["group_id"]:
            consumer_config["group_id"] = config["group_id"]

        logger.info(f"Kafka Consumer Config: {consumer_config}")
        consumer = KafkaConsumer(**consumer_config)

        # Assign partitions manually
        partitions = consumer.partitions_for_topic(topic)
        if partitions:
            topic_partitions = [TopicPartition(topic, p) for p in partitions]
            consumer.assign(topic_partitions)

        return consumer

    def _consume_messages(self, consumer: KafkaConsumer, topic: str) -> Iterator[Dict[str, Any]]:
        """Consumes messages from Kafka and manages offsets."""
        try:
            while True:
                messages = consumer.poll(timeout_ms=5000)
                if not messages:
                    logger.info("No new messages in Kafka, stopping consumer...")
                    break

                for batch in messages.items():
                    for message in batch:
                        record = {
                            "key": message.key.decode("utf-8") if message.key else None,
                            "value": message.value,
                            "offset": message.offset,
                            "partition": message.partition,
                            "timestamp": message.timestamp,
                        }
                        yield record

                    # Manually commit offsets after processing each batch
                    consumer.commit()
        except Exception as e:
            logger.error(f"Kafka consumption error: {e}")
        finally:
            consumer.close()
