"""
Example URI:

kafka://localhost:9092?group_id=mygroup&security_protocol=PLAINTEXT&use_avro=true&schema_registry=http://localhost:8081
"""

import dlt
import logging
from typing import Iterator, List, Dict, Any
from dlt.extract.source import DltSource
from ferry.src.sources.source_base import SourceBase
from ferry.src.data_models.ingest_model import ResourceConfig
from urllib.parse import urlparse, parse_qs
from confluent_kafka import Consumer, TopicPartition
from dlt.extract.incremental import Incremental
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import json

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
            topic_name = resource_config.source_table_name
            logger.info(f"Processing Kafka topic: {topic_name}")

            # consumer = self._create_kafka_consumer(kafka_broker, topic_name, kafka_config)
            consumer, avro_deserializer = self._create_kafka_consumer(
                kafka_broker, topic_name, kafka_config
            )

            incremental_key = (
                Incremental(resource_config.incremental_config.incremental_key)
                if resource_config.incremental_config
                else None
            )

            kafka_resource = dlt.resource(
                self._consume_messages(consumer, topic_name, avro_deserializer),
                name=resource_config.destination_table_name,
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
        parsed_uri = urlparse(uri)
        query_params = parse_qs(parsed_uri.query)

        broker = parsed_uri.netloc
        registry_url = query_params.get("schema_registry", [None])[0]

        if not broker:
            raise ValueError("Missing Kafka broker.")

        kafka_config = {
            "group_id": query_params.get("group_id", [None])[0],
            "security_protocol": query_params.get("security_protocol", ["PLAINTEXT"])[0],
            "sasl_mechanisms": query_params.get("sasl_mechanisms", ["PLAIN"])[0],
            "sasl_username": query_params.get("sasl_username", [None])[0],
            "sasl_password": query_params.get("sasl_password", [None])[0],
            "schema_registry": registry_url,
            "use_avro": query_params.get("use_avro", ["false"])[0].lower() == "true",
        }

        return broker, kafka_config

    def _create_kafka_consumer(self, broker: str, topic: str, config: Dict[str, Any]):
        avro_deserializer = None
        if config.get("use_avro"):
            if not config.get("schema_registry"):
                raise ValueError(
                    "Avro deserialization requested but schema_registry URL not provided."
                )

            schema_registry_conf = {"url": config["schema_registry"]}
            schema_registry_client = SchemaRegistryClient(schema_registry_conf)
            avro_deserializer = AvroDeserializer(
                schema_registry_client=schema_registry_client, from_dict=lambda obj, ctx: obj
            )

        consumer = Consumer(
            {
                "bootstrap.servers": broker,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "group.id": config.get("group_id", "default_group"),
                **(
                    {
                        "security.protocol": config["security_protocol"],
                        "sasl.mechanism": config["sasl_mechanisms"],
                        "sasl.username": config["sasl_username"],
                        "sasl.password": config["sasl_password"],
                    }
                    if config["security_protocol"] != "PLAINTEXT"
                    else {}
                ),
            }
        )

        md = consumer.list_topics(timeout=10)
        if topic not in md.topics:
            raise ValueError(f"Topic '{topic}' not found in Kafka.")

        partitions = md.topics[topic].partitions
        topic_partitions = [TopicPartition(topic, p) for p in partitions]
        consumer.assign(topic_partitions)

        return consumer, avro_deserializer

    def _consume_messages(
        self, consumer: Consumer, topic: str, avro_deserializer
    ) -> Iterator[Dict[str, Any]]:
        try:
            while True:
                message = consumer.poll(timeout=5.0)

                if message is None:
                    logger.info("No new messages in Kafka, stopping consumer...")
                    break

                if message.error():
                    logger.warning(f"Kafka message error: {message.error()}")
                    continue

                try:
                    raw_value = message.value()
                    logger.debug(f"Raw bytes: {raw_value}")

                    parsed_value = None

                    if avro_deserializer:
                        # Avro Deserialization
                        parsed_value = avro_deserializer(
                            raw_value, SerializationContext(topic, MessageField.VALUE)
                        )
                    else:
                        # Fallback to JSON or string
                        decoded_str = raw_value.decode("utf-8", errors="replace")
                        try:
                            # Try parsing as JSON
                            parsed_value = json.loads(decoded_str)
                        except json.JSONDecodeError:
                            # Try parsing as CSV-style string with optional headers
                            values = decoded_str.strip().split(",")
                            logger.debug(f"CSV-like string split: {values}")

                            # Attempt to infer keys if headers are present in message key
                            key_str = (
                                message.key().decode("utf-8", errors="replace")
                                if message.key()
                                else None
                            )

                            if key_str and key_str.startswith("header:"):
                                headers = key_str.replace("header:", "").split(",")
                                if len(headers) == len(values):
                                    parsed_value = dict(zip(headers, values))
                                else:
                                    parsed_value = {"raw": decoded_str}
                            else:
                                parsed_value = {"raw": decoded_str}

                    record = {
                        **parsed_value,
                        "offset": message.offset(),
                        "partition": message.partition(),
                        "timestamp": message.timestamp()[1],
                        "key": message.key().decode("utf-8", errors="replace")
                        if message.key()
                        else None,
                    }

                    yield record

                except Exception as e:
                    logger.error(f"Failed to deserialize or yield message: {e}")

        except Exception as e:
            logger.error(f"Kafka consumption error: {e}")
        finally:
            consumer.close()
