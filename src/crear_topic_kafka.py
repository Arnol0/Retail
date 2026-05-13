"""
=====================================================
RAPÍDEX - CREAR TOPIC EN KAFKA
=====================================================
Autor: Grupo 11 - Módulo Streaming
Descripción: Crea el topic 'rapidex-eventos' en Kafka
             con 3 particiones para distribución de carga.

Ejecutar: python crear_topic_kafka.py
=====================================================
"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME   = 'rapidex-eventos'

def crear_topic():
    print("=" * 55)
    print("  RAPÍDEX — CONFIGURACIÓN DE KAFKA")
    print("=" * 55)

    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BROKER,
        client_id='rapidex-admin'
    )

    topic = NewTopic(
        name=TOPIC_NAME,
        num_partitions=3,
        replication_factor=1
    )

    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"  ✅ Topic '{TOPIC_NAME}' creado correctamente")
        print(f"     Particiones      : 3")
        print(f"     Replicación      : 1")
        print(f"     Broker           : {KAFKA_BROKER}")
    except TopicAlreadyExistsError:
        print(f"  ℹ️  Topic '{TOPIC_NAME}' ya existe — no se vuelve a crear")
    finally:
        admin_client.close()

    # Verificar topics existentes
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BROKER)
    topics = consumer.topics()
    consumer.close()

    print(f"\n  📋 Topics disponibles en Kafka:")
    for t in sorted(topics):
        marca = "→" if t == TOPIC_NAME else " "
        print(f"     {marca} {t}")
    print("=" * 55)

if __name__ == '__main__':
    crear_topic()
