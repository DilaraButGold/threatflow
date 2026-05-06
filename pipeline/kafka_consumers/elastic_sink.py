import json
from confluent_kafka import Consumer
from elasticsearch import Elasticsearch, helpers

# Bağlantı ayarları
BOOTSTRAP_SERVERS = 'localhost:9092'
ANOMALY_TOPIC = 'anomaly_scores'
ES_HOST = 'http://localhost:9200'
INDEX_NAME = 'threatflow_anomalies'

def main():
    # Kafka Consumer
    consumer_conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'elastic_sink_group',
        'auto.offset.reset': 'earliest'  # tüm skorları al
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([ANOMALY_TOPIC])

    # Elasticsearch istemcisi
    es = Elasticsearch(ES_HOST)

    buffer = []
    print("Elasticsearch sink başlatıldı. Skorlar bekleniyor...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Hata: {msg.error()}")
                continue

            record = json.loads(msg.value().decode('utf-8'))
            doc = {
                "_index": INDEX_NAME,
                "_source": record
            }
            buffer.append(doc)

            # 10 mesajda bir toplu yaz
            if len(buffer) >= 10:
                helpers.bulk(es, buffer)
                print(f"[✓] {len(buffer)} belge Elasticsearch'e yazıldı.")
                buffer = []

    except KeyboardInterrupt:
        if buffer:
            helpers.bulk(es, buffer)
            print(f"[✓] Kalan {len(buffer)} belge yazıldı.")
        consumer.close()
        print("Sink durduruldu.")

if __name__ == '__main__':
    main()