import json
from datetime import datetime
from confluent_kafka import Consumer, Producer

BOOTSTRAP_SERVERS = 'localhost:9092'
RAW_TOPIC = 'raw_netflow'
ANOMALY_TOPIC = 'anomaly_scores'

def score_record(record):
    bytes_out = record.get('bytes_out', 0)
    duration = record.get('duration_s', 0)
    if bytes_out > 10000 and duration < 0.5:
        return 0.95
    elif bytes_out > 5000 and duration < 1.0:
        return 0.7
    elif duration < 0.1:
        return 0.4
    else:
        return 0.1

def main():
    # Consumer
    consumer_conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'anomaly_scorer_group',
        'auto.offset.reset': 'latest'
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([RAW_TOPIC])

    # Producer
    producer_conf = {'bootstrap.servers': BOOTSTRAP_SERVERS}
    producer = Producer(producer_conf)

    print("Anomaly scorer başladı. Mesajlar bekleniyor...")
    try:
        while True:
            msg = consumer.poll(1.0)  # 1 saniye bekle
            if msg is None:
                continue
            if msg.error():
                print(f"Tüketici hatası: {msg.error()}")
                continue

            record = json.loads(msg.value().decode('utf-8'))
            score = score_record(record)
            event = {
                'timestamp': record.get('timestamp'),
                'src_ip': record.get('src_ip'),
                'dst_ip': record.get('dst_ip'),
                'src_port': record.get('src_port'),
                'dst_port': record.get('dst_port'),
                'bytes_out': record.get('bytes_out'),
                'duration_s': record.get('duration_s'),
                'anomaly_score': score,
                'processed_at': datetime.utcnow().isoformat()
            }
            producer.produce(ANOMALY_TOPIC, value=json.dumps(event).encode('utf-8'))
            producer.poll(0)
            print(f"[✓] {record['src_ip']}:{record['src_port']} -> {record['dst_ip']}:{record['dst_port']} "
                  f"| skor={score}")

    except KeyboardInterrupt:
        print("Anomaly scorer durduruldu.")
        consumer.close()
        producer.flush()

if __name__ == '__main__':
    main()