import json
import time
import sys
from confluent_kafka import Producer

sys.path.append('.')
from collector.network_sniffer.sample_generator import generate_netflow

def create_producer():
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'acks': 'all',
        'retries': 3
    }
    return Producer(conf)

def delivery_callback(err, msg):
    if err:
        print(f'[✗] Hata: {err}')
    else:
        print(f'[✓] {msg.key()} teslim edildi, partition {msg.partition()}')

def main():
    print("Kafka producer başlatılıyor...")
    producer = create_producer()
    topic = 'raw_netflow'

    try:
        while True:
            record = generate_netflow()
            # confluent-kafka'da value bytes olmalı
            producer.produce(
                topic,
                value=json.dumps(record).encode('utf-8'),
                callback=delivery_callback
            )
            producer.poll(0)  # callback'leri tetikle
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("\nProducer durduruldu. Kalan mesajlar gönderiliyor...")
        producer.flush()
        print("Çıkış yapıldı.")

if __name__ == "__main__":
    main()