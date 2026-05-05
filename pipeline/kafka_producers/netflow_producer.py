import json
import time
import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Kendi modülümüzü import edebilmek için (threatflow ana dizininden çalıştırmalıyız)
sys.path.append('.')
from collector.network_sniffer.sample_generator import generate_netflow

def create_producer():
    """
    Kafka broker'a (host.docker.internal:9092) bağlanan bir producer oluşturur.
    Mesajlar JSON formatında, UTF-8 byte dizisi olarak gönderilir.
    """
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # Hata durumunda yeniden deneme
        retries=3,
        # Lider kopyaya yazıldığında onay al (dayanıklılık)
        acks='all'
    )

def main():
    print("Kafka producer başlatılıyor...")
    try:
        producer = create_producer()
        print("Producer oluşturuldu. raw_netflow topic'ine mesaj gönderiliyor...")
        topic = 'raw_netflow'

        while True:
            # Sahte bir NetFlow kaydı oluştur
            record = generate_netflow()

            # Kafka'ya gönder
            future = producer.send(topic, value=record)

            # Gönderim sonucunu bekle (senkron) - hata kontrolü için
            try:
                future.get(timeout=5)
                print(f"[✓] {record['src_ip']} -> {record['dst_ip']}:{record['dst_port']} "
                      f"({record['protocol']}) {record['bytes_out']}B")
            except KafkaError as e:
                print(f"[✗] Gönderim hatası: {e}")

            # Trafik yoğunluğunu simüle etmek için bekle
            time.sleep(0.1)

    except KeyboardInterrupt:
        print("\nProducer durduruldu. Bağlantı kapatılıyor...")
        producer.close()
        print("Çıkış yapıldı.")

if __name__ == "__main__":
    main()