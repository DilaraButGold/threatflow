import json
from datetime import datetime
import joblib
from confluent_kafka import Consumer, Producer
import numpy as np

BOOTSTRAP_SERVERS = 'localhost:9092'
RAW_TOPIC = 'raw_netflow'
ANOMALY_TOPIC = 'anomaly_scores'
MODEL_PATH = "isolation_forest.pkl"   # Ana dizinde duruyor, MinIO'ya gerek yok

def load_model():
    """Eğitilmiş Isolation Forest modelini yükler."""
    model = joblib.load(MODEL_PATH)
    print("ML modeli yüklendi (isolation_forest.pkl).")
    return model

def score_with_ml(model, record):
    """
    Gelen NetFlow kaydını modele sorar ve 0-1 arası anomali olasılığı döndürür.
    """
    features = np.array([[
        record.get('duration_s', 0),
        record.get('bytes_out', 0),
        record.get('bytes_in', 0),
        record.get('dst_port', 0)
    ]])

    raw_score = model.decision_function(features)[0]   # negatif = anormal
    normalized = round(1.0 - (raw_score + 0.5), 4)     # 0-1 arası
    return normalized

def main():
    model = load_model()

    consumer_conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'anomaly_scorer_ml',
        'auto.offset.reset': 'latest'
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([RAW_TOPIC])

    producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})

    print("ML tabanlı anomali scorer başladı. Mesajlar bekleniyor...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Hata: {msg.error()}")
                continue

            record = json.loads(msg.value().decode('utf-8'))
            score = score_with_ml(model, record)

            event = {
                'timestamp': record.get('timestamp'),
                'src_ip': record.get('src_ip'),
                'dst_ip': record.get('dst_ip'),
                'src_port': record.get('src_port'),
                'dst_port': record.get('dst_port'),
                'bytes_out': record.get('bytes_out'),
                'bytes_in': record.get('bytes_in'),
                'duration_s': record.get('duration_s'),
                'anomaly_score': score,
                'processed_at': datetime.utcnow().isoformat()
            }

            producer.produce(ANOMALY_TOPIC, value=json.dumps(event).encode('utf-8'))
            producer.poll(0)

            alert = "⚠️ YÜKSEK" if score > 0.7 else "   NORMAL"
            print(f"{alert} | {record['src_ip']} -> {record['dst_ip']} | skor: {score}")

    except KeyboardInterrupt:
        consumer.close()
        producer.flush()
        print("ML scorer durduruldu.")

if __name__ == '__main__':
    main()