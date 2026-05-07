import time
import json
from confluent_kafka import Producer
from OTXv2 import OTXv2
import urllib3

# SSL uyarılarını gizle (test için)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- AYARLAR (TEST MODU) ---
OTX_API_KEY = "61f4ee77b17ddab755e6d267a51fcec68ce345c1257afdddd9ea718151cf0524"          # demo her zaman çalışır
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'osint_indicators'
FETCH_INTERVAL = 10           # test için 10 saniyede bir
# ---------------------------

def create_producer():
    conf = {'bootstrap.servers': BOOTSTRAP_SERVERS}
    return Producer(conf)

def fetch_pulses():
    """
    Son 24 saatin tehdit pulse'larını çeker ve IoC'leri Kafka'ya gönderir.
    """
    # verify=False ile SSL doğrulamasını atlayarak ağ sorunlarını aş
    otx = OTXv2(OTX_API_KEY, verify=False)
    producer = create_producer()

    print(f"OTX bağlantısı kuruldu ({OTX_API_KEY[:4]}...). Pulse'lar çekiliyor...")
    try:
        pulses = otx.getall()  # Son 24 saat
    except Exception as e:
        print(f"OTX API hatası: {e}")
        return

    count = 0
    for pulse in pulses.get('results', []):
        pulse_name = pulse.get('name', 'Unknown')
        pulse_tags = pulse.get('tags', [])
        for indicator in pulse.get('indicators', []):
            record = {
                'type': indicator['type'],
                'indicator': indicator['indicator'],
                'description': pulse_name,
                'tags': pulse_tags,
                'timestamp': pulse.get('created', '')
            }
            producer.produce(TOPIC, value=json.dumps(record).encode('utf-8'))
            count += 1

    producer.flush()
    print(f"Toplam {count} IoC '{TOPIC}' topic'ine gönderildi.")

if __name__ == '__main__':
    print("OSINT Fetcher başlatıldı. Periyodik tarama başlıyor...")
    while True:
        fetch_pulses()
        print(f"{FETCH_INTERVAL} saniye bekleniyor...")
        time.sleep(FETCH_INTERVAL)