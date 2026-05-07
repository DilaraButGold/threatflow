import json
import time
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'osint_indicators'
DUMP_FILE = 'otx_dump.json'
FETCH_INTERVAL = 3600  # 1 saat (test için 10 yapabilirsin)

def create_producer():
    return Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})

def send_iocs_from_file():
    try:
        with open(DUMP_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"{DUMP_FILE} bulunamadı. Lütfen önce safe_otx_dump.py'yi çalıştırın.")
        return
    except json.JSONDecodeError:
        print(f"{DUMP_FILE} geçersiz JSON içeriyor.")
        return

    producer = create_producer()
    count = 0
    for pulse in data.get('results', []):
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
    print(f"{count} IoC '{TOPIC}' topic'ine gönderildi (kaynak: {DUMP_FILE}).")

if __name__ == '__main__':
    print("Dosya tabanlı OSINT Fetcher başladı.")
    while True:
        send_iocs_from_file()
        print(f"{FETCH_INTERVAL} saniye bekleniyor...")
        time.sleep(FETCH_INTERVAL)