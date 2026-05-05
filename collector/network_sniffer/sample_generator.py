import random
import json
from datetime import datetime, timezone

def generate_netflow():
    """
    Rastgele bir NetFlow kaydı üretir.
    Gerçek ağ akışlarında bulunan alanları simüle eder:
    - IP adresleri (kaynak/hedef)
    - Portlar (istemci tarafı yüksek, sunucu tarafı bilinen portlar)
    - Protokol (TCP/UDP)
    - Aktarılan byte miktarı
    - TCP bayrakları
    - Akış süresi
    """
    # İstemciler genelde 192.168.x.x bloğundan gelir
    src_ip = f"192.168.{random.randint(1,255)}.{random.randint(1,255)}"
    # Sunucular 10.0.0.x ağında olsun
    dst_ip = f"10.0.0.{random.randint(1,20)}"

    # Rastgele portlar
    src_port = random.randint(49152, 65535)  # Ephemeral port aralığı
    dst_port = random.choice([22, 53, 80, 443, 3389, 8080, 1433, 3306])

    # Protokol seçimi
    protocol = random.choice(["TCP", "UDP"])

    # TCP bayrakları (sadece TCP için anlamlı, ama her ikisi için üretiyoruz)
    tcp_flags = random.choice(["SYN", "SYN-ACK", "ACK", "RST", "FIN", "PSH-ACK"])

    # Veri miktarı (byte)
    bytes_in = random.randint(40, 1500)   # Gelen veri
    bytes_out = random.randint(40, 15000) # Giden veri (daha geniş aralık)

    # Akış süresi (saniye)
    duration = round(random.uniform(0.001, 4.0), 4)

    # Zaman damgası (UTC)
    timestamp = datetime.now(timezone.utc).isoformat()

    return {
        "timestamp": timestamp,
        "src_ip": src_ip,
        "dst_ip": dst_ip,
        "src_port": src_port,
        "dst_port": dst_port,
        "protocol": protocol,
        "bytes_in": bytes_in,
        "bytes_out": bytes_out,
        "tcp_flags": tcp_flags,
        "duration_s": duration
    }

# Test etmek için direkt çalıştırılırsa
if __name__ == "__main__":
    # Sonsuz döngüde her 0.1 saniyede bir kayıt üret ve ekrana yaz
    import time
    while True:
        record = generate_netflow()
        print(json.dumps(record))
        time.sleep(0.1)  # Saniyede 10 mesaj