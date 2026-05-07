# collector/osint_feeds/safe_otx_dump.py
import json

# Demo veri (bağlantı olmadan da çalışır)
demo_data = {
    "results": [
        {
            "name": "Demo Pulse - Test IoC",
            "tags": ["test", "demo"],
            "created": "2025-01-01T00:00:00Z",
            "indicators": [
                {"type": "IPv4", "indicator": "185.130.5.253"},
                {"type": "domain", "indicator": "evil-c2.example.com"}
            ]
        }
    ]
}

with open("otx_dump.json", "w", encoding="utf-8") as f:
    json.dump(demo_data, f, indent=2)
print("Geçerli otx_dump.json oluşturuldu (demo veri).")