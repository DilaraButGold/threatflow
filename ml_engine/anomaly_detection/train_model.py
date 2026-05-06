import numpy as np
import pandas as pd
import joblib
from pyod.models.iforest import IForest
from minio import Minio
import os

def generate_synthetic_data(n_samples=2000, anomaly_ratio=0.05):
    """
    Gerçek NetFlow özelliklerine benzer sentetik veri üretir.
    Normal trafik + seyrek anormal trafik.
    """
    np.random.seed(42)
    n_anomalies = int(n_samples * anomaly_ratio)
    n_normal = n_samples - n_anomalies

    # Normal trafik: ortalama süre 2.5s, çıkış byte'ı 3-8KB
    normal_duration = np.random.gamma(shape=2.0, scale=1.2, size=n_normal)
    normal_bytes_out = np.random.normal(loc=6000, scale=2000, size=n_normal)
    normal_bytes_in = np.random.normal(loc=4000, scale=1500, size=n_normal)
    normal_dst_port = np.random.choice([80, 443, 53, 22, 8080], size=n_normal)

    # Anormal trafik: çok kısa sürede çok büyük veri (sızıntı)
    anom_duration = np.random.uniform(0.01, 0.3, size=n_anomalies)
    anom_bytes_out = np.random.uniform(12000, 16000, size=n_anomalies)
    anom_bytes_in = np.random.uniform(100, 500, size=n_anomalies)
    anom_dst_port = np.random.choice([53, 443, 1433], size=n_anomalies)  # DNS, SSL, MSSQL gibi

    # Birleştir ve DataFrame oluştur
    X = np.vstack([
        np.column_stack([normal_duration, normal_bytes_out, normal_bytes_in, normal_dst_port]),
        np.column_stack([anom_duration, anom_bytes_out, anom_bytes_in, anom_dst_port])
    ])
    # Etiketler (0: normal, 1: anomali) – sadece referans için, model denetimsiz
    y = np.hstack([np.zeros(n_normal), np.ones(n_anomalies)])

    df = pd.DataFrame(X, columns=['duration_s', 'bytes_out', 'bytes_in', 'dst_port'])
    df['label'] = y.astype(int)
    return df

def train_model():
    """
    Isolation Forest modelini eğitir ve diske/Model deposuna kaydeder.
    """
    print("Eğitim verisi oluşturuluyor...")
    df = generate_synthetic_data(3000, anomaly_ratio=0.05)
    X_train = df[['duration_s', 'bytes_out', 'bytes_in', 'dst_port']].values

    # Model oluştur ve eğit
    model = IForest(contamination=0.05, random_state=42)
    model.fit(X_train)

    # Yerel dosyaya kaydet
    model_path = "isolation_forest.pkl"
    joblib.dump(model, model_path)
    print(f"Model {model_path} olarak kaydedildi.")

    # MinIO'ya yükle
    upload_to_minio(model_path)
    return model_path

def upload_to_minio(file_path):
    """
    Eğitilmiş modeli MinIO model deposuna yükler.
    """
    client = Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    bucket_name = "models"
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"'{bucket_name}' bucket oluşturuldu.")

    client.fput_object(bucket_name, file_path, file_path)
    print(f"Model MinIO://{bucket_name}/{file_path} adresine yüklendi.")

if __name__ == "__main__":
    train_model()