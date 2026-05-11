# Büyük Veri Dönem Projesi — Global Daily Climate Data

**Ders:** Büyük Veri Analizine Giriş  
**Danışman:** Dr. Ayşe Gül Eker  
**Veri Seti:** Global Daily Climate Data (#13) — Kaggle  
**Problem:** Regresyon — Günlük Ortalama Sıcaklık Tahmini (`avg_temp_c`)

## Proje Mimarisi

```
Kafka Producer → Apache Kafka → Spark Structured Streaming
                                        ↓
                              Delta Lake (Bronze/Silver/Gold)
                                        ↓
                              EDA + Feature Engineering
                                        ↓
                              5 ML Modeli (Spark MLlib + MLflow)
                                        ↓
                              Dashboard (Matplotlib/Seaborn)
```

## Ekip ve İş Bölümü

| Kişi | Sorumluluk | Branch |
|---|---|---|
| Kişi 1 | Docker + Kafka Producer | `feature/docker-kafka` |
| Kişi 2 | Spark + Delta Lake + EDA | `feature/spark-eda` |
| Kişi 3 | Feature Eng. + ML + Dashboard | `feature/ml-dashboard` |

## Kurulum

```bash
git clone https://github.com/cawnip/bigdata-climate-pipeline.git
cd bigdata-climate-pipeline
pip install -r requirements.txt
```

## Docker ile Çalıştırma

```bash
docker compose up --build
```

## Veri Seti

- **Kaynak:** [Kaggle — guillemservera/global-daily-climate-data](https://www.kaggle.com/datasets/guillemservera/global-daily-climate-data)
- **Boyut:** ~27.6M kayıt, 14 sütun
- **Hedef değişken:** `avg_temp_c` (günlük ortalama sıcaklık)
