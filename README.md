# 🏥 Epidemic Big Data Pipeline – Spark & Scala

## 📌 Overview

This project implements a **large-scale Big Data pipeline for epidemiological surveillance**, built with **Apache Spark (Scala)** and based exclusively on **publicly accessible REST APIs**.

It is designed to demonstrate a **production-grade health data platform**, capable of ingesting, validating, transforming, and analyzing epidemic data at scale.

The pipeline follows modern **data engineering best practices** and is suitable for:

* National public health monitoring
* International health organizations (WHO, NGOs)
* Research & analytics platforms
* Senior / Lead Data Engineer portfolios

---

## 🎯 Business Use Case

**Epidemic surveillance and monitoring**:

* Track confirmed cases, deaths, and trends per country and continent
* Produce reliable indicators for decision-makers
* Support early warning systems and analytical dashboards

Typical diseases covered by the data source include:

* COVID-19
* Dengue
* Cholera
* Influenza (depending on API availability)

---

## 🌐 Data Source (Public REST API)

This project relies on a **public, real-time REST API**:

* **disease.sh** – Global epidemiological data
* Endpoint example:

  ```
  https://disease.sh/v3/covid-19/countries
  ```

✔ No private database
✔ No credentials required
✔ Fully reproducible

---

## 🏗️ Architecture

The pipeline follows a **Medallion Architecture**:

```
Public REST API
        ↓
Bronze Layer (Raw Ingestion)
        ↓
Silver Layer (Data Quality & Normalization)
        ↓
Gold Layer (Analytics & Indicators)
        ↓
Delta Lake Storage
```

### Layers description

* **Bronze**: Raw JSON ingestion from API (no transformation)
* **Silver**: Data quality checks, cleansing, normalization
* **Gold**: Aggregated epidemiological indicators ready for BI & ML

---

## ⚙️ Technology Stack

* **Language**: Scala 2.13
* **Processing**: Apache Spark 3.x
* **Storage**: Delta Lake
* **Ingestion**: REST API (HTTP)
* **Build Tool**: sbt
* **Data Architecture**: Bronze / Silver / Gold

---

## 🧪 Data Quality Strategy

A dedicated **Data Quality layer** is implemented to ensure reliability of health data:

### Rules applied

* Mandatory fields must not be null
* Epidemiological metrics must be positive
* Invalid or corrupted records are automatically filtered

This approach aligns with **health data governance standards** and prevents analytical bias.

---

## 📊 Analytical Outputs (Gold Layer)

The Gold layer produces aggregated indicators such as:

* Total confirmed cases per continent
* Total deaths per continent
* Average number of new cases

These datasets are optimized for:

* BI tools (Power BI, Superset, Tableau)
* Epidemiological research
* Machine learning pipelines

---

## 📂 Project Structure

```
epidemic-spark-pipeline/
 ├── config/          # Spark configuration
 ├── ingestion/       # REST API ingestion (Bronze)
 ├── quality/         # Data Quality rules (Silver)
 ├── transformation/ # Normalization logic (Silver)
 ├── analytics/       # Aggregations & indicators (Gold)
 ├── storage/         # Delta Lake persistence
 └── pipeline/        # Orchestration entry point
```

---

## ▶️ How to Run

### Prerequisites

* Java 11+
* Apache Spark 3.x
* sbt

### Run the pipeline

```bash
sbt run
```

The pipeline will:

1. Fetch data from the public API
2. Apply data quality checks
3. Generate analytical indicators
4. Persist results in Delta Lake format

---

## 🔐 Security & Compliance Considerations

* No personal or identifiable health data (PHI)
* Public, aggregated epidemiological information only
* Architecture compatible with **RGPD / HIPAA-compliant extensions** if extended to private data

---

## 🚀 Possible Extensions

* Real-time streaming with Kafka & Spark Structured Streaming
* Machine Learning for epidemic trend prediction
* Alerting system for outbreak detection
* Cloud deployment (AWS / Azure / GCP)
* Integration with national health information systems

---

## 👤 Author

**RANOELISON Dimbisoa Adrianno**
Senior Data Engineer | Big Data | AI | Health Data

---

## 📄 License

This project is provided for **educational, research, and professional demonstration purposes**.

---

✅ *This repository showcases a realistic, scalable, and production-oriented Big Data health pipeline.*
