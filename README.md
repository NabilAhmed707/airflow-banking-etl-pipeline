# 🏦 Airflow Banking ETL Pipeline

## 📌 Project Overview

This project demonstrates an **end-to-end ETL (Extract, Transform, Load) pipeline** for a banking domain using **Apache Airflow with Docker**.

The pipeline automates data ingestion, validation, transformation, and dashboard generation, simulating real-world banking workflows.

---

## 🚀 Key Features

* Fully containerized using Docker
* Automated ETL pipeline with Apache Airflow
* Data validation and cleaning (nulls, duplicates, formats)
* Incremental data loading
* Rejection handling for invalid records
* Dashboard generation and PDF reporting

---

## 🛠️ Tech Stack

* **Python (Pandas, Faker, Matplotlib)**
* **Apache Airflow (Dockerized)**
* **SQLite / MySQL**
* **Docker & Docker Compose**
* **SQL**

---

## ⚙️ Setup Using Docker

### 1. Clone Repository

```bash
git clone https://github.com/NabilAhmed707/airflow-banking-etl-pipeline/tree/main
cd airflow-banking-etl-pipeline
```

### 2. Start Docker Containers

```bash
docker-compose up -d
```

### 3. Access Airflow UI

Open browser:

```
http://localhost:8080
```

Default login:

* Username: admin
* Password: admin

---

## 🔄 ETL Workflow

1. Airflow DAG triggers pipeline
2. Data ingestion from CSV files
3. Data validation and transformation
4. Load into target database
5. Generate dashboards and PDF reports

---

## 📊 Use Cases

* Banking data processing
* Loan analysis
* Customer insights
* Transaction monitoring

---

## ❗ Challenges Solved

* Data inconsistency handling
* Duplicate removal
* Automated scheduling with Airflow
* Scalable pipeline using Docker

---

## 📌 Future Improvements

* Cloud deployment (AWS/GCP)
* Real-time streaming (Kafka)
* Advanced dashboards (Power BI/Tableau)

---

## 👨‍💻 Author

** Syed Nabil Ahmed (Data Analyst Intern) **

---

## ⭐ Support

If you like this project, give it a ⭐ on GitHub!
