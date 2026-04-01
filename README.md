# 🏦 Airflow Banking ETL Pipeline

## 📌 Project Overview

This project demonstrates an **end-to-end ETL (Extract, Transform, Load) pipeline** for a banking domain using **Apache Airflow**. The pipeline automates data ingestion, validation, transformation, and dashboard generation.

It simulates real-world banking data processing workflows including customer, account, transaction, and loan data.

---

## 🚀 Key Features

* Automated data ingestion using Airflow DAGs
* Data validation and cleaning (handling nulls, duplicates, invalid formats)
* Incremental data loading into target tables
* Rejection handling for invalid records
* Dashboard generation and PDF reporting
* Modular and scalable project structure

---

## 🛠️ Tech Stack

* **Python** (Pandas, Faker, Matplotlib)
* **Apache Airflow**
* **SQLite / MySQL**
* **SQL**
* **Linux (WSL)**
* **Git & GitHub**

---


## 🔄 ETL Workflow

1. **Extract**

   * CSV files are placed in the `incoming` folder
   * Airflow detects and triggers ingestion

2. **Transform**

   * Data cleaning (format standardization, null handling)
   * Validation rules applied
   * Duplicate removal

3. **Load**

   * Clean data loaded into target tables
   * Invalid data moved to rejection folder

4. **Reporting**

   * Dashboards generated using Matplotlib
   * Combined into a single PDF report

---

## 📊 Data Domains Covered

* Customer Data
* Account Data
* Transaction Data
* Loan Data

---

## ⚙️ Setup Instructions

### 1. Clone Repository

```bash
git clone <your-repo-link>
cd airflow-banking-etl-pipeline
```

### 2. Create Virtual Environment

```bash
python3 -m venv airflow_env
source airflow_env/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Setup Airflow

```bash
export AIRFLOW_HOME=~/airflow-workspace
airflow db init
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```

### 5. Run Airflow

```bash
airflow scheduler
airflow webserver --port 8080
```

---

## 📈 Sample Use Case

This pipeline can be used for:

* Banking analytics
* Fraud detection preprocessing
* Loan performance analysis
* Customer segmentation

---

## ❗ Challenges Solved

* Handling inconsistent date formats
* Removing duplicate records
* Ensuring data quality and validation
* Automating full pipeline using Airflow

---

## 📌 Future Improvements

* Real-time data streaming integration
* Cloud deployment (AWS/GCP)
* Advanced dashboards (Power BI/Tableau)
* API-based data ingestion

---

## 👨‍💻 Author

** Syed Nabil Ahmed (Data Analyst Intern) **

---

## ⭐ Support

If you like this project, give it a ⭐ on GitHub!
