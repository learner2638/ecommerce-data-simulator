# 🚀 Synthetic E-commerce Data Simulator

A configurable **synthetic e-commerce data generation platform** designed for **Data Warehouse learning**, **SQL practice**, and **Big Data pipeline experiments**.

This project solves a common problem in data engineering learning:

> ❌ Tutorials assume data already exists  
> ✅ This platform generates realistic business data from scratch

---

## ✨ Features

- ✅ Configurable dataset scale
- ✅ Realistic order lifecycle simulation
- ✅ Refund & fulfillment workflow
- ✅ Data consistency validation
- ✅ Hive ODS table export
- ✅ Web UI control panel
- ✅ Docker one-command deployment
- ✅ Reproducible data generation

---

## 🧱 Architecture

```
Data Generator
      ↓
ODS Dataset Export
      ↓
Hive / MySQL / Spark
      ↓
Data Warehouse Modeling Practice
```

---

## 🐳 Quick Start (Recommended)

### 1️⃣ Build Docker Image

```bash
docker build -t data-sim .
```

---

### 2️⃣ Run Service

#### Windows PowerShell

```bash
docker run --rm -p 8000:8000 -v ${PWD}\out:/app/out data-sim
```

#### macOS / Linux

```bash
docker run --rm -p 8000:8000 -v $(pwd)/out:/app/out data-sim
```

---

### 3️⃣ Open Web Console

Web UI:

```
http://127.0.0.1:8000/ui/
```

API Documentation:

```
http://127.0.0.1:8000/docs
```

---

## ⚙️ Usage

1. Open Web UI
2. Configure dataset parameters
3. Click **Generate Data**
4. Download generated dataset
5. Import into Hive / MySQL / Spark

---

## 📦 Output

Generated datasets are exported to:

```
out/
└── ods.zip
```

The folder is mounted from your host machine via Docker volume mapping.

---

## 📂 Project Structure

```
web/            FastAPI service + Web UI
config.py       Simulation configuration
pipeline.py     Dataset pipeline builder
facts.py        Fact table generator
dims.py         Dimension generator
exporter.py     ODS export logic
service.py      Job execution service
```

---

## 🧠 Design Goals

This project aims to provide:

- Realistic datasets for Data Warehouse practice
- Repeatable ETL experiments
- SQL interview preparation datasets
- Teaching & demonstration environments
- Big Data ecosystem testing data

---

## 📘 Use Cases

- Data Warehouse learning
- SQL practice
- ETL pipeline testing
- Hive modeling exercises
- Spark experimentation
- Teaching demonstrations

---

## 🔥 Why Docker?

Docker enables:

- No environment setup required
- One-command startup
- Identical runtime environments
- Easy sharing & evaluation

Anyone can run the platform with:

```bash
docker run data-sim
```

---

## 🛠 Tech Stack

- Python
- FastAPI
- Data Warehouse Modeling
- Hive SQL
- Docker

---

## 🪪 License

MIT License

---

## ⭐ If this project helps you

Give it a **Star** ⭐ on GitHub!

```
Real data engineering starts from real data.
```

---

## 👨‍💻 Author

Built for data engineering learners who want **realistic practice environments**.
