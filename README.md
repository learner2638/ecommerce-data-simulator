<<<<<<< HEAD
# Ecommerce Data Simulator

## Project Introduction

This project simulates e-commerce business data for data warehouse learning.

Workflow:
Data Generation → ODS → Hive Modeling → API Service

## Tech Stack

* Python
* FastAPI
* Hive SQL
* Data Warehouse Modeling

## Run

pip install -r requirements.txt

uvicorn web.app:app --host 0.0.0.0 --port 10000
=======
# Synthetic E-commerce Data Simulator

A configurable synthetic data generation platform for e-commerce data warehouse practice.

This project simulates realistic e-commerce datasets for data warehouse learning, SQL practice, and big data pipeline experiments.

---

## ✨ Features

* Configurable data scale
* Realistic order lifecycle simulation
* Refund & fulfillment workflow
* Data consistency validation
* Hive ODS export
* Web UI control panel
* Docker one-command deployment

---

## 🚀 Quick Start (Docker)

### Build Image

docker build -t data-sim .

### Run Service (Windows PowerShell)

docker run --rm -p 8000:8000 -v ${PWD}\out:/app/out data-sim

### Run Service (macOS / Linux)

docker run --rm -p 8000:8000 -v $(pwd)/out:/app/out data-sim

---

### Open Web Console

Web UI
http://127.0.0.1:8000/ui/

API Docs
http://127.0.0.1:8000/docs

---

## 📦 Output Files

Generated datasets are exported to:

out/
└── ods.zip

The folder is mounted from host machine via Docker volume mapping.

---

## ⚙️ Usage

1. Open Web UI
2. Configure dataset parameters
3. Click Generate Data
4. Download ods.zip
5. Import into Hive / MySQL / Spark

---

## 🧱 Project Structure

web/            FastAPI service + Web UI
config.py       Simulation configuration
pipeline.py     Dataset pipeline builder
facts.py        Fact table generator
dims.py         Dimension generator
exporter.py     ODS export logic
service.py      Job execution service

---

## 🐳 Why Docker?

Docker provides:

* No environment setup required
* One-command startup
* Reproducible data generation
* Easy sharing & evaluation

Anyone can run the platform with:

docker run data-sim

---

## 📘 Use Cases

* Data Warehouse practice
* SQL interview preparation
* ETL testing
* Big Data learning
* Teaching demonstrations

---

## License

MIT License
>>>>>>> 7834130 (add docker one-click run and improve README)
