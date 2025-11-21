# Kafka-based System - Big Data Assignment

A **real-time data streaming system** built using **Apache Kafka**, featuring **Avro serialization**, **Dead Letter Queue (DLQ)** support, **retry logic**, and **consumer metrics**.  
This system simulates order-based data processing for Big Data coursework.

---

## 📋 Project Overview

This project demonstrates a complete Kafka pipeline including:

- **Producer**  
  Generates mock order data, serializes using Avro, and publishes to a Kafka topic.

- **Consumer**  
  Consumes orders, processes them, calculates running averages, retries failed messages, and sends permanently failed messages to a DLQ.

- **Dead Letter Queue (DLQ)**  
  Stores messages that fail processing after all retry attempts.

- **Avro Schema**  
  Enforces structure and data consistency.

---
## 🚀 Quick Start

### ✅ Prerequisites
- Docker  
- Docker Compose  

---

## ▶️ Installation & Running

### 1. Clone the repository
```bash
git clone https://github.com/DhananjiMadhushika/Kafka-based-system-Big-Data-Assignment-.git
cd Kafka-based-system-Big-Data-Assignment

# Start Kafka, producer, and consumer
docker-compose up -d
