# FocusTrack: Real-Time Student Engagement Monitoring System

##  Overview

**FocusTrack** is a real-time academic analytics platform designed to monitor and analyze student engagement in large-scale educational environments.

It processes high-frequency student activity data using **Apache Kafka** and **Apache Flink** to compute a **Focus Index (0–100)**, detect performance trends, identify **at-risk students**, and generate **mentor alerts** in real-time.

---

##  Architecture

```
Node.js Producer → Kafka → Flink → Kafka → Backend → MongoDB → WebSocket → React Dashboard
```

---

##  Tech Stack

* **Frontend:** React (Vite, Tailwind CSS)
* **Backend:** Node.js (Express, Socket.io)
* **Streaming:** Apache Kafka
* **Processing:** Apache Flink (Java, DataStream API)
* **Database:** MongoDB

---

##  Key Features

*  Real-time student activity tracking
*  Focus Index calculation (0–100)
*  Trend detection (Improving / Declining / Stable)
*  At-risk student identification
*  Intelligent mentor alert system
*  Department & section-level analytics
*  Live dashboard updates using WebSockets

---

##  Core Logic

### Focus Index Calculation

```
Focus Index = (Accuracy * 70) + (Engagement * 30)
```

* **Accuracy:** Ratio of correct responses
* **Engagement:** Normalized time spent

---

##  System Workflow

1. **Producer:** Generates student activity events
2. **Kafka:** Streams data through topics
3. **Flink:** Processes events using windowing and state
4. **Backend:** Stores results and exposes APIs
5. **Frontend:** Displays real-time analytics dashboard

---

##  How to Run the Project

### 1. Start Kafka & Zookeeper

```
bin\windows\zookeeper-server-start.bat config\zookeeper.properties
bin\windows\kafka-server-start.bat config\server.properties
```

---

### 2. Start Flink

```
bin\start-cluster.bat
```

---

### 3. Run Producer

```
cd producer
node producer.js
```

---

### 4. Run Backend

```
cd backend
node index.js
```

---

### 5. Run Frontend

```
cd frontend
npm install
npm run dev
```


##  Results

* Handles **2000+ students in real-time**
* Low-latency data processing pipeline
* Accurate engagement monitoring
* Instant UI updates without refresh

---

##  Future Scope

*  AI-based performance prediction
*  Mobile application integration
*  Cloud deployment (AWS / GCP)
*  Advanced analytics dashboards

---

##  Author

**T. Sai Manish**
B.E IT – CBIT

---

##  Project Type

Academic Mini Project – Real-Time Big Data Streaming System

---

##  Note

This project demonstrates a **production-style real-time streaming architecture** using Kafka and Flink integrated with a full-stack web application.
