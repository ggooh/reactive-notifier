# Reactive Notifier 

A scalable, asynchronous, event-driven microservices architecture built to handle high-volume notification dispatches with strict fault tolerance. 

This system decouples order ingestion from heavy processing (HTML email generation and delivery) using a Message Broker topology.

---

## Architecture Overview

The system is built as a distributed microservices network leveraging **FastAPI** for low-latency ingestion, **RabbitMQ** for reliable message queuing, and standalone **Python Workers** for horizontal processing.

* **Producer (API Gateway):** A REST API built with FastAPI that validates incoming payloads using Pydantic and instantly publishes orders to the message broker.
* **Message Broker (RabbitMQ):** Manages asynchronous communication using a robust queue architecture with dead-lettering capabilities.
* **Consumers (Workers):** Scalable Python services using `pika` that process messages, render transactional HTML templates, and dispatch outbound emails via SMTP.
* **Mailpit:** Used in development as an SMTP trap to intercept and inspect outgoing emails in a web GUI.

---

## Key Engineering Features

### 1. Robust Fault Tolerance & Failed Message Handling (DLQ)
Equipped with a **Dead Letter Exchange (DLX)** topology. If a consumer encounters unrecoverable payload corruption or validation failures (e.g., missing critical fields like `order_id` or a negative `amount`), the message is rejected with `requeue=False`. RabbitMQ automatically routes these "Corrupted messages" into a dedicated `dead_letter_queue` for auditing, preventing system deadlocks and ensuring zero data loss.

### 2. Horizontal Scalability & Fair Dispatch
The architecture supports **Horizontal Scaling (Scale-Out)** without requiring single-line code changes. Combined with RabbitMQ's Prefetch QoS configuration (`prefetch_count=1`), the system ensures *Fair Dispatch*. Instead of blind round-robin distribution, messages are delivered only to available workers, avoiding processing bottlenecks.

### 3. Configuration Isolation (`.env`)
Adheres to **The Twelve-Factor App** methodologies. Infrastructure connections, ports, and credentials are completely decoupled from the codebase using OS-level environment variables, managed seamlessly via Docker Compose secrets injection.

---

## Tech Stack

* **Language:** Python 3.11+
* **Framework:** FastAPI (Uvicorn)
* **Message Broker:** RabbitMQ
* **Protocols:** AMQP, SMTP, HTTP
* **Containerization:** Docker & Docker Compose
* **Testing SMTP Tool:** Mailpit

---

## Getting Started 

### Prerequisites
Make sure you have **Docker** and **Docker Compose** installed on your system (or running inside WSL).

### 1. Setup Environment Variables
Create a `.env` file in the root directory of the project:
```ini
RABBITMQ_HOST=rabbitmq_broker
SMTP_SERVER=mailpit
SMTP_PORT=1025
SENDER_EMAIL=store@reactive-notifier.com
```

### 2. Boot Up Infrastructure with Scaled Workers
To spin up the entire ecosystem with 3 parallel workers running simultaneously to divide workloads, run:

docker compose up -d --scale worker=3

### 3. Verify Services
Once running, you can access the interactive interfaces:

* **API Documentation (Swagger):** http://localhost:8000/docs

* **RabbitMQ Management Console:** http://localhost:15672 (User/Password: guest/guest)

* **Mailpit Dashboard (Email Capture):** http://localhost:8025

### 4. Inspect Live Concurrent Logs
To see the microservices collaborating and handling load in real-time, tail the unified logs:

docker compose logs -f worker