# Real-Time Flight Tracker

A production-grade real-time flight tracking system demonstrating big data engineering, microservices architecture, and cloud data processing.

## Features

- **Real-time Flight Tracking**: Live flight data from OpenSky Network API
- **Big Data Processing**: Databricks integration for scalable analytics
- **Microservices Architecture**: Containerized services with Docker
- **Real-time Dashboard**: React frontend with interactive maps
- **Workflow Orchestration**: Apache Airflow for pipeline management
- **Streaming Pipeline**: Apache Kafka for real-time data processing

## System Architecture

```
OpenSky API → Kafka → FastAPI → Databricks → React Frontend
     ↓
  Airflow (Orchestration)
```

## Tech Stack

**Backend & Data:**
- FastAPI - High-performance backend
- Apache Kafka - Real-time streaming
- Databricks - Cloud data lakehouse
- Apache Airflow - Workflow orchestration
- Docker - Containerization

**Frontend:**
- React - Modern web dashboard
- Leaflet - Interactive maps
- Vite - Build tool

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Databricks account (free tier available)
- Python 3.8+

### 1. Clone Repository
```bash
git clone https://github.com/yourusername/realtime-flight-tracker.git
cd realtime-flight-tracker
```

### 2. Environment Setup
```bash
# Copy environment template
cp .env.example .env

# Edit .env with your credentials
# Add Databricks server hostname, token, and http path
```

### 3. Start Services
```bash
# Using PowerShell (Windows)
.\start-flight-tracker.ps1

# Using Docker Compose
docker-compose up -d
```

### 4. Access Applications
- **Frontend Dashboard**: http://localhost:3000
- **Backend API**: http://localhost:8000
- **Airflow UI**: http://localhost:8080 (admin/admin)
- **API Documentation**: http://localhost:8000/docs

## API Endpoints

### Flight Data
- `GET /flights/current` - Get active flights
- `GET /flights/stats` - Get flight statistics
- `POST /flights/ingest` - Ingest flight data

### System Health
- `GET /health` - Service health check
- `GET /databricks/status` - Databricks connection status

## Project Structure

```
realtime-flight-tracker/
├── airflow/          # Workflow orchestration
├── flight-backend/   # FastAPI backend service
├── flight-frontend/  # React dashboard
├── flight-producer/  # Data ingestion service
├── docker-compose.yml # Multi-container setup
└── README.md
```

## Services Overview

### Flight Producer
- Fetches real-time data from OpenSky API every 30 seconds
- Processes and enriches flight data
- Sends data to backend API and Kafka

### FastAPI Backend
- REST API for flight data
- Databricks integration for data storage
- Business logic and data processing

### React Frontend
- Real-time flight visualization
- Interactive map with Leaflet
- Auto-refreshing statistics

### Airflow
- Workflow orchestration
- Data pipeline monitoring
- Scheduled data processing

## Data Flow

1. **Data Ingestion**: OpenSky API → Flight Producer
2. **Stream Processing**: Producer → Kafka → Backend
3. **Data Storage**: Backend → Databricks Delta Tables
4. **Visualization**: Backend → React Frontend
5. **Orchestration**: Airflow monitors entire pipeline

## Business Use Cases

- **Airlines**: Real-time fleet monitoring
- **Airports**: Traffic management and analytics
- **Travel Industry**: Flight status tracking
- **Data Teams**: Big data processing examples

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request


## Acknowledgments

- OpenSky Network for flight data API
- Databricks for data lakehouse platform
- Apache Foundation for Kafka and Airflow
