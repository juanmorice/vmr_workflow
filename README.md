# VMR Scorecard Pipeline

Automated VMR (Vendor Market Research) scorecard generation using Apache Airflow and Docker.

## Quick Start (New Computer)

### Prerequisites
- Docker Desktop installed
- Git installed

### Setup Steps

```bash
# 1. Clone the repository
git clone https://github.com/YOUR_USERNAME/AIRFLOW_project.git
cd AIRFLOW_project

# 2. Create your .env file from template
cp .env.example .env

# 3. Edit .env with your credentials
# - YELLOWBRICK_USER / YELLOWBRICK_PASSWORD
# - SMTP email credentials

# 4. Create required folders
mkdir -p outputs logs plugins

# 5. Start Airflow
docker-compose up -d

# 6. Access Airflow UI
# URL: http://localhost:8080
# Login: airflow / airflow
```

## Project Structure

```
├── .env.example          # Template for environment variables
├── docker-compose.yaml   # Docker configuration
├── config/               # Airflow configuration
├── dags/                 # Airflow DAGs
│   └── vmr_dag.py       # Main VMR pipeline DAG
├── src/                  # Source code
│   ├── vmr_standalone.py # Standalone script (no Airflow)
│   ├── excelfilefetcher.py
│   ├── gettinglmcdataframe.py
│   ├── runningvmrscorecard_excel.py
│   ├── local_modules/    # Yellowbrick & LMC utilities
│   ├── other_modules/    # Excel updater
│   ├── reports/          # Scorecard generation logic
│   └── templates/        # PowerPoint template
├── outputs/              # Generated reports (gitignored)
└── logs/                 # Runtime logs (gitignored)
```

## Running Without Airflow

```bash
# Activate virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\Activate.ps1 # Windows

# Install dependencies
pip install -e .

# Run standalone
python src/vmr_standalone.py --dry-run     # Preview
python src/vmr_standalone.py               # Process all
python src/vmr_standalone.py --id 24       # Process specific ID
```

## Common Commands

```bash
# Start Airflow
docker-compose up -d

# Stop Airflow
docker-compose down

# View logs
docker logs airflow_project-airflow-worker-1 --tail 100

# Restart worker (after code changes)
docker restart airflow_project-airflow-worker-1
```
