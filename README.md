# Weather Data Pipeline

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
[![Airflow](https://img.shields.io/badge/Airflow-3.0.3-red.svg)](https://airflow.apache.org/)
[![DBT](https://img.shields.io/badge/DBT-1.9-orange.svg)](https://www.getdbt.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14-blue.svg)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Enabled-blue.svg)](https://www.docker.com/)

> A modern data engineering project that fetches, transforms, and visualizes real-time weather data using Apache Airflow, DBT, and Superset in a fully containerized environment.

## Architecture

```
┌─────────────┐     ┌──────────┐     ┌──────┐     ┌───────────┐
│ Weather API │ --> │ Airflow  │ --> │ DBT  │ --> │ Superset  │
│ (Source)    │     │ (ETL)    │     │(Transform)│ │(Visualize)│
└─────────────┘     └──────────┘     └──────┘     └───────────┘
                          │
                          v
                    ┌──────────┐
                    │PostgreSQL│
                    │(Storage) │
                    └──────────┘
```

## Tech Stack

- **Orchestration**: Apache Airflow 3.0.3
- **Data Warehouse**: PostgreSQL 14.18
- **Transformation**: DBT (dbt-postgres 1.9)
- **Visualization**: Apache Superset 3.0.0
- **Caching**: Redis 7
- **Containerization**: Docker & Docker Compose

## Project Structure

```
weather_data/
├── src/                          # Source code
│   ├── pipelines/                # Data ingestion pipelines
│   │   ├── __init__.py
│   │   ├── api_request.py        # API fetching logic
│   │   └── insert_records.py     # Database insertion logic
│   └── check_api_data.py         # API testing utility
├── airflow/                      # Airflow orchestration
│   └── dags/                     # DAG definitions
│       └── orchestrator.py       # Main ETL workflow
├── dbt/                          # DBT transformations
│   ├── my_project/               # DBT project
│   │   ├── models/               # SQL models
│   │   │   ├── staging/          # Staging models
│   │   │   │   └── stg_weather_data.sql
│   │   │   ├── mart/             # Data mart models
│   │   │   │   ├── mart_current_weather.sql
│   │   │   │   ├── mart_daily_summary.sql
│   │   │   │   ├── mart_weather_trends.sql
│   │   │   │   └── mart_air_quality.sql
│   │   │   └── sources/          # Source definitions
│   │   │       └── sources.yml
│   │   ├── dbt_project.yml       # DBT config
│   │   ├── target/               # Compiled SQL (gitignored)
│   │   └── README.md             # DBT documentation
│   └── profiles.yml              # DBT connection profiles
├── postgres/                     # PostgreSQL initialization
│   ├── airflow_init.sql          # Airflow DB setup
│   └── superset_init.sql         # Superset DB setup
├── tests/                        # Test suite
│   ├── unit/                     # Unit tests
│   │   ├── test_api_request.py
│   │   └── test_insert_records.py
│   └── integration/              # Integration tests
│       └── test_pipeline.py
├── docker-compose.yml            # Multi-container setup
├── requirements.txt              # Python dependencies
├── .env                          # Environment variables (gitignored)
├── .env.example                  # Environment template
├── .gitignore                    # Git ignore rules
├── weatherstack.txt              # API reference documentation
├── create_superset_charts.py     # Superset automation script
└── README.md                     # This file
```

## Features

- Automated weather data collection every 5 minutes
- Real-time data ingestion from WeatherStack API
- Data deduplication and transformation
- Time zone aware data processing
- Docker-based deployment
- Airflow-orchestrated ETL pipeline
- Superset dashboards for visualization

## Prerequisites

- Docker Desktop
- Docker Compose
- 8GB+ RAM recommended
- Ports available: 5001, 8000, 8088, 6379

## Quick Start

### 1. Clone and Setup

```bash
cd weather_data
cp .env.example .env
# Edit .env and add your WeatherStack API key
```

### 2. Start Services

```bash
docker-compose up -d
```

### 3. Access Services

- **Airflow UI**: http://localhost:8000
  - Username: Check Airflow logs for auto-generated credentials
  - Default: `admin` / check container logs

- **Superset UI**: http://localhost:8088
  - Configure admin user via environment variables

- **PostgreSQL**:
  - Host: localhost
  - Port: 5001
  - Connection details in `.env` file

## Configuration

### Environment Variables

Copy `.env.example` to `.env` and configure:

```bash
# WeatherStack API
WEATHER_API_KEY=your_weatherstack_api_key_here
WEATHER_API_CITY="New York"
WEATHER_API_BASE_URL=http://api.weatherstack.com/current

POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=your_db_name
POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_postgres_password

# Airflow Database Config
AIRFLOW_DB_USER=your_airflow_user
AIRFLOW_DB_PASSWORD=your_airflow_password
AIRFLOW_DB_NAME=your_airflow_db_name

PROJECT_ROOT=/path/to/your/weather-data-pipeline
DOCKER_NETWORK=weather-data-pipeline

# Superset Config
SUPERSET_SECRET_KEY=your_secret_key_change_in_production
SUPERSET_ADMIN_USERNAME=your_admin_username
SUPERSET_ADMIN_PASSWORD=your_admin_password
DATABASE_DIALECT=postgresql
DATABASE_HOST=postgres
DATABASE_PORT=5432
DATABASE_DB=your_superset_db_name
DATABASE_USER=your_superset_user
DATABASE_PASSWORD=your_superset_password
REDIS_HOST=redis_cache
REDIS_PORT=6379
```

### Database Initialization

The project includes SQL initialization files in the `postgres/` directory (`airflow_init.sql` and `superset_init.sql`). These files contain placeholder credentials to ensure security when committing to version control.

**Before running the project**, you must update these files to match the credentials you configured in your `.env` file:

1.  **Open** `postgres/airflow_init.sql` and replace `your_user`, `your_password`, and `your_database` with your actual Airflow database credentials.
2.  **Open** `postgres/superset_init.sql` and replace `your_user`, `your_password`, and `your_database` with your actual Superset database credentials.

> **Note**: These SQL files are only executed the *first time* the database volume is created. If you have already built the containers and need to apply changes, you will need to remove the volume (`docker-compose down -v`) and rebuild.

### Airflow DAG Schedule

Default: Runs every 5 minutes

To modify, edit `airflow/dags/orchestrator.py`:

```python
schedule_interval=timedelta(minutes=5)  # Change interval here
```

## DBT Models

### Staging Layer
- `stg_weather_data`: Deduplicated and cleaned raw data

### Mart Layer
- `weather_report`: Current weather metrics
- `daily_avg`: Daily aggregated statistics

### Run DBT Manually

```bash
docker exec -it dbt_container dbt run
docker exec -it dbt_container dbt test
```

## Development

### Install Python Dependencies

```bash
pip install -r requirements.txt
```

### Running Tests

```bash
pytest tests/
```

### Code Style

This project follows PEP 8 guidelines.

## Data Pipeline Flow

1. **Extract**: Airflow triggers Python operator to fetch weather data from API
2. **Load**: Raw data inserted into `dev.raw_weather_data` table
3. **Transform**: Docker operator triggers DBT to transform data
4. **Serve**: Transformed data available in staging/mart schemas for Superset

## Monitoring

### Check Airflow DAG Status

```bash
docker exec -it airflow_container airflow dags list
docker exec -it airflow_container airflow dags state weather-api-orchestrator
```

### View Logs

```bash
# Airflow logs
docker logs airflow_container

# DBT logs
docker logs dbt_container

# PostgreSQL logs
docker logs postgres_container
```

### Database Queries

```bash
# Connect to database (use your configured credentials)
docker exec -it postgres_container psql -U <your_user> -d <your_db>

# Check raw data count
SELECT COUNT(*) FROM dev.raw_weather_data;

# View latest weather data
SELECT * FROM dev.raw_weather_data ORDER BY inserted_at DESC LIMIT 10;
```

## Troubleshooting

### DAG Not Appearing in Airflow

- Check DAG file syntax: `docker exec airflow_container python /opt/airflow/dags/orchestrator.py`
- Restart Airflow: `docker restart airflow_container`

### DBT Connection Issues

- Verify `dbt/profiles.yml` has correct database credentials
- Ensure `db` service is running: `docker ps | grep postgres`

### Superset Can't Connect to Database

- Use host `db` (Docker network name) not `localhost`
- Port: 5432 (internal container port)

## Production Considerations

- [ ] Use secret management (AWS Secrets Manager, Vault)
- [ ] Set up monitoring (Prometheus, Grafana)
- [ ] Configure proper logging (ELK stack)
- [ ] Implement data quality checks
- [ ] Add alerting for pipeline failures
- [ ] Use managed databases (AWS RDS, Cloud SQL)
- [ ] Set up CI/CD pipeline
- [ ] Add data lineage tracking
- [ ] Implement proper backup strategy

## API Rate Limits

WeatherStack Free Plan:
- 1000 requests/month
- ~33 requests/day
- Current schedule: ~288 requests/day (every 5 min)

**Recommendation**: Adjust schedule to hourly for production use.

## Contributing

1. Create feature branch
2. Make changes
3. Write tests
4. Submit pull request

## License

MIT License

## Support

For issues and questions, please open an issue in the repository.

## Acknowledgments

- Weather data provided by [WeatherStack API](https://weatherstack.com/)
- Built with Apache Airflow, DBT, and Superset
