# Modified infrastructure/config/settings.py with load_dotenv and default values
from dotenv import load_dotenv
import os

# Load environment variables from .env file if present
load_dotenv()

class Settings:
    """Configurações da aplicação (env vars)."""
    # — API Keys
    PIPEDRIVE_API_KEY      = os.getenv("PIPEDRIVE_API_KEY")
    GITHUB_PAT             = os.getenv("GITHUB_PAT")

    # — Postgres
    POSTGRES_DB            = os.getenv("POSTGRES_DB")
    POSTGRES_USER          = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD      = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_HOST          = os.getenv("POSTGRES_HOST")
    POSTGRES_PORT          = os.getenv("POSTGRES_PORT")
    DATABASE_URL           = os.getenv("DATABASE_URL")

    # — Redis
    REDIS_URL              = os.getenv("REDIS_URL")

    # — Docker / ETL
    IMAGE_NAME             = os.getenv("IMAGE_NAME")
    PREFECT_WORK_POOL_NAME = os.getenv("PREFECT_WORK_POOL_NAME")
    DEFAULT_DOCKER_NETWORK_NAME = os.getenv("DEFAULT_DOCKER_NETWORK_NAME")

    # — Docker registry
    DOCKER_USER            = os.getenv("DOCKER_USER")
    DOCKER_PASS            = os.getenv("DOCKER_PASS")
    DOCKER_REGISTRY_URL    = os.getenv("DOCKER_REGISTRY_URL")

    # — Host ports (defaults provided)
    HOST_PREFECT_PORT      = int(os.getenv("HOST_PREFECT_PORT", "4200"))
    HOST_METRICS_PORT      = int(os.getenv("HOST_METRICS_PORT", "8082"))
    HOST_GRAFANA_PORT      = int(os.getenv("HOST_GRAFANA_PORT", "3000"))
    HOST_POSTGRES_PORT     = int(os.getenv("HOST_POSTGRES_PORT", "5432"))

    # — Outros
    PUSHGATEWAY_ADDRESS    = os.getenv("PUSHGATEWAY_ADDRESS")
    PREFECT_API_URL        = os.getenv("PREFECT_API_URL")

    BATCH_OPTIMIZER_CONFIG = {
        "memory_threshold": 0.8,
        "reduce_factor":    0.7,
        "duration_threshold": 30,
        "increase_factor":  1.2,
        "history_window":   5,
    }

settings = Settings()
