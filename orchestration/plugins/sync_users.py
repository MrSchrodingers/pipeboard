import os
from dotenv import load_dotenv
from infrastructure.config import settings
from orchestration.flows.sync_users_flow import sync_pipedrive_users_flow

load_dotenv()

DOCKER_NETWORK_NAME = settings.DEFAULT_DOCKER_NETWORK_NAME
IMAGE_NAME = settings.IMAGE_NAME
all_env_vars = dict(os.environ.items())

sync_pipedrive_users_flow.deploy(
    name="Sync Pipedrive Users",
    description="Sincroniza usuários (users) do Pipedrive.",
    tags=["pipedrive", "sync", "users"],
    schedule={"cron": "30 3 * * *", "timezone": "America/Sao_Paulo"},
    work_pool_name=settings.PREFECT_WORK_POOL_NAME,
    image=IMAGE_NAME,
    push=False,
    job_variables={
        "env": all_env_vars,
        "pull_policy": "Never"
    }
)