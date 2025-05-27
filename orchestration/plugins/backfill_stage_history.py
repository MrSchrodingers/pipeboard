import os
from dotenv import load_dotenv
from infrastructure.config import settings
from orchestration.flows.backfill_stage_history_flow import backfill_stage_history_flow

load_dotenv()

DOCKER_NETWORK_NAME = settings.DEFAULT_DOCKER_NETWORK_NAME
IMAGE_NAME = settings.IMAGE_NAME

all_env_vars = dict(os.environ.items())

backfill_stage_history_flow.deploy(
    name="Pipedrive Backfill Stage History",
    description="Preenche o histórico de stages para deals antigos.",
    parameters={"daily_deal_limit": 10000, "db_batch_size": 1000},
    work_pool_name=settings.PREFECT_WORK_POOL_NAME,
    image=IMAGE_NAME,
    push=False,
    job_variables={
        "image_pull_policy": "Always",
        "networks": [DOCKER_NETWORK_NAME],
        "auto_remove": True,
        "env": all_env_vars
    }
)