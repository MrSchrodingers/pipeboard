import os
from dotenv import load_dotenv
from infrastructure.config import settings
from orchestration.flows.backfill_stage_history_flow import backfill_pipedrive_stage_history_flow
from prefect.events import DeploymentEventTrigger

load_dotenv()

DOCKER_NETWORK_NAME = settings.DEFAULT_DOCKER_NETWORK_NAME
IMAGE_NAME = settings.IMAGE_NAME
all_env_vars = dict(os.environ.items())

trigger = DeploymentEventTrigger(
    expect={"prefect.flow-run.Completed"},
    match_related={"prefect.resource.name": "Main BI Transformation"},
)

backfill_pipedrive_stage_history_flow.deploy(
    name="Backfill Pipedrive Stage History",
    description="Sincroniza histórico de estágios de negócios.",
    tags=["pipedrive", "sync", "stage-history"],
    work_pool_name=settings.PREFECT_WORK_POOL_NAME,
    image=IMAGE_NAME,
    push=False,
    triggers=[trigger],
    job_variables={
        "env": all_env_vars,
        "image_pull_policy": "Never"
    }
)