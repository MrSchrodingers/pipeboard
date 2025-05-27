import os
from dotenv import load_dotenv
from infrastructure.config import settings
from orchestration.flows.sync_leads_flow import sync_pipedrive_leads_flow
from prefect.events import DeploymentEventTrigger

load_dotenv()

DOCKER_NETWORK_NAME = settings.DEFAULT_DOCKER_NETWORK_NAME
IMAGE_NAME = settings.IMAGE_NAME
all_env_vars = dict(os.environ.items())

trigger = DeploymentEventTrigger(
    expect={"prefect.flow-run.Completed"},
    match_related={"prefect.resource.name": "Sync Pipedrive Persons"}
)

sync_pipedrive_leads_flow.deploy(
    name="Sync Pipedrive Leads",
    description="Sincroniza leads do Pipedrive.",
    tags=["pipedrive", "sync", "leads"],
    work_pool_name=settings.PREFECT_WORK_POOL_NAME,
    image=IMAGE_NAME,
    push=False,
    triggers=[trigger],
    job_variables={
        "image_pull_policy": "Always",
        "networks": [DOCKER_NETWORK_NAME],
        "auto_remove": True,
        "env": all_env_vars
    }
)