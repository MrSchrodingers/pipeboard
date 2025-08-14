import os
from dotenv import load_dotenv
from infrastructure.config import settings
from orchestration.flows.sync_main_flow import main_bi_transformation_flow
from prefect.events import DeploymentEventTrigger

load_dotenv()

DOCKER_NETWORK_NAME = settings.DEFAULT_DOCKER_NETWORK_NAME
IMAGE_NAME = settings.IMAGE_NAME
all_env_vars = dict(os.environ.items())

trigger = DeploymentEventTrigger(
    expect={"prefect.flow-run.Completed"},
    match_related={"prefect.resource.name": "Sync Pipedrive Activity Types"}
)

main_bi_transformation_flow.deploy(
    name="Main BI Transformation",
    description="Executa transformações nos dados sincronizados para a camada de BI.",
    tags=["pipedrive", "etl", "bi", "main"],
    work_pool_name=settings.PREFECT_WORK_POOL_NAME,
    # image=IMAGE_NAME,
    push=False,
    triggers=[trigger],
    job_variables={
        "env": all_env_vars
    }
)