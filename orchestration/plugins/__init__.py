from .sync_deals import sync_pipedrive_deals_flow
from .sync_leads import sync_pipedrive_leads_flow
from .sync_main import main_bi_transformation_flow
from .sync_organizations import sync_pipedrive_organizations_flow
from .sync_persons import sync_pipedrive_persons_flow
from .sync_pipelines import sync_pipedrive_pipelines_flow
from .sync_stages import sync_pipedrive_stages_flow
from .sync_users import sync_pipedrive_users_flow
from .sync_activities import sync_pipedrive_activities_flow
from .sync_activity_types import sync_pipedrive_activity_types_flow

print(f"Módulos de plugin em orchestration.plugins importados; deployments devem ter sido registrados.")