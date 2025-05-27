from infrastructure.clients.api.cache_decorator import with_cache
from infrastructure.clients.api.route_registry import route_registry
from infrastructure.clients.api.base_client import BasePipedriveAPIClient

@route_registry.register(endpoint="/activityFields", version="v1", cost=20)
@with_cache(ttl=86400, key_prefix="pipedrive:activity_fields") # Cache por 1 dia
def fetch_activity_fields(client: BasePipedriveAPIClient, params: dict | None = None):
    url = f"{client.BASE_URL_V1}/activityFields"
    return client.get(url, params=params).json()