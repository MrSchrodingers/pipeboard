from infrastructure.clients.api.cache_decorator import with_cache
from infrastructure.clients.api.route_registry import route_registry

@route_registry.register(endpoint="/persons", version="v2", cost=10)
@with_cache(ttl=3600, key_prefix="pipedrive:persons")
def fetch_persons(client, params=None):
    """
    Busca todos os persons (contatos) usando V2.
    """
    url = f"{client.BASE_URL_V2}/persons"
    return client.get(url, params=params).json()

@route_registry.register(endpoint="/persons/detail", version="v2", cost=1)
@with_cache(ttl=3600, key_prefix="pipedrive:person_by_id")
def fetch_person_by_id(client, person_id: int):
    """
    Busca um contato específico (persons/{id}) em V2.
    """
    url = f"{client.BASE_URL_V2}/persons/{person_id}"
    return client.get(url).json()
