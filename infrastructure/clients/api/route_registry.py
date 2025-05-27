from typing import Callable, Dict

class RouteInfo:
    def __init__(self, endpoint: str, version: str, cost: int, fetch_fn: Callable):
        self.endpoint = endpoint
        self.version = version  # 'v1' ou 'v2'
        self.cost = cost
        self.fetch_fn = fetch_fn

class RouteRegistry:
    def __init__(self):
        self.routes: Dict[str, RouteInfo] = {}

    def register(self, endpoint: str, version: str, cost: int = 10):
        def decorator(fetch_fn: Callable):
            self.routes[endpoint] = RouteInfo(endpoint, version, cost, fetch_fn)
            return fetch_fn
        return decorator

    def get_route_info(self, endpoint: str) -> RouteInfo:
        if endpoint not in self.routes:
            raise ValueError(f"Endpoint {endpoint} not registered in the RouteRegistry.")
        return self.routes[endpoint]

    def all_routes(self) -> Dict[str, RouteInfo]:
        return self.routes

# Instância global
route_registry = RouteRegistry()
