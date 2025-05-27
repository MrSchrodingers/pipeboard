import base64
import gzip
import json
from datetime import timedelta
from typing import Any, Optional

import redis
import structlog

log = structlog.get_logger(__name__)

class RedisCache:
    def __init__(self, connection_string: str, enable_compression: bool = False):
        self.client = redis.Redis.from_url(
            connection_string,
            decode_responses=True,
            socket_timeout=3,
            socket_connect_timeout=3,
            retry_on_timeout=True
        )
        self.enable_compression = enable_compression

    # --- compressão opcional para payloads grandes ---
    def _compress(self, value: str) -> str:
        return base64.b64encode(gzip.compress(value.encode("utf-8"))).decode("utf-8")

    def _decompress(self, value: str) -> str:
        return gzip.decompress(base64.b64decode(value.encode("utf-8"))).decode("utf-8")

    # --- Métodos principais ---
    def get(self, key: str) -> Optional[Any]:
        value = self.client.get(key)
        if value is not None:
            log.debug("Redis cache hit", key=key)
            try:
                if self.enable_compression:
                    value = self._decompress(value)
                return json.loads(value)
            except (json.JSONDecodeError, gzip.BadGzipFile, ValueError):
                log.warning("Failed to decode cached value", key=key)
                return value
        else:
            log.debug("Redis cache miss", key=key)
        return None

    def set(self, key: str, value: Any, ex_seconds: int = 3600):
        if not isinstance(value, (dict, list, str, int, float, bool)):
            raise TypeError(f"Cannot cache value of type {type(value)}")

        serialized = json.dumps(value) if isinstance(value, (dict, list)) else str(value)
        if self.enable_compression:
            serialized = self._compress(serialized)

        self.client.setex(
            name=key,
            time=timedelta(seconds=ex_seconds),
            value=serialized
        )
        log.debug("Redis cache set", key=key, ttl=ex_seconds)

    def delete(self, key: str):
        self.client.delete(key)
        log.debug("Redis cache delete", key=key)

    # --- Métodos auxiliares ---
    def exists(self, key: str) -> bool:
        return self.client.exists(key) == 1

    def ttl(self, key: str) -> int:
        return self.client.ttl(key)

    def clear_prefix(self, prefix: str):
        deleted = 0
        for key in self.client.scan_iter(f"{prefix}*"):
            self.client.delete(key)
            deleted += 1
        log.info("Redis cache clear by prefix", prefix=prefix, keys_deleted=deleted)
