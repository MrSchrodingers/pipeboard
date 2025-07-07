from contextlib import contextmanager
from psycopg2.pool import SimpleConnectionPool
from infrastructure.config.settings import settings

class PostgresPool:
    def __init__(
        self,
        dsn: str | None = None,
        minconn: int = 1,
        maxconn: int = 10
    ):
        """
        dsn: se não informado, usa settings.DATABASE_URL
        minconn/maxconn: tamanho mínimo/máximo do pool
        """
        raw = dsn or settings.DATABASE_URL
        if raw.startswith("postgresql+asyncpg://"):
            raw = raw.replace("postgresql+asyncpg://", "postgresql://", 1)
        self._dsn = raw
        self._pool = SimpleConnectionPool(minconn, maxconn, dsn=self._dsn)

    @contextmanager
    def connection(self):
        """
        Context manager para pegar e liberar uma conexão do pool.
        Uso:
            with db_pool.connection() as conn:
                cur = conn.cursor()
                ...
        """
        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("SET TIMEZONE 'America/Sao_Paulo'")
            yield conn
        finally:
            self._pool.putconn(conn)

    def close(self):
        """Fecha todas as conexões do pool."""
        self._pool.closeall()

    @property
    def active(self) -> int:
        """Número de conexões emprestadas (ativas)."""
        return len(getattr(self._pool, "_used", []))

    @property
    def idle(self) -> int:
        """Número de conexões disponíveis (ociosas)."""
        return len(getattr(self._pool, "_pool", []))

def get_postgres_conn() -> PostgresPool:
    return PostgresPool()