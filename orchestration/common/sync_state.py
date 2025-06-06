from __future__ import annotations

from datetime import datetime, timezone
from typing import Tuple, Optional

from infrastructure.db.postgres_adapter import get_postgres_conn

# ──────────────────────────────────────────────────────────────
#  Esquema
#
#  • entity        → nome do recurso (ex.: 'Activity', 'Deal')
#  • last_cursor   → cursor “start” mais recente (NULL = não usado)
#  • backfill_done → TRUE quando concluiu backfill integral
#  • updated_at    → carimbo automático para auditoria
# ──────────────────────────────────────────────────────────────
_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS etl_sync_state (
    entity         TEXT        PRIMARY KEY,
    last_cursor    BIGINT      NULL,
    backfill_done  BOOLEAN     NOT NULL DEFAULT FALSE,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def _ensure_table(cur) -> None:
    """Garante que a tabela etl_sync_state exista."""
    cur.execute(_TABLE_SQL)


# ──────────────────────────────── API pública
def get_sync_state(entity: str) -> Tuple[Optional[int], bool]:
    """
    Retorna (last_cursor, backfill_done).
    • Se ainda não existir linha → (None, False)
    """
    with get_postgres_conn().connection() as conn, conn.cursor() as cur:
        _ensure_table(cur)

        cur.execute(
            "SELECT last_cursor, backfill_done FROM etl_sync_state WHERE entity = %s",
            (entity,),
        )
        row = cur.fetchone()
        return (row[0], row[1]) if row else (None, False)


def save_sync_state(entity: str, cursor: Optional[int], done: bool) -> None:
    """
    Upsert do estado.
    • cursor = None mantém a coluna NULL.
    • done   = marca se o backfill terminou.
    """
    with get_postgres_conn().connection() as conn, conn.cursor() as cur:
        _ensure_table(cur)

        cur.execute(
            """
            INSERT INTO etl_sync_state (entity, last_cursor, backfill_done)
            VALUES (%s, %s, %s)
            ON CONFLICT (entity)
            DO UPDATE SET
                last_cursor   = EXCLUDED.last_cursor,
                backfill_done = EXCLUDED.backfill_done,
                updated_at    = now()
            """,
            (entity, cursor, done),
        )
        conn.commit()
