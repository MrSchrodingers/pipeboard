from datetime import timedelta, timezone as dt_timezone

from infrastructure.db.postgres_adapter import get_postgres_conn
from orchestration.common.utils import get_last_successful_run_ts, has_recent_updates_in_dependencies


def compute_updated_since(
    *, flow_id: str, deps_cfg: dict, logger, skew_sec: int = 5
) -> tuple[str | None, bool]:
    """
    Retorna (updated_since_iso, is_full_sync)
    """
    with get_postgres_conn().connection() as conn, conn.cursor() as cur:
        last_ok = get_last_successful_run_ts(flow_id, cur)
        if not last_ok:                           # nunca rodou
            return None, True                    # full

        deps_changed = has_recent_updates_in_dependencies(
            dependent_config=deps_cfg,
            since_timestamp=last_ok,
            conn=conn,
            logger=logger,
        )
        if deps_changed:
            return None, True                    # full

        skewed = (last_ok - timedelta(seconds=skew_sec)).astimezone(
            dt_timezone.utc
        )
        return skewed.strftime("%Y-%m-%dT%H:%M:%SZ"), False
