from datetime import datetime, timedelta
from typing import Literal, Optional

from orchestration.common.utils import get_last_successful_run_ts

Mode = Literal["FULL", "INCREMENTAL", "SKIP"]

SAFETY_SKEW = timedelta(seconds=5) 

def decide_sync_mode(
    *,
    flow_id: str,
    endpoint_version: Literal["v1", "v2"],
    deps_changed: bool,
    cur,
) -> tuple[Mode, Optional[datetime]]:
    """
    Retorna ('FULL' | 'INCREMENTAL' | 'SKIP', updated_since_timestamp_or_none)
    """
    last_ok = get_last_successful_run_ts(flow_id, cur)

    if last_ok is None:
        return "FULL", None                         # 1. nunca rodou

    if endpoint_version == "v1":                    # 2. v1 sempre full
        return "FULL", None

    if deps_changed:                                # 3a. v2 + deps alteradas → incremental
        return "INCREMENTAL", last_ok - SAFETY_SKEW

    return "SKIP", None                             # 3b. nada mudou
