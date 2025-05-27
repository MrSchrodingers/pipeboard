import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List

import structlog

from core.services.etl.extract_service import ExtractService
from core.services.etl.load_service import LoadService
from core.services.etl.monitor_service import MonitorService
from core.services.etl.transform_service import TransformService
from infrastructure.observability.metrics import (
    etl_counter,
    etl_duration_hist,
    etl_last_successful_run_timestamp
)

log = structlog.get_logger()

class ETLService:
    def __init__(
        self,
        extract: ExtractService,
        transform: TransformService,
        load: LoadService,
        monitor: MonitorService,
        batch_size: int = 1000
    ) -> None:
        self.extract = extract
        self.transform = transform
        self.load = load
        self.monitor = monitor
        self.batch_size = batch_size
        self.log = log.bind(service="ETLService")

    def run(self, flow_type: str) -> Dict[str, Any]:
        run_start = time.monotonic()
        run_start_utc = datetime.now(timezone.utc)
        etl_counter.labels(flow_type=flow_type).inc()

        self.monitor.start_memory_tracking()

        result = {
            "status": "error",
            "total_fetched": 0,
            "total_validated": 0,
            "total_loaded": 0,
            "total_failed": 0,
            "start_time": run_start_utc.isoformat(),
            "end_time": None,
            "duration_seconds": 0,
            "peak_memory_mb": 0,
            "message": "ETL did not complete"
        }

        total_fetched = total_validated = total_loaded = total_failed = 0
        latest_update_time: Optional[datetime] = None

        try:
            self.log.info("Starting ETL run", flow_type=flow_type)
            last_timestamp = self.extract.get_last_update_timestamp()
            deal_stream = self.extract.fetch_deals_stream(updated_since=last_timestamp)

            batch = []
            for deal in deal_stream:
                total_fetched += 1
                batch.append(deal)
                update_str = deal.get("update_time")
                if update_str:
                    try:
                        update_dt = datetime.strptime(update_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                        if not latest_update_time or update_dt > latest_update_time:
                            latest_update_time = update_dt
                    except Exception:
                        self.log.warning("Failed to parse update_time", value=update_str)

                if len(batch) >= self.batch_size:
                    validated, failed = self.transform.validate_and_transform_batch(batch, flow_type)
                    total_validated += len(validated)
                    total_failed += failed
                    total_loaded += self.load.load_batch(validated, flow_type)
                    batch.clear()

            # Processa o restante
            if batch:
                validated, failed = self.transform.validate_and_transform_batch(batch, flow_type)
                total_validated += len(validated)
                total_failed += failed
                total_loaded += self.load.load_batch(validated, flow_type)

            if latest_update_time and total_loaded > 0:
                self.extract.update_last_timestamp(latest_update_time)

            result.update({
                "status": "success",
                "total_fetched": total_fetched,
                "total_validated": total_validated,
                "total_loaded": total_loaded,
                "total_failed": total_failed,
                "message": "ETL completed successfully"
            })
        except Exception as e:
            self.log.error("ETL critical failure", error=str(e), exc_info=True)
            result["message"] = f"ETL failed: {str(e)}"
            result["total_failed"] = max(total_failed, total_fetched - total_loaded)
        finally:
            end_utc = datetime.now(timezone.utc)
            duration = time.monotonic() - run_start
            result["duration_seconds"] = round(duration, 3)
            result["end_time"] = end_utc.isoformat()
            result["peak_memory_mb"] = self.monitor.stop_memory_tracking(flow_type)
            self.monitor.collect_system_metrics(flow_type)
            etl_duration_hist.labels(flow_type=flow_type).observe(duration)
            etl_last_successful_run_timestamp.labels(flow_type=flow_type).set(int(end_utc.timestamp()))
            log_level = self.log.info if result["status"] == "success" else self.log.error
            log_level("ETL run completed", **result)
            return result
