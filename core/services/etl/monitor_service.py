import psutil
import tracemalloc
import structlog
from infrastructure.observability.metrics import (
    memory_usage_gauge,
    etl_cpu_usage_percent,
    etl_thread_count,
    etl_disk_usage_bytes,
    etl_heartbeat,
)

log = structlog.get_logger()

class MonitorService:
    def __init__(self, mount_point: str = "/") -> None:
        self.mount_point = mount_point
        self.log = log.bind(service="MonitorService")

    def start_memory_tracking(self) -> None:
        if not tracemalloc.is_tracing():
            tracemalloc.start()
            self.log.debug("Memory tracking started via tracemalloc")

    def stop_memory_tracking(self, flow_type: str) -> float:
        """
        Para o tracemalloc e retorna o pico de memória em MB.
        """
        peak_mb = 0.0
        if tracemalloc.is_tracing():
            try:
                current, peak = tracemalloc.get_traced_memory()
                peak_mb = peak / 1e6
                memory_usage_gauge.labels(flow_type=flow_type).set(peak_mb)
                self.log.debug("Memory usage recorded", current=current, peak=peak)
            except Exception as e:
                self.log.error("Error retrieving memory usage", error=str(e))
            finally:
                tracemalloc.stop()
        return round(peak_mb, 2)

    def collect_system_metrics(self, flow_type: str) -> None:
        """
        Coleta métricas de CPU, threads, disco e atualiza heartbeat.
        """
        try:
            etl_cpu_usage_percent.labels(flow_type=flow_type).set(psutil.cpu_percent())
            etl_thread_count.labels(flow_type=flow_type).set(len(psutil.Process().threads()))
            etl_disk_usage_bytes.labels(mount_point=self.mount_point).set(psutil.disk_usage(self.mount_point).used)
            etl_heartbeat.labels(flow_type=flow_type).set_to_current_time()
            self.log.debug("System metrics collected successfully")
        except Exception as e:
            self.log.warning("Failed to collect system metrics", error=str(e))
