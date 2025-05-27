from datetime import datetime, timedelta
from typing import Generator, Dict, Optional

from core.ports.pipedrive_client_port import PipedriveClientPort
import structlog

log = structlog.get_logger()

class ExtractService:
    def __init__(self, client: PipedriveClientPort) -> None:
        self.client = client
        self.log = log.bind(service="ExtractService")

    def fetch_deals_stream(
        self, updated_since: Optional[str] = None
    ) -> Generator[Dict, None, None]:
        """
        Retorna um gerador de deals do Pipedrive, a partir de um timestamp opcional.
        """
        self.log.info("Fetching deals stream", updated_since=updated_since)
        return self.client.fetch_all_deals_stream(updated_since=updated_since)

    def get_last_update_timestamp(self) -> Optional[str]:
        """
        Busca o último timestamp salvo para controle incremental.
        """
        try:
            last_timestamp = self.client.get_last_timestamp()
            self.log.debug("Fetched last update timestamp", last_timestamp=last_timestamp)
            return last_timestamp
        except Exception as e:
            self.log.error("Failed to fetch last update timestamp", error=str(e))
            return None

    def update_last_timestamp(self, latest_update_time: datetime) -> None:
        """
        Atualiza o timestamp incremental com um pequeno buffer de 1 segundo.
        """
        try:
            buffered_time = latest_update_time.replace(microsecond=0) + timedelta(seconds=1)
            iso_timestamp = buffered_time.strftime('%Y-%m-%dT%H:%M:%SZ')
            self.client.update_last_timestamp(iso_timestamp)
            self.log.info("Updated last timestamp after ETL run", updated_timestamp=iso_timestamp)
        except Exception as e:
            self.log.error("Failed to update last timestamp after ETL", error=str(e))