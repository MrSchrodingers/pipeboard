import structlog
from typing import Dict, List

from core.ports.data_repository_port import DataRepositoryPort
from infrastructure.clients.pipedrive_api_client import PipedriveAPIClient

log = structlog.get_logger(__name__)

class SyncService:
    def __init__(self, client: PipedriveAPIClient, repository: DataRepositoryPort):
        self.client = client
        self.repository = repository
        self.log = log.bind(service="SyncService")

    def sync_users(self) -> int:
        self.log.info("Starting users sync...")
        users = self.client.call("users.list")
        if not users:
            self.log.warning("No users fetched from API.")
            return 0
        inserted = self.repository.upsert_users(users)
        self.log.info("Users sync completed.", records=inserted)
        return inserted

    def sync_persons(self) -> int:
        self.log.info("Starting persons sync...")
        persons = self.client.call("persons.list")
        if not persons:
            self.log.warning("No persons fetched from API.")
            return 0
        inserted = self.repository.upsert_persons(persons)
        self.log.info("Persons sync completed.", records=inserted)
        return inserted

    def sync_organizations(self) -> int:
        self.log.info("Starting organizations sync...")
        organizations = self.client.call("organizations.list")
        if not organizations:
            self.log.warning("No organizations fetched from API.")
            return 0
        inserted = self.repository.upsert_organizations(organizations)
        self.log.info("Organizations sync completed.", records=inserted)
        return inserted

    def sync_pipelines(self) -> int:
        self.log.info("Starting pipelines sync...")
        pipelines = self.client.call("pipelines.list")
        if not pipelines:
            self.log.warning("No pipelines fetched from API.")
            return 0
        inserted = self.repository.upsert_pipelines(pipelines)
        self.log.info("Pipelines sync completed.", records=inserted)
        return inserted

    def sync_stages(self) -> int:
        self.log.info("Starting stages sync...")
        stages = self.client.call("stages.list")
        if not stages:
            self.log.warning("No stages fetched from API.")
            return 0
        inserted = self.repository.upsert_stages(stages)
        self.log.info("Stages sync completed.", records=inserted)
        return inserted

    def sync_deals(self) -> List[Dict]:
        self.log.info("Starting deals fetch (streaming)...")
        deals = list(self.client.call("deals.stream"))
        self.log.info("Deals fetch completed.", records=len(deals))
        return deals

    def sync_deal_fields(self) -> List[Dict]:
        self.log.info("Fetching deal fields mapping...")
        fields = self.client.call("dealFields.list")
        self.log.info("Deal fields fetched.", fields=len(fields))
        return fields

    def sync_filters(self) -> List[Dict]:
        self.log.info("Fetching filters...")
        filters = self.client.call("filters.list")
        self.log.info("Filters fetched.", records=len(filters))
        return filters

    def sync_leads(self) -> List[Dict]:
        self.log.info("Fetching leads...")
        leads = self.client.call("leads.list")
        self.log.info("Leads fetched.", records=len(leads))
        return leads

    def sync_person_fields(self) -> List[Dict]:
        self.log.info("Fetching person fields...")
        fields = self.client.call("personFields.list")
        self.log.info("Person fields fetched.", fields=len(fields))
        return fields

    def sync_organization_fields(self) -> List[Dict]:
        self.log.info("Fetching organization fields...")
        fields = self.client.call("organizationFields.list")
        self.log.info("Organization fields fetched.", fields=len(fields))
        return fields

    def sync_currencies(self) -> List[Dict]:
        self.log.info("Fetching currencies...")
        currencies = self.client.call("currencies.list")
        self.log.info("Currencies fetched.", currencies=len(currencies))
        return currencies

    def full_sync(self) -> Dict[str, int]:
        """
        Sincroniza todas as entidades persistentes.
        Atenção: apenas entidades realmente salvas no banco (não leads, filtros etc.).
        """
        self.log.info("Starting full sync of Pipedrive entities...")
        result = {
            "users": self.sync_users(),
            "persons": self.sync_persons(),
            "organizations": self.sync_organizations(),
            "pipelines": self.sync_pipelines(),
            "stages": self.sync_stages(),
        }
        self.log.info("Full sync completed.", summary=result)
        return result
