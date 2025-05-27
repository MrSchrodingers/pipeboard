#!/usr/bin/env python3
import asyncio
import os
from typing import Optional

from dotenv import load_dotenv
import structlog

from prefect.blocks.core import Block
from prefect.blocks.system import JSON, Secret
from prefect_docker.credentials import DockerRegistryCredentials
from prefect_docker.host import DockerHost

GITHUB_SECRET_BLOCK_NAME = "github-access-token"
POSTGRES_JSON_BLOCK_NAME = "postgres-pool"
REDIS_JSON_BLOCK_NAME = "redis-cache"
DOCKER_REGISTRY_BLOCK_NAME = "docker-registry"
DOCKER_HOST_BLOCK_NAME = "docker-host"
DEFAULT_NETWORK_NAME = os.getenv("COMPOSE_NETWORK_NAME", "prefect_internal_network")

if not structlog.is_configured():
    try:
        structlog.configure(
            processors=[
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.dev.ConsoleRenderer(),
            ],
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )
    except structlog.exceptions.AlreadyConfiguredError:
        pass
log = structlog.get_logger(__name__)


async def save_block_safe(block_instance: Block, name: str, overwrite: bool = True) -> Optional[Block]:
    try:
        await block_instance.save(name=name, overwrite=overwrite)
        log.info(f"Bloco '{name}' salvo com sucesso.")
        return block_instance
    except Exception:
        log.error(f"Falha ao salvar o bloco '{name}'.", exc_info=True)
        return None


async def load_block_safe(block_class: type[Block], name:str) -> Optional[Block]:
    try:
        loaded_block = await block_class.load(name=name)
        log.info(f"Bloco '{name}' carregado com sucesso.")
        return loaded_block
    except ValueError:
        log.warn(f"Bloco '{name}' da classe '{block_class.__name__}' não encontrado ou falha ao carregar.")
    except Exception:
        log.error(f"Erro ao carregar o bloco '{name}' da classe '{block_class.__name__}'.", exc_info=True)
    return None


async def setup_all_blocks():
    log.info("--- Iniciando a configuração dos Blocos Prefect ---")
    load_dotenv()

    github_pat = os.getenv("GITHUB_PAT")
    database_url = os.getenv("DATABASE_URL")
    redis_url = os.getenv("REDIS_URL")
    docker_user = os.getenv("DOCKER_USER")
    docker_pass = os.getenv("DOCKER_PASS")
    docker_registry_url = os.getenv("DOCKER_REGISTRY_URL")

    log.info("--- Configurando blocos padrão ---")

    if github_pat:
        await save_block_safe(Secret(value=github_pat), GITHUB_SECRET_BLOCK_NAME)
    else:
        log.warn(f"Variável de ambiente GITHUB_PAT não definida. Pulando criação do bloco '{GITHUB_SECRET_BLOCK_NAME}'.")

    if database_url:
        db_config = {
            "dsn": database_url,
            "min_size": int(os.getenv("DB_MIN_CONN", 1)),
            "max_size": int(os.getenv("DB_MAX_CONN", 10)),
            "max_queries": 500,
            "max_inactive_connection_lifetime": 300
        }
        log.info(
            "AVISO DE DEPRECIAÇÃO: O bloco 'prefect.blocks.system.JSON' será removido após Junho de 2025. "
            f"Planeje a migração do '{POSTGRES_JSON_BLOCK_NAME}' para 'prefect.variables.Variable'."
        )
        await save_block_safe(JSON(value=db_config), POSTGRES_JSON_BLOCK_NAME)
    else:
        log.warn(f"Variável de ambiente DATABASE_URL não definida. Pulando criação do bloco '{POSTGRES_JSON_BLOCK_NAME}'.")

    if redis_url:
        redis_config = {"connection_string": redis_url}
        log.info(
            "AVISO DE DEPRECIAÇÃO: O bloco 'prefect.blocks.system.JSON' será removido após Junho de 2025. "
            f"Planeje a migração do '{REDIS_JSON_BLOCK_NAME}' para 'prefect.variables.Variable'."
        )
        await save_block_safe(JSON(value=redis_config), REDIS_JSON_BLOCK_NAME)
    else:
        log.warn(f"Variável de ambiente REDIS_URL não definida. Pulando criação do bloco '{REDIS_JSON_BLOCK_NAME}'.")

    if docker_user and docker_pass:
        registry_creds_block = DockerRegistryCredentials(
            username=docker_user,
            password=docker_pass,
            registry_url=docker_registry_url,
            reauth=True
        )
        await save_block_safe(registry_creds_block, DOCKER_REGISTRY_BLOCK_NAME)
    else:
        log.info(
            f"Variáveis DOCKER_USER ou DOCKER_PASS não definidas. "
            f"Pulando criação do bloco de credenciais '{DOCKER_REGISTRY_BLOCK_NAME}'."
        )

    log.info(f"Configurando o bloco '{DOCKER_HOST_BLOCK_NAME}' para usar a rede Docker: '{DEFAULT_NETWORK_NAME}'.")
    docker_host_config = DockerHost(networks=[DEFAULT_NETWORK_NAME])
    await save_block_safe(docker_host_config, DOCKER_HOST_BLOCK_NAME)

    log.info(f"Verificando configuração do bloco '{DOCKER_HOST_BLOCK_NAME}' após salvamento...")
    loaded_docker_host = await load_block_safe(DockerHost, DOCKER_HOST_BLOCK_NAME)
    if loaded_docker_host:
        config_data = loaded_docker_host.model_dump(exclude_unset=True)
        log.info(
            f"Configuração carregada para '{DOCKER_HOST_BLOCK_NAME}': "
            f"base_url='{config_data.get('base_url')}', "
            f"networks='{config_data.get('networks')}', "
            f"max_pool_size='{config_data.get('max_pool_size')}'"
        )
    else:
        log.warn(f"Não foi possível carregar '{DOCKER_HOST_BLOCK_NAME}' para verificação.")

    log.info("--- Configuração dos Blocos Prefect Finalizada ---")


if __name__ == "__main__":
    asyncio.run(setup_all_blocks())