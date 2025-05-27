````markdown
# Pipeboard ETL Integration

[![Python Version](https://img.shields.io/badge/python-3.12-blue)](https://www.python.org/) [![Prefect Version](https://img.shields.io/badge/prefect-3.4.1-green)](https://prefect.io/) [![License](https://img.shields.io/badge/license-MIT-lightgrey)](LICENSE)

## Descrição

Pipeboard é uma solução de ETL (Extract, Transform, Load) e sincronização incremental de dados entre o Pipedrive e um banco de dados PostgreSQL, projetada para cargas em larga escala e operações de backfill. Utiliza Prefect para orquestração, Redis para cache, e oferece monitoramento completo via Prometheus e dashboards no Grafana.

## 🔖 Table of Contents

- [Visão Geral](#visão-geral)
- [Funcionalidades](#funcionalidades)
- [Arquitetura](#arquitetura)
- [Tecnologias](#tecnologias)
- [Instalação e Execução](#instalação-e-execução)
- [Fluxo de Sincronização](#fluxo-de-sincronização)
- [Funcionamento Técnico](#funcionamento-técnico)
- [Contribuição](#contribuição)
- [Licença](#licença)

## Visão Geral

Este projeto separa claramente as camadas de negócio (`core/`), infraestrutura (`infrastructure/`), e orquestração de fluxo (`orchestration/`). Objetivo principal:

- Extrair entidades do Pipedrive (deals, pessoas, organizações, etc.)
- Transformar e normalizar dados (campos customizados, histórico de estágios)
- Carregar em banco PostgreSQL com adição dinâmica de colunas
- Monitorar métricas de performance e custos de API

## Funcionalidades

- Sincronização incremental e backfill completo
- Streaming paginado de entidades para uso eficiente de memória
- Transformações robustas (Pydantic, casting, enriquecimento de endereço)
- Atualização dinâmica de esquema de banco de dados
- Métricas e alertas com Prometheus e Grafana
- Circuit breaker, retries e cache inteligente (Redis)

## Arquitetura

```text
┌────────────────────────────────┐
│  orchestration/ (Prefect Flows)│
├─────────────┬──────────────────┤
│ core/       │ infrastructure/  │
│  ├ schemas  │  ├ clients HTTP   │
│  ├ services │  ├ db repositories│
│  └ utils    │  ├ redis cache    │
└─────────────┴──────────────────┘
````

* Hexagonal/Clean Architecture
* Ports e adaptadores desacoplados
* DependencyFactory para composição de serviços

## Tecnologias

* **Linguagem**: Python 3.12
* **Orquestração**: Prefect 3.4.1
* **Banco de Dados**: PostgreSQL 14
* **Cache**: Redis 7
* **Mensuração**: Prometheus + Grafana
* **HTTP Client**: `requests`, `tenacity`, `pybreaker`
* **Validação**: Pydantic
* **Logging**: structlog
* **Docker**: imagens para orion, worker e flows
* **CI/CD**: GitHub Actions

## Instalação e Execução

1. Clone o repositório:

   ```bash
   git clone https://github.com/seu-usuario/pipeboard.git
   cd pipeboard
   ```
2. Defina variáveis de ambiente em `.env`:

   ```dotenv
   PIPEDRIVE_API_TOKEN=seu_token
   DATABASE_URL=postgresql://user:password@host:5432/db
   REDIS_URL=redis://host:6379/0
   ```
3. Suba serviços via Docker Compose:

   ```bash
   docker-compose up -d
   ```
4. Instale dependências Python:

   ```bash
   pip install -r requirements.txt
   ```
5. Crie blocos e deployments Prefect:

   ```bash
   python orchestration/scripts/blocks_and_deployments.py
   ```
6. Execute fluxo de sincronização:

   ```bash
   prefect deployment run core-sync/deals
   ```

## Fluxo de Sincronização

1. **Extração**: `PipedriveAPIClient.stream_all_entities` faz paginação por cursor.
2. **Transformação**: validação com Pydantic, flatten de custom\_fields, normalização de endereços.
3. **Carregamento**: `TableManagerService` gerencia criação/atualização dinâmica de colunas e particionamento.
4. **Medição**: métricas exportadas via `/metrics` endpoint do flow.
5. **Alertas**: configurados no Grafana com base em latência e custos de token.

## Funcionamento Técnico

* **Schema Dinâmico**: Colunas extras detectadas em runtime são adicionadas automaticamente.
* **Tratamento de Estágios**: `StageBackfillService` mapeia IDs de estágios para colunas `moved_to_stage_*` com sufixos.
* **Cache Redis**: evita chamadas repetidas à API para dados estáticos (custom fields, stage details).
* **Circuit Breaker & Retries**: protege contra falhas de rede e limitações de API.
* **Orquestração Prefect**: fluxos modulares por entidade, com triggers e schedules configuráveis.

## Contribuição

1. Fork no GitHub
2. Crie branch para sua feature: `git checkout -b feature/nome`
3. Commit com Conventional Commits
4. Abra Pull Request

## Licença

Este projeto está licenciado sob a [MIT License](LICENSE).
