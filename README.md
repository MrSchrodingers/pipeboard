# Pipeboard: Sincronização com a base de dados do Pipedrive 🚀

[![Versão do Python](https://img.shields.io/badge/python-3.12-blue)](https://www.python.org/) [![Versão do Prefect](https://img.shields.io/badge/prefect-3.x-green)](https://prefect.io/) [![Licença](https://img.shields.io/badge/license-MIT-lightgrey)](LICENSE)

Objetivos:
* **Criar relatórios e dashboards totalmente personalizados;**
* **Análises profundas e cruzamento de dados;**
* **Tomada de decisão baseada em dados;**
* **Histórico completo e confiável.**

---

## 🔖 Sumário

* [O que é o Pipeboard?](#o-que-é-o-pipeboard)
* [Principais Funcionalidades](#principais-funcionalidades)
* [Arquitetura do Projeto](#arquitetura-do-projeto)
* [Tecnologias Utilizadas](#tecnologias-utilizadas)
* [Pré-requisitos](#pré-requisitos)
* [Configuração e Instalação](#configuração-e-instalação)
* [Executando o Projeto](#executando-o-projeto)
* [Fluxo de Sincronização](#fluxo-de-sincronização)
* [Funcionamento Técnico Detalhado](#funcionamento-técnico-detalhado)
* [Monitoramento](#monitoramento)
* [Estrutura de Diretórios](#estrutura-de-diretórios)
* [Variáveis de Ambiente Essenciais](#variáveis-de-ambiente-essenciais)
* [Desenvolvimento e Contribuição](#desenvolvimento-e-contribuição)
* [Solução de Problemas (FAQ)](#solução-de-problemas-faq)
* [Licença](#licença)

## O que é o Pipeboard?

Pipeboard é uma solução robusta de ETL (Extract, Transform, Load) e sincronização de dados, projetada especificamente para integrar o Pipedrive a um banco de dados PostgreSQL, que pode ser usado como fonte do Metabase. Seu foco é lidar com grandes volumes de dados, oferecendo sincronização incremental eficiente, capacidade de backfill completo, e um sistema de orquestração resiliente e monitorável.

O projeto visa fornecer uma base sólida para Business Intelligence (BI).

## Principais Funcionalidades

O Pipeboard é construído com um conjunto de funcionalidades poderosas para garantir uma integração de dados eficiente e confiável:

* **Sincronização Incremental e Backfill Completo:** Mantém seu banco de dados atualizado com as últimas alterações do Pipedrive e permite a carga inicial de todo o histórico de dados.
* **Extração de Dados Eficiente:** Utiliza streaming paginado para consumir as APIs V1 e V2 do Pipedrive, garantindo baixo consumo de memória mesmo com grandes volumes de dados.
* **Transformação de Dados Robusta:** Validação de dados com Pydantic, achatamento (flattening) de campos customizados complexos e normalização de informações como endereços e contatos.
* **Gerenciamento Dinâmico de Esquema no Banco de Dados:** Cria e atualiza tabelas e colunas no PostgreSQL automaticamente conforme novos campos são detectados no Pipedrive, sem necessidade de migrações manuais.
* **Orquestração Avançada com Prefect:** Fluxos de trabalho modulares e reutilizáveis, com gerenciamento de dependências entre sincronizações de diferentes entidades (ex: usuários são sincronizados antes de negócios), agendamentos flexíveis (via cron) e gatilhos baseados em eventos.
* **Monitoramento Abrangente:** Integração nativa com Prometheus para coleta de métricas detalhadas e um dashboard Grafana pré-configurado para visualização da saúde da API, performance dos fluxos ETL, uso de cache, estado do banco de dados e muito mais.
* **Resiliência e Confiabilidade:** Implementa padrões como Circuit Breaker e retries exponenciais para chamadas à API Pipedrive, protegendo o sistema contra falhas de rede e limitações temporárias da API.
* **Cache Inteligente:** Utiliza Redis para armazenar em cache respostas da API Pipedrive (especialmente para metadados e entidades menos voláteis), reduzindo o número de chamadas e o consumo de tokens da API.
* **Containerização com Docker:** Todos os componentes são containerizados, facilitando a implantação, o dimensionamento e a portabilidade entre diferentes ambientes.

## Arquitetura do Projeto

O Pipeboard adota uma arquitetura hexagonal (também conhecida como Ports and Adapters), separando claramente as responsabilidades:

```text
┌────────────────────────────────┐
│  orchestration/ (Fluxos Prefect)│<-- Camada de Orquestração e Regras de Negócio da Sincronização
├─────────────┬──────────────────┤
│ core/       │ infrastructure/  │<-- Camada de Lógica de Negócio Pura e Camada de Infraestrutura
│  ├ schemas  │  ├ clients HTTP  │   (Adaptadores para tecnologias externas)
│  ├ services │  ├ db repositories│
│  └ utils    │  ├ redis cache   │
│             │  ├ observability │
│             │  └ config        │
└─────────────┴──────────────────┘
````

  * **`core/`**: Contém a lógica de negócio principal, como os esquemas Pydantic (definição das entidades do Pipedrive) e utilitários de transformação de dados. É independente de frameworks.
  * **`infrastructure/`**: Implementa os adaptadores para tecnologias externas: cliente HTTP para a API Pipedrive, repositórios para interação com o PostgreSQL, cache Redis, configuração de logging e observabilidade (métricas).
  * **`orchestration/`**: Define os fluxos de ETL utilizando Prefect, incluindo a lógica de sincronização para cada entidade, agendamentos e dependências entre fluxos.

## Tecnologias Utilizadas

  * **Linguagem Principal**: Python 3.12
  * **Orquestração de Workflow**: Prefect 3.x
  * **Banco de Dados (Destino)**: PostgreSQL 14+
  * **Cache**: Redis 7+
  * **Monitoramento e Métricas**: Prometheus, Grafana, Pushgateway
  * **Cliente HTTP e Resiliência**: `requests`, `tenacity` (para retries), `pybreaker` (para circuit breaker)
  * **Validação de Dados**: Pydantic
  * **Logging Estruturado**: `structlog`
  * **Containerização**: Docker & Docker Compose
  * **CI/CD**: GitHub Actions (para build e push de imagens Docker)

## Pré-requisitos

  * Docker Engine
  * Docker Compose
  * Python 3.12 (para desenvolvimento local e execução de scripts auxiliares, se necessário fora dos containers)
  * Acesso a uma instância Pipedrive com uma chave de API válida.
  * (Opcional) Conta no Docker Hub se você planeja modificar e hospedar suas próprias imagens.

## Configuração e Instalação

1.  **Clone o Repositório:**

    ```bash
    git clone [https://github.com/seu-usuario/pipeboard.git](https://github.com/seu-usuario/pipeboard.git)
    cd pipeboard
    ```

2.  **Crie e Configure o Arquivo `.env`:**
    Copie o arquivo `.env.example` (se existir) para `.env` na raiz do projeto ou crie um novo. Preencha com suas credenciais e configurações. Veja a seção [Variáveis de Ambiente Essenciais](https://www.google.com/search?q=%23vari%C3%A1veis-de-ambiente-essenciais) para mais detalhes.
    Exemplo de `.env`:

    ```dotenv
    # Pipedrive
    PIPEDRIVE_API_KEY=SEU_API_KEY_DO_PIPEDRIVE

    # PostgreSQL (usado pelo docker-compose e pela aplicação)
    POSTGRES_DB=pipeboard_db
    POSTGRES_USER=pipeboard_user
    POSTGRES_PASSWORD=sua_senha_segura
    POSTGRES_HOST=db # Nome do serviço no docker-compose
    POSTGRES_PORT=5432
    DATABASE_URL=postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}

    # Redis (usado pelo docker-compose e pela aplicação)
    REDIS_URL=redis://redis:6379/0 # 'redis' é o nome do serviço no docker-compose

    # Prefect
    PREFECT_API_URL=http://orion:4200/api # 'orion' é o nome do serviço no docker-compose
    PREFECT_SERVER_API_HOST=0.0.0.0
    PREFECT_SERVER_API_PORT=4200 # Porta interna do container Orion
    HOST_PREFECT_PORT=4200 # Porta exposta no host para acessar a UI do Prefect

    # Configurações do Worker e Deployments Prefect
    PREFECT_WORK_POOL_NAME=pipeboard-docker-pool
    IMAGE_NAME=mrschrodingers/pipeboard-flow # Imagem base para os fluxos
    DEFAULT_DOCKER_NETWORK_NAME=prefect_internal_network # Nome da rede Docker criada pelo compose

    # Monitoramento
    PUSHGATEWAY_ADDRESS=pushgateway:9091 # 'pushgateway' é o nome do serviço
    HOST_METRICS_PORT=8082 # Porta exposta para métricas do worker
    HOST_GRAFANA_PORT=3000 # Porta exposta para Grafana

    # Docker Registry (opcional, se for fazer push para um registry privado)
    DOCKER_USER=
    DOCKER_PASS=
    DOCKER_REGISTRY_URL=
    ```

3.  **Construa as Imagens Docker e Inicie os Serviços:**
    O script `run_project.sh` automatiza a construção das imagens Docker, o push (se configurado para um registry) e o reinício dos serviços via Docker Compose. Este é o método recomendado para iniciar o ambiente.

    ```bash
    chmod +x run_project.sh
    ./run_project.sh
    ```

    Alternativamente, para apenas subir os serviços usando as imagens definidas no `docker-compose.yml` (que podem ser do Docker Hub ou construídas localmente se você remover as diretivas `image:` e deixar apenas `build:`):

    ```bash
    docker compose up -d --build
    ```

    O serviço `prefect-setup` no `docker-compose.yml` cuidará automaticamente da criação dos blocos Prefect necessários e do registro dos deployments dos fluxos na primeira vez que o ambiente é iniciado (ou sempre que o Orion é reiniciado sem persistência de seus dados).

4.  **Verifique os Logs:**
    Acompanhe os logs para garantir que todos os serviços iniciaram corretamente:

    ```bash
    docker compose logs -f
    ```

    Procure por mensagens de sucesso do `prefect-setup` e do `worker`.

## Executando o Projeto

  * **Iniciar todos os serviços:**
    ```bash
    ./run_project.sh
    ```
    ou, se as imagens já estiverem construídas/puxadas:
    ```bash
    docker compose up -d
    ```
  * **Acessar a Interface do Prefect:**
    Abra seu navegador e acesse `http://localhost:${HOST_PREFECT_PORT:-4200}` (por padrão, `http://localhost:4200`).
  * **Acessar o Grafana:**
    Abra seu navegador e acesse `http://localhost:${HOST_GRAFANA_PORT:-3000}` (por padrão, `http://localhost:3000`). O dashboard do Pipeboard deve estar disponível (pode requerer importação manual do JSON em `docs/grafana/grafana_dashboard.json` na primeira vez ou configuração de provisionamento).
  * **Execução dos Fluxos de Sincronização:**
      * O fluxo `Sync Pipedrive Users` é agendado para rodar a cada 4 horas via cron.
      * Os demais fluxos são encadeados e disparados automaticamente pela conclusão bem-sucedida do fluxo anterior, conforme o diagrama abaixo.
      * Você também pode disparar execuções manualmente pela UI do Prefect.

## Fluxo de Sincronização

Os fluxos de sincronização do Pipedrive são orquestrados pelo Prefect, seguindo uma cadeia de dependências para garantir a integridade dos dados. O fluxo principal inicia-se com a sincronização de usuários e prossegue até a transformação final dos dados para BI.

```mermaid
graph TD
    A["Sync Pipedrive Users </br>(Default: A cada 4h)"] --> B(Sync Pipedrive Pipelines);
    B --> C(Sync Pipedrive Stages);
    C --> D(Sync Pipedrive Organizations);
    D --> E(Sync Pipedrive Persons);
    E --> F(Sync Pipedrive Deals);
    F --> G(Sync Pipedrive Activities);
    G --> H(Sync Pipedrive Activity Types);
    H --> I(Main BI Transformation);

    classDef cronNode fill:#D1E8FF,stroke:#3478BE,stroke-width:2px;
    class A cronNode;
```

**Etapas do Processo:**

1.  **Extração (Extract):** O `PipedriveAPIClient` busca os dados de cada entidade (Negócios, Pessoas, Organizações, etc.) da API do Pipedrive, utilizando paginação por cursor (API v2) ou start/limit (API v1) para lidar com grandes volumes de dados de forma eficiente.
2.  **Transformação (Transform):**
      * Os dados brutos são validados e parseados utilizando esquemas Pydantic específicos para cada entidade.
      * O `PipedriveEntitySynchronizer` "explode" campos complexos e customizados em colunas atômicas e normaliza os dados.
3.  **Carregamento (Load):** O `RepositorioBase` persiste os DataFrames transformados no PostgreSQL. Ele gerencia a criação e migração dinâmica de tabelas e colunas, e utiliza estratégias de `UPSERT` (INSERT ON CONFLICT DO UPDATE) otimizadas (via `execute_values` para lotes menores ou `COPY` para um staging table para lotes maiores).
4.  **Enriquecimento (Enrichment):** Após o carregamento principal de cada entidade, alguns fluxos executam um passo de enriquecimento SQL para adicionar informações de tabelas relacionadas (ex: adicionar nome do proprietário a um negócio).
5.  **Transformação BI (BI Transformation):** O fluxo `Main BI Transformation` consolida dados de várias tabelas sincronizadas (negócios, pessoas, organizações, atividades) em uma tabela `main_bi` denormalizada, pronta para análises e dashboards.

## Funcionamento Técnico Detalhado

  * **Extração de Dados Paginada:** O `PipedriveAPIClient` usa o método `stream_all_entities` para buscar dados. Para endpoints V2, ele gerencia a paginação por cursor (`next_cursor`). Para endpoints V1, utiliza o sistema de `start` e `limit`. Isso garante que os dados sejam processados em lotes, evitando o consumo excessivo de memória.
  * **Transformação e Validação com `PipedriveEntitySynchronizer`:**
      * Esta classe é o coração da sincronização de cada entidade.
      * **Validação:** Utiliza `utils.validate_parallel` (que emprega um `ThreadPoolExecutor`) para validar os dados brutos da API contra os esquemas Pydantic correspondentes em paralelo.
      * **Manipuladores Específicos:** Permite a definição de funções `specific_field_handlers` para tratar colunas com estruturas aninhadas ou formatos especiais (ex: emails, telefones, endereços), transformando-as em colunas normalizadas.
      * **Campos Customizados:** Busca metadados dos campos customizados (`/dealFields`, `/personFields`, etc.) e utiliza um mapa de funções (`_FN_MAP`) para "explodir" os valores desses campos em colunas dedicadas no DataFrame, com nomes `slugificados`.
  * **Carregamento Inteligente com `RepositorioBase`:**
      * **Criação/Migração de Esquema:** Antes de carregar os dados, o repositório verifica se a tabela existe. Se não, cria-a com base nos tipos de dados do DataFrame e na `SchemaConfig` (PKs, índices). Se a tabela existe, compara as colunas e tipos do DataFrame com o esquema existente e aplica `ALTER TABLE` para adicionar novas colunas ou modificar tipos (com cautela para não quebrar colunas PK ou de particionamento).
      * **Upsert Otimizado:** Escolhe entre duas estratégias de upsert:
          * `_upsert_dynamic`: Para DataFrames menores que `staging_threshold` (default 50 linhas, mas recomendável aumentar), usa `psycopg2.extras.execute_values` com `INSERT ... ON CONFLICT ... DO UPDATE`. É eficiente para um número moderado de linhas.
          * `_upsert_staging`: Para DataFrames maiores, cria uma tabela temporária, carrega os dados nela via `COPY` (muito mais rápido para grandes volumes) e, em seguida, executa `UPDATE` nos registros existentes e `INSERT` nos novos registros a partir da tabela temporária.
  * **Lógica de Sincronização Incremental:**
      * A cada execução bem-sucedida de um fluxo, o timestamp é registrado na tabela `etl_flow_meta`.
      * Nas execuções subsequentes, o `PipedriveEntitySynchronizer` recebe o parâmetro `updated_since` (com um pequeno `skew` de segurança), permitindo que a API do Pipedrive retorne apenas registros alterados desde essa data.
      * Alguns fluxos (ex: `sync_deals_flow`) também verificam se suas entidades dependentes (ex: `pessoas`, `organizacoes`) foram atualizadas recentemente. Se sim, eles podem optar por um `full_refresh` para garantir a consistência, mesmo que o próprio fluxo de deals não tenha sido marcado para full refresh.
  * **Orquestração com Prefect:**
      * **Deployments:** Cada fluxo de sincronização é definido como um Prefect Deployment nos arquivos dentro de `orchestration/plugins/`. Esses deployments especificam o nome, descrição, tags, agendamento (para `sync_users_flow`) ou gatilhos, o work pool (`PREFECT_WORK_POOL_NAME`), a imagem Docker a ser usada (`IMAGE_NAME`), e variáveis de ambiente.
      * **Work Pool:** Um work pool do tipo Docker é configurado (default `pipeboard-docker-pool`) para executar os fluxos em containers Docker.
      * **Triggers:** A maioria dos fluxos é acionada pela conclusão bem-sucedida do fluxo anterior na cadeia, usando `DeploymentEventTrigger`.

## Monitoramento

O Pipeboard oferece um sistema de monitoramento robusto utilizando Prometheus e Grafana:

  * **Prometheus:**
      * Coleta métricas expostas pelos serviços `orion` (Prefect Server) e `worker` (que inclui métricas da aplicação ETL).
      * Utiliza o `Pushgateway` para permitir que os fluxos (que são processos de curta duração) enviem suas métricas.
  * **Grafana:**
      * Um dashboard pré-configurado (`docs/grafana/grafana_dashboard.json`) está disponível para visualização das métricas.
      * **Métricas Chave Monitoradas:**
          * **Performance da API Pipedrive:** Tokens consumidos/restantes, latência das requisições, total de chamadas por endpoint, status do rate limit, aberturas do circuit breaker.
          * **Saúde dos Fluxos ETL:** Total de execuções, falhas, registros processados, tempo desde a última execução bem-sucedida/heartbeat, duração média das execuções.
          * **Performance Detalhada do ETL:** Throughput (linhas/segundo), duração de processamento de batches, tamanho dos batches.
          * **Uso de Recursos:** Consumo de memória e CPU pelos processos ETL, contagem de threads.
          * **Cache da API:** Taxa de acertos (hits) vs. falhas (misses) do cache Redis.
          * **Saúde do Banco de Dados:** Número de conexões ativas/ociosas no pool.
          * **Status da Aplicação:** Uptime dos processos.

## Estrutura de Diretórios

```
pipeboard/
├── .github/workflows/        # CI/CD com GitHub Actions
│   └── docker-ci.yml
├── core/                     # Lógica de negócio principal e schemas Pydantic
│   ├── schemas/              # Modelos de dados Pydantic para entidades Pipedrive
│   └── utils/                # Utilitários de schema
├── docs/
│   └── grafana/
│       └── grafana_dashboard.json # Dashboard Grafana pré-configurado
├── docker/                   # Configurações Docker, Dockerfiles e scripts de entrypoint
│   ├── scripts/              # Scripts de inicialização para containers
│   ├── utils/                # Scripts Python para setup de blocos/deployments Prefect
│   ├── Dockerfile.orion      # Dockerfile para o servidor Prefect (Orion)
│   ├── Dockerfile.setup      # Dockerfile para o container de setup do Prefect
│   ├── docker-compose.yml    # Orquestração dos containers
│   └── prometheus.yml        # Configuração do Prometheus
├── infrastructure/           # Adaptadores para tecnologias externas
│   ├── clients/              # Cliente HTTP para a API Pipedrive
│   ├── config/               # Configurações da aplicação (variáveis de ambiente)
│   ├── db/                   # Adaptador para o pool de conexões PostgreSQL
│   ├── logging.py            # Configuração do logging estruturado (structlog)
│   ├── observability/        # Definição de métricas Prometheus e servidor de métricas
│   ├── prefect/              # Configurações específicas do Prefect (worker, flow Dockerfile)
│   │   ├── flow/
│   │   │   └── Dockerfile.flow # Dockerfile base para os fluxos Prefect
│   │   └── worker/
│   │       ├── Dockerfile.worker
│   │       └── run_worker_with_metrics.py
│   ├── redis/                # Cliente para cache Redis
│   └── repositories/         # Repositórios para persistência de dados no PostgreSQL
├── orchestration/            # Fluxos de ETL e lógica de orquestração com Prefect
│   ├── common/               # Utilitários e lógica compartilhada pelos fluxos
│   ├── flows/                # Definição dos fluxos Prefect para cada entidade
│   └── plugins/              # Scripts para registrar os deployments dos fluxos no Prefect
├── .env.example              # Arquivo de exemplo para variáveis de ambiente
├── README.md                 # Este arquivo
├── requirements.txt          # Dependências Python (a serem geradas/atualizadas)
└── run_project.sh            # Script principal para construir e iniciar o ambiente Docker
```

## Variáveis de Ambiente Essenciais

Estas variáveis devem ser definidas no seu arquivo `.env` na raiz do projeto:

  * `PIPEDRIVE_API_KEY`: Sua chave de API do Pipedrive.
  * `POSTGRES_DB`: Nome do banco de dados PostgreSQL.
  * `POSTGRES_USER`: Usuário do PostgreSQL.
  * `POSTGRES_PASSWORD`: Senha do usuário do PostgreSQL.
  * `POSTGRES_HOST`: Host do PostgreSQL (geralmente o nome do serviço Docker, ex: `db`).
  * `POSTGRES_PORT`: Porta do PostgreSQL.
  * `DATABASE_URL`: URL de conexão completa para o PostgreSQL (ex: `postgresql://user:pass@host:port/db`).
  * `REDIS_URL`: URL de conexão para o Redis (ex: `redis://redis:6379/0`).
  * `PREFECT_API_URL`: URL da API do servidor Prefect (Orion) (ex: `http://orion:4200/api`).
  * `PREFECT_WORK_POOL_NAME`: Nome do work pool do Prefect que o worker escutará (ex: `pipeboard-docker-pool`).
  * `IMAGE_NAME`: Nome da imagem Docker base a ser usada para os jobs dos fluxos Prefect (ex: `user/pipeboard-flow`).
  * `DEFAULT_DOCKER_NETWORK_NAME`: Nome da rede Docker interna usada pelo Docker Compose (ex: `prefect_internal_network`).
  * `HOST_PREFECT_PORT`: Porta no seu host para acessar a UI do Prefect (ex: `4200`).
  * `HOST_GRAFANA_PORT`: Porta no seu host para acessar o Grafana (ex: `3000`).
  * `PUSHGATEWAY_ADDRESS`: Endereço do Prometheus Pushgateway (ex: `pushgateway:9091`).

## Desenvolvimento e Contribuição

1.  **Fork o Repositório** no GitHub.
2.  **Crie uma Branch para sua Feature:**
    ```bash
    git checkout -b feature/sua-feature-incrivel
    ```
3.  **Desenvolva e Faça Commits:** Siga o padrão de [Conventional Commits](https://www.conventionalcommits.org/) para mensagens de commit.
4.  **Abra um Pull Request:** Detalhe as mudanças realizadas e o motivo.

Para o ambiente de desenvolvimento local, após clonar o repositório e configurar o `.env`, você pode usar o `run_project.sh` ou `docker compose up -d` para iniciar todos os serviços. As alterações no código Python dentro dos volumes montados (se configurado no `docker-compose.yml` para desenvolvimento) podem ser refletidas nos containers (especialmente para scripts, ou se o servidor Prefect/worker for reiniciado ou usar um modo de desenvolvimento que recarregue o código).

## Solução de Problemas (FAQ)

  * **Problema: Erros de conexão com o banco de dados ou Redis ao iniciar.**
      * **Solução:** Verifique se os serviços `db` e `redis` estão saudáveis no `docker compose ps`. Confira as variáveis `DATABASE_URL` e `REDIS_URL` no seu arquivo `.env` e se os nomes dos hosts correspondem aos nomes dos serviços no `docker-compose.yml`. Garanta que as credenciais do PostgreSQL estão corretas.
  * **Problema: Fluxos Prefect não são executados ou ficam presos no estado "Pending".**
      * **Solução:**
          * Verifique se o serviço `worker` está rodando e conectado ao `PREFECT_API_URL` correto.
          * Confira os logs do `worker` (`docker compose logs -f worker`).
          * Certifique-se de que o `PREFECT_WORK_POOL_NAME` configurado nos deployments dos fluxos (em `orchestration/plugins/`) corresponde ao nome do work pool que o worker está escutando.
          * Verifique se a imagem Docker especificada nos deployments (`IMAGE_NAME`) está acessível (puxada localmente ou em um registry).
  * **Problema: API do Pipedrive retorna erros 401 (Unauthorized).**
      * **Solução:** Confirme se a `PIPEDRIVE_API_KEY` no seu arquivo `.env` é válida e possui as permissões necessárias.
  * **Problema: API do Pipedrive retorna erros 403 (Forbidden) ou 429 (Rate Limit).**
      * **Solução:** O Pipeboard possui retries e circuit breaker. Se os erros 429 persistirem, você pode estar excedendo os limites da API do Pipedrive. Verifique o dashboard do Grafana para o consumo de tokens. O erro 403 pode indicar falta de permissão para acessar um endpoint específico.
  * **Problema: O dashboard do Grafana não mostra dados.**
      * **Solução:**
          * Verifique se o Prometheus está coletando métricas dos targets (`orion`, `worker`, `pushgateway`). Acesse a UI do Prometheus (`http://localhost:9090`).
          * Confirme se a fonte de dados (datasource) Prometheus está configurada corretamente no Grafana.
          * Garanta que os fluxos ETL estão rodando e enviando métricas para o Pushgateway.

## Licença

Este projeto está licenciado sob a [Licença GNU](https://www.gnu.org/licenses/gpl-3.0.html).
