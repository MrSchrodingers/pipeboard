"""
Orquestrador de repositórios que unifica o processo de sincronização
de dados principais, entidades auxiliares e histórico de etapas.
"""

import structlog
import pandas as pd
from typing import Dict

from infrastructure.repositories.repository_stage_history import RepositorioHistoricoEtapas

logger = structlog.get_logger(__name__)


class BoardRepository:
    def __init__(self):
        self.historico = RepositorioHistoricoEtapas()

    def persistir_negocios(self, df: pd.DataFrame):
        self.negocios.salvar_dados_negocios(df)

    def persistir_auxiliar(self, nome: str, df: pd.DataFrame):
        self.auxiliares.salvar_dataframe_auxiliar(nome, df)

    def aplicar_historico_etapas(self, atualizacoes: Dict[str, pd.DataFrame]):
        self.historico.aplicar_atualizacoes_de_historico(atualizacoes)
