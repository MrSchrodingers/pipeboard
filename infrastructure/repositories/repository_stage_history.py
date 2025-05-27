"""
Repositório para sincronização do histórico de movimentação entre etapas
(pipeline stages), atualizando colunas como `movido_para_etapa_X`.
"""

import psycopg2
import structlog
import pandas as pd
from typing import Dict
from infrastructure.db.postgres_adapter import get_postgres_conn

logger = structlog.get_logger(__name__)


class RepositorioHistoricoEtapas:
    def __init__(self, nome_tabela: str = "negocios_visualizacao"):
        self.nome_tabela = nome_tabela

    def aplicar_atualizacoes_de_historico(self, atualizacoes: Dict[str, pd.DataFrame]) -> None:
        if not atualizacoes:
            logger.info("Nenhuma atualização de histórico a aplicar.")
            return

        with get_postgres_conn().connection() as conn:
            with conn.cursor() as cur:
                for nome_coluna, df_updates in atualizacoes.items():
                    if df_updates.empty:
                        logger.warning("DataFrame vazio para coluna de histórico", coluna=nome_coluna)
                        continue

                    logger.info("Aplicando atualização para coluna de histórico", coluna=nome_coluna)
                    updates_sql = """
                        UPDATE {tabela}
                        SET {coluna} = dados.{coluna}
                        FROM (VALUES %s) AS dados (id, {coluna})
                        WHERE {tabela}.id = dados.id
                    """.format(tabela=self.nome_tabela, coluna=nome_coluna)

                    values = list(df_updates.itertuples(index=False, name=None))
                    psycopg2.extras.execute_values(cur, updates_sql, values)

                conn.commit()
        logger.info("Atualizações de histórico finalizadas com sucesso")
