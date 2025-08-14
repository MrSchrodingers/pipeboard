from __future__ import annotations
from typing import Dict, List, Optional
from psycopg2 import sql

# candidatos padrão quando value_col não existir
_DEFAULT_VALUE_COL_CANDIDATES: List[str] = ["title", "name", "label", "display_name"]

def _list_columns_via_regclass(conn, regclass_str: str) -> set[str]:
    """
    Lista colunas da *mesma* relação que o FROM usará.
    Aceita 'schema.table' ou 'table' (honra o search_path).
    """
    q = """
        SELECT attname
        FROM   pg_attribute
        WHERE  attrelid = %s::regclass
          AND  attnum > 0
          AND  NOT attisdropped
    """
    with conn.cursor() as c:
        c.execute(q, (regclass_str,))
        return {r[0] for r in c.fetchall()}

def _qualify_for_sql(src_table: str, src_schema: Optional[str]):
    """
    Retorna (sql_identifier, regclass_string) para a mesma relação.
    """
    if src_schema:
        return (
            sql.SQL("{}.{}").format(sql.Identifier(src_schema), sql.Identifier(src_table)),
            f"{src_schema}.{src_table}",
        )
    return (sql.Identifier(src_table), src_table)

def enrich_with_lookups_sql(
    table: str,
    lookups_mapping: Dict[str, Dict],
    connection,
    logger=None,
):
    """
    Enriquecimento genérico:
      - UPDATE tgt SET target_col = src.value_col FROM src_table src WHERE join...
      - Resolve relação via regclass (evita cross-schema)
      - Fallback de value_col com candidatos configuráveis
    Mapping esperado (por coluna de JOIN do destino):
      {
        <dest_table>: {
          "<join_col_in_dest>": {
            "source": "src_table",
            "schema": "public",                     # opcional (recomendado)
            "key": "id",                            # coluna de join na origem
            "value_col": "title",                   # coluna preferida de valor
            "value_candidates": ["title","name"],   # opcional: override de fallback
            "target_col": "pipeline_name"           # coluna a ser preenchida no destino
          },
          ...
        }
      }
    """
    mapping = lookups_mapping.get(table, {})
    if not mapping:
        if logger:
            logger.info(f"Sem lookups para {table}.")
        return

    # resolve a tabela de destino via regclass
    tgt_cols = _list_columns_via_regclass(connection, table)

    with connection.cursor() as cur:
        for dest_join_col, cfg in mapping.items():
            src_table   = cfg["source"]
            src_schema  = cfg.get("schema")  # opcional
            src_key     = cfg["key"]
            src_val     = cfg.get("value_col")
            target_col  = cfg["target_col"]

            # qualifica origem para SQL e para regclass
            src_ident, src_regclass = _qualify_for_sql(src_table, src_schema)
            src_cols = _list_columns_via_regclass(connection, src_regclass)

            # sanity-join
            missing = []
            if dest_join_col not in tgt_cols:
                missing.append(f"{table}.{dest_join_col} (JOIN destino)")
            if src_key not in src_cols:
                missing.append(f"{src_regclass}.{src_key} (JOIN origem)")
            if missing:
                msg = "Lookup inválido. Faltam colunas:\n - " + "\n - ".join(missing)
                if logger: logger.error(msg)
                raise ValueError(msg)

            # candidatos para value_col (o primeiro existente ganha)
            user_candidates = cfg.get("value_candidates") or []
            candidates = []
            if src_val and src_val not in user_candidates:
                candidates.append(src_val)
            candidates += [c for c in user_candidates if c not in candidates]
            for c in _DEFAULT_VALUE_COL_CANDIDATES:
                if c not in candidates:
                    candidates.append(c)

            chosen_val = None
            for cand in candidates:
                if cand in src_cols:
                    chosen_val = cand
                    break

            if not chosen_val:
                msg = (
                    f"Nenhum value_col disponível em {src_regclass}. "
                    f"Tentados: {', '.join(candidates)}. "
                    f"Colunas existentes: {', '.join(sorted(src_cols))}"
                )
                if logger: logger.error(msg)
                raise ValueError(msg)

            # cria coluna de destino se necessário
            cur.execute(sql.SQL(
                "ALTER TABLE {main} ADD COLUMN IF NOT EXISTS {target} TEXT"
            ).format(
                main=sql.Identifier(table),
                target=sql.Identifier(target_col),
            ))

            # UPDATE … FROM …
            stmt = sql.SQL(
                """
                UPDATE {main} AS tgt
                   SET {target} = src.{src_val}
                  FROM {src} AS src
                 WHERE tgt.{dest_col}::text = src.{src_key}::text
                """
            ).format(
                main=sql.Identifier(table),
                target=sql.Identifier(target_col),
                src_val=sql.Identifier(chosen_val),
                src=src_ident,
                dest_col=sql.Identifier(dest_join_col),
                src_key=sql.Identifier(src_key),
            )

            if logger:
                try:
                    logger.debug("SQL enrichment:\n%s", stmt.as_string(connection))
                except Exception:
                    pass
                logger.info(
                    "Enriching %s.%s using %s (%s -> %s)…",
                    table, target_col, src_regclass, dest_join_col, chosen_val
                )

            cur.execute(stmt)

        connection.commit()

    if logger:
        logger.info("SQL enrichment complete for %s.", table)
