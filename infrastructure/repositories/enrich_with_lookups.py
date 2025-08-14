from __future__ import annotations
from typing import Dict, List, Optional
from psycopg2 import sql

_DEFAULT_VALUE_COL_CANDIDATES: List[str] = ["title", "name", "label", "display_name"]

def _list_columns_via_regclass(conn, regclass_str: str) -> set[str]:
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
    mapping = lookups_mapping.get(table, {})
    if not mapping:
        if logger:
            logger.info(f"Sem lookups para {table}.")
        return

    # destino via regclass (mesma relação que o FROM usará)
    tgt_cols = _list_columns_via_regclass(connection, table)

    with connection.cursor() as cur:
        for dest_join_col, cfg in mapping.items():
            src_table   = cfg["source"]
            src_schema  = cfg.get("schema")
            src_key     = cfg["key"]
            src_val     = cfg.get("value_col")
            target_col  = cfg["target_col"]

            # se o JOIN do destino não existe, apenas loga e pula
            if dest_join_col not in tgt_cols:
                if logger:
                    logger.warning(
                        "Lookup pulado: %s.%s não existe (JOIN destino).",
                        table, dest_join_col
                    )
                continue

            src_ident, src_regclass = _qualify_for_sql(src_table, src_schema)
            src_cols = _list_columns_via_regclass(connection, src_regclass)

            if src_key not in src_cols:
                msg = f"Lookup inválido: {src_regclass}.{src_key} não existe (JOIN origem)."
                if logger: logger.error(msg)
                raise ValueError(msg)

            # candidatos p/ value_col
            user_candidates = cfg.get("value_candidates") or []
            candidates = []
            if src_val and src_val not in user_candidates:
                candidates.append(src_val)
            candidates += [c for c in user_candidates if c not in candidates]
            for c in _DEFAULT_VALUE_COL_CANDIDATES:
                if c not in candidates:
                    candidates.append(c)

            chosen_val = next((c for c in candidates if c in src_cols), None)
            if not chosen_val:
                msg = (
                    f"Nenhum value_col disponível em {src_regclass}. "
                    f"Tentados: {', '.join(candidates)}. "
                    f"Colunas existentes: {', '.join(sorted(src_cols))}"
                )
                if logger: logger.error(msg)
                raise ValueError(msg)

            # cria coluna de destino (valor) se necessário
            cur.execute(sql.SQL(
                "ALTER TABLE {main} ADD COLUMN IF NOT EXISTS {target} TEXT"
            ).format(
                main=sql.Identifier(table),
                target=sql.Identifier(target_col),
            ))

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
