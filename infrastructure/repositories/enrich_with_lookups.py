import psycopg2
from psycopg2 import sql

def enrich_with_lookups_sql(
    table: str,
    lookups_mapping: dict[str, dict],
    connection,
    logger=None,
):
    """
    Aplica enrichment (lookups) via SQL UPDATE ... FROM para os campos definidos em lookups_mapping.
    """
    mapping = lookups_mapping.get(table, {})
    with connection.cursor() as cur:
        for col, cfg in mapping.items():
            src_table = cfg['source']
            src_key = cfg['key']
            src_val = cfg['value_col']
            target_col = cfg['target_col']

            # Cria coluna se não existir (para não falhar)
            cur.execute(sql.SQL(
                "ALTER TABLE {main} ADD COLUMN IF NOT EXISTS {target} TEXT"
            ).format(
                main=sql.Identifier(table),
                target=sql.Identifier(target_col)
            ))

            # UPDATE usando JOIN
            enrich_sql = sql.SQL(
                """
                UPDATE {main}
                SET {target} = src.{src_val}
                FROM {src_table} AS src
                WHERE {main}.{col} = src.{src_key}
                  AND {main}.{col} IS NOT NULL
                """
            ).format(
                main=sql.Identifier(table),
                target=sql.Identifier(target_col),
                src=sql.Identifier(src_table),
                src_val=sql.Identifier(src_val),
                src_table=sql.Identifier(src_table),
                col=sql.Identifier(col),
                src_key=sql.Identifier(src_key)
            )

            if logger:
                logger.info(f"Enriching {table}.{target_col} using {src_table} ({col} -> {src_val})...")

            cur.execute(enrich_sql)
        connection.commit()
    if logger:
        logger.info(f"SQL enrichment complete for {table}.")

