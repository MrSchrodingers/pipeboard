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

            # 1. Cria a coluna de destino se não existir
            cur.execute(sql.SQL(
                "ALTER TABLE {main} ADD COLUMN IF NOT EXISTS {target} TEXT"
            ).format(
                main=sql.Identifier(table),
                target=sql.Identifier(target_col)
            ))

            # 2. Constrói e executa a consulta de UPDATE
            enrich_sql = sql.SQL(
                """
                UPDATE {main} AS tgt
                SET {target} = src.{src_val}
                FROM {src_table} AS src
                WHERE tgt.{col}::text = src.{src_key}::text
                """
            ).format(
                main=sql.Identifier(table),
                target=sql.Identifier(target_col),
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