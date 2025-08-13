from psycopg2 import sql

def _list_columns(conn, table_name: str) -> set[str]:
    q = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
          AND table_schema = ANY (current_schemas(true))
    """
    with conn.cursor() as c:
        c.execute(q, (table_name,))
        return {r[0] for r in c.fetchall()}

def enrich_with_lookups_sql(
    table: str,
    lookups_mapping: dict[str, dict],
    connection,
    logger=None,
):
    mapping = lookups_mapping.get(table, {})
    if not mapping:
        if logger:
            logger.info(f"Sem lookups para {table}.")
        return

    tgt_cols = _list_columns(connection, table)

    with connection.cursor() as cur:
        for col, cfg in mapping.items():
            src_table  = cfg['source']
            src_key    = cfg['key']
            src_val    = cfg['value_col']
            target_col = cfg['target_col']

            src_cols = _list_columns(connection, src_table)

            # valida colunas mínimas de join
            missing = []
            if col not in tgt_cols:
                missing.append(f"{table}.{col} (JOIN destino)")
            if src_key not in src_cols:
                missing.append(f"{src_table}.{src_key} (JOIN origem)")
            if missing:
                msg = "Lookup inválido. Faltam colunas:\n - " + "\n - ".join(missing)
                if logger: logger.error(msg)
                raise ValueError(msg)

            # fallback amigável para value_col
            chosen_val = src_val
            if chosen_val not in src_cols:
                for alt in ("title", "name", "label"):
                    if alt in src_cols:
                        if logger:
                            logger.warning(
                                "value_col '%s' não existe em %s; usando fallback '%s'.",
                                chosen_val, src_table, alt
                            )
                        chosen_val = alt
                        break
                else:
                    msg = (
                        f"value_col '{src_val}' não existe em {src_table}. "
                        f"Colunas disponíveis: {', '.join(sorted(src_cols))}"
                    )
                    if logger: logger.error(msg)
                    raise ValueError(msg)

            # cria coluna alvo se necessário
            cur.execute(sql.SQL(
                "ALTER TABLE {main} ADD COLUMN IF NOT EXISTS {target} TEXT"
            ).format(
                main=sql.Identifier(table),
                target=sql.Identifier(target_col)
            ))

            # UPDATE ... FROM ...
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
                src_val=sql.Identifier(chosen_val),
                src_table=sql.Identifier(src_table),
                col=sql.Identifier(col),
                src_key=sql.Identifier(src_key)
            )

            if logger:
                try:
                    logger.debug("SQL enrichment:\n%s", enrich_sql.as_string(connection))
                except Exception:
                    pass
                logger.info(
                    "Enriching %s.%s using %s (%s -> %s)...",
                    table, target_col, src_table, col, chosen_val
                )

            cur.execute(enrich_sql)

        connection.commit()

    if logger:
        logger.info("SQL enrichment complete for %s.", table)
