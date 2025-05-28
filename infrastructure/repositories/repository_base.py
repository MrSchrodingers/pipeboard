import io
import json
from dataclasses import dataclass, field
import re
from typing import Any, Dict, List, Literal, Optional
import csv

import numpy as np
import pandas as pd
import pydantic
import structlog
from psycopg2 import sql, errors
from psycopg2.extras import execute_values

from infrastructure.db.postgres_adapter import get_postgres_conn

logger = structlog.get_logger(__name__)


@dataclass
class PartitioningConfig:
    method: Optional[Literal["HASH", "RANGE"]] = None
    key: Optional[str] = None
    num_partitions: int = 4


@dataclass
class SchemaConfig:
    pk: List[str]
    types: Dict[str, str] = field(default_factory=dict)
    indexes: List[str] = field(default_factory=list)
    partitioning: Optional[PartitioningConfig] = None
    allow_column_dropping: bool = True
    table_options: Optional[str] = None


class RepositorioBase:
    PANDAS_TO_PG = {
        "Int8": "SMALLINT",
        "Int16": "SMALLINT",
        "Int32": "INTEGER",
        "Int64": "BIGINT",
        "float16": "REAL",
        "float32": "REAL",
        "float64": "DOUBLE PRECISION",
        "Float64": "DOUBLE PRECISION",
        "bool": "BOOLEAN",
        "boolean": "BOOLEAN",
        "object": "TEXT",
        "string": "TEXT",
        "category": "TEXT",
        "datetime64[ns]": "TIMESTAMP WITHOUT TIME ZONE",
        "datetime64[ns, UTC]": "TIMESTAMP WITH TIME ZONE",
    }

    PG_CAST = {
        "SMALLINT": "smallint",
        "INTEGER": "integer",
        "BIGINT": "bigint",
        "REAL": "real",
        "DOUBLE PRECISION": "double precision",
        "NUMERIC": "numeric", 
        "BOOLEAN": "boolean",
        "JSONB": "jsonb",
        "TEXT": "text",
        "VARCHAR": "varchar", 
        "CHAR": "char",
        "TIMESTAMP WITHOUT TIME ZONE": "timestamp without time zone",
        "TIMESTAMP WITH TIME ZONE": "timestamp with time zone",
        "DATE": "date",
        "TIME": "time",
    }

    def __init__(self, table_name: str, schema_config: SchemaConfig):
        self.table_name = table_name
        self.schema_config = schema_config
        self.staging_table_name_base = f"{table_name}_staging_temp"
        self.logger = logger.bind(table_name=self.table_name)

    def _sanitize_string_value(self, v: Any) -> Any:
        return v.replace("\x00", "") if isinstance(v, str) else v

    def _safe_json(self, v: Any) -> Optional[str]:
        def to_serializable(val):
            # Se for modelo Pydantic
            if isinstance(val, pydantic.BaseModel):
                return val.model_dump()
            # Se for lista, recursivamente converte cada item
            if isinstance(val, list):
                return [to_serializable(x) for x in val]
            # Se for dict, recursivamente converte valores
            if isinstance(val, dict):
                return {k: to_serializable(x) for k, x in val.items()}
            # Se for string, faz sanitize
            if isinstance(val, str):
                return self._sanitize_string_value(val)
            # Se for nan, pd.NA, pd.NaT, None, retorna None
            if val in (None, pd.NA) or (isinstance(val, float) and np.isnan(val)) or val is pd.NaT:
                return None
            return val

        try:
            sanitized = to_serializable(v)
            return json.dumps(sanitized, ensure_ascii=False, default=str)
        except Exception as exc:
            self.logger.warning(
                "Value not JSON serializable; nullified.",
                type=type(v), error=str(exc), preview=str(v)[:120],
            )
            return None

    def _convert_value_for_db_adapter(self, v: Any) -> Any:
        if isinstance(v, np.generic):
            return v.item()
        if v is None or (np.isscalar(v) and pd.isna(v)):
            return None
        if isinstance(v, (list, dict, np.ndarray)):
            if not isinstance(v, str):
                return json.dumps(v, ensure_ascii=False, default=str)
        return v

    def _optimize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df_opt = df.copy()
        if df_opt.columns.duplicated().any():
            counts: Dict[str, int] = {}
            df_opt.columns = [
                f"{c}__dup{counts.setdefault(c, 0) or counts.update({c: counts[c] + 1}) or counts[c]}"
                if c in counts else c for c in df_opt.columns
            ]
        for col in df_opt.columns:
            ser = df_opt[col]
            col_pg_type = self._get_pg_type(col, ser.dtype)
            
            if "TIMESTAMP" in col_pg_type or col_pg_type == "DATE":
                try:
                    df_opt[col] = pd.to_datetime(ser, errors='coerce', utc=True if "WITH TIME ZONE" in col_pg_type else None)
                    if col_pg_type == "DATE":
                         df_opt[col] = df_opt[col].dt.normalize().dt.date 
                    self.logger.debug(f"Coluna '{col}' convertida para datetime/date pandas.")
                except Exception as e_conv:
                    self.logger.warning(f"Falha ao converter coluna '{col}' para datetime/date pandas: {e_conv}. Mantendo como objeto.")
                
            if self.schema_config.types.get(col, "").upper() == "JSONB":
                if not ser.empty:
                    val_antes = ser.iloc[0]
                    tipo_antes = type(val_antes)
                    self.logger.info(f"'{col}' ANTES de apply(_safe_json) - Tipo: {tipo_antes}, Valor (preview): {str(val_antes)[:100]}")
                
                coluna_serializada = ser.apply(self._safe_json)
                df_opt[col] = coluna_serializada.astype(object)
                
                if not df_opt[col].empty:
                    val_depois = df_opt[col].iloc[0]
                    tipo_depois = type(val_depois)
                    self.logger.info(f"'{col}' DEPOIS de apply(_safe_json) - Tipo: {tipo_depois}, Valor (preview): {str(val_depois)[:100]}")
                    if isinstance(val_depois, str):
                        try:
                            json.loads(val_depois)
                            self.logger.info(f"'{col}' DEPOIS - json.loads bem-sucedido para o primeiro valor.")
                        except json.JSONDecodeError as e_json:
                            self.logger.error(f"'{col}' DEPOIS - json.loads FALHOU: {e_json}. Valor: {val_depois[:200]}")
                    elif val_depois is not None:
                         self.logger.warning(f"'{col}' DEPOIS - Primeiro valor NÃO é string nem None, é {tipo_depois}")
                continue
            if ser.dtype == "object" or pd.api.types.is_string_dtype(ser.dtype):
                try:
                    ser = ser.apply(self._sanitize_string_value)
                    df_opt[col] = ser
                except Exception as exc:
                    self.logger.debug("Sanitize failed (%s): %s", col, exc)
            if ser.dtype == "object":
                try:
                    df_opt[col] = pd.to_numeric(ser, errors='ignore')
                except Exception:
                    pass
            if df_opt[col].dtype == "object":
                try:
                    if df_opt[col].apply(lambda x: isinstance(x, str) or pd.isna(x)).all():
                        df_opt[col] = df_opt[col].astype("string")
                except Exception:
                    pass
            dt = df_opt[col].dtype
            if pd.api.types.is_integer_dtype(dt) and not str(dt).startswith("Int"):
                try: df_opt[col] = df_opt[col].astype(f"Int{dt.itemsize * 8}")
                except Exception: pass
            elif pd.api.types.is_float_dtype(dt) and not str(dt).startswith("Float"):
                try: df_opt[col] = df_opt[col].astype(f"Float{dt.itemsize * 8}")
                except Exception: pass
            elif pd.api.types.is_bool_dtype(dt) and str(dt) != "boolean":
                try: df_opt[col] = df_opt[col].astype("boolean")
                except Exception: pass
        return df_opt.replace({pd.NaT: None, np.nan: None, pd.NA: None})

    def _get_pg_type(self, col: str, pd_type: Any) -> str:
        if col in self.schema_config.types:
            return self.schema_config.types[col].upper()
        return self.PANDAS_TO_PG.get(str(pd_type), "TEXT")

    def _column_defs(self, df: pd.DataFrame) -> str:
        return ", ".join(f'{sql.Identifier(c).as_string(None)} {self._get_pg_type(c, dt)}' for c, dt in df.dtypes.items())

    def _create_table(self, cur, df: pd.DataFrame) -> None:
        missing = [pk for pk in self.schema_config.pk if pk not in df.columns]
        if missing:
            raise ValueError(f"PK(s) ausentes no DataFrame: {missing}")

        pk_sql_parts = [sql.Identifier(pk).as_string(cur) for pk in self.schema_config.pk]
        pk_sql = ", ".join(pk_sql_parts)

        part_sql_str = "" 
        pconf = self.schema_config.partitioning
        if pconf and pconf.method == "HASH":
            key = pconf.key or self.schema_config.pk[0]
            part_sql_str = f'PARTITION BY HASH ({sql.Identifier(key).as_string(cur)})'
        
        cols_sql_str = ", ".join(
            f"{sql.Identifier(c).as_string(cur)} {self._get_pg_type(c, dt)}" 
            for c, dt in df.dtypes.items()
        )

        create_sql = sql.SQL(
            "CREATE TABLE IF NOT EXISTS {tbl} ({cols}, PRIMARY KEY ({pk})) {part} {opts};"
        ).format(
            tbl=sql.Identifier(self.table_name),
            cols=sql.SQL(cols_sql_str),
            pk=sql.SQL(pk_sql), 
            part=sql.SQL(part_sql_str),
            opts=sql.SQL(self.schema_config.table_options or ""),
        )
        self.logger.debug("Executing CREATE TABLE IF NOT EXISTS", sql_statement=create_sql.as_string(cur))
        cur.execute(create_sql)
        
        if pconf and pconf.method == "HASH":
            for i in range(pconf.num_partitions):
                cur.execute(
                    sql.SQL(
                        'CREATE TABLE IF NOT EXISTS {p} PARTITION OF {t} FOR VALUES WITH (MODULUS {m}, REMAINDER {r});'
                    ).format(
                        p=sql.Identifier(f"{self.table_name}_p{i}"),
                        t=sql.Identifier(self.table_name),
                        m=sql.Literal(pconf.num_partitions),
                        r=sql.Literal(i),
                    )
                )

    def _schema_migration(self, cur, df: pd.DataFrame) -> None:
        cur.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = %s",
            (self.table_name,),
        )
        existing = {row[0]: row[1].upper() for row in cur.fetchall()}
        pk_and_part = set(self.schema_config.pk)
        if self.schema_config.partitioning and self.schema_config.partitioning.key:
            pk_and_part.add(self.schema_config.partitioning.key)

        for col in df.columns:
            expected_type = self._get_pg_type(col, df[col].dtype)
            if col not in existing:
                self.logger.info(f"Adicionando coluna '{col}' com tipo '{expected_type}' à tabela '{self.table_name}'.")
                cur.execute(
                    sql.SQL('ALTER TABLE {t} ADD COLUMN {c} {tp}').format( 
                        t=sql.Identifier(self.table_name),
                        c=sql.Identifier(col),
                        tp=sql.SQL(expected_type),
                    )
                )
            elif col not in pk_and_part and existing[col] != expected_type:
                self.logger.info(f"Tentando alterar tipo da coluna '{col}' de '{existing[col]}' para '{expected_type}' na tabela '{self.table_name}'.")
                try:
                    cur.execute(
                        sql.SQL(
                            'ALTER TABLE {t} ALTER COLUMN {c} TYPE {tp} USING {c_raw}::{tp_raw}'
                        ).format(
                            t=sql.Identifier(self.table_name),
                            c=sql.Identifier(col),
                            tp=sql.SQL(expected_type),
                            c_raw=sql.Identifier(col), 
                            tp_raw=sql.SQL(expected_type) 
                        )
                    )
                except Exception as e_alter:
                    self.logger.error(
                        f"FALHA AO ALTERAR COLUNA '{col}' de '{existing[col]}' para '{expected_type}'. "
                        f"Erro: {e_alter}. Dados na coluna podem ser incompatíveis. "
                        f"Verifique SchemaConfig.types ou limpe dados na origem.",
                        col_data_preview=df[col].dropna().head(3).to_list() if col in df and not df[col].dropna().empty else "N/A"
                    )
                    raise


    def _get_cast_type_for_execute_values(self, pg_type_full: str) -> str:
        pg_type_base = pg_type_full.split('(')[0].strip().upper()
        return self.PG_CAST.get(pg_type_base, pg_type_full.lower())

    def _convert_value_for_db_processing(self, value: Any, pg_target_type: str) -> Any:
        if isinstance(value, np.generic):
            value = value.item()

        is_null_or_na = False
        if value is None:
            is_null_or_na = True
        elif isinstance(value, float) and np.isnan(value): 
            is_null_or_na = True
        elif hasattr(value, '__iter__') and not isinstance(value, (str, bytes, dict)):
            try:
                if len(value) == 0: 
                    is_null_or_na = True
            except TypeError: 
                pass 
        elif pd.isna(value):
             is_null_or_na = True
        
        if is_null_or_na:
            return None

        pg_target_type_base = pg_target_type.split('(')[0].strip().upper()
        if pg_target_type_base in ("TEXT", "VARCHAR", "CHAR"):
            if not isinstance(value, str):
                return str(value) 
            return self._sanitize_string_value(value)

        if pg_target_type_base == "JSONB" and isinstance(value, (list, dict)):
             return self._safe_json(value)

        return value
    
    def _upsert_dynamic(self, conn, cur, df: pd.DataFrame) -> None:
        if df.empty: return
        cols = list(df.columns)
        
        tmpl_parts = []
        pg_col_types = {c: self._get_pg_type(c, df[c].dtype) for c in cols}

        for c in cols:
            pg_type_full = pg_col_types[c]
            cast_signature = self._get_cast_type_for_execute_values(pg_type_full)
            tmpl_parts.append(f"%s::{cast_signature}")
        
        tmpl = "(" + ", ".join(tmpl_parts) + ")"
        
        rows = []
        for r_tuple in df.itertuples(index=False, name=None):
            processed_row = []
            for idx, col_name in enumerate(cols):
                value = r_tuple[idx]
                pg_target_type_for_col = pg_col_types[col_name] 
                processed_value = self._convert_value_for_db_processing(value, pg_target_type_for_col)
                processed_row.append(processed_value)
            rows.append(tuple(processed_row))
        
        set_sql_parts = [sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in cols if c not in self.schema_config.pk]
        
        action_sql = sql.SQL("NOTHING") if not set_sql_parts else sql.SQL("UPDATE SET ") + sql.SQL(", ").join(set_sql_parts)

        stmt = sql.SQL(
            "INSERT INTO {t} ({cols_ident}) VALUES %s ON CONFLICT ({pk_ident}) DO {action}"
        ).format(
            t=sql.Identifier(self.table_name),
            cols_ident=sql.SQL(", ").join(map(sql.Identifier, cols)),
            pk_ident=sql.SQL(", ").join(map(sql.Identifier, self.schema_config.pk)),
            action=action_sql,
        )

        if self.table_name == "usuarios":
            self.logger.info(
                "DEBUG UPSERT_DYNAMIC (usuarios): Pre-Execute",
                num_rows=len(rows),
                template=tmpl,
                columns_and_target_pg_types=" | ".join([f"Col: '{c}', PGType: '{pg_col_types[c]}'" for c in cols]),
                first_row_processed_data_sample=rows[0] if rows else "N/A",
            )

        execute_values(cur, stmt, rows, template=tmpl, page_size=250)

    def _upsert_staging(self, conn, cur, df: pd.DataFrame) -> None:
        if df.empty: return
        tmp_name = f"{self.staging_table_name_base}_{pd.Timestamp.utcnow():%Y%m%d%H%M%S%f}"
        ident_tmp = sql.Identifier(tmp_name)
        
        cols_sql_str_staging = ", ".join(
            f"{sql.Identifier(c).as_string(cur)} {self._get_pg_type(c, dt)}" 
            for c, dt in df.dtypes.items()
        )
        cur.execute(
            sql.SQL("CREATE TEMP TABLE {tmp} ({cols}) ON COMMIT DROP").format(
                tmp=ident_tmp, cols=sql.SQL(cols_sql_str_staging)
            )
        )
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False, sep=",", na_rep="", quoting=csv.QUOTE_MINIMAL, escapechar="\\")
        buf.seek(0)
        cur.copy_expert(
            sql.SQL("COPY {tmp} FROM STDIN WITH (FORMAT CSV, NULL '', DELIMITER ',', QUOTE '\"', ESCAPE '\\')").format(tmp=ident_tmp),
            buf,
        )
        not_pk = [c for c in df.columns if c not in self.schema_config.pk]
        if not_pk:
            set_clause = sql.SQL(", ").join(sql.SQL('{c}=s.{c}').format(c=sql.Identifier(col)) for col in not_pk)
            join_cond_update = sql.SQL(" AND ").join(sql.SQL('t.{p}=s.{p}').format(p=sql.Identifier(pk)) for pk in self.schema_config.pk)
            cur.execute(
                sql.SQL("UPDATE {t} t SET {set} FROM {tmp} s WHERE {join}").format(
                    t=sql.Identifier(self.table_name), set=set_clause, tmp=ident_tmp, join=join_cond_update,
                )
            )
        cols_ident_insert = sql.SQL(", ").join(map(sql.Identifier, df.columns))
        join_cond_insert = sql.SQL(" AND ").join(sql.SQL('t.{p}=s.{p}').format(p=sql.Identifier(pk)) for pk in self.schema_config.pk)
        cur.execute(
            sql.SQL("INSERT INTO {t} ({cols}) SELECT {cols_select} FROM {tmp} s WHERE NOT EXISTS (SELECT 1 FROM {t} t WHERE {join})").format(
                t=sql.Identifier(self.table_name), cols=cols_ident_insert, cols_select=cols_ident_insert, tmp=ident_tmp, join=join_cond_insert,
            )
        )

    def save(self, df: pd.DataFrame, use_staging_threshold: int = 500) -> None:
        if not isinstance(df, pd.DataFrame):
            self.logger.warning("save() recebeu tipo inválido, esperado pd.DataFrame.", received_type=type(df))
            return
        if df.empty:
            self.logger.info("DataFrame vazio; nada a salvar.")
            return
        
        df_processed = self._optimize_dataframe(df)
        if df_processed.empty and not df.empty :
            self.logger.warning("DataFrame ficou vazio após otimização, nada a salvar.")
            return

        conn_mgr = get_postgres_conn()
        with conn_mgr.connection() as conn, conn.cursor() as cur:
            try:
                self._create_table(cur, df_processed) 
                self._schema_migration(cur, df_processed)
                if len(df_processed) >= use_staging_threshold:
                    self.logger.info("Usando _upsert_staging para salvar dados.", num_rows=len(df_processed))
                    self._upsert_staging(conn, cur, df_processed)
                else:
                    self.logger.info("Usando _upsert_dynamic para salvar dados.", num_rows=len(df_processed))
                    self._upsert_dynamic(conn, cur, df_processed)
                for expr in self.schema_config.indexes:
                    idx_name = f"idx_{self.table_name}_{re.sub(r'[^0-9a-zA-Z_]+', '', expr)[:40]}"
                    cur.execute(
                        sql.SQL("CREATE INDEX IF NOT EXISTS {i} ON {t} ({expr})").format(
                            i=sql.Identifier(idx_name), t=sql.Identifier(self.table_name), expr=sql.SQL(expr),
                        )
                    )
                conn.commit()
                self.logger.info("Save OK.", num_registros=len(df_processed))
            except Exception as exc:
                self.logger.error("Erro no save(); rollback será executado pelo context manager da conexão.", error=str(exc), exc_info=True)
                conn.rollback()
                raise

    def get_table_columns(self, cur) -> List[str]:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = %s",
            (self.table_name,),
        )
        return [r[0] for r in cur.fetchall()]

    def drop_fully_null_columns(self, protected: Optional[List[str]] = None) -> None:
        if not self.schema_config.allow_column_dropping:
            self.logger.info("Dropping de colunas desabilitado por SchemaConfig.")
            return
        with get_postgres_conn().connection() as conn, conn.cursor() as cur:
            cols_in_db = self.get_table_columns(cur)
            to_keep = set(self.schema_config.pk)
            if protected:
                to_keep.update(protected)
            if self.schema_config.partitioning and self.schema_config.partitioning.key:
                to_keep.add(self.schema_config.partitioning.key)
            
            dropped_count = 0
            for col_name in cols_in_db:
                if col_name in to_keep:
                    continue
                try:
                    query_count = sql.SQL('SELECT COUNT({c}) FROM {t} WHERE {c} IS NOT NULL LIMIT 1').format(
                        c=sql.Identifier(col_name), t=sql.Identifier(self.table_name)
                    )
                    cur.execute(query_count)
                    count_not_null = cur.fetchone()[0]
                    
                    if count_not_null == 0:
                        self.logger.info(f"Coluna '{col_name}' está inteiramente nula. Removendo...")
                        query_drop = sql.SQL('ALTER TABLE {t} DROP COLUMN {c}').format(
                            t=sql.Identifier(self.table_name), c=sql.Identifier(col_name)
                        )
                        cur.execute(query_drop)
                        dropped_count += 1
                except Exception as e_drop_col:
                    self.logger.error(f"Erro ao tentar verificar ou remover coluna '{col_name}'", error=str(e_drop_col), exc_info=True)
            
            if dropped_count > 0:
                conn.commit()
                self.logger.info(f"Removidas {dropped_count} colunas totalmente nulas da tabela '{self.table_name}'.")
            else:
                self.logger.info(f"Nenhuma coluna totalmente nula encontrada para remover da tabela '{self.table_name}'.")