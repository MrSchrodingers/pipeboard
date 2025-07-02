# -*- coding: utf-8 -*-
"""
Infra • Repositório genérico Postgres (CRUD → “upsert” incremental).

Principais características
───────────────────────────────────────────────────────────────────────────────
• Cria a tabela on-the-fly (schema + PK + partições).  
• Migra/expande colunas automaticamente (tipagem forte, JSONB incluso).  
• Dois modos de escrita      ─  dinâmica (execute_values) ou staging + COPY.  
• (Opcional) desliga índices durante a carga – speed-up em tabelas grandes.  
• Otimizações agressivas de DataFrame   (dtypes, sanitização, JSON…).  
• Funções auxiliares p/ limpeza de colunas 100 % nulas.

Uso básico
──────────
repo = RepositorioBase("deals", SchemaConfig(pk=["id"]))
repo.save(df)                       # upsert incremental
"""

from __future__ import annotations

# ───────────────────────── Imports
import csv
import io
import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional
from psycopg2 import sql, errors as pgerr

import numpy as np
import pandas as pd
import psycopg2
import pydantic
import structlog
from psycopg2 import sql
from psycopg2.extras import execute_values

from infrastructure.db.postgres_adapter import get_postgres_conn

_LOG = structlog.get_logger(__name__)

# ───────────────────────── Configurações & tipos
@dataclass
class PartitioningConfig:
    method: Optional[Literal["HASH", "RANGE"]] = None   # hoje só HASH é usado
    key: Optional[str] = None                           # default = primeiro PK
    num_partitions: int = 4


@dataclass
class SchemaConfig:
    pk: List[str]
    types: Dict[str, str] = field(default_factory=dict)     # override manual de tipos
    indexes: List[str] = field(default_factory=list)        # lista “expr1, expr2”
    partitioning: Optional[PartitioningConfig] = None
    allow_column_dropping: bool = True
    disable_indexes_during_load: bool = True
    table_options: Optional[str] = None                    # ex: “WITH (fillfactor=80)”


# linha de corte: acima disso use COPY-staging
COPY_THRESHOLD_DEFAULT = 50

# Mapping pandas → Postgres
_PANDAS_TO_PG: Dict[str, str] = {
    # ints
    "Int8": "SMALLINT",
    "Int16": "SMALLINT",
    "Int32": "INTEGER",
    "Int64": "BIGINT",
    # floats
    "float16": "REAL",
    "float32": "REAL",
    "float64": "DOUBLE PRECISION",
    "Float64": "DOUBLE PRECISION",
    # misc
    "bool": "BOOLEAN",
    "boolean": "BOOLEAN",
    "object": "TEXT",
    "string": "TEXT",
    "category": "TEXT",
    "datetime64[ns]": "TIMESTAMP WITHOUT TIME ZONE",
    "datetime64[ns, UTC]": "TIMESTAMP WITH TIME ZONE",
}

# Simpl. de cast usado no execute_values
_PG_CAST: Dict[str, str] = {
    k.split()[0]: v for k, v in {
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
    }.items()
}

_HARD_LIMIT = 1_600          # MaxTupleAttributeNumber
_BUFFER     = 100            # slots de segurança
_CAP        = _HARD_LIMIT - _BUFFER
_OVERFLOW = "custom_fields_overflow"

# ───────────────────────── Classe principal
class RepositorioBase:
    """
    Persistência resiliente: cria, migra e faz upsert incremental.
    """

    def __init__(self, table_name: str, schema_config: SchemaConfig):
        self.table_name = table_name
        self.schema_config = schema_config
        self._staging_base = f"{table_name}_staging_temp"
        self.log = _LOG.bind(table=table_name)
        # caches simples para acelerar lookups repetidos
        self._pg_type_cache: Dict[tuple[str, str], str] = {}
        self._pg_cast_cache: Dict[str, str] = {}

    # ══════════ helpers de tipagem ══════════
    def _get_pg_type(self, col: str, pd_type: Any) -> str:
        """
        Retorna o tipo PostgreSQL da coluna (override manual ou inferido).
        """
        return (self.schema_config.types.get(col)
                or _PANDAS_TO_PG.get(str(pd_type), "TEXT")).upper()

    @staticmethod
    def _sanitize_str(v: Any) -> Any:
        return v.replace("\x00", "") if isinstance(v, str) else v

    def _safe_json(self, v: Any) -> Optional[str]:
        """
        Serialização segura para JSONB (tratando BaseModel, listas, dicts, NaN, etc).
        """
        def to_serializable(x):
            if isinstance(x, pydantic.BaseModel):
                return x.model_dump()
            if isinstance(x, list):
                return [to_serializable(i) for i in x]
            if isinstance(x, dict):
                return {k: to_serializable(i) for k, i in x.items()}
            if isinstance(x, str):
                return self._sanitize_str(x)
            if x in (None, pd.NA) or (isinstance(x, float) and np.isnan(x)) or x is pd.NaT:
                return None
            return x

        try:
            return json.dumps(to_serializable(v), ensure_ascii=False, default=str)
        except Exception as exc:  # pragma: no cover – debug only
            self.log.warning("json-fail", err=str(exc), preview=str(v)[:120])
            return None

    def ensure_table(self) -> None:
        """
        Cria (ou migra) a tabela mesmo que não existam dados a gravar.
        Não insere nenhuma linha.
        """
        df_model = pd.DataFrame({c: pd.Series(dtype="object") for c in self.schema_config.pk})

        with get_postgres_conn().connection() as conn, conn.cursor() as cur:
            self._create_table(cur, df_model)
            conn.commit()
        self.log.info("ensure-table-ok")
        
    def _convert_val_for_psycopg(self, v: Any, pg_type: str) -> Any:
        """
        Converte valores antes de enviar para Psycopg, lidando com JSONB, texto, NaN etc.
        """
        if isinstance(v, np.generic):
            v = v.item()
        if v is None or (np.isscalar(v) and pd.isna(v)):
            return None

        base_type = pg_type.split("(")[0].strip().upper()
        if base_type == "JSONB":
            return self._safe_json(v)
        if base_type in ("TEXT", "VARCHAR", "CHAR"):
            return self._sanitize_str(str(v))
        return v

    def _send_missing_to_overflow(self, cur, df: pd.DataFrame) -> pd.DataFrame:
        """
        Move para JSONB tudo que ainda não existe na tabela *depois* da
        _schema_migration.

        • Evita UndefinedColumn em UPDATE / INSERT
        • Respeita o mesmo formato usado no synchronizer
        """
        cur.execute("""
            SELECT column_name
            FROM   information_schema.columns
            WHERE  table_schema = current_schema()
            AND    table_name   = %s
        """, (self.table_name,))
        in_table = {c for (c,) in cur.fetchall()}

        # colunas que sobraram no DF mas não foram criadas (bateram no fusível)
        missing = [c for c in df.columns if c not in in_table]

        if not missing:
            return df  # nada a fazer

        # joga no overflow
        df = df.copy()
        df[_OVERFLOW] = (
            df[missing]
            .apply(lambda r: {k: r[k] for k in missing if pd.notna(r[k])}, axis=1)
            .where(lambda s: s.astype(bool), None)
        )
        df.drop(columns=missing, inplace=True, errors="ignore")
        self.log.info("overflow-discarded", n=len(missing))
        return df

    def _get_cast_for(self, pg_type: str) -> str:
        """
        Retorna a string de cast para o execute_values (ex: 'integer' ou 'text'),
        baseada em _PG_CAST. Se não encontrar, retorna pg_type em lowercase.
        """
        base = pg_type.split("(")[0].strip().upper()
        if base not in self._pg_cast_cache:
            self._pg_cast_cache[base] = _PG_CAST.get(base, base.lower())
        return self._pg_cast_cache[base]

    # ══════════ DataFrame otimização ══════════
    def _optimize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Casting, sanitização, unificação de dtypes (rápido e in-place).
        """
        df = df.copy()

        # Duplicated columns? → renomeia mantendo todas
        if df.columns.duplicated().any():
            counts: Dict[str, int] = {}
            new_cols = []
            for c in df.columns:
                if c in counts:
                    counts[c] += 1
                    new_cols.append(f"{c}__dup{counts[c]}")
                else:
                    counts[c] = 0
                    new_cols.append(c)
            df.columns = new_cols

        for col in df.columns:
            ser = df[col]
            pg_type = self._get_pg_type(col, ser.dtype)

            # STRING & SANITIZE
            if ser.dtype == "object" or pd.api.types.is_string_dtype(ser.dtype):
                df[col] = ser.apply(self._sanitize_str)

            # DATETIME coerente
            if "TIMESTAMP" in pg_type or pg_type == "DATE":
                try:
                    df[col] = pd.to_datetime(ser, errors="coerce",
                                             utc="WITH TIME ZONE" in pg_type)
                    if pg_type == "DATE":
                        df[col] = df[col].dt.normalize().dt.date
                except Exception:
                    self.log.debug("datetime-fail", col=col, err=str(col))

            # JSONB – serializa já aqui (evita custo na iteração row-a-row)
            if pg_type == "JSONB":
                df[col] = ser.apply(self._safe_json).astype(object)
                continue

            # Ajustes numéricos automáticos
            if pd.api.types.is_integer_dtype(df[col]) and not str(df[col].dtype).startswith("Int"):
                df[col] = df[col].astype(f"Int{df[col].dtype.itemsize * 8}", errors="ignore")
            elif pd.api.types.is_float_dtype(df[col]) and not str(df[col].dtype).startswith("Float"):
                df[col] = df[col].astype(f"Float{df[col].dtype.itemsize * 8}", errors="ignore")
            elif pd.api.types.is_bool_dtype(df[col]) and str(df[col].dtype) != "boolean":
                df[col] = df[col].astype("boolean", errors="ignore")

        return df.replace({pd.NaT: None, np.nan: None, pd.NA: None})

    # ═════════════ Schema management ═════════════
    def _create_table(self, cur, df: pd.DataFrame) -> None:
        """
        Cria a tabela (se não existir), com PK e partições.  
        Verifica se todas as PKs estão no DataFrame, senão lança ValueError.
        """
        missing = [pk for pk in self.schema_config.pk if pk not in df.columns]
        if missing:
            raise ValueError(f"PK(s) ausentes no DataFrame: {missing}")

        pk_sql = sql.SQL(", ").join(map(sql.Identifier, self.schema_config.pk))
        pconf = self.schema_config.partitioning
        part_sql = (
            sql.SQL("PARTITION BY HASH ({})").format(
                sql.Identifier(pconf.key or self.schema_config.pk[0])
            )
            if pconf and pconf.method == "HASH"
            else sql.SQL("")
        )

        cols_def = sql.SQL(", ").join(
            sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(self._get_pg_type(col, dtype)))
            for col, dtype in df.dtypes.items()
        )

        cur.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {t} ({cols}, PRIMARY KEY ({pk})) {part} {opts};"
            ).format(
                t=sql.Identifier(self.table_name),
                cols=cols_def,
                pk=pk_sql,
                part=part_sql,
                opts=sql.SQL(self.schema_config.table_options or ""),
            )
        )

        # cria sub-partições (HASH)
        if pconf and pconf.method == "HASH":
            for i in range(pconf.num_partitions):
                cur.execute(
                    sql.SQL(
                        "CREATE TABLE IF NOT EXISTS {p} PARTITION OF {t} "
                        "FOR VALUES WITH (MODULUS {m}, REMAINDER {r});"
                    ).format(
                        p=sql.Identifier(f"{self.table_name}_p{i}"),
                        t=sql.Identifier(self.table_name),
                        m=sql.Literal(pconf.num_partitions),
                        r=sql.Literal(i),
                    )
                )

    @staticmethod
    def ensure_overflow_column(cur, table: str, col: str = "custom_fields_overflow") -> None:
        """
        Garante que a coluna JSONB exista.  Usa o `ADD COLUMN IF NOT EXISTS`,
        portanto é idempotente e dispensa PL/pgSQL.
        """
        cur.execute(
            sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} JSONB")
            .format(sql.Identifier(table), sql.Identifier(col))
        )
            
    def _schema_migration(self, cur, df: pd.DataFrame) -> None:
        # 1) colunas visíveis + tipo
        cur.execute("""
            SELECT column_name, data_type
            FROM   information_schema.columns
            WHERE  table_schema = current_schema()
            AND  table_name   = %s
        """, (self.table_name,))
        existing: dict[str, str] = {n: t.upper() for n, t in cur.fetchall()}

        # 2) total de atributos físicos (inclui dropados!)
        cur.execute("""
            SELECT COALESCE(MAX(attnum),0)
            FROM   pg_attribute
            WHERE  attrelid = %s::regclass
        """, (self.table_name,))
        (attr_used,) = cur.fetchone()
        attr_used = int(attr_used)

        protected = {*self.schema_config.pk}
        if (p := self.schema_config.partitioning) and p.key:
            protected.add(p.key)

        add_cols, alter_cols = [], []

        for col in df.columns:
            pg_type = self._get_pg_type(col, df[col].dtype)

            if col not in existing:
                if attr_used >= _CAP:
                    # sem slot físico: envia p/ overflow, não falha
                    self.log.warning(
                        "pg-cap-hit (%s/%s) – '%s' ficará em JSONB overflow",
                        attr_used, _HARD_LIMIT, col,
                    )
                    continue
                add_cols.append((col, pg_type))
                attr_used += 1
            elif col not in protected and existing[col] != pg_type:
                alter_cols.append((col, pg_type))

        # nada a fazer
        if not add_cols and not alter_cols:
            return

        # 3) ADD COLUMN em lote (tenta; se estourar, pula as sobras)
        for col, pg_type in add_cols:
            try:
                cur.execute(
                    sql.SQL("ALTER TABLE {} ADD COLUMN {} {}")
                    .format(sql.Identifier(self.table_name),
                            sql.Identifier(col),
                            sql.SQL(pg_type))
                )
            except pgerr.TooManyColumns:
                self.log.warning("hard-limit atingido ao adicionar '%s' – skip", col)
                break
        if add_cols:
            self.log.info("schema-added-cols", n=len(add_cols))

        # 4) ALTER TYPE (um-a-um)
        for col, pg_type in alter_cols:
            try:
                cur.execute(
                    sql.SQL(
                        "ALTER TABLE {t} ALTER COLUMN {c} TYPE {tp} USING {c}::{tp}"
                    ).format(
                        t=sql.Identifier(self.table_name),
                        c=sql.Identifier(col),
                        tp=sql.SQL(pg_type),
                    )
                )
            except Exception as exc:
                self.log.error("type-migrate-fail", col=col, to=pg_type, err=str(exc))
                raise
        if alter_cols:
            self.log.info("schema-altered-cols", n=len(alter_cols))

    # ═════════════ Upsert strategies ═════════════
    # -- execute_values (rápido < COPY_THRESHOLD_DEFAULT filas)
    def _upsert_dynamic(self, cur, df: pd.DataFrame) -> None:
        """
        Estratégia para DataFrames pequenos: usa execute_values para inserção
        com ON CONFLICT … DO UPDATE.
        """
        cols = list(df.columns)
        pg_types = {c: self._get_pg_type(c, df[c].dtype) for c in cols}

        # Template de cast (ex: %s::integer, %s::text, etc)
        tmpl = "(" + ", ".join(
            f"%s::{self._get_cast_for(pg_types[c])}"
            for c in cols
        ) + ")"

        # Converte cada valor via _convert_val_for_psycopg
        rows = [
            tuple(self._convert_val_for_psycopg(v, pg_types[c])
                  for v, c in zip(row, cols))
            for row in df.itertuples(index=False, name=None)
        ]

        set_parts = [
            sql.SQL("{c}=EXCLUDED.{c}").format(c=sql.Identifier(c))
            for c in cols if c not in self.schema_config.pk
        ]
        on_conflict = (
            sql.SQL("NOTHING")
            if not set_parts
            else sql.SQL("UPDATE SET ") + sql.SQL(", ").join(set_parts)
        )

        stmt = sql.SQL(
            "INSERT INTO {t} ({cols}) VALUES %s "
            "ON CONFLICT ({pk}) DO {act}"
        ).format(
            t=sql.Identifier(self.table_name),
            cols=sql.SQL(", ").join(map(sql.Identifier, cols)),
            pk=sql.SQL(", ").join(map(sql.Identifier, self.schema_config.pk)),
            act=on_conflict,
        )

        execute_values(cur, stmt, rows, template=tmpl, page_size=250)

    # -- COPY → staging (alto volume)
    def _upsert_staging(self, cur, df: pd.DataFrame) -> None:
        """
        Estratégia high-volume: cria um staging TEMPORARY com
        **o mesmo schema do DataFrame**, faz COPY e depois faz
        MERGE (UPDATE + INSERT).
        """
        tmp = f"{self._staging_base}_{pd.Timestamp.utcnow():%Y%m%d%H%M%S%f}"
        ident_tmp = sql.Identifier(tmp)

        # 1) cria staging com colunas == df.columns (NÃO usa LIKE)
        cols_def = ", ".join(
            f"{sql.Identifier(c).as_string(cur)} {self._get_pg_type(c, dt)}"
            for c, dt in df.dtypes.items()
        )
        cur.execute(
            sql.SQL(
                "CREATE TEMP TABLE {tmp} ({cols}) ON COMMIT DROP"
            ).format(tmp=ident_tmp, cols=sql.SQL(cols_def))
        )

        # 2) COPY CSV → staging
        buf = io.StringIO()
        df.to_csv(
            buf, index=False, header=False, sep=",", na_rep="",
            quoting=csv.QUOTE_MINIMAL, escapechar="\\"
        )
        buf.seek(0)
        cur.copy_expert(
            sql.SQL(
                "COPY {tmp} FROM STDIN WITH (FORMAT CSV, NULL '', DELIMITER ',', QUOTE '\"', ESCAPE '\\')"
            ).format(tmp=ident_tmp),
            buf,
        )

        # 3) UPDATE registros que já existem
        not_pk = [c for c in df.columns if c not in self.schema_config.pk]
        if not_pk:
            cur.execute(
                sql.SQL(
                    "UPDATE {t} AS tgt SET {set_cols} "
                    "FROM {tmp} AS src "
                    "WHERE {join}"
                ).format(
                    t=sql.Identifier(self.table_name),
                    tmp=ident_tmp,
                    set_cols=sql.SQL(", ").join(
                        sql.SQL("{c}=src.{c}").format(c=sql.Identifier(c))
                        for c in not_pk
                    ),
                    join=sql.SQL(" AND ").join(
                        sql.SQL("tgt.{pk}=src.{pk}").format(pk=sql.Identifier(pk))
                        for pk in self.schema_config.pk
                    ),
                )
            )

        # 4) INSERT novos
        cols_ident = sql.SQL(", ").join(map(sql.Identifier, df.columns))
        cur.execute(
            sql.SQL(
                "INSERT INTO {t} ({cols}) "
                "SELECT {cols} FROM {tmp} src "
                "WHERE NOT EXISTS ("
                "SELECT 1 FROM {t} tgt WHERE {join}"
                ")"
            ).format(
                t=sql.Identifier(self.table_name),
                cols=cols_ident,
                tmp=ident_tmp,
                join=sql.SQL(" AND ").join(
                    sql.SQL("tgt.{pk}=src.{pk}").format(pk=sql.Identifier(pk))
                    for pk in self.schema_config.pk
                ),
            )
        )

    # ═════════════ API pública ═════════════
    def save(self, df: pd.DataFrame, *, staging_threshold: int = COPY_THRESHOLD_DEFAULT) -> None:
        """
        Upsert incremental (cria/migra tabela conforme necessário).
        """
        if df.empty:
            self.log.info("nothing-to-save")
            return

        df = self._optimize_dataframe(df)
        if df.empty:
            self.log.info("empty-after-optimize")
            return

        with get_postgres_conn().connection() as conn, conn.cursor() as cur:
            # 1) cria tabela / migra esquema
            self._create_table(cur, df)
            self.ensure_overflow_column(cur, self.table_name)
            self._schema_migration(cur, df)
            
            df = self._send_missing_to_overflow(cur, df)
            if df.empty:
                self.log.info("empty-after-overflow")
                return

            # 2) desliga índices (opcional)
            disabled_idx: list[str] = []
            if self.schema_config.disable_indexes_during_load and self.schema_config.indexes:
                cur.execute("""
                    SELECT i.indexname
                    FROM pg_indexes i
                    JOIN pg_class c   ON c.relname = i.indexname
                    JOIN pg_index x   ON x.indexrelid = c.oid
                    WHERE i.schemaname = current_schema()
                      AND i.tablename  = %s
                      AND x.indisprimary = FALSE
                      AND x.indnatts = 1              -- só índice 1-col
                      AND x.indexprs IS NULL          -- não é expression index
                """, (self.table_name,))
                for (idx,) in cur.fetchall():
                    cur.execute(
                        sql.SQL("ALTER INDEX {} SET (statistics_target = 0)").format(
                            sql.Identifier(idx)
                        )
                    )
                    disabled_idx.append(idx)
                if disabled_idx:
                    self.log.info("idx-disabled", n=len(disabled_idx))

            # 3) upsert (dynamic ou staging)
            if len(df) >= staging_threshold:
                self._upsert_staging(cur, df)
            else:
                self._upsert_dynamic(cur, df)

            # 4) restaura índices
            if disabled_idx:
                for idx in disabled_idx:
                    cur.execute(
                        sql.SQL("ALTER INDEX {} RESET (statistics_target)").format(
                            sql.Identifier(idx)
                        )
                    )
                cur.execute(sql.SQL("REINDEX TABLE {}").format(sql.Identifier(self.table_name)))
                self.log.info("idx-rebuilt", n=len(disabled_idx))

            conn.commit()
            self.log.info("save-ok", rows=len(df))

    # ══════════ util – drop colunas totalmente nulas ══════════
    def drop_fully_null_columns(self, *, protected: Optional[List[str]] = None) -> None:
        """
        Remove colunas cujo conteúdo seja exclusivamente NULL ou, no
        caso de arrays, arrays vazios.  Agora é *idempotente* e ignora
        colunas já removidas durante o laço.
        """
        if not self.schema_config.allow_column_dropping:
            return

        with get_postgres_conn().connection() as conn, conn.cursor() as cur:
            while True:  # reaproveita mesma função depois de cada modificação
                cur.execute(
                    """
                    SELECT column_name, udt_name
                    FROM   information_schema.columns
                    WHERE  table_schema = current_schema()
                      AND  table_name   = %s
                    """,
                    (self.table_name,),
                )
                cols_meta = cur.fetchall()
                if not cols_meta:
                    return  # tabela vazia?

                keep = set(self.schema_config.pk) | set(protected or [])
                if self.schema_config.partitioning and self.schema_config.partitioning.key:
                    keep.add(self.schema_config.partitioning.key)

                dropped_any = False
                for col, udt_name in cols_meta:
                    if col in keep:
                        continue

                    is_array = udt_name.startswith("_")
                    cond = (
                        sql.SQL("cardinality({c}) > 0").format(c=sql.Identifier(col))
                        if is_array
                        else sql.SQL("{c} IS NOT NULL").format(c=sql.Identifier(col))
                    )

                    try:
                        cur.execute(
                            sql.SQL("SELECT 1 FROM {t} WHERE {cond} LIMIT 1").format(
                                t=sql.Identifier(self.table_name), cond=cond
                            )
                        )
                    except psycopg2.errors.UndefinedColumn:
                        # Coluna já foi removida em iteração anterior
                        continue

                    if not cur.fetchone():  # coluna vazia
                        cur.execute(
                            sql.SQL("ALTER TABLE {t} DROP COLUMN {c}").format(
                                t=sql.Identifier(self.table_name),
                                c=sql.Identifier(col),
                            )
                        )
                        dropped_any = True

                if not dropped_any:
                    break  # nada mais para remover

            conn.commit()
            if dropped_any:
                self.log.info("cols-dropped", n="many" if dropped_any else 0)
