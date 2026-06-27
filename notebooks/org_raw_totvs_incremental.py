# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 0: Configurações iniciais

# COMMAND ----------

from pyspark.sql import functions as F, Window as W
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parâmetros (desenvolvimento) — oculto; use o bloco de widgets abaixo

# COMMAND ----------

# # Uso para desenvolvimento e testes
# scope_api      = "production"
# table_name     = "SD3"
# catalog        = "ihara_datalake_incremental"
# table = {
#     "table_name": table_name,
#     "file_path":  f"schema_table/totvs/{table_name}.json",
# }
# df    = spark.read.json(f"/Volumes/{catalog}/raw/{table['file_path']}")
# table = df.collect()[0].asDict()
# sink           = f"{catalog}.raw.{table['table_name'].lower()}"
# control_sink   = f"{catalog}.raw._ingestion_control"
# load_type      = table["load_type"]
# lookback_hours = 1
# primary_keys   = list(table["primary_keys"]) if table.get("primary_keys") else []

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parâmetros (produção via widgets / job parameters)

# COMMAND ----------

scope_api  = "production"
catalog    = dbutils.widgets.get("catalog")
path_files = dbutils.widgets.get("path_files")
table_raw  = dbutils.widgets.get("table")

df    = spark.read.json(f"/Volumes/{catalog}/{path_files}")
table = df.collect()[0].asDict()

table_name     = table["table_name"]
primary_keys   = list(table["primary_keys"]) if table.get("primary_keys") else []
load_type      = table["load_type"]
lookback_hours = 1

sink         = f"{catalog}.raw.{table_raw.lower()}"
control_sink = f"{catalog}.raw._ingestion_control"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Obtenção das secrets
# MAGIC Credenciais Oracle via Databricks Secrets.

# COMMAND ----------

oracle_port         = "1521"
oracle_host         = dbutils.secrets.get(scope_api, "totvs_oracle_host")
oracle_service_name = dbutils.secrets.get(scope_api, "totvs_oracle_service_name")
oracle_user         = dbutils.secrets.get(scope_api, "totvs_oracle_user")
oracle_password     = dbutils.secrets.get(scope_api, "totvs_oracle_password")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Extração Oracle

# COMMAND ----------

def read_from_oracle(query: str) -> DataFrame:
    jdbc_url = f"jdbc:oracle:thin:@//{oracle_host}:{oracle_port}/{oracle_service_name}"
    connection_properties = {
        "user": oracle_user,
        "password": oracle_password,
        "driver": "oracle.jdbc.OracleDriver",
        "fetchsize": "5000",
    }
    return spark.read.jdbc(url=jdbc_url, table=f"({query}) t", properties=connection_properties)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Controle de watermark

# COMMAND ----------

def _create_control_table_if_not_exists():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {control_sink} (
            table_name      STRING,
            last_hwm        TIMESTAMP,
            last_run_ts     TIMESTAMP,
            last_run_status STRING,
            rows_read       LONG,
            rows_merged     LONG
        )
        USING DELTA
        PARTITIONED BY (table_name)
    """)


def get_last_hwm() -> datetime:
    _create_control_table_if_not_exists()
    rows = spark.sql(
        f"SELECT last_hwm FROM {control_sink} WHERE table_name = '{table_name}'"
    ).collect()
    return rows[0]["last_hwm"] if rows else datetime(1900, 1, 1)


def update_control(last_hwm, rows_read: int, rows_merged: int, status: str = "SUCCESS"):
    row_df = spark.createDataFrame([{
        "table_name":      table_name,
        "last_hwm":        last_hwm,
        "last_run_ts":     datetime.utcnow(),
        "last_run_status": status,
        "rows_read":       rows_read,
        "rows_merged":     rows_merged,
    }])
    (DeltaTable.forName(spark, control_sink).alias("t")
        .merge(row_df.alias("s"), "t.table_name = s.table_name")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Lógica de carga

# COMMAND ----------

def __is_first_load() -> bool:
    try:
        spark.table(sink)
        return False
    except Exception:
        return True

# COMMAND ----------

def load_full():
    print(f"[FULL LOAD] Iniciando: {table_name}")

    query = f"SELECT * FROM {table_name}010 WHERE D_E_L_E_T_ = ' '"
    if table_name == "SYS_COMPANY":
        query = f"SELECT * FROM {table_name} WHERE D_E_L_E_T_ = ' '"

    df = read_from_oracle(query)
    df = df.withColumn(
        "dh_insercao_raw",
        F.from_utc_timestamp(F.current_timestamp(), "Brazil/East").cast("timestamp"),
    )

    (df.write
        .format("delta")
        .option("mergeSchema", "true")
        .option("overwriteSchema", "true")
        .saveAsTable(sink, mode="overwrite"))

    print(f"[FULL LOAD] Concluído. Destino: {sink}")

# COMMAND ----------

def load_incremental():
    print(f"[INCREMENTAL] Iniciando: {table_name}")

    last_hwm      = get_last_hwm()
    effective_hwm = (last_hwm - timedelta(hours=lookback_hours)).strftime("%Y-%m-%d %H:%M:%S")
    print(f"  Watermark anterior : {last_hwm}")
    print(f"  Filtro efetivo     : {effective_hwm}  (lookback: {lookback_hours}h)")

    query = f"""
        SELECT * FROM {table_name}010
        WHERE (  S_T_A_M_P_ >= TIMESTAMP '{effective_hwm}'
              OR I_N_S_D_T_ >= TIMESTAMP '{effective_hwm}')
    """
    raw_df = read_from_oracle(query)
    raw_df.cache()

    rows_read = raw_df.count()
    print(f"  Linhas lidas do Oracle : {rows_read}")

    if rows_read == 0:
        raw_df.unpersist()
        update_control(last_hwm, 0, 0)
        print("  Sem novos dados. Encerrando.")
        return

    new_hwm = raw_df.select(F.greatest(F.max("S_T_A_M_P_"), F.max("I_N_S_D_T_"))).collect()[0][0]
    if new_hwm is None:
        new_hwm = last_hwm  # no valid watermark in batch — keep previous to avoid UTC/local mismatch

    # Deduplicar por chave primária, mantendo o registro mais recente.
    # COALESCE garante que registros novos (S_T_A_M_P_ nulo, só I_N_S_D_T_ preenchido)
    # não sejam descartados por ordenação nula.
    w = W.partitionBy(*primary_keys).orderBy(
        F.coalesce(F.col("S_T_A_M_P_"), F.col("I_N_S_D_T_")).desc()
    )
    staged_df = (
        raw_df
        .withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn")
        .withColumn(
            "dh_insercao_raw",
            F.from_utc_timestamp(F.current_timestamp(), "Brazil/East").cast("timestamp"),
        )
    )

    rows_merged     = staged_df.count()
    merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in primary_keys])

    (DeltaTable.forName(spark, sink).alias("target")
        .merge(staged_df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

    raw_df.unpersist()
    update_control(new_hwm, rows_read, rows_merged)
    print(f"  Linhas merged      : {rows_merged}")
    print(f"  Novo watermark     : {new_hwm}")
    print(f"[INCREMENTAL] Concluído. Destino: {sink}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Executar

# COMMAND ----------

if __is_first_load():
    load_full()
    dbutils.notebook.exit("First load completed")
elif load_type == "full":
    load_full()
else:
    load_incremental()
