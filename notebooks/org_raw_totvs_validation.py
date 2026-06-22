# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 0: Configurações iniciais

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parâmetros (desenvolvimento)
# MAGIC Para produção, substituir este bloco por `dbutils.widgets.get()`

# COMMAND ----------

scope_api      = "production"
catalog        = "ihara_datalake_incremental"
table_name     = "SD3"
primary_keys   = ["R_E_C_N_O_"]
tolerance_pct  = 0.01  # 1% — diferença máxima aceitável entre contagens

sink            = f"{catalog}.raw.{table_name.lower()}"
validation_sink = f"{catalog}.raw._validation_log"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Obtenção das secrets

# COMMAND ----------

oracle_port         = "1521"
oracle_host         = dbutils.secrets.get(scope_api, "totvs_oracle_host")
oracle_service_name = dbutils.secrets.get(scope_api, "totvs_oracle_service_name")
oracle_user         = dbutils.secrets.get(scope_api, "totvs_oracle_user")
oracle_password     = dbutils.secrets.get(scope_api, "totvs_oracle_password")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Funções auxiliares

# COMMAND ----------

def read_from_oracle(query: str) -> DataFrame:
    jdbc_url = f"jdbc:oracle:thin:@//{oracle_host}:{oracle_port}/{oracle_service_name}"
    connection_properties = {
        "user": oracle_user,
        "password": oracle_password,
        "driver": "oracle.jdbc.OracleDriver",
        "fetchsize": "5000",
    }
    df = spark.read.jdbc(url=jdbc_url, table=f"({query}) t", properties=connection_properties)
    # Normalize Oracle column names to lowercase
    return df.toDF(*[c.lower() for c in df.columns])

# COMMAND ----------

def _create_validation_log_if_not_exists():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {validation_sink} (
            validated_at     TIMESTAMP,
            table_name       STRING,
            oracle_count     LONG,
            delta_count      LONG,
            count_diff       LONG,
            count_diff_pct   DOUBLE,
            oracle_max_pk    STRING,
            delta_max_pk     STRING,
            max_pk_match     BOOLEAN,
            delta_duplicates LONG,
            status           STRING,
            notes            STRING
        )
        USING DELTA
    """)


def save_validation_result(result: dict):
    _create_validation_log_if_not_exists()
    (spark.createDataFrame([result])
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(validation_sink))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Coletar métricas

# COMMAND ----------

pk_col = primary_keys[0]

# Oracle: total de registros ativos e maior chave primária
oracle_query = f"""
    SELECT
        COUNT(*)      AS oracle_count,
        MAX({pk_col}) AS oracle_max_pk
    FROM {table_name}010
    WHERE D_E_L_E_T_ = ' '
"""
oracle_row   = read_from_oracle(oracle_query).collect()[0]
oracle_count = int(oracle_row["oracle_count"])
oracle_max   = int(float(oracle_row["oracle_max_pk"])) if oracle_row["oracle_max_pk"] is not None else 0

print(f"Oracle — registros: {oracle_count:>12,}  |  max({pk_col}): {oracle_max:,}")

# COMMAND ----------

# Delta: total de registros, maior chave primária e verificação de duplicatas
delta_row = spark.sql(f"""
    SELECT
        COUNT(*)                            AS delta_count,
        MAX(CAST({pk_col} AS BIGINT))       AS delta_max_pk,
        COUNT(*) - COUNT(DISTINCT {pk_col}) AS delta_duplicates
    FROM {sink}
""").collect()[0]

delta_count      = int(delta_row["delta_count"])
delta_max        = int(delta_row["delta_max_pk"]) if delta_row["delta_max_pk"] is not None else 0
delta_duplicates = int(delta_row["delta_duplicates"])

print(f"Delta  — registros: {delta_count:>12,}  |  max({pk_col}): {delta_max:,}  |  duplicatas: {delta_duplicates:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Comparar e salvar resultado

# COMMAND ----------

count_diff     = delta_count - oracle_count
count_diff_pct = abs(count_diff) / oracle_count if oracle_count > 0 else 0.0
max_pk_match   = oracle_max == delta_max

notes = []
if delta_duplicates > 0:
    notes.append(f"{delta_duplicates:,} chave(s) duplicada(s) em Delta")
if not max_pk_match:
    notes.append(f"max({pk_col}) diverge — Oracle: {oracle_max:,}  Delta: {delta_max:,}")
if count_diff_pct > tolerance_pct:
    notes.append(f"diferença de contagem {count_diff_pct:.4%} acima da tolerância ({tolerance_pct:.2%})")

if delta_duplicates > 0 or not max_pk_match:
    status = "FAIL"
elif count_diff_pct > tolerance_pct:
    status = "WARN"
else:
    status = "PASS"

result = {
    "validated_at":     datetime.utcnow(),
    "table_name":       table_name,
    "oracle_count":     oracle_count,
    "delta_count":      delta_count,
    "count_diff":       count_diff,
    "count_diff_pct":   round(count_diff_pct, 6),
    "oracle_max_pk":    str(oracle_max),
    "delta_max_pk":     str(delta_max),
    "max_pk_match":     max_pk_match,
    "delta_duplicates": delta_duplicates,
    "status":           status,
    "notes":            " | ".join(notes) if notes else "ok",
}

save_validation_result(result)

# COMMAND ----------

# Resumo final
divider = "=" * 54
print(divider)
print(f"  VALIDAÇÃO {table_name:<10}  →  {status}")
print(divider)
print(f"  Registros Oracle  : {oracle_count:>14,}")
print(f"  Registros Delta   : {delta_count:>14,}")
print(f"  Diferença         : {count_diff:>+14,}  ({count_diff_pct:.4%})")
print(f"  max({pk_col})     : {'✅  IGUAL' if max_pk_match else '❌  DIVERGE'}")
print(f"  Duplicatas Delta  : {delta_duplicates:>14,}  {'✅' if delta_duplicates == 0 else '❌'}")
if notes:
    print()
    for note in notes:
        print(f"  ⚠  {note}")
print(divider)

if status == "FAIL":
    raise Exception(f"Validação FALHOU para {table_name}. Verifique _validation_log para detalhes.")
