# Databricks Lakeflow / DLT - Gold Layer
# Modelo dimensional Gold:
# - fact_transactions_vm
# - dim_merchant_vm
# - dim_user_vm
# - dim_channel_vm
# - dim_date_vm

import dlt

from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    lit,
    current_timestamp,
    sha2,
    concat,
    split,
    substring,
    row_number,
    when
)

from pyspark.sql.window import Window


# =============================================================================
# 1. Parametros base
# =============================================================================

CATALOG        = "fintech_finpay"
SILVER_SCHEMA  = "silver"
GOLD_SCHEMA    = "gold"

# Tablas origen Silver
SILVER_TRANSACTIONS = f"{CATALOG}.{SILVER_SCHEMA}.transactions"
SILVER_MERCHANTS    = f"{CATALOG}.{SILVER_SCHEMA}.merchants"
SILVER_USERS        = f"{CATALOG}.{SILVER_SCHEMA}.users"

# Tablas destino Gold
GOLD_FACT_TRANSACTIONS = f"{CATALOG}.{GOLD_SCHEMA}.fact_transactions_vm"
GOLD_DIM_MERCHANT      = f"{CATALOG}.{GOLD_SCHEMA}.dim_merchant_vm"
GOLD_DIM_USER          = f"{CATALOG}.{GOLD_SCHEMA}.dim_user_vm"
GOLD_DIM_CHANNEL       = f"{CATALOG}.{GOLD_SCHEMA}.dim_channel_vm"
GOLD_DIM_DATE          = f"{CATALOG}.{GOLD_SCHEMA}.dim_date_vm"


# =============================================================================
# 2. fact_transactions_vm
# readStream: transactions Silver es tabla DLT del mismo pipeline
# =============================================================================

@dlt.table(
    name=GOLD_FACT_TRANSACTIONS,
    comment="""
    Fact Gold con metricas base para analitica:
    transacciones, montos y reversas.
    Granularidad: fecha x usuario x comercio x canal.
    """,
    table_properties={"quality": "gold"}
)
def fact_transactions_vm():

    return (
        dlt.readStream(SILVER_TRANSACTIONS)
        .groupBy(
            col("transaction_date"),
            col("user_id"),
            col("merchant_id"),
            col("channel").alias("channel_id")
        )
        .agg(
            count(lit(1))
                .alias("transaction_count"),

            sum(when(col("status") == "aprobado",  1).otherwise(0))
                .alias("approved_transactions"),

            sum(when(col("status") == "rechazado", 1).otherwise(0))
                .alias("rejected_transactions"),

            sum(when(col("status") == "pendiente", 1).otherwise(0))
                .alias("pending_transactions"),

            sum(when(col("transaction_type") == "reversa", 1).otherwise(0))
                .alias("reversal_transactions"),

            sum(col("amount"))
                .alias("total_amount"),

            avg(col("amount"))
                .alias("avg_amount")
        )
        .withColumn("_gold_processed_timestamp", current_timestamp())
    )


# =============================================================================
# 3. dim_merchant_vm
# readStream: merchants Silver es tabla DLT del mismo pipeline
# =============================================================================

@dlt.table(
    name=GOLD_DIM_MERCHANT,
    comment="Dimension Gold de comercios.",
    table_properties={"quality": "gold"}
)
def dim_merchant_vm():

    return (
        dlt.readStream(SILVER_MERCHANTS)
        .select(
            col("merchant_id"),
            col("merchant_name"),
            col("category"),
            col("country"),
            col("status").alias("affiliation_status"),
            col("risk_level"),
            col("affiliation_date"),
            current_timestamp().alias("_gold_processed_timestamp")
        )
    )


# =============================================================================
# 4. dim_user_vm
#
# Por que stream + batch (dlt.read) y no todo readStream:
#
# El canal preferido requiere Window + row_number sobre TODOS los registros
# del usuario para elegir el canal con mas transacciones. En streaming esto
# no es posible: DLT procesa micro-batches parciales y nunca tiene la vista
# completa del historial de un usuario en un solo batch.
#
# Solucion: stream + static join (patron soportado por DLT)
#   - users        -> dlt.readStream  (recibe altas y actualizaciones)
#   - transactions -> dlt.read        (snapshot completo para Window function)
# =============================================================================

@dlt.table(
    name=GOLD_DIM_USER,
    comment="""
    Dimension Gold de usuarios con masking PII y canal preferido.
    Usa stream + static join: users en stream, transactions como snapshot
    para calcular canal preferido via Window function.
    """,
    table_properties={
        "quality": "gold",
        "pii_masked": "true"
    }
)
def dim_user_vm():

    # -- Snapshot de transactions para calcular canal preferido --
    transactions_df = dlt.read(SILVER_TRANSACTIONS)

    user_channel_df = (
        transactions_df
        .groupBy(col("user_id"), col("channel"))
        .agg(count(lit(1)).alias("channel_transaction_count"))
    )

    window_spec = (
        Window
        .partitionBy("user_id")
        .orderBy(
            col("channel_transaction_count").desc(),
            col("channel").asc()          # desempate deterministico
        )
    )

    preferred_channel_df = (
        user_channel_df
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .select(
            col("user_id"),
            col("channel").alias("preferred_channel")
        )
    )

    # -- Stream de usuarios + join con snapshot --
    return (
        dlt.readStream(SILVER_USERS).alias("u")
        .join(
            preferred_channel_df.alias("pc"),
            col("u.user_id") == col("pc.user_id"),
            "left"
        )
        .select(
            col("u.user_id"),

            sha2(col("u.full_name"), 256)
                .alias("full_name_masked"),

            concat(lit("****"), substring(col("u.document_id"), -4, 4))
                .alias("document_id_masked"),

            concat(lit("***@"), split(col("u.email"), "@").getItem(1))
                .alias("email_masked"),

            concat(lit("*******"), substring(col("u.phone"), -4, 4))
                .alias("phone_masked"),

            col("u.country"),
            col("u.segment").alias("risk_segment"),
            col("u.registration_date"),
            col("pc.preferred_channel"),
            current_timestamp().alias("_gold_processed_timestamp")
        )
    )


# =============================================================================
# 5. dim_channel_vm
#
# Por que NO puede ser readStream:
# Datos generados en memoria con spark.createDataFrame hardcodeado.
# No existe tabla ni stream de origen. Batch es la unica opcion.
# =============================================================================

@dlt.table(
    name=GOLD_DIM_CHANNEL,
    comment="Dimension Gold de canales. Catalogo estatico de referencia.",
    table_properties={"quality": "gold"}
)
def dim_channel_vm():

    return (
        spark.createDataFrame(
            [
                ("web", "Web", "Canal web"),
                ("app", "App", "Aplicacion movil"),
                ("pos", "POS", "Punto de venta fisico")
            ],
            ["channel_id", "channel_name", "channel_description"]
        )
        .withColumn("_gold_processed_timestamp", current_timestamp())
    )


# =============================================================================
# 6. dim_date_vm
#
# Por que NO puede ser readStream:
# Calendario generado con spark.sql + sequence(). Datos sinteticos calculados
# en el momento, no hay tabla ni stream de origen. Batch es la unica opcion.
# =============================================================================

@dlt.table(
    name=GOLD_DIM_DATE,
    comment="Dimension Gold calendario. Tabla de referencia estatica (2020-2035).",
    table_properties={"quality": "gold"}
)
def dim_date_vm():

    return spark.sql("""
        SELECT
            CAST(date_format(calendar_date, 'yyyyMMdd') AS INT)  AS date_id,
            calendar_date,
            dayofmonth(calendar_date)                            AS day_number,
            dayofweek(calendar_date)                             AS day_of_week_number,
            date_format(calendar_date, 'E')                      AS day_name,
            weekofyear(calendar_date)                            AS week_number,
            month(calendar_date)                                 AS month_number,
            date_format(calendar_date, 'MMMM')                   AS month_name,
            quarter(calendar_date)                               AS quarter_number,
            year(calendar_date)                                  AS year_number,
            current_timestamp()                                  AS _gold_processed_timestamp
        FROM (
            SELECT explode(
                sequence(
                    to_date('2020-01-01'),
                    to_date('2035-12-31'),
                    interval 1 day
                )
            ) AS calendar_date
        )
    """)