# Databricks Delta Live Tables / Lakeflow - Silver Layer
# Con soporte de tabla de cuarentena para registros que no superan reglas de calidad criticas

import dlt

from pyspark.sql.functions import (
    col,
    trim,
    lower,
    upper,
    regexp_replace,
    when,
    to_date,
    coalesce,
    current_timestamp,
    to_json,
    struct,
    lit
)

from pyspark.sql.types import DecimalType


# =============================================================================
# CONFIGURACION
# =============================================================================

CATALOG = "fintech_finpay"

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

BRONZE_TRANSACTIONS_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.transactions"
BRONZE_MERCHANTS_TABLE    = f"{CATALOG}.{BRONZE_SCHEMA}.merchants"
BRONZE_USERS_TABLE        = f"{CATALOG}.{BRONZE_SCHEMA}.users"

SILVER_TRANSACTIONS_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.transactions"
SILVER_MERCHANTS_TABLE    = f"{CATALOG}.{SILVER_SCHEMA}.merchants"
SILVER_USERS_TABLE        = f"{CATALOG}.{SILVER_SCHEMA}.users"
SILVER_QUARANTINE_TABLE   = f"{CATALOG}.{SILVER_SCHEMA}.quarantine"


# =============================================================================
# TABLA DE CUARENTENA - Creacion unica en Silver
# Persiste registros rechazados con trazabilidad completa para auditoria
# =============================================================================

dlt.create_streaming_table(
    name=SILVER_QUARANTINE_TABLE,
    comment="Tabla de cuarentena Silver. Almacena registros rechazados por reglas de calidad criticas con trazabilidad completa para auditoria y reprocesamiento.",
    table_properties={
        "quality": "quarantine",
        "delta.enableChangeDataFeed": "true"
    }
)


# =============================================================================
# UTILIDAD: Construir flag de validacion y motivo de rechazo por campo
# Cada tabla define sus propias condiciones de validacion usando esta funcion
# =============================================================================

def build_rejection_reason(df, validations: list[tuple]):
    """
    Agrega columna '_rejection_reason' al DataFrame con el primer motivo de rechazo encontrado.

    Args:
        df          : DataFrame con los registros a evaluar
        validations : Lista de tuplas (condicion_invalida_col, nombre_regla, campo_afectado)
                      condicion_invalida_col es una Column de Spark que evalua True cuando el registro es INVALIDO

    Returns:
        DataFrame con columnas adicionales:
            _is_valid        : True si pasa todas las reglas, False si falla alguna
            _rejection_reason: Nombre de la regla que fallo (primera encontrada)
            _rejected_field  : Campo que causo el rechazo
    """
    rejection_reason_expr = lit(None).cast("string")
    rejected_field_expr   = lit(None).cast("string")

    # Recorrer en orden inverso para que el primer fallo prevalezca (when encadenado)
    for invalid_condition, rule_name, field_name in reversed(validations):
        rejection_reason_expr = when(invalid_condition, lit(rule_name)).otherwise(rejection_reason_expr)
        rejected_field_expr   = when(invalid_condition, lit(field_name)).otherwise(rejected_field_expr)

    is_valid_expr = lit(True)
    for invalid_condition, _, _ in validations:
        is_valid_expr = when(invalid_condition, lit(False)).otherwise(is_valid_expr)

    return (
        df
        .withColumn("_rejection_reason", rejection_reason_expr)
        .withColumn("_rejected_field",   rejected_field_expr)
        .withColumn("_is_valid",         is_valid_expr)
    )


def to_quarantine_shape(df, source_table: str):
    """
    Transforma un DataFrame de rechazos al esquema de la tabla de cuarentena.

    Args:
        df           : DataFrame con columnas _rejection_reason y _rejected_field
        source_table : Nombre de la tabla de origen (ej: 'bronze.transactions')

    Returns:
        DataFrame con el esquema de fintech_finpay.silver.quarantine
    """
    original_cols = [c for c in df.columns if not c.startswith("_")]

    return (
        df
        .filter(col("_is_valid") == False)  # noqa: E712
        .withColumn("source_table",    lit(source_table))
        .withColumn("rejection_reason", col("_rejection_reason"))
        .withColumn("rejected_field",   col("_rejected_field"))
        .withColumn("processed_at",     current_timestamp())
        .withColumn("original_record",  to_json(struct(*[col(c) for c in original_cols])))
        .select(
            "source_table",
            "rejection_reason",
            "rejected_field",
            "processed_at",
            "original_record"
        )
    )


# =============================================================================
# TRANSACTIONS
# =============================================================================

# -- Paso 1: Staging con transformaciones y expects como metricas (sin drop) --
# Se usa @dlt.expect en lugar de @dlt.expect_or_drop para que DLT registre
# las metricas de calidad en el UI sin descartar los registros invalidos.
# El filtrado lo controlamos manualmente en los pasos siguientes.

@dlt.table(
    name="stg_transactions_silver",
    comment="Staging de transactions: transformaciones aplicadas + flags de validacion. Base para Silver y Quarantine."
)
@dlt.expect("transaction_id_not_null",        "transaction_id IS NOT NULL")
@dlt.expect("transaction_id_valid_format",    "transaction_id RLIKE '^TXN-[0-9]{8}-[0-9]{5}$'")
@dlt.expect("user_id_not_null",               "user_id IS NOT NULL")
@dlt.expect("user_id_valid_format",           "user_id RLIKE '^USR-[0-9]{6}$'")
@dlt.expect("merchant_id_not_null",           "merchant_id IS NOT NULL")
@dlt.expect("merchant_id_valid_format",       "merchant_id RLIKE '^MCH-[0-9]{5}$'")
@dlt.expect("channel_valid_domain",           "channel IN ('web', 'app', 'pos')")
@dlt.expect("transaction_type_valid_domain",  "transaction_type IN ('pago', 'reversa', 'retiro')")
@dlt.expect("amount_not_null",                "amount IS NOT NULL")
@dlt.expect("amount_positive",                "amount > 0")
@dlt.expect("currency_valid_domain",          "currency IN ('PEN', 'USD', 'COP', 'MXN', 'CLP', 'ARS')")
@dlt.expect("transaction_date_not_null",      "transaction_date IS NOT NULL")
@dlt.expect("status_valid_domain",            "status IN ('aprobado', 'rechazado', 'pendiente')")
@dlt.expect(
    "reference_id_rule",
    """
    (
        transaction_type = 'reversa'
        AND reference_id IS NOT NULL
        AND reference_id RLIKE '^TXN-[0-9]{8}-[0-9]{5}$'
    )
    OR
    (
        transaction_type IN ('pago', 'retiro')
        AND reference_id IS NULL
    )
    """
)
def stg_transactions_silver():

    df = spark.readStream.table(BRONZE_TRANSACTIONS_TABLE)

    amount_raw = trim(col("amount"))

    amount_clean = (
        when(
            amount_raw.rlike(r"^[0-9]{1,3}(\.[0-9]{3})+,[0-9]{1,2}$"),
            regexp_replace(
                regexp_replace(amount_raw, r"\.", ""),
                ",",
                "."
            )
        )
        .otherwise(amount_raw)
    )

    df_transformed = (
        df
        .select(
            trim(col("transaction_id")).alias("transaction_id"),
            trim(col("user_id")).alias("user_id"),
            trim(col("merchant_id")).alias("merchant_id"),
            lower(trim(col("channel"))).alias("channel"),
            lower(trim(col("transaction_type"))).alias("transaction_type"),
            amount_clean.cast(DecimalType(18, 2)).alias("amount"),
            upper(trim(col("currency"))).alias("currency"),
            coalesce(
                to_date(trim(col("transaction_date")), "yyyy-MM-dd"),
                to_date(trim(col("transaction_date")), "dd/MM/yyyy")
            ).alias("transaction_date"),
            lower(trim(col("status"))).alias("status"),
            when(
                trim(col("reference_id")) == "",
                None
            ).otherwise(
                trim(col("reference_id"))
            ).alias("reference_id"),
            col("_source_name"),
            col("_source_file"),
            col("_ingestion_timestamp"),
            col("ingestion_date"),
            current_timestamp().alias("_silver_processed_timestamp")
        )
    )

    # Definir reglas de validacion: (condicion_de_invalidez, nombre_regla, campo)
    validations = [
        (col("transaction_id").isNull(),                                                          "nulo_critico",      "transaction_id"),
        (~col("transaction_id").rlike(r"^TXN-[0-9]{8}-[0-9]{5}$"),                              "formato_incorrecto", "transaction_id"),
        (col("user_id").isNull(),                                                                 "nulo_critico",      "user_id"),
        (~col("user_id").rlike(r"^USR-[0-9]{6}$"),                                               "formato_incorrecto", "user_id"),
        (col("merchant_id").isNull(),                                                             "nulo_critico",      "merchant_id"),
        (~col("merchant_id").rlike(r"^MCH-[0-9]{5}$"),                                           "formato_incorrecto", "merchant_id"),
        (~col("channel").isin("web", "app", "pos"),                                               "dominio_invalido",   "channel"),
        (~col("transaction_type").isin("pago", "reversa", "retiro"),                              "dominio_invalido",   "transaction_type"),
        (col("amount").isNull(),                                                                  "nulo_critico",      "amount"),
        (col("amount") <= 0,                                                                      "valor_invalido",     "amount"),
        (~col("currency").isin("PEN", "USD", "COP", "MXN", "CLP", "ARS"),                        "dominio_invalido",   "currency"),
        (col("transaction_date").isNull(),                                                        "nulo_critico",      "transaction_date"),
        (~col("status").isin("aprobado", "rechazado", "pendiente"),                               "dominio_invalido",   "status"),
        (
            ~(
                ((col("transaction_type") == "reversa") & col("reference_id").isNotNull() & col("reference_id").rlike(r"^TXN-[0-9]{8}-[0-9]{5}$"))
                | (col("transaction_type").isin("pago", "retiro") & col("reference_id").isNull())
            ),
            "regla_referencia_invalida",
            "reference_id"
        ),
    ]

    return build_rejection_reason(df_transformed, validations)


# -- Paso 2a: Vista de registros validos para Silver --

@dlt.view(name="vw_transactions_silver_prepared")
def vw_transactions_silver_prepared():
    return (
        dlt.readStream("stg_transactions_silver")
        .filter(col("_is_valid") == True)  # noqa: E712
        .drop("_is_valid", "_rejection_reason", "_rejected_field")
    )


# -- Paso 2b: Vista de registros invalidos para Quarantine --


# -- Paso 3: Tabla Silver de transactions --

dlt.create_streaming_table(
    name=SILVER_TRANSACTIONS_TABLE,
    comment="Tabla Silver de transacciones curada, casteada y validada desde Bronze.",
    table_properties={
        "quality": "silver",
        "primary_key": "transaction_id"
    }
)

dlt.apply_changes(
    target=SILVER_TRANSACTIONS_TABLE,
    source="vw_transactions_silver_prepared",
    keys=["transaction_id"],
    sequence_by=col("_ingestion_timestamp"),
    stored_as_scd_type=1
)




# =============================================================================
# MERCHANTS
# =============================================================================

@dlt.table(
    name="stg_merchants_silver",
    comment="Staging de merchants: transformaciones aplicadas + flags de validacion. Base para Silver y Quarantine."
)
@dlt.expect("merchant_id_not_null",           "merchant_id IS NOT NULL")
@dlt.expect("merchant_id_valid_format",       "merchant_id RLIKE '^MCH-[0-9]{5}$'")
@dlt.expect("merchant_name_not_null_or_empty","merchant_name IS NOT NULL AND length(merchant_name) > 0")
@dlt.expect(
    "category_valid_domain",
    """
    category IN (
        'retail', 'restaurante', 'farmacia', 'supermercado', 'tecnologia',
        'transporte', 'educacion', 'salud', 'entretenimiento', 'moda'
    )
    """
)
@dlt.expect("country_valid_domain",           "country IN ('PE', 'CO', 'MX', 'CL', 'AR')")
@dlt.expect("affiliation_date_valid",         "affiliation_date IS NOT NULL")
@dlt.expect("status_valid_domain",            "status IN ('activo', 'inactivo', 'suspendido')")
@dlt.expect("risk_level_valid_domain",        "risk_level IS NULL OR risk_level IN ('bajo', 'medio', 'alto')")
def stg_merchants_silver():

    df = spark.readStream.table(BRONZE_MERCHANTS_TABLE)

    category_raw   = lower(trim(col("category")))
    country_raw    = upper(trim(col("country")))
    status_raw     = lower(trim(col("status")))
    risk_level_raw = lower(trim(col("risk_level")))

    valid_categories = [
        "retail", "restaurante", "farmacia", "supermercado", "tecnologia",
        "transporte", "educacion", "salud", "entretenimiento", "moda"
    ]
    valid_countries = ["PE", "CO", "MX", "CL", "AR"]
    valid_statuses  = ["activo", "inactivo", "suspendido"]
    valid_risks     = ["bajo", "medio", "alto"]

    df_transformed = (
        df
        .select(
            trim(col("merchant_id")).alias("merchant_id"),
            when(
                trim(col("merchant_name")) == "",
                None
            ).otherwise(
                trim(col("merchant_name"))
            ).alias("merchant_name"),
            category_raw.alias("category"),
            country_raw.alias("country_raw"),
            coalesce(
                to_date(trim(col("affiliation_date")), "yyyy-MM-dd"),
                to_date(trim(col("affiliation_date")), "dd/MM/yyyy")
            ).alias("affiliation_date"),
            status_raw.alias("status_raw"),
            when(
                (risk_level_raw == "") |
                (risk_level_raw == "n/a") |
                (risk_level_raw == "sin clasificar"),
                None
            ).otherwise(
                risk_level_raw
            ).alias("risk_level_raw"),
            col("_source_name"),
            col("_source_file"),
            col("_ingestion_timestamp"),
            col("ingestion_date"),
            current_timestamp().alias("_silver_processed_timestamp")
        )
        .withColumn(
            "country",
            when(col("country_raw").isin("PE", "PERU", "PER"), "PE")
            .when(col("country_raw").isin("CO", "COL", "COLOMBIA"), "CO")
            .when(col("country_raw").isin("MX", "MEX", "MEXICO"), "MX")
            .when(col("country_raw").isin("CL", "CHI", "CHILE"), "CL")
            .when(col("country_raw").isin("AR", "ARG", "ARGENTINA"), "AR")
            .otherwise(col("country_raw"))
        )
        .withColumn(
            "status",
            when(col("status_raw").isin("activo", "active"), "activo")
            .when(col("status_raw").isin("inactivo", "inactive"), "inactivo")
            .when(col("status_raw").isin("suspendido", "suspended"), "suspendido")
            .otherwise(col("status_raw"))
        )
        .withColumn(
            "risk_level",
            when(col("risk_level_raw").isin("bajo"), "bajo")
            .when(col("risk_level_raw").isin("medio"), "medio")
            .when(col("risk_level_raw").isin("alto"), "alto")
            .otherwise(col("risk_level_raw"))
        )
        .drop("country_raw", "status_raw", "risk_level_raw")
    )

    validations = [
        (col("merchant_id").isNull(),                                              "nulo_critico",      "merchant_id"),
        (~col("merchant_id").rlike(r"^MCH-[0-9]{5}$"),                            "formato_incorrecto", "merchant_id"),
        (col("merchant_name").isNull() | (col("merchant_name") == ""),             "campo_invalido",     "merchant_name"),
        (~col("category").isin(valid_categories),                                  "dominio_invalido",   "category"),
        (~col("country").isin(valid_countries),                                    "dominio_invalido",   "country"),
        (col("affiliation_date").isNull(),                                         "nulo_critico",      "affiliation_date"),
        (~col("status").isin(valid_statuses),                                      "dominio_invalido",   "status"),
        (col("risk_level").isNotNull() & ~col("risk_level").isin(valid_risks),     "dominio_invalido",   "risk_level"),
    ]

    return build_rejection_reason(df_transformed, validations)


@dlt.view(name="vw_merchants_silver_prepared")
def vw_merchants_silver_prepared():
    return (
        dlt.readStream("stg_merchants_silver")
        .filter(col("_is_valid") == True)  # noqa: E712
        .drop("_is_valid", "_rejection_reason", "_rejected_field")
    )



dlt.create_streaming_table(
    name=SILVER_MERCHANTS_TABLE,
    comment="Tabla Silver de comercios curada, casteada y validada desde Bronze.",
    table_properties={
        "quality": "silver",
        "primary_key": "merchant_id"
    }
)

dlt.apply_changes(
    target=SILVER_MERCHANTS_TABLE,
    source="vw_merchants_silver_prepared",
    keys=["merchant_id"],
    sequence_by=col("_ingestion_timestamp"),
    stored_as_scd_type=1
)




# =============================================================================
# USERS
# =============================================================================

@dlt.table(
    name="stg_users_silver",
    comment="Staging de users: transformaciones aplicadas + flags de validacion. Base para Silver y Quarantine."
)
@dlt.expect("user_id_not_null",              "user_id IS NOT NULL")
@dlt.expect("user_id_valid_format",          "user_id RLIKE '^USR-[0-9]{6}$'")
@dlt.expect("full_name_not_null_or_empty",   "full_name IS NOT NULL AND length(full_name) > 0")
@dlt.expect("document_id_not_null_or_empty", "document_id IS NOT NULL AND length(document_id) > 0")
@dlt.expect(
    "email_valid_format",
    """
    email IS NOT NULL
    AND email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
    """
)
@dlt.expect("phone_valid_format",            "phone IS NOT NULL AND phone RLIKE '^\\+[0-9]{8,15}$'")
@dlt.expect("country_valid_domain",          "country IN ('PE', 'CO', 'MX', 'CL', 'AR')")
@dlt.expect("segment_valid_domain",          "segment IS NULL OR segment IN ('premium', 'estandar', 'nuevo')")
@dlt.expect("registration_date_not_null",    "registration_date IS NOT NULL")
def stg_users_silver():

    df = spark.readStream.table(BRONZE_USERS_TABLE)

    country_raw = trim(col("country"))
    segment_raw = trim(lower(col("segment")))
    email_raw   = trim(lower(col("email")))
    phone_raw   = trim(col("phone"))

    email_regex = r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$"
    phone_regex = r"^\+[0-9]{8,15}$"

    valid_countries = ["PE", "CO", "MX", "CL", "AR"]
    valid_segments  = ["premium", "estandar", "nuevo"]

    df_transformed = (
        df
        .select(
            trim(col("user_id")).alias("user_id"),
            when(
                trim(col("full_name")) == "",
                None
            ).otherwise(
                trim(col("full_name"))
            ).alias("full_name"),
            trim(col("document_id")).alias("document_id"),
            when(
                (email_raw == "") |
                (email_raw == "na") |
                (email_raw == "nodisponible") |
                (email_raw == "sin_correo"),
                None
            ).otherwise(
                email_raw
            ).alias("email"),
            regexp_replace(phone_raw, "[^0-9]", "").alias("phone_digits"),
            upper(country_raw).alias("country_raw"),
            when(
                segment_raw == "",
                None
            ).otherwise(
                segment_raw
            ).alias("segment_raw"),
            coalesce(
                to_date(trim(col("registration_date")), "yyyy-MM-dd"),
                to_date(trim(col("registration_date")), "dd/MM/yyyy")
            ).alias("registration_date"),
            col("_source_name"),
            col("_source_file"),
            col("_ingestion_timestamp"),
            col("ingestion_date"),
            current_timestamp().alias("_silver_processed_timestamp")
        )
        .withColumn(
            "country",
            when(col("country_raw").isin("PE", "PERU", "PER"), "PE")
            .when(col("country_raw").isin("CO", "COL", "COLOMBIA"), "CO")
            .when(col("country_raw").isin("MX", "MEX", "MEXICO"), "MX")
            .when(col("country_raw").isin("CL", "CHI", "CHILE"), "CL")
            .when(col("country_raw").isin("AR", "ARG", "ARGENTINA"), "AR")
            .otherwise(col("country_raw"))
        )
        .withColumn(
            "segment",
            when(col("segment_raw").isin("premium"), "premium")
            .when(col("segment_raw").isin("estandar"), "estandar")
            .when(col("segment_raw").isin("nuevo"), "nuevo")
            .otherwise(col("segment_raw"))
        )
        .withColumn(
            "phone",
            when(
                col("phone_digits").rlike("^[0-9]{9}$"),
                regexp_replace(col("phone_digits"), "^", "+51")
            )
            .when(
                col("phone_digits").rlike("^[0-9]{10,15}$"),
                regexp_replace(col("phone_digits"), "^", "+")
            )
            .otherwise(None)
        )
        .drop("country_raw", "segment_raw", "phone_digits")
    )

    validations = [
        (col("user_id").isNull(),                                                                      "nulo_critico",      "user_id"),
        (~col("user_id").rlike(r"^USR-[0-9]{6}$"),                                                    "formato_incorrecto", "user_id"),
        (col("full_name").isNull() | (trim(col("full_name")) == ""),                                   "campo_invalido",     "full_name"),
        (col("document_id").isNull() | (trim(col("document_id")) == ""),                               "campo_invalido",     "document_id"),
        (col("email").isNull() | ~col("email").rlike(email_regex),                                     "formato_incorrecto", "email"),
        (col("phone").isNull() | ~col("phone").rlike(phone_regex),                                     "formato_incorrecto", "phone"),
        (~col("country").isin(valid_countries),                                                        "dominio_invalido",   "country"),
        (col("segment").isNotNull() & ~col("segment").isin(valid_segments),                            "dominio_invalido",   "segment"),
        (col("registration_date").isNull(),                                                            "nulo_critico",      "registration_date"),
    ]

    return build_rejection_reason(df_transformed, validations)


@dlt.view(name="vw_users_silver_prepared")
def vw_users_silver_prepared():
    return (
        dlt.readStream("stg_users_silver")
        .filter(col("_is_valid") == True)  # noqa: E712
        .drop("_is_valid", "_rejection_reason", "_rejected_field")
    )



dlt.create_streaming_table(
    name=SILVER_USERS_TABLE,
    comment="Tabla Silver de usuarios curada, casteada y validada desde Bronze.",
    table_properties={
        "quality": "silver",
        "primary_key": "user_id",
        "pii": "true"
    }
)

dlt.apply_changes(
    target=SILVER_USERS_TABLE,
    source="vw_users_silver_prepared",
    keys=["user_id"],
    sequence_by=col("_ingestion_timestamp"),
    stored_as_scd_type=1
)


# =============================================================================
# QUARANTINE - Vista unificada y carga unica
# Une los rechazos de las 3 tablas en una sola vista para evitar
# multiples apply_changes al mismo target (causa DUPLICATE_QUERY_NAME)
# =============================================================================

@dlt.view(
    name="vw_all_quarantine",
    comment="Union de rechazos de transactions, merchants y users hacia cuarentena."
)
def vw_all_quarantine():
    txn       = dlt.readStream("stg_transactions_silver")
    merchants = dlt.readStream("stg_merchants_silver")
    users     = dlt.readStream("stg_users_silver")

    return (
        to_quarantine_shape(txn,       "bronze.transactions")
        .union(to_quarantine_shape(merchants, "bronze.merchants"))
        .union(to_quarantine_shape(users,     "bronze.users"))
    )


dlt.apply_changes(
    target=SILVER_QUARANTINE_TABLE,
    source="vw_all_quarantine",
    keys=["source_table", "original_record"],
    sequence_by=col("processed_at"),
    stored_as_scd_type=1
)
