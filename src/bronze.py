# Databricks Delta Live Tables / Lakeflow - Bronze Metadata Driven

import dlt

from pyspark.sql.functions import (
    current_timestamp,
    current_date,
    col,
    lit
)

# =============================================================================
# 1. Parametros base
# =============================================================================

MAX_FILES_PER_TRIGGER = 10

CATALOG = "fintech_finpay"
BRONZE_SCHEMA = "bronze"

LANDING_ZONE = "/Volumes/fintech_finpay/default/vol_landing"

CONFIG_PATH = f"{LANDING_ZONE}/metadata/ingestion_archetypes.json"
SCHEMA_PATH = f"{LANDING_ZONE}/_schema"


# =============================================================================
# 2. Funciones auxiliares
# =============================================================================

def normalize_file_format(file_format: str) -> str:
    fmt = file_format.lower().strip()

    if fmt in ("txt", "text"):
        return "csv"

    return fmt


def read_json_config(config_path: str):
    configs_df = (
        spark.read
            .option("multiline", "true")
            .json(config_path)
    )

    configs = [
        row.asDict(recursive=True)
        for row in configs_df.collect()
    ]

    return [
        cfg
        for cfg in configs
        if cfg.get("active", False) is True
    ]


def build_cloud_files_options(config: dict) -> dict:
    source_name = config["source_name"]
    file_format = normalize_file_format(config["file_format"])

    options = {
        "cloudFiles.format": file_format,
        "cloudFiles.schemaLocation": f"{SCHEMA_PATH}/{source_name}",
        "cloudFiles.schemaEvolutionMode": "addNewColumns",
        "rescuedDataColumn": "_rescued_data",
        "cloudFiles.maxFilesPerTrigger": str(MAX_FILES_PER_TRIGGER),
    }

    if file_format == "json":
        options["multiLine"] = str(
            config.get("multiline", False)
        ).lower()

    if file_format == "csv":
        options["header"] = str(
            config.get("header", True)
        ).lower()

        options["delimiter"] = config.get(
            "delimiter",
            ","
        )

        options["cloudFiles.inferColumnTypes"] = "false"

    return options


def create_bronze_table(config: dict):
    source_name  = config["source_name"]
    target_table = config["target_table"]

    source_path = (
        f"{LANDING_ZONE.rstrip('/')}/"
        f"{config['source_path'].lstrip('/')}"
    )

    dlt_table_name = f"{CATALOG}.{BRONZE_SCHEMA}.{target_table}"

    options = build_cloud_files_options(config)

    # Leer partition_by del JSON de configuracion.
    # Puede ser un string simple "affiliation_date"
    # o una lista ["year", "month"] si se necesita particionar por varios campos.
    # Si no viene en el config, la tabla no tendra particion fisica.
    partition_by_raw = config.get("partition_by", None)

    if partition_by_raw is None:
        partition_cols = None
    elif isinstance(partition_by_raw, list):
        partition_cols = [c.strip() for c in partition_by_raw if c.strip()]
    else:
        # string simple -> convertir a lista de un elemento
        partition_cols = [partition_by_raw.strip()]

    @dlt.table(
        name=dlt_table_name,
        comment=(
            f"Tabla Bronze ingestada desde la fuente {source_name} "
            "usando Auto Loader y metadata-driven ingestion."
        ),
        table_properties={
            "quality": "bronze",
            "source_name": source_name,
            "pipelines.autoOptimize.managed": "true"
        },
        # partition_cols solo se pasa si viene definido en el config.
        # DLT crea las particiones fisicas en Delta automaticamente.
        **({"partition_cols": partition_cols} if partition_cols else {})
    )
    def bronze_table():
        return (
            spark.readStream
                .format("cloudFiles")
                .options(**options)
                .load(source_path)
                .withColumn("_source_name",          lit(source_name))
                .withColumn("_source_file",          col("_metadata.file_path"))
                .withColumn("_ingestion_timestamp",  current_timestamp())
                .withColumn("ingestion_date",        current_date())
        )

    return bronze_table


# =============================================================================
# 3. Leer metadata y declarar tablas DLT dinamicamente
# =============================================================================

active_configs = read_json_config(CONFIG_PATH)

if len(active_configs) == 0:
    raise ValueError(
        f"No existen fuentes activas en el archivo de configuracion: {CONFIG_PATH}"
    )

for config in active_configs:
    create_bronze_table(config)