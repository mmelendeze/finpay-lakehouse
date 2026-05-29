# FinPay Lakehouse

Proyecto de implementación de una plataforma Lakehouse moderna utilizando Databricks, Unity Catalog, Lakeflow Declarative Pipelines (DLT), Materialized Views y AI/BI Dashboards para el procesamiento analítico de transacciones financieras de FinPay.

---

# 1. Descripción del Caso de Uso

FinPay es una plataforma ficticia de pagos digitales que procesa transacciones financieras provenientes de múltiples canales y comercios.

El objetivo del proyecto es construir una arquitectura Lakehouse moderna que permita:

* Ingestar datos desde múltiples formatos.
* Procesar información mediante arquitectura Medallion.
* Aplicar controles de calidad y observabilidad.
* Construir un modelo dimensional analítico.
* Exponer dashboards AI/BI para monitoreo operacional.

El proyecto implementa:

* Arquitectura Medallion (Bronze, Silver, Gold).
* Unity Catalog.
* Lakeflow Declarative Pipelines.
* Materialized Views.
* SQL Warehouses Serverless.
* Dashboards AI/BI.
* Databricks Asset Bundles.
* GitHub como repositorio fuente.

---

# 2. Arquitectura de la Solución

## Arquitectura General

```text
Fuentes Demo
    │
    ▼
Landing Zone (Volumes)
    │
    ▼
Bronze Layer
(Ingesta cruda)
    │
    ▼
Silver Layer
(Estandarización + Calidad)
    │
    ▼
Gold Layer
(Modelo dimensional)
    │
    ▼
Materialized Views
    │
    ▼
Dashboard AI/BI
```

---

## Componentes Principales

### Landing Zone

Ubicación:

```text
/Volumes/fintech_finpay/default/vol_landing
```

Contiene archivos:

* TXT
* CSV
* JSON

---

### Bronze Layer

Responsable de:

* Ingesta incremental.
* Estandarización inicial.
* Persistencia Delta.

Tablas principales:

* bronze_users
* bronze_transactions
* bronze_merchants

---

### Silver Layer

Responsable de:

* Limpieza de datos.
* Validaciones de calidad.
* Reglas de negocio.
* Expectations DLT.

Tablas principales:

* silver_users
* silver_transactions
* silver_merchants

---

### Gold Layer

Responsable de:

* Modelo dimensional.
* KPIs analíticos.
* Agregaciones de negocio.

Tablas principales:

* gold_dim_user
* gold_dim_merchant
* gold_dim_channel
* gold_dim_date
* gold_fact_transactions

---

### Materialized Views

Se utilizan para:

* Optimización de consultas.
* Exposición semántica.
* Consumo analítico.

Vistas materializadas:

* dim_user
* dim_merchant
* dim_channel
* dim_date
* fact_transactions

---

### Observabilidad

Se implementa observabilidad mediante:

* Event Logs de Lakeflow.
* Queries de validación.
* Dashboard AI/BI.

Métricas monitoreadas:

* Registros procesados.
* Registros rechazados.
* Tasa de error.
* Expectations fallidas.
* Tendencias de procesamiento.

---

# 3. Estructura del Repositorio

```text
finpay-lakehouse/
├── .github/
│   └── workflows/
│
├── databricks.yml
│
├── resources/
│   ├── finpay_etl_pipeline.yml
│   ├── finpay_ingestion_job.yml
│   ├── finpay_semantic_job.yml
│   └── finpay_observability_dashboard.yml
│
├── src/
│   ├── utils.py
│   ├── bronze.py
│   ├── silver.py
│   └── gold.py
│
├── notebooks/
│   ├── 00_setup.ipynb
│   ├── 01_create_materialized_views.ipynb
│   ├── 02_refresh_materialized_views.ipynb
│   └── 03_observability_queries.ipynb
│
├── dashboard/
│   └── observability.lvdash.json
│
└── README.md
```

---

# 4. Tecnologías Utilizadas

* Databricks
* Unity Catalog
* Delta Lake
* Lakeflow Declarative Pipelines (DLT)
* Databricks Asset Bundles
* PySpark
* SQL
* GitHub
* AI/BI Dashboards

---

# 5. Configuración del Proyecto

## Catálogo

```text
fintech_finpay
```

## Schemas

* bronze
* silver
* gold
* observability

---

# 6. Roles y Seguridad

Se implementaron grupos de seguridad:

| Rol        | Permisos                          |
| ---------- | --------------------------------- |
| ingenieria | CREATE TABLE, MODIFY              |
| riesgo     | SELECT sobre silver y gold        |
| auditoria  | SELECT sobre gold y observability |

---

# 7. Instrucciones de Despliegue

## 7.1 Clonar Repositorio

```bash
git clone https://github.com/mmelendeze/finpay-lakehouse.git
```

---

## 7.2 Configurar Databricks CLI

```bash
databricks auth login --host https://<workspace-url>
```

---

## 7.3 Validar Bundle

```bash
databricks bundle validate -t dev
```

---

## 7.4 Deploy Bundle

```bash
databricks bundle deploy -t dev
```

---

## 7.5 Ejecutar Pipeline ETL

```bash
databricks bundle run finpay_ingestion_job -t dev
```

---

## 7.6 Ejecutar Modelo Semántico

```bash
databricks bundle run finpay_semantic_job -t dev
```

---

# 8. Dashboards AI/BI

El proyecto incluye un dashboard AI/BI de observabilidad:

* Registros exitosos.
* Registros rechazados.
* Error rate.
* Tendencias diarias.
* Métricas DLT.

Archivo:

```text
dashboard/observability.lvdash.json
```

---

# 9. Observabilidad

El notebook:

```text
03_observability_queries.ipynb
```

incluye consultas para:

* Validación de event logs.
* Métricas de calidad.
* Expectations fallidas.
* Eventos de error.
* Trazabilidad de pipelines.

---

# 10. Buenas Prácticas Implementadas

* Arquitectura Medallion.
* Unity Catalog.
* Separación por capas.
* Versionamiento Git.
* Infraestructura declarativa.
* Parametrización.
* SQL Warehouse Serverless.
* Materialized Views.
* Observabilidad centralizada.

---
# 11. Pasos a seguir
* (1) En Produccio ingresar a la plataforma y crear el catalogo que se ba a usar (fintech_finpay).
* (2) Ejecutar en la consola de Visual Studio los comandos para validar y deployar.
    •	databricks bundle validate -t prod
    •	databricks bundle deploy -t prod
* (3) En  produccion ejecutar el notebook de inicializacion(00_setup.ipynb).
* (4) Ejecutar en la consola de Visual Studio ejecutar el comando para ejecutar el job de ingesta.  
    •	databricks bundle run finpay_ingestion_job -t prod
* (5) En  produccion ejecutar la query que crea las vistas materializadas (01_create_materialized_views.sql).
* (6) Ejecutar en la consola de Visual Studio ejecutar el comando para ejecutar el job de refresco de vistas.  
    •	databricks bundle run finpay_semantic_job -t prod
* (7) Para genera roles y permisos debe usar databricks de alguna nube y seguir los siguientes pasos
    •	Debe generar en la paltaforma los roles necesarios 
    •	Debe en produccion ejecutar el notebook de genración de permisos y enmascaramiento(04_Asig_Role.ipynb).


---
# 12. Autor

Marco Melendez

Proyecto académico desarrollado utilizando Databricks Lakehouse Platform.
