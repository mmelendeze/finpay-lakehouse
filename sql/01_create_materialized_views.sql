CREATE OR REPLACE MATERIALIZED VIEW {{catalog}}.{{schema_gold}}.dim_merchant AS
SELECT
    merchant_id,
    merchant_name,
    category,
    country,
    affiliation_status,
    risk_level,
    affiliation_date
FROM {{catalog}}.{{schema_gold}}.gold_dim_merchant;

CREATE OR REPLACE MATERIALIZED VIEW {{catalog}}.{{schema_gold}}.dim_user AS
SELECT
    user_id,
    full_name_masked,
    document_id_masked,
    email_masked,
    phone_masked,
    country,
    risk_segment,
    preferred_channel,
    registration_date
FROM {{catalog}}.{{schema_gold}}.gold_dim_user;

CREATE OR REPLACE MATERIALIZED VIEW {{catalog}}.{{schema_gold}}.dim_channel AS
SELECT
    channel_id,
    channel_name,
    channel_description
FROM {{catalog}}.{{schema_gold}}.gold_dim_channel;

CREATE OR REPLACE MATERIALIZED VIEW {{catalog}}.{{schema_gold}}.dim_date AS
SELECT
    date_id,
    calendar_date,
    day_number,
    day_of_week_number,
    day_name,
    week_number,
    month_number,
    month_name,
    quarter_number,
    year_number
FROM {{catalog}}.{{schema_gold}}.gold_dim_date;

CREATE OR REPLACE MATERIALIZED VIEW {{catalog}}.{{schema_gold}}.fact_transactions AS
SELECT
    CAST(date_format(f.transaction_date, 'yyyyMMdd') AS INT) AS date_id,

    f.transaction_date,

    f.user_id,

    f.merchant_id,

    f.channel_id,

    f.transaction_count,

    f.approved_transactions,

    f.rejected_transactions,

    f.pending_transactions,

    f.reversal_transactions,

    f.total_amount,

    f.avg_amount,

    f.reversal_transactions / f.transaction_count AS reversal_rate,

    f.rejected_transactions / f.transaction_count AS rejected_rate,

    CASE
        WHEN m.risk_level = 'alto' THEN 70
        WHEN m.risk_level = 'medio' THEN 40
        ELSE 20
    END

    +

    CASE
        WHEN f.reversal_transactions / f.transaction_count >= 0.20 THEN 20
        WHEN f.reversal_transactions / f.transaction_count >= 0.10 THEN 10
        ELSE 0
    END

    +

    CASE
        WHEN f.rejected_transactions / f.transaction_count >= 0.30 THEN 10
        ELSE 0
    END

    AS risk_score,

    CASE
        WHEN (
            CASE
                WHEN m.risk_level = 'alto' THEN 70
                WHEN m.risk_level = 'medio' THEN 40
                ELSE 20
            END

            +

            CASE
                WHEN f.reversal_transactions / f.transaction_count >= 0.20 THEN 20
                WHEN f.reversal_transactions / f.transaction_count >= 0.10 THEN 10
                ELSE 0
            END

            +

            CASE
                WHEN f.rejected_transactions / f.transaction_count >= 0.30 THEN 10
                ELSE 0
            END

        ) >= 80 THEN 'alto'

        WHEN (
            CASE
                WHEN m.risk_level = 'alto' THEN 70
                WHEN m.risk_level = 'medio' THEN 40
                ELSE 20
            END

            +

            CASE
                WHEN f.reversal_transactions / f.transaction_count >= 0.20 THEN 20
                WHEN f.reversal_transactions / f.transaction_count >= 0.10 THEN 10
                ELSE 0
            END

            +

            CASE
                WHEN f.rejected_transactions / f.transaction_count >= 0.30 THEN 10
                ELSE 0
            END

        ) >= 50 THEN 'medio'

        ELSE 'bajo'
    END AS risk_classification,

    CASE
        WHEN f.reversal_transactions / f.transaction_count >= 0.20
          OR f.rejected_transactions / f.transaction_count >= 0.30
        THEN true
        ELSE false
    END AS anomaly_flag

FROM {{catalog}}.{{schema_gold}}.gold_fact_transactions f

LEFT JOIN {{catalog}}.{{schema_gold}}.gold_dim_merchant m
    ON f.merchant_id = m.merchant_id;