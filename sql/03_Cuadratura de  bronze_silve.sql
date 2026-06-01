WITH
-- =============================================================================
-- USERS
-- =============================================================================

bronze_users AS (
    SELECT DISTINCT user_id
    FROM fintech_finpay.bronze.users
    WHERE user_id IS NOT NULL
),

silver_users AS (
    SELECT DISTINCT user_id
    FROM fintech_finpay.silver.users
    WHERE user_id IS NOT NULL
),

bronze_users_not_in_silver AS (
    SELECT b.user_id
    FROM bronze_users b
    LEFT ANTI JOIN silver_users s
        ON b.user_id = s.user_id
),

quarantine_users AS (
    SELECT DISTINCT
        get_json_object(original_record, '$.user_id') AS user_id
    FROM fintech_finpay.silver.quarantine
    WHERE source_table = 'bronze.users'
      AND get_json_object(original_record, '$.user_id') IS NOT NULL
),

quarantine_users_not_in_silver AS (
    SELECT q.user_id
    FROM quarantine_users q
    INNER JOIN bronze_users_not_in_silver b
        ON q.user_id = b.user_id
),

users_result AS (
    SELECT
        'users' AS table_name,
        (SELECT COUNT(*) FROM bronze_users) AS total_bronze_distinct,
        (SELECT COUNT(*) FROM silver_users) AS total_silver_distinct,
        (SELECT COUNT(*) FROM quarantine_users_not_in_silver) AS total_quarantine_not_in_silver,
        (SELECT COUNT(*) FROM bronze_users_not_in_silver) AS total_bronze_not_in_silver
),

-- =============================================================================
-- MERCHANTS
-- =============================================================================

bronze_merchants AS (
    SELECT DISTINCT merchant_id
    FROM fintech_finpay.bronze.merchants
    WHERE merchant_id IS NOT NULL
),

silver_merchants AS (
    SELECT DISTINCT merchant_id
    FROM fintech_finpay.silver.merchants
    WHERE merchant_id IS NOT NULL
),

bronze_merchants_not_in_silver AS (
    SELECT b.merchant_id
    FROM bronze_merchants b
    LEFT ANTI JOIN silver_merchants s
        ON b.merchant_id = s.merchant_id
),

quarantine_merchants AS (
    SELECT DISTINCT
        get_json_object(original_record, '$.merchant_id') AS merchant_id
    FROM fintech_finpay.silver.quarantine
    WHERE source_table = 'bronze.merchants'
      AND get_json_object(original_record, '$.merchant_id') IS NOT NULL
),

quarantine_merchants_not_in_silver AS (
    SELECT q.merchant_id
    FROM quarantine_merchants q
    INNER JOIN bronze_merchants_not_in_silver b
        ON q.merchant_id = b.merchant_id
),

merchants_result AS (
    SELECT
        'merchants' AS table_name,
        (SELECT COUNT(*) FROM bronze_merchants) AS total_bronze_distinct,
        (SELECT COUNT(*) FROM silver_merchants) AS total_silver_distinct,
        (SELECT COUNT(*) FROM quarantine_merchants_not_in_silver) AS total_quarantine_not_in_silver,
        (SELECT COUNT(*) FROM bronze_merchants_not_in_silver) AS total_bronze_not_in_silver
),

-- =============================================================================
-- TRANSACTIONS
-- =============================================================================

bronze_transactions AS (
    SELECT DISTINCT transaction_id
    FROM fintech_finpay.bronze.transactions
    WHERE transaction_id IS NOT NULL
),

silver_transactions AS (
    SELECT DISTINCT transaction_id
    FROM fintech_finpay.silver.transactions
    WHERE transaction_id IS NOT NULL
),

bronze_transactions_not_in_silver AS (
    SELECT b.transaction_id
    FROM bronze_transactions b
    LEFT ANTI JOIN silver_transactions s
        ON b.transaction_id = s.transaction_id
),

quarantine_transactions AS (
    SELECT DISTINCT
        get_json_object(original_record, '$.transaction_id') AS transaction_id
    FROM fintech_finpay.silver.quarantine
    WHERE source_table = 'bronze.transactions'
      AND get_json_object(original_record, '$.transaction_id') IS NOT NULL
),

quarantine_transactions_not_in_silver AS (
    SELECT q.transaction_id
    FROM quarantine_transactions q
    INNER JOIN bronze_transactions_not_in_silver b
        ON q.transaction_id = b.transaction_id
),

transactions_result AS (
    SELECT
        'transactions' AS table_name,
        (SELECT COUNT(*) FROM bronze_transactions) AS total_bronze_distinct,
        (SELECT COUNT(*) FROM silver_transactions) AS total_silver_distinct,
        (SELECT COUNT(*) FROM quarantine_transactions_not_in_silver) AS total_quarantine_not_in_silver,
        (SELECT COUNT(*) FROM bronze_transactions_not_in_silver) AS total_bronze_not_in_silver
)

-- =============================================================================
-- RESULTADO FINAL
-- =============================================================================

SELECT * FROM users_result

UNION ALL

SELECT * FROM merchants_result

UNION ALL

SELECT * FROM transactions_result;