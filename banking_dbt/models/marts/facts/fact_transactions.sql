{{ config(materialized='incremental', unique_key='transaction_id') }}

WITH deduped AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_time DESC) AS rn
        FROM {{ ref('stg_transactions') }}
        {% if is_incremental() %}
        WHERE transaction_time > (SELECT COALESCE(MAX(transaction_time), '1900-01-01') FROM {{ this }})
        {% endif %}
    ) t
    WHERE rn = 1
)

SELECT
    t.transaction_id,
    t.account_id,
    a.customer_id,
    t.amount,
    t.related_account_id,
    t.status,
    t.transaction_type,
    t.transaction_time,
    CURRENT_TIMESTAMP AS load_timestamp
FROM deduped t
LEFT JOIN {{ ref('stg_accounts') }} a
    ON t.account_id = a.account_id
