SELECT
    t.trade_id,
    t.client_id,
    t.symbol,
    t.version,
    t.timestamp AS original_trade_time,
    t.price,
    t.quantity,
    t.maturity_date,
    CASE
        WHEN t.maturity_date < CURRENT_DATE() AND t.status = 'APPROVED' THEN 'EXPIRED'
        WHEN t.status IN ('APPROVED', 'REPLACED') THEN 'VALID'
        ELSE t.status
    END AS final_status,
    'VALIDATION_SUCCESS' AS source_type
FROM
    `trade_analysis.approved_trades_validated` AS t

UNION ALL

SELECT
    r.trade_id,
    r.client_id,
    r.symbol,
    r.version,
    r.timestamp AS original_trade_time,
    NULL AS price,
    NULL AS quantity,
    NULL AS maturity_date,
    'REJECTED' AS final_status,
    'VALIDATION_FAILURE' AS source_type
FROM
    `trade_analysis.rejected_trades_audit` AS r;