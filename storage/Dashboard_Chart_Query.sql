
SELECT
    final_status,
    COUNT(*) AS total_count
FROM
    `vibrant-mantis-289406.trade_analysis.v_trade_dashboard`
GROUP BY 1
ORDER BY 2 DESC;