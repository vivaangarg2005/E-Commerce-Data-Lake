-- Query 9: Data quality exception audit: quarantined violations grouped by entity and rule
SELECT 
    entity_name,
    rejection_stage,
    rule_code,
    COUNT(quarantine_id) AS violation_count,
    MIN(rejection_reason) AS example_reason
FROM quarantine_exceptions
GROUP BY entity_name, rejection_stage, rule_code
ORDER BY violation_count DESC;
