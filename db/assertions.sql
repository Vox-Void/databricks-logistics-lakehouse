-- Run after wave B. Set history_table to the registered Silver table.
SET TIME ZONE 'UTC';

WITH cutpoints AS (
    SELECT 'tie_40' AS label, 'S100' AS shipment_id,
        named_struct('effective_at', TIMESTAMP '2026-01-07 10:00:00',
                     'source_position', CAST(40 AS BIGINT)) AS cutpoint
    UNION ALL
    SELECT 'tie_41', 'S100',
        named_struct('effective_at', TIMESTAMP '2026-01-07 10:00:00',
                     'source_position', CAST(41 AS BIGINT))
    UNION ALL
    SELECT 'delete_gap', 'S200',
        named_struct('effective_at', TIMESTAMP '2026-01-07 10:30:00',
                     'source_position', CAST(9223372036854775807 AS BIGINT))
)
SELECT c.label, h.status
FROM cutpoints c
LEFT JOIN IDENTIFIER(:history_table) h
    ON h.shipment_id = c.shipment_id
   AND h.__START_AT <= c.cutpoint
   AND (h.__END_AT IS NULL OR c.cutpoint < h.__END_AT)
ORDER BY c.label;

-- Expected results, ordered by label: NULL, OUT_FOR_DELIVERY, DELIVERED.
