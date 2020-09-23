WITH STG AS (
    SELECT DISTINCT
    a.CUSTOMER_PK, a.ORDER_FK, a.BOOKING_FK, a.LOADDATE, a.RECORD_SOURCE
    FROM (
        SELECT b.*,
        ROW_NUMBER() OVER(
            PARTITION BY b.CUSTOMER_PK
            ORDER BY b.LOADDATE, b.RECORD_SOURCE ASC
        ) AS RN
        FROM `georgian-os`.`dbtvault_test`.`raw_source` AS b
        WHERE b.ORDER_FK IS NOT NULL
        AND b.BOOKING_FK IS NOT NULL
    ) AS a
    WHERE RN = 1
)

SELECT c.* FROM STG AS c
LEFT JOIN `georgian-os`.`dbtvault_test`.`test_link_macro_correctly_generates_sql_for_incremental_single_source` AS d 
ON c.CUSTOMER_PK = d.CUSTOMER_PK
WHERE d.CUSTOMER_PK IS NULL