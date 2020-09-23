WITH STG_1 AS (
    SELECT DISTINCT
    a.CUSTOMER_PK, a.ORDER_FK, a.BOOKING_FK, a.LOADDATE, a.RECORD_SOURCE
    FROM (
        SELECT CUSTOMER_PK, ORDER_FK, BOOKING_FK, LOADDATE, RECORD_SOURCE,
        ROW_NUMBER() OVER(
            PARTITION BY CUSTOMER_PK
            ORDER BY LOADDATE ASC
        ) AS RN
        FROM `georgian-os`.`dbtvault_test`.`raw_source`
    ) AS a
    WHERE RN = 1
),
STG_2 AS (
    SELECT DISTINCT
    a.CUSTOMER_PK, a.ORDER_FK, a.BOOKING_FK, a.LOADDATE, a.RECORD_SOURCE
    FROM (
        SELECT CUSTOMER_PK, ORDER_FK, BOOKING_FK, LOADDATE, RECORD_SOURCE,
        ROW_NUMBER() OVER(
            PARTITION BY CUSTOMER_PK
            ORDER BY LOADDATE ASC
        ) AS RN
        FROM `georgian-os`.`dbtvault_test`.`raw_source_2`
    ) AS a
    WHERE RN = 1
),
STG AS (
    SELECT DISTINCT
    b.CUSTOMER_PK, b.ORDER_FK, b.BOOKING_FK, b.LOADDATE, b.RECORD_SOURCE
    FROM (
        SELECT *,
        ROW_NUMBER() OVER(
            PARTITION BY CUSTOMER_PK
            ORDER BY LOADDATE, RECORD_SOURCE ASC
        ) AS RN
        FROM (
            SELECT * FROM STG_1
            UNION ALL
            SELECT * FROM STG_2
        )
        WHERE ORDER_FK IS NOT NULL
        AND BOOKING_FK IS NOT NULL
    ) AS b
    WHERE RN = 1
)

SELECT c.* FROM STG AS c
LEFT JOIN `georgian-os`.`dbtvault_test`.`test_link_macro_correctly_generates_sql_for_incremental_multi_source` AS d 
ON c.CUSTOMER_PK = d.CUSTOMER_PK
WHERE d.CUSTOMER_PK IS NULL