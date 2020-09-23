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