{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert'
    )
}}

WITH DIM_DATE AS (
    SELECT * FROM PC_DBT_DB.DBT_ABAVISKAR.DIM_DATE
),
DIM_ACCOUNT AS (
    SELECT * FROM PC_DBT_DB.DBT_ABAVISKAR.DIM_ACCOUNT
),
DIM_USER AS (
    SELECT * FROM PC_DBT_DB.DBT_ABAVISKAR.DIM_USER
),
ORD AS (
    SELECT * FROM PC_DBT_DB.PUBLIC."ORDER"
),
FACT_ORDER AS (
    SELECT 
        U.USER_KEY,
        A.ACCOUNT_KEY,
        D.DATE_KEY AS CREATED_DATE_KEY,
        O.ID,
        O.TOTALAMOUNT AS TOTAL_AMOUNT
    FROM 
        DIM_USER U,
        DIM_ACCOUNT A,
        DIM_DATE D,
        ORD O
    WHERE
        O.OWNERID = U.USER_ID
    AND O.ACCOUNTID = A.ACCOUNT_ID
    AND TO_CHAR(O.CREATEDDATE,'YYYY-MM-DD') = D.FULL_DATE
)
SELECT * FROM FACT_ORDER
