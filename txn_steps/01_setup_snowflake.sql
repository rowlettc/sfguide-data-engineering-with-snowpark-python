/*-----------------------------------------------------------------------------
Hands-On Lab: Data Engineering with Snowpark
Script:       01_setup_snowflake.sql
Author:       Jeremiah Hansen
Last Updated: 1/1/2023
-----------------------------------------------------------------------------*/


-- ----------------------------------------------------------------------------
-- Step #1: Accept Anaconda Terms & Conditions
-- ----------------------------------------------------------------------------

-- See Getting Started section in Third-Party Packages (https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages.html#getting-started)


-- ----------------------------------------------------------------------------
-- Step #2: Create the account level objects
-- ----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- Roles
SET MY_USER = CURRENT_USER();
CREATE OR REPLACE ROLE TXN_ROLE;
GRANT ROLE TXN_ROLE TO ROLE SYSADMIN;
GRANT ROLE TXN_ROLE TO USER IDENTIFIER($MY_USER);

GRANT EXECUTE TASK ON ACCOUNT TO ROLE TXN_ROLE;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE TXN_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE TXN_ROLE;

-- Databases
CREATE OR REPLACE DATABASE TXN_DB;
GRANT OWNERSHIP ON DATABASE TXN_DB TO ROLE TXN_ROLE;

-- Warehouses
CREATE OR REPLACE WAREHOUSE TXN_WH WAREHOUSE_SIZE = XSMALL, AUTO_SUSPEND = 300, AUTO_RESUME= TRUE;
GRANT OWNERSHIP ON WAREHOUSE TXN_WH TO ROLE TXN_ROLE;


-- ----------------------------------------------------------------------------
-- Step #3: Create the database level objects
-- ----------------------------------------------------------------------------
USE ROLE TXN_ROLE;
USE WAREHOUSE TXN_WH;
USE DATABASE TXN_DB;

-- Schemas
CREATE OR REPLACE SCHEMA EXTERNAL;
CREATE OR REPLACE SCHEMA RAW_TXN;
CREATE OR REPLACE SCHEMA RAW_EXT_TXN;
CREATE OR REPLACE SCHEMA HARMONIZED;
CREATE OR REPLACE SCHEMA ANALYTICS;

-- External Frostbyte objects
USE SCHEMA EXTERNAL;
CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
    TYPE = PARQUET
    COMPRESSION = SNAPPY
;
CREATE OR REPLACE STAGE FROSTBYTE_RAW_STAGE
    URL = 's3://sfquickstarts/data-engineering-with-snowpark-python/'
;

-- ANALYTICS objects
USE SCHEMA ANALYTICS;
-- This will be added in step 5
--CREATE OR REPLACE FUNCTION ANALYTICS.FAHRENHEIT_TO_CELSIUS_UDF(TEMP_F NUMBER(35,4))
--RETURNS NUMBER(35,4)
--AS
--$$
--    (temp_f - 32) * (5/9)
--$$;

/*
CREATE OR REPLACE FUNCTION ANALYTICS.INCH_TO_MILLIMETER_UDF(INCH NUMBER(35,4))
RETURNS NUMBER(35,4)
    AS
$$
    inch * 25.4
$$;
 */