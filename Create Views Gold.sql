--CALENDAR VIEW
CREATE VIEW gold.calendar AS
SELECT
*
FROM
OPENROWSET(
    BULK 'https://adestoragedatalakev2.blob.core.windows.net/silver-transformed/AdventureWorks_Calendar/',
    FORMAT='PARQUET'
) AS Query1;
--CUSTOMERS VIEW
CREATE VIEW gold.customers AS
SELECT
*
FROM
OPENROWSET(
    BULK 'https://adestoragedatalakev2.blob.core.windows.net/silver-transformed/AdventureWorks_Customers/',
    FORMAT='PARQUET'
) AS Query2;
--PROCAT VIEW
CREATE VIEW gold.procat AS
SELECT
*
FROM
OPENROWSET(
    BULK 'https://adestoragedatalakev2.blob.core.windows.net/silver-transformed/AdventureWorks_Product_Categories/',
    FORMAT='PARQUET'
) AS Query3;
--PROSUBCAT VIEW
CREATE VIEW gold.prosubcat AS
SELECT
*
FROM
OPENROWSET(
    BULK 'https://adestoragedatalakev2.blob.core.windows.net/silver-transformed/AdventureWorks_Product_Subcategories/',
    FORMAT='PARQUET'
) AS Query4;
--PRODUCTS VIEW
CREATE VIEW gold.products AS
SELECT
*
FROM
OPENROWSET(
    BULK 'https://adestoragedatalakev2.blob.core.windows.net/silver-transformed/AdventureWorks_Products/',
    FORMAT='PARQUET'
) AS Query5;
--RETURNS VIEW
CREATE VIEW gold.returns AS
SELECT
*
FROM
OPENROWSET(
    BULK 'https://adestoragedatalakev2.blob.core.windows.net/silver-transformed/AdventureWorks_Returns/',
    FORMAT='PARQUET'
) AS Query6;
--SALES VIEW
CREATE VIEW gold.sales AS
SELECT
*
FROM
OPENROWSET(
    BULK 'https://adestoragedatalakev2.blob.core.windows.net/silver-transformed/AdventureWorks_Sales/',
    FORMAT='PARQUET'
) AS Query7;
--TERRITORIES VIEW
CREATE VIEW gold.territories AS
SELECT
*
FROM
OPENROWSET(
    BULK 'https://adestoragedatalakev2.blob.core.windows.net/silver-transformed/AdventureWorks_Territories/',
    FORMAT='PARQUET'
) AS Query8;