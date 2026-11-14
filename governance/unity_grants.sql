-- Pattern for Hive Metastore -> Unity Catalog cutover.
-- Replace catalog/schema names in the workspace; do not copy production grants.

CREATE CATALOG IF NOT EXISTS lakehouse_prod;
CREATE SCHEMA IF NOT EXISTS lakehouse_prod.silver;

GRANT USE CATALOG ON CATALOG lakehouse_prod TO `data-engineers`;
GRANT USE SCHEMA ON SCHEMA lakehouse_prod.silver TO `data-engineers`;
GRANT SELECT ON SCHEMA lakehouse_prod.silver TO `analysts`;

ALTER TABLE hive_metastore.legacy.orders
SET TBLPROPERTIES ('upgraded_to' = 'lakehouse_prod.silver.orders');
