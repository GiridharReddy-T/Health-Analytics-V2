-- =============================================================================
-- One-time setup: create TEST and PROD Unity Catalogs
-- Run this as a workspace admin or metastore admin BEFORE deploying the bundle
-- to test or prod targets for the first time.
-- =============================================================================

-- ── TEST catalog ──────────────────────────────────────────────────────
CREATE CATALOG IF NOT EXISTS test_catalog
  COMMENT 'Integration test environment — deployed automatically by Azure DevOps on merge to main';

CREATE DATABASE IF NOT EXISTS test_catalog.project_db
  COMMENT 'Health Analytics project schema (test)';

-- Grant the Azure DevOps service principal USE + CREATE on test
GRANT USE CATALOG ON CATALOG test_catalog TO `<devops-service-principal>`;
GRANT USE SCHEMA  ON DATABASE test_catalog.project_db TO `<devops-service-principal>`;
GRANT CREATE TABLE ON DATABASE test_catalog.project_db TO `<devops-service-principal>`;

-- ── PROD catalog ──────────────────────────────────────────────────────
CREATE CATALOG IF NOT EXISTS prod_catalog
  COMMENT 'Production environment — deployed by Azure DevOps on release tag';

CREATE DATABASE IF NOT EXISTS prod_catalog.project_db
  COMMENT 'Health Analytics project schema (prod)';

-- Grant the Azure DevOps service principal USE + CREATE on prod
GRANT USE CATALOG ON CATALOG prod_catalog TO `<devops-service-principal>`;
GRANT USE SCHEMA  ON DATABASE prod_catalog.project_db TO `<devops-service-principal>`;
GRANT CREATE TABLE ON DATABASE prod_catalog.project_db TO `<devops-service-principal>`;

-- ── Verify ─────────────────────────────────────────────────────────────
SHOW CATALOGS LIKE '*_catalog';
