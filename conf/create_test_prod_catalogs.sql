-- =============================================================================
-- One-time setup: create TEST and PROD Unity Catalogs
-- Run this as a workspace admin or metastore admin BEFORE deploying the bundle
-- to test or prod targets for the first time.
-- =============================================================================

-- ── TEST catalog ──────────────────────────────────────────────────────
CREATE CATALOG IF NOT EXISTS test_catalog
  COMMENT 'Integration test environment — deployed automatically by GitHub Actions on merge to main';

CREATE DATABASE IF NOT EXISTS test_catalog.project_db
  COMMENT 'Health Analytics project schema (test)';

-- Grant the GitHub Actions user USE + CREATE on test
-- (uncomment and replace with your PAT owner email or service principal if needed)
-- GRANT USE CATALOG  ON CATALOG  test_catalog            TO `your-email@example.com`;
-- GRANT USE SCHEMA   ON SCHEMA   test_catalog.project_db TO `your-email@example.com`;
-- GRANT CREATE TABLE ON SCHEMA   test_catalog.project_db TO `your-email@example.com`;

-- ── PROD catalog ──────────────────────────────────────────────────────
CREATE CATALOG IF NOT EXISTS prod_catalog
  COMMENT 'Production environment — deployed by GitHub Actions on release tag';

CREATE DATABASE IF NOT EXISTS prod_catalog.project_db
  COMMENT 'Health Analytics project schema (prod)';

-- Grant the GitHub Actions user USE + CREATE on prod
-- (uncomment and replace with your PAT owner email or service principal if needed)
-- GRANT USE CATALOG  ON CATALOG  prod_catalog            TO `your-email@example.com`;
-- GRANT USE SCHEMA   ON SCHEMA   prod_catalog.project_db TO `your-email@example.com`;
-- GRANT CREATE TABLE ON SCHEMA   prod_catalog.project_db TO `your-email@example.com`;

-- ── Verify ─────────────────────────────────────────────────────────────
SHOW CATALOGS LIKE '*_catalog';
