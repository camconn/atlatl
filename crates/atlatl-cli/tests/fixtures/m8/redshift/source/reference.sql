CREATE SCHEMA IF NOT EXISTS est;
CREATE SCHEMA IF NOT EXISTS fund;
CREATE TABLE fund.fund_sec_cover (sec_id VARCHAR, name VARCHAR, country VARCHAR, active BOOLEAN, shares BIGINT);
INSERT INTO fund.fund_sec_cover VALUES
  ('S0001-A', 'Alpha Holdings', 'US', true, 1000000),
  ('S0002-B', 'Beta''s Group', 'GB', true, 2500000),
  ('S0003-C', 'Gamma \ Co', NULL, false, NULL);
CREATE TABLE fund.fund_sec_hist (sec_id VARCHAR, start_date DATE, end_date DATE, entity_id VARCHAR);
INSERT INTO fund.fund_sec_hist VALUES
  ('S0001-A', '2019-01-01', '2021-06-30', 'E-100'),
  ('S0001-A', '2021-07-01', NULL, 'E-101'),
  ('S0002-B', '2020-03-15', NULL, 'E-200');
CREATE TABLE est.est_sec_map (sec_id VARCHAR, entity_id VARCHAR, mapped_at TIMESTAMPTZ);
INSERT INTO est.est_sec_map VALUES
  ('S0001-A', 'E-101', TIMESTAMPTZ '2024-02-01 09:30:00+02'),
  ('S0002-B', 'E-200', TIMESTAMPTZ '2024-02-02 23:15:00-05');
