CREATE SCHEMA IF NOT EXISTS est;
CREATE TABLE est.est_basic_act_af (sec_id VARCHAR, metric VARCHAR, fp_end DATE, currency VARCHAR, value DECIMAL(28,8), published_on DATE);
INSERT INTO est.est_basic_act_af VALUES
  ('S0001-A', 'EPS', '2024-12-31', 'USD', 1.23456789, '2025-02-10'),
  ('S0001-A', 'SALES', '2024-12-31', 'USD', 123456789012.5, '2025-02-10'),
  ('S0002-B', 'EPS', '2024-12-31', 'GBP', -0.5, NULL),
  ('S0003-C', 'EPS', '2023-12-31', 'USD', 0.01, '2024-03-01');
CREATE TABLE est.est_basic_act_qf (sec_id VARCHAR, metric VARCHAR, fp_end DATE, currency VARCHAR, value DECIMAL(28,8), published_on DATE);
INSERT INTO est.est_basic_act_qf VALUES
  ('S0001-A', 'EPS', '2025-03-31', 'USD', 0.31, '2025-05-01'),
  ('S0002-B', 'EPS', '2025-03-31', 'GBP', NULL, NULL);
CREATE TABLE est.est_basic_act_saf (sec_id VARCHAR, metric VARCHAR, fp_end DATE, currency VARCHAR, value DECIMAL(28,8), published_on DATE);
CREATE TABLE est.est_adv_act_af (sec_id VARCHAR, metric VARCHAR, fp_end DATE, currency VARCHAR, value DECIMAL(28,8), published_on DATE);
INSERT INTO est.est_adv_act_af VALUES
  ('S0001-A', 'EBITDA', '2024-12-31', 'USD', 99999999999999999999.12345678, '2025-02-11'),
  ('S0002-B', 'EBITDA', '2024-12-31', 'GBP', 42, '2025-02-12');
