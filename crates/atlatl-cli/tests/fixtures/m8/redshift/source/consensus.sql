CREATE SCHEMA IF NOT EXISTS est;
CREATE TABLE est.est_basic_cons_af (sec_id VARCHAR, metric VARCHAR, fp_end DATE, window_start DATE, mean DECIMAL(28,8), high DECIMAL(28,8), low DECIMAL(28,8), n_est INTEGER);
INSERT INTO est.est_basic_cons_af
  SELECT 'S' || lpad((i % 50)::VARCHAR, 4, '0'), 'EPS', DATE '2025-12-31', current_date - (i // 50 * 3)::INTEGER,
         (i * 0.01234567)::DECIMAL(28,8), (i * 0.02)::DECIMAL(28,8), (i * -0.01)::DECIMAL(28,8), (i % 17)::INTEGER
  FROM range(2000) t(i);
CREATE TABLE est.est_basic_cons_qf (sec_id VARCHAR, metric VARCHAR, fp_end DATE, window_start DATE, mean DECIMAL(28,8), high DECIMAL(28,8), low DECIMAL(28,8), n_est INTEGER);
INSERT INTO est.est_basic_cons_qf VALUES ('S0001', 'EPS', '2025-09-30', current_date - 3, 0.5, 0.6, 0.4, 7);
CREATE TABLE est.est_basic_cons_rec (sec_id VARCHAR, metric VARCHAR, window_start DATE, buy INTEGER, hold INTEGER, sell INTEGER);
INSERT INTO est.est_basic_cons_rec VALUES
  ('S0001', 'REC', current_date - 2, 10, 3, 1),
  ('S0002', 'REC', current_date - 90, 2, 8, 4);
-- The large table of the family.
CREATE TABLE est.est_adv_cons_af (sec_id VARCHAR, metric VARCHAR, fp_end DATE, window_start DATE, mean DECIMAL(38,10), n_est INTEGER, updated_ts TIMESTAMPTZ);
INSERT INTO est.est_adv_cons_af
  SELECT 'S' || lpad((i % 400)::VARCHAR, 4, '0'), 'M' || (i // 400 % 5)::VARCHAR, DATE '2026-12-31', current_date - (i // 2000 * 10)::INTEGER,
         ('12345678901234567890.' || lpad(i::VARCHAR, 10, '0'))::DECIMAL(38,10), (i % 23)::INTEGER,
         TIMESTAMPTZ '2025-01-01 00:00:00+00' + INTERVAL (i) SECOND
  FROM range(20000) t(i);
