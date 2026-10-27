CREATE SCHEMA IF NOT EXISTS fund;
-- Created out of order: discovery must not depend on creation order or server collation.
CREATE TABLE fund.fund_basic_saf (sec_id VARCHAR, period_end DATE, currency VARCHAR, sales DECIMAL(38,10), eps DECIMAL(18,4), employees INTEGER);
CREATE TABLE fund.fund_basic_qf (sec_id VARCHAR, period_end DATE, currency VARCHAR, sales DECIMAL(38,10), eps DECIMAL(18,4), employees INTEGER);
CREATE TABLE fund.fund_basic_af (sec_id VARCHAR, period_end DATE, currency VARCHAR, sales DECIMAL(38,10), eps DECIMAL(18,4), employees INTEGER);
CREATE TABLE fund.fund_basic_der_af (sec_id VARCHAR, period_end DATE, margin DOUBLE);
CREATE TABLE fund."fund_basic_Zf" (sec_id VARCHAR, period_end DATE, flag BOOLEAN);
CREATE TABLE fund.fund_basic_tmp (sec_id VARCHAR, period_end DATE);
CREATE TABLE fund."fund_basic_x.y" (sec_id VARCHAR, period_end DATE);
CREATE VIEW fund.fund_basic_view AS SELECT * FROM fund.fund_basic_af;
CREATE TABLE fund.fund_adv_af (sec_id VARCHAR, period_end DATE, capex DECIMAL(38,10));
CREATE TABLE fund.fund_adv_qf (sec_id VARCHAR, period_end DATE, capex DECIMAL(38,10));
INSERT INTO fund.fund_basic_af VALUES
  ('S0001-A', current_date - 10, 'USD', 12345678901234567890.1234567890, 1.2345, 1200),
  ('S0001-A', current_date - 400, 'USD', 11000000000.5, 1.1, 1100),
  ('S0002-B', current_date - 30, 'GBP', 5.0000000001, -0.0001, NULL);
INSERT INTO fund.fund_basic_qf VALUES
  ('S0001-A', current_date - 5, 'USD', 3000000000, 0.3, 1200),
  ('S0001-A', current_date - 200, 'USD', 2900000000, 0.29, 1150);
INSERT INTO fund.fund_basic_saf VALUES ('S0002-B', current_date - 60, 'GBP', 7, 0.7, 50);
INSERT INTO fund.fund_basic_der_af VALUES ('S0001-A', current_date - 10, 0.25), ('S0002-B', current_date - 500, 0.1);
INSERT INTO fund."fund_basic_Zf" VALUES ('S0001-A', current_date - 10, true);
INSERT INTO fund.fund_basic_tmp VALUES ('SKIP', current_date);
INSERT INTO fund."fund_basic_x.y" VALUES ('SKIP', current_date);
INSERT INTO fund.fund_adv_af VALUES ('S0001-A', current_date - 10, 10.5), ('S0003-C', current_date - 800, 1);
INSERT INTO fund.fund_adv_qf VALUES ('S0001-A', current_date - 5, 2.5);
