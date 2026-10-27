-- duckdb seed.duckdb < seed.sql
-- One row per member's personal-token client (Client Center / Manage Account → personal token).
CREATE TABLE IF NOT EXISTS members (
    member VARCHAR PRIMARY KEY,
    client_id VARCHAR NOT NULL,        -- ARI-PERSONAL-TK.<...>
    client_secret VARCHAR NOT NULL
);
