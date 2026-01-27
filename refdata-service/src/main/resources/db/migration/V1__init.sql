CREATE TABLE instruments (
    instrument_id VARCHAR(64) PRIMARY KEY,
    symbol VARCHAR(32) NOT NULL,
    name VARCHAR(128) NOT NULL,
    type VARCHAR(32) NOT NULL,
    currency VARCHAR(8) NOT NULL,
    status VARCHAR(16) NOT NULL
);

CREATE TABLE portfolios (
    portfolio_id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(128) NOT NULL
);

CREATE TABLE books (
    book_id VARCHAR(64) PRIMARY KEY,
    portfolio_id VARCHAR(64) NOT NULL,
    name VARCHAR(128) NOT NULL
);

CREATE TABLE limits (
    limit_id VARCHAR(64) PRIMARY KEY,
    portfolio_id VARCHAR(64) NOT NULL,
    book_id VARCHAR(64) NOT NULL,
    limit_type VARCHAR(32) NOT NULL,
    threshold NUMERIC(18, 4) NOT NULL,
    currency VARCHAR(8) NOT NULL
);
