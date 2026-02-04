
CREATE DATABASE epidemic_db;

USE epidemic_db;

CREATE TABLE epidemic_cases (
    country       VARCHAR(100),
    countryInfo   JSON,
    cases         BIGINT,
    todayCases    BIGINT,
    deaths        BIGINT,
    recovered     BIGINT,
    updated       BIGINT
);
