CREATE DATABASE epidemic_db;

USE epidemic_db;

CREATE TABLE epidemic_cases (
  country VARCHAR(100) NOT NULL,
  updated BIGINT NOT NULL,
  iso2 VARCHAR(5),
  iso3 VARCHAR(5),
  continent VARCHAR(50),
  population BIGINT,
  cases BIGINT,
  deaths BIGINT,
  recovered BIGINT,
  active BIGINT,
  critical BIGINT,
  tests BIGINT, 
  casesPerOneMillion BIGINT, 
  deathsPerOneMillion BIGINT,
  PRIMARY KEY (country, updated)
);

CREATE TABLE epidemic_cases_staging LIKE epidemic_cases;
