INSERT INTO epidemic_cases (
  country, updated, iso2, iso3, continent, population,
  cases, today_cases, deaths, today_deaths, recovered, active, critical
)

SELECT
  country, updated, iso2, iso3, continent, population,
  cases, today_cases, deaths, today_deaths, recovered, active, critical
FROM epidemic_cases_staging

ON CONFLICT (country, updated)

DO UPDATE SET
  cases = EXCLUDED.cases,
  deaths = EXCLUDED.deaths,
  recovered = EXCLUDED.recovered,
  active = EXCLUDED.active,
  critical = EXCLUDED.critical;
