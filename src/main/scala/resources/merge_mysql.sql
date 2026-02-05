INSERT INTO epidemic_cases (
  country, updated, iso2, iso3, continent, population,
  cases, today_cases, deaths, today_deaths, recovered, active, critical
)

SELECT
  country, updated, iso2, iso3, continent, population,
  cases, today_cases, deaths, today_deaths, recovered, active, critical
FROM epidemic_cases_staging

ON DUPLICATE KEY UPDATE
  cases = VALUES(cases),
  deaths = VALUES(deaths),
  recovered = VALUES(recovered),
  active = VALUES(active),
  critical = VALUES(critical);
