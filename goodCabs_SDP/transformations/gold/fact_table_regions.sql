CREATE VIEW goodcabs.gold.fact_trips_jaipur AS (
  SELECT * FROM goodcabs.gold.fact_trips
  WHERE city_id = 'RJ01'
);

CREATE VIEW goodcabs.gold.fact_trips_lucknow AS (
  SELECT * FROM goodcabs.gold.fact_trips
  WHERE city_id = 'UP01'
);

CREATE VIEW goodcabs.gold.fact_trips_surat AS (
  SELECT * FROM goodcabs.gold.fact_trips
  WHERE city_id = 'GJ01'
);

CREATE VIEW goodcabs.gold.fact_trips_kochi AS (
  SELECT * FROM goodcabs.gold.fact_trips
  WHERE city_id = 'KL01'
);

CREATE VIEW goodcabs.gold.fact_trips_indore AS (
  SELECT * FROM goodcabs.gold.fact_trips
  WHERE city_id = 'MP01'
);

CREATE VIEW goodcabs.gold.fact_trips_chandingarh AS (
  SELECT * FROM goodcabs.gold.fact_trips
  WHERE city_id = 'CH01'
);

CREATE VIEW goodcabs.gold.fact_trips_vadodara AS (
  SELECT * FROM goodcabs.gold.fact_trips
  WHERE city_id = 'GJ01'
);

CREATE VIEW goodcabs.gold.fact_trips_visakhapatnam AS (
  SELECT * FROM goodcabs.gold.fact_trips
  WHERE city_id = 'AP01'
);

CREATE VIEW goodcabs.gold.fact_trips_coimbatore AS (
  SELECT * FROM goodcabs.gold.fact_trips
  WHERE city_id = 'TN01'
);

CREATE VIEW goodcabs.gold.fact_trips_mysore AS (
  SELECT * FROM goodcabs.gold.fact_trips
  WHERE city_id = 'KA01'
);
