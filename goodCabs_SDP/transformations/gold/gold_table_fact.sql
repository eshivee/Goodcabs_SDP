CREATE OR REPLACE VIEW goodcabs.gold.fact_trips AS(
  SELECT t.id,
  t.business_date,
  t.city_id,
  c.city_name,
  t.passenger_category,
  t.distance_kms,
  t.fare_amount,
  t.passenger_rating,
  t.driver_rating,
  d.month,
  d.day_of_week,
  d.day_of_month,
  d.month_name,
  d.month_year,
  d.quarter,
  d.quarter_year,
  d.week_of_the_year,
  d.is_weekday,
  d.is_weekend,
  d.is_holiday,
  d.holiday_name
  FROM goodcabs.silver.trips t
  JOIN goodcabs.silver.city c ON t.city_id = c.city_id
  JOIN goodcabs.silver.date_trip d ON t.business_date = d.date
)