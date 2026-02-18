{{ config(materialized='table') }}

with trips as (
    select * from {{ ref('int_trips') }}
), 
pickup_zone as (
    select * from {{ ref('dim_zones') }}
    -- IMPORTANTE: He borrado el 'where borough != Unknown'
), 
dropoff_zone as (
    select * from {{ ref('dim_zones') }}
    -- IMPORTANTE: He borrado el 'where borough != Unknown'
)
select 
    trips.trip_id, 
    trips.vendor_id, 
    trips.service_type,
    trips.rate_code as rate_code_id, 
    trips.pickup_location_id, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trips.dropoff_location_id,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    trips.pickup_datetime, 
    trips.dropoff_datetime, 
    cast(trips.store_and_fwd_flag as string) as store_and_fwd_flag,
    trips.passenger_count, 
    trips.trip_distance, 
    timestamp_diff(trips.dropoff_datetime, trips.pickup_datetime, MINUTE) as trip_duration_minutes,
    trips.trip_type, 
    trips.fare_amount, 
    trips.extra, 
    trips.mta_tax, 
    trips.tip_amount, 
    trips.tolls_amount, 
    trips.ehail_fee, 
    trips.improvement_surcharge, 
    trips.total_amount, 
    trips.payment_type, 
    trips.payment_type_description
from trips
-- CRUCIAL: LEFT JOIN para no perder los viajes con zona desconocida
left join pickup_zone
on trips.pickup_location_id = pickup_zone.location_id
left join dropoff_zone
on trips.dropoff_location_id = dropoff_zone.location_id