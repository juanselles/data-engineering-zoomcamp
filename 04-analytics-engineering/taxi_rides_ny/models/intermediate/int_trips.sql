{{ config(materialized='table') }}

with trips as (
    select * from {{ ref('int_trips_unioned') }}
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    -- AQUÍ AÑADIMOS LA COLUMNA NUEVA
    trips.trip_id,
    
    trips.vendor_id, 
    trips.service_type,
    trips.rate_code, 
    trips.pickup_location_id, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trips.dropoff_location_id,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    trips.pickup_datetime, 
    trips.dropoff_datetime, 
    trips.store_and_fwd_flag, 
    trips.passenger_count, 
    trips.trip_distance, 
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
    trips.payment_type_description, 
    trips.congestion_surcharge
from trips
inner join dim_zones as pickup_zone
on trips.pickup_location_id = pickup_zone.location_id
inner join dim_zones as dropoff_zone
on trips.dropoff_location_id = dropoff_zone.location_id