{{ config(materialized='table') }}

with green_tripdata as (
    select *, 
        'Green' as service_type 
    from {{ ref('stg_green_tripdata') }}
), 
yellow_tripdata as (
    select *, 
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
), 
trips_unioned as (
    select 
        vendor_id, 
        pickup_location_id,
        dropoff_location_id,
        rate_code,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance, 
        trip_type, 
        fare_amount, 
        extra, 
        mta_tax, 
        tip_amount, 
        tolls_amount, 
        ehail_fee, 
        improvement_surcharge, 
        total_amount, 
        payment_type, 
        payment_type_description, 
        congestion_surcharge,
        service_type
    from green_tripdata
    union all 
    select 
        vendor_id, 
        pickup_location_id,
        dropoff_location_id,
        rate_code,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance, 
        cast(null as integer) as trip_type, 
        fare_amount, 
        extra, 
        mta_tax, 
        tip_amount, 
        tolls_amount, 
        cast(null as numeric) as ehail_fee, 
        improvement_surcharge, 
        total_amount, 
        payment_type, 
        payment_type_description, 
        congestion_surcharge,
        service_type
    from yellow_tripdata
)

select 
    -- FIX: Clave única robusta incluyendo ubicación
    {{ dbt_utils.generate_surrogate_key(['service_type', 'vendor_id', 'pickup_datetime', 'pickup_location_id']) }} as trip_id,
    *
from trips_unioned
-- Deduplicación lógica: Solo borramos si coinciden en servicio, hora Y ubicación exacta
qualify row_number() over (partition by service_type, vendor_id, pickup_datetime, pickup_location_id order by pickup_datetime) = 1