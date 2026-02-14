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
        vendorid, 
        service_type,
        ratecodeid,
        pickup_locationid, 
        dropoff_locationid,
        pickup_datetime, 
        dropoff_datetime, 
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
        congestion_surcharge
    from green_tripdata
    union all 
    select 
        vendorid, 
        service_type,
        ratecodeid,
        pickup_locationid, 
        dropoff_locationid,
        pickup_datetime, 
        dropoff_datetime, 
        passenger_count, 
        trip_distance, 
        null as trip_type, -- Yellow no tiene trip_type
        fare_amount, 
        extra, 
        mta_tax, 
        tip_amount, 
        tolls_amount, 
        null as ehail_fee, -- Yellow no tiene ehail_fee
        improvement_surcharge, 
        total_amount, 
        payment_type, 
        congestion_surcharge
    from yellow_tripdata
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    trips_unioned.vendorid, 
    trips_unioned.service_type,
    trips_unioned.ratecodeid, 
    trips_unioned.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trips_unioned.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime, 
    trips_unioned.passenger_count, 
    trips_unioned.trip_distance, 
    trips_unioned.trip_type, 
    trips_unioned.fare_amount, 
    trips_unioned.extra, 
    trips_unioned.mta_tax, 
    trips_unioned.tip_amount, 
    trips_unioned.tolls_amount, 
    trips_unioned.ehail_fee, 
    trips_unioned.improvement_surcharge, 
    trips_unioned.total_amount, 
    trips_unioned.payment_type, 
    trips_unioned.congestion_surcharge
from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_locationid = dropoff_zone.locationid