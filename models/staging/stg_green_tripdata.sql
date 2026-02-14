{{ config(materialized='view') }}

with tripdata as (
  select *,
    row_number() over(partition by vendorid, lpep_pickup_datetime) as rn
  from {{ source('staging', 'green') }}
  where vendorid is not null
)
select
    -- Identificadores
    cast(vendorid as integer) as vendorid,
    cast(pulocationid as integer) as pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,

    -- Fechas
    cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,

    -- Info de viaje
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as double) as trip_distance,
    cast(trip_type as integer) as trip_type,

    -- Info de pago (Agregamos todo lo que faltaba)
    cast(ratecodeid as integer) as ratecodeid,
    cast(fare_amount as double) as fare_amount,
    cast(extra as double) as extra,
    cast(mta_tax as double) as mta_tax,
    cast(tip_amount as double) as tip_amount,
    cast(tolls_amount as double) as tolls_amount,
    cast(ehail_fee as double) as ehail_fee,
    cast(improvement_surcharge as double) as improvement_surcharge,
    cast(total_amount as double) as total_amount,
    cast(payment_type as integer) as payment_type,
    cast(congestion_surcharge as double) as congestion_surcharge

from tripdata
where rn = 1