with source as (
    select * from {{ source('olist_dw', 'olist_geolocation_dataset') }}
),

renamed as (
    select
        geolocation_zip_code_prefix,
        geolocation_lat::numeric      as latitude,
        geolocation_lng::numeric      as longitude,
        geolocation_city,
        geolocation_state
    from source
)

select * from renamed