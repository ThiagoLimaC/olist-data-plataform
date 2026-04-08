with source as (
    select * from {{ source('olist_dw', 'olist_order_payments_dataset') }}
),

renamed as (
    select
        order_id,
        payment_sequential::int     as payment_sequential,
        payment_type,
        payment_installments::int   as payment_installments,
        payment_value::numeric      as payment_value
    from source
)

select * from renamed