{% snapshot stock_prices_snapshot %}

{{
    config(
      target_database='analytics',
      target_schema='snapshots',
      unique_key='symbol',
      strategy='timestamp',
      updated_at='date',
      invalidate_hard_deletes=True,
    )
}}

select * from {{ source('raw', 'stock_prices') }}
qualify row_number() over (partition by symbol order by date desc) = 1

{% endsnapshot %}