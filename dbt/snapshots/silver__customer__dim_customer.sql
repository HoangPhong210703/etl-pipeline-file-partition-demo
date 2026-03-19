{% snapshot silver__customer__dim_customer %}

{{
    config(
        target_schema='sil__accounting',
        unique_key='id',
        strategy='timestamp',
        updated_at='write_date',
    )
}}

select
    id,
    name,
    phone,
    email,
    is_company,
    write_date
from {{ source('stg__accounting__postgres_crm', 'res_partner') }}

{% endsnapshot %}
