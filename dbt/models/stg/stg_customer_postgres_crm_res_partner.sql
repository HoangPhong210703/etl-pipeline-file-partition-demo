{{ config(materialized='table', schema='stg') }}

select distinct on (id)
    *
from {{ source('stg', 'stg_customer_postgres_crm_res_partner') }}
order by id, _dlt_load_id desc
