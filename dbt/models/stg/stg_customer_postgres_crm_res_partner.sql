{{ config(materialized='table', schema='stg') }}

select distinct on (id)
    *
from {{ source('stg__accounting__postgres_crm', 'res_partner') }}
order by id, _dlt_load_id desc
