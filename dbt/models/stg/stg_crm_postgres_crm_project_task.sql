{{ config(materialized='table', schema='stg') }}

select distinct on (id)
    *
from {{ source('stg_accounting_postgres_crm', 'project_task') }}
order by id, _dlt_load_id desc
