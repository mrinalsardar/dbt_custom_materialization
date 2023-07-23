
{% macro csnapshot_merge_sql(target, source, insert_cols, strategy) -%}
  {{ adapter.dispatch('csnapshot_merge_sql', 'dbt')(target, source, insert_cols, strategy) }}
{%- endmacro %}


{% macro default__csnapshot_merge_sql(target, source, insert_cols, strategy) -%}
    {%- set insert_cols_csv = insert_cols | join(', ') -%}

    merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source }} as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id

    when matched
     and DBT_INTERNAL_DEST.dbt_valid_to is null
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ({{ insert_cols_csv }})
        values ({{ insert_cols_csv }})

    {%- if strategy.invalidate_hard_deletes %}
    ;
    insert into {{ target }} as DBT_INTERNAL_DEST
    select * from {{ source }} as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type = 'delinsert'
    {%- endif %}

{% endmacro %}
