{# Create the target snapshot table if not exists #}
{% macro build_csnapshot_table(strategy, sql) -%}
  {{ adapter.dispatch('build_csnapshot_table', 'dbt')(strategy, sql) }}
{% endmacro %}

{% macro default__build_csnapshot_table(strategy, sql) %}

    select *,
        {{ strategy.scd_id }} as dbt_scd_id,
        {{ strategy.updated_at }} as dbt_updated_at,
        {{ strategy.updated_at }} as dbt_valid_from,
        '9999-12-31 23:59:58'::timestamp as dbt_valid_to,
        'Insert' as operation_type
    from (
        {{ sql }}
    ) sbq

{% endmacro %}

{# Append to the target snapshot table if exists #}
{% macro csnapshot_staging_table(strategy, source_sql, target_relation) -%}
  {{ adapter.dispatch('csnapshot_staging_table', 'dbt')(strategy, source_sql, target_relation) }}
{% endmacro %}

{% macro default__csnapshot_staging_table(strategy, source_sql, target_relation) -%}

    with snapshot_query as (

        {{ source_sql }}

    ),

    snapshotted_data as (

        select *,
            {{ strategy.unique_key }} as dbt_unique_key

        from {{ target_relation }}
        where dbt_valid_to = '9999-12-31 23:59:58'::timestamp as

    ),

    insertions_source_data as (

        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at,
            {{ strategy.updated_at }} as dbt_valid_from,
            '9999-12-31 23:59:58'::timestamp as dbt_valid_to,
            {{ strategy.scd_id }} as dbt_scd_id

        from snapshot_query
    ),

    updates_source_data as (

        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at,
            {{ strategy.updated_at }} as dbt_valid_from,
            {{ strategy.updated_at }} as dbt_valid_to

        from snapshot_query
    ),

    {%- if strategy.invalidate_hard_deletes %}

    deletes_source_data as (

        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key
        from snapshot_query
    ),
    {% endif %}

    insertions as (

        select
            'insert' as dbt_change_type,
            source_data.*,
            case
                when snapshotted_data.dbt_unique_key is null then 'Insert'
                else 'Update'
            end as operation_type

        from insertions_source_data as source_data
        left outer join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where snapshotted_data.dbt_unique_key is null
           or (
                snapshotted_data.dbt_unique_key is not null
            and (
                {{ strategy.row_changed }}
            )
        )

    ),

    updates as (

        select
            'update' as dbt_change_type,
            source_data.*,
            snapshotted_data.dbt_scd_id,
            'Update' as operation_type

        from updates_source_data as source_data
        join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where (
            {{ strategy.row_changed }}
        )
    )

    {%- if strategy.invalidate_hard_deletes -%}
    ,

    deletes as (

        select
            'delete' as dbt_change_type,
            source_data.*,
            {{ snapshot_get_time() }} as dbt_valid_from,
            {{ snapshot_get_time() }} as dbt_updated_at,
            {{ snapshot_get_time() }} as dbt_valid_to,
            snapshotted_data.dbt_scd_id,
            snapshotted_data.operation_type

        from snapshotted_data
        left join deletes_source_data as source_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where source_data.dbt_unique_key is null
    ),

    deletes_extra_record as (
        select
            'delinsert' as dbt_change_type,
            {{ snapshot_get_time() }} as dbt_valid_from,
            {{ snapshot_get_time() }} as dbt_updated_at,
            '9999-12-31 23:59:58'::timestamp as dbt_valid_to,
            snapshotted_data.dbt_scd_id,
            'Delete' as operation_type
    )
    {%- endif %}

    select * from insertions
    union all
    select * from updates
    {%- if strategy.invalidate_hard_deletes %}
    union all
    select * from deletes
    union all
    select * from deletes_extra_record
    {%- endif %}

{%- endmacro %}


{% macro build_csnapshot_staging_table(strategy, sql, target_relation) %}
    {% set temp_relation = make_temp_relation(target_relation) %}

    {% set select = csnapshot_staging_table(strategy, sql, target_relation) %}

    {% call statement('build_csnapshot_staging_relation') %}
        {{ create_table_as(True, temp_relation, select) }}
    {% endcall %}

    {% do return(temp_relation) %}
{% endmacro %}
