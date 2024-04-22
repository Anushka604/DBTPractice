{% snapshot SCD2_USER_SNAPSHOT %}
{{
    config(
      target_database='PC_DBT_DB',
      target_schema='DBT_ABAVISKAR',
      unique_key='ID',
      strategy='check',
      check_cols=["USERNAME","EMAIL","FIRSTNAME","LASTNAME"]
    )
}}
select ID,USERNAME,EMAIL,FIRSTNAME,LASTNAME
 from {{ source('raw_database_source', 'USER') }}
{% endsnapshot %}