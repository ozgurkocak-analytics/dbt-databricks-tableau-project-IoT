{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    
    {# Eğer modelde 'schema' config'i tanımlıysa (bronze, silver vb.), sadece onu kullan #}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    
    {# Tanımlı değilse ana target şemasını (default) kullan #}
    {%- else -%}
        {{ default_schema }}
    {%- endif -%}
{%- endmacro %}