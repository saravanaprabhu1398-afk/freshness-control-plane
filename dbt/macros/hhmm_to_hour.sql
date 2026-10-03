{# BTS times are local 'hhmm' strings; '2400' means midnight at the end of the day. #}
{% macro hhmm_to_hour(col) -%}
    case when {{ col }} is null or trim({{ col }}) = '' then null
         else (cast(lpad(trim({{ col }}), 4, '0')[1:2] as integer) % 24) end
{%- endmacro %}
