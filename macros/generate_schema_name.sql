{% macro generate_schema_name(custom_schema_name, node) -%}
  {# If a model has +schema configured (custom_schema_name), use it exactly.
     Otherwise fall back to target.schema. #}
  {{ custom_schema_name if custom_schema_name is not none else target.schema }}
{%- endmacro %}