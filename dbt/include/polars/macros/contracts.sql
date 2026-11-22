{#- Renders nothing: this only fires dbt's warnings for constraints dbt-polars does not enforce -#}
{% macro polars_warn_unenforced_constraints() %}
  {%- if config.get('contract').enforced -%}
    {%- do adapter.render_raw_columns_constraints(raw_columns=model['columns']) -%}
    {%- do adapter.render_raw_model_constraints(raw_constraints=model['constraints']) -%}
  {%- endif -%}
{% endmacro %}
