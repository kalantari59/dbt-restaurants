{% macro regex_collapse_whitespace(expr) -%}
    regexp_replace({{ expr }}, '\s+', ' ', 'g')
{%- endmacro %}

{% macro regex_strip_line_breaks(expr) -%}
    regexp_replace({{ expr }}, '\r|\n', ' ', 'g')
{%- endmacro %}

{% macro regex_digits_only(expr) -%}
    regexp_replace({{ expr }}, '\D', '', 'g')
{%- endmacro %}

{% macro regex_is_price_range(expr) -%}
    {{ expr }} ~ '\d+\s*[-–]\s*\d+'
{%- endmacro %}

{% macro regex_is_price_plus(expr) -%}
    {{ expr }} ~ '\d+\+'
{%- endmacro %}

{% macro regex_price_range_min(expr) -%}
    split_part(regexp_replace({{ expr }}, '[^0-9\-]', '', 'g'), '-', 1)::int
{%- endmacro %}

{% macro regex_price_range_max(expr) -%}
    split_part(regexp_replace({{ expr }}, '[^0-9\-]', '', 'g'), '-', 2)::int
{%- endmacro %}

{% macro regex_extract_digits(expr) -%}
    regexp_replace({{ expr }}, '[^0-9]', '', 'g')::int
{%- endmacro %}

{% macro regex_strip_http(expr) -%}
    regexp_replace({{ expr }}, '^https?://', '')
{%- endmacro %}

{% macro regex_strip_path(expr) -%}
    regexp_replace({{ expr }}, '/.*$', '')
{%- endmacro %}

{% macro regex_extract_yelp_id(expr) -%}
    substring({{ expr }} from '/biz/([^/?]+)')
{%- endmacro %}
