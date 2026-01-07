{% macro create_index_safe(index_name, table_name, columns, unique=true) %}

{% if unique %}
    {% set unique_sql = 'UNIQUE' %}
{% else %}
    {% set unique_sql = '' %}
{% endif %}


{% set sql %}
DECLARE
    existe_indice_com_esse_nome NUMBER;
BEGIN
    SELECT COUNT(*)
    INTO existe_indice_com_esse_nome
    FROM user_indexes
    WHERE index_name = UPPER('{{ index_name }}');

    IF existe_indice_com_esse_nome = 0 THEN
        EXECUTE IMMEDIATE '
            CREATE {{ unique_sql }} INDEX {{ index_name }}
            ON {{ table_name }} ({{ columns | join(", ") }})
        ';
    END IF;
END;
{% endset %}

{{ return(sql) }}

{% endmacro %}
