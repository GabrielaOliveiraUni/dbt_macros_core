{# ============================================================================
   MACRO: criar_index
   * Cria o indice das colunas - Apenas na primeira vez que a consulta é executada, 
     caso não for, ela bate no if e quebra
   
   Parâmetros:
   - nome: Nome que você quer dar para o index
   - tabela: Nome da tabela do index
   - colunas: Colunas a serem utilizadas no index ( devem ser escritas dessa forma ['CD_TENTATIVA','CD_UNIMED','...'])
   - unique: Se o indice é unico (Padrão - true)
============================================================================ #}
{% macro criar_index(nome, tabela, colunas, unique=true) %}

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
    WHERE nome = UPPER('{{ nome }}');

    IF existe_indice_com_esse_nome = 0 THEN
        EXECUTE IMMEDIATE '
            CREATE {{ unique_sql }} INDEX {{ nome }}
            ON {{ tabela }} ({{ colunas | join(", ") }})
        ';
    END IF;
END;
{% endset %}

{{ return(sql) }}

{% endmacro %}
