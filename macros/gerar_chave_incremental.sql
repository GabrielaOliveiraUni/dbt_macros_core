{# ============================================================================
    MACRO: gerar_chave_incremental
    * Mantém IDs antigos e gera novos sequenciais
   
   Parâmetros:
   - colunas_ordenacao: Lista de colunas para ordenação
   - nome_coluna: Nome da coluna de ID (padrão: 'CD_CHAVE_IDENTIFICACAO')
   - tabela_destino: Tabela de destino (padrão: this)
============================================================================ #}
{% macro gerar_chave_incremental(colunas_ordenacao, nome_coluna='CD_CHAVE_IDENTIFICACAO', tabela_destino=this) %}
    {% set ordem = colunas_ordenacao | join(', ') %}
    
    {% if is_incremental() %}
        {% set maior_cd_tabela %}
            SELECT NVL(MAX({{ nome_coluna }}), 0) AS maior_cd
            FROM {{ tabela_destino }}
        {% endset %}
        
        {% set resultado = run_query(maior_cd_tabela) %}
        {% set maior_cd = resultado.columns[0].values()[0]
           if resultado and resultado.columns[0].values() | length > 0
           else 0
        %}
        
        {{ max_id }} + ROW_NUMBER() OVER (ORDER BY {{ ordem }}) AS {{ nome_coluna }}
    {% else %}
            ROW_NUMBER() OVER (ORDER BY {{ ordem }}) AS {{ nome_coluna }}
        {% endif %}
    {% endif %}
{% endmacro %}