{% macro parse_jpy(column_name) %}

    {#
        Convert a Japanese monetary string listed in SUUMO to yen.

        Examples:
          7.2万円       -> 72000
          7万円         -> 70000
          無料          -> 0
          なし          -> 0
          -             -> 0
    #}

    {% set raw %}
        trim(cast({{ column_name }} as varchar))
    {% endset %}

    {#
        Convert full-width Japanese numbers/punctuation:
            ７２．５ -> 72.5
    #}
    {% set normalized %}
        translate(
            {{ raw }},
            '０１２３４５６７８９．',
            '0123456789.'
        )
    {% endset %}

    case
        -- SQL NULL
        when {{ column_name }} is null
            then null

        -- empty
        when {{ raw }} = ''
            then null

        -- placeholders
        when regexp_full_match(
            {{ raw }},
            '(-|--|―|ー|–|—)'
        )
            then 0

        -- explicitly zero
        when regexp_matches(
            {{ raw }},
            '(無料|なし|無し|不要|ゼロ)'
        )
            then 0

        else
            try_cast(
                try_cast(
                regexp_extract(
                    {{ normalized }},
                    '([0-9]+(?:\.[0-9]+)?)万円',
                    1
                ) as double
            ) * 10000
            as int)
    end

{% endmacro %}