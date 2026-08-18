{% macro parse_jpy(column_name) %}

    {#
        Convert a Japanese monetary string to yen.

        Examples:
          72,000円             -> 72000
          7.2万円              -> 72000
          7万2000円            -> 72000
          7万2千円             -> 72000
          1億2500万円          -> 125000000
          1億2,345万6,789円    -> 123456789
          ￥７２，０００        -> 72000
          ¥72,000              -> 72000
          約7.2万円            -> 72000
          月額7万円            -> 70000
          無料                  -> 0
          なし                  -> 0

        Intentionally returns NULL:
          7〜8万円             -> NULL  (range)
          1ヶ月                -> NULL  (not an amount in yen)
          応相談                -> NULL
          七万円                -> NULL  (kanji numerals unsupported)
          -                    -> NULL

        Assumption:
          column_name contains ONE monetary value, not something like
          "家賃7万円 + 管理費3000円".
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
            '０１２３４５６７８９．＋－',
            '0123456789.+-'
        )
    {% endset %}

    {#
        Keep only numeric characters and Japanese numeric units.

        e.g.
          "約 7.2万円" -> "7.2万"
          "￥72,000"  -> "72000"
    #}
    {% set compact %}
        regexp_replace(
            {{ normalized }},
            '[^0-9.+\-兆億万千百十]',
            '',
            'g'
        )
    {% endset %}

    {% set unsigned %}
        regexp_replace(
            {{ compact }},
            '^[-+]',
            ''
        )
    {% endset %}

    (
        case

            -- SQL NULL
            when {{ column_name }} is null
                then null

            -- empty / placeholder
            when {{ raw }} = ''
                then null

            when regexp_full_match(
                {{ raw }},
                '(-|--|―|ー|–|—)'
            )
                then null

            -- explicitly zero
            when regexp_matches(
                {{ raw }},
                '(無料|なし|無し|不要|ゼロ)'
            )
                then 0

            -- values whose amount cannot be determined
            when regexp_matches(
                {{ raw }},
                '(応相談|要相談|要確認|未定|お問い合わせ)'
            )
                then null

            /*
             * "敷金1ヶ月" is not itself a yen value.
             *
             * This deliberately does NOT match:
             *   70000円/月
             *   月額70000円
             */
            when regexp_matches(
                {{ normalized }},
                '[0-9]+([.][0-9]+)?(ヶ|か|カ|ヵ|ケ)?月'
            )
                then null

            -- Reject ranges instead of choosing one endpoint
            when regexp_matches(
                {{ normalized }},
                '[0-9.]+[ ]*[-–—〜～~][ ]*[0-9.]+'
            )
                then null

            /*
             * Don't silently misinterpret kanji numerals.
             *
             * Units such as 万/億 are supported, but:
             *   七万円
             *   一億円
             *
             * are not.
             */
            when regexp_matches(
                {{ raw }},
                '[〇零一二三四五六七八九壱弐参拾佰仟萬]'
            )
                then null

            -- Must actually contain a number
            when not regexp_matches(
                {{ compact }},
                '[0-9]'
            )
                then null

            /*
             * Validate the supported grammar.
             *
             * Valid:
             *   72000
             *   7.2万
             *   7万2000
             *   7万2千
             *   1億2500万6789
             *
             * This prevents malformed strings from being
             * partially interpreted.
             */
            when not regexp_full_match(
                {{ compact }},
                '[-+]?[0-9]+([.][0-9]+)?([兆億万千百十][0-9]+([.][0-9]+)?)*[兆億万千百十]?'
            )
                then null

            else
                cast(
                    round(
                        (
                            case
                                when regexp_matches(
                                    {{ compact }},
                                    '^-'
                                )
                                    then -1
                                else 1
                            end
                        )
                        *
                        list_sum(
                            list_transform(
                                regexp_extract_all(
                                    {{ unsigned }},
                                    '[0-9]+([.][0-9]+)?[兆億万千百十]?'
                                ),

                                token ->
                                    cast(
                                        regexp_extract(
                                            token,
                                            '^([0-9]+([.][0-9]+)?)',
                                            1
                                        )
                                        as decimal(38, 6)
                                    )
                                    *
                                    case regexp_extract(
                                        token,
                                        '[兆億万千百十]$'
                                    )
                                        when '兆' then 1000000000000
                                        when '億' then 100000000
                                        when '万' then 10000
                                        when '千' then 1000
                                        when '百' then 100
                                        when '十' then 10
                                        else 1
                                    end
                            )
                        )
                    )
                    as bigint
                )

        end
    )

{% endmacro %}