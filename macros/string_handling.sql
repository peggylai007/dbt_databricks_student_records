{% macro NonPrintableCharsHandling(column) %}
          REPLACE(
              REPLACE(
                  REPLACE({{ column }}, CHR(9), ''), -- Remove Tabs
              CHR(13), ''), -- Remove Carriage Returns
          CHR(10), '') -- Remove Line Feeds
{% endmacro %}