WITH init_crse_topics AS (
    SELECT
        A.CRSE_ID,
        to_date(date_format(A.EFFDT, 'dd-MMM-yyyy'), 'dd-MMM-yyyy') as CRSE_EFFDT,
        A.CRS_TOPIC_ID,
        A.DESCRSHORT,
        A.DESCRFORMAL,
        A.CRSE_REPEATABLE,
        A.UNITS_REPEAT_LIMIT,
        A.CRSE_REPEAT_LIMIT,
        LEFT(A.DESCRLONG, 254) AS IN_CRS_TOPIC_DESCRLONG
    FROM
    {{ source('raw_campus_solutions_source','ps_crse_topics') }} AS A
),

final AS (
    SELECT
        CRSE_ID,
        CRSE_EFFDT,
        CRS_TOPIC_ID, -- Consider handling default values if necessary
        DESCRSHORT AS CRS_TOPIC_DESCRSHORT,
        DESCRFORMAL AS CRS_TOPIC_DESCRFORMAL,
        -- Assuming `NonPrintableCharsHandling` is a dbt macro or function that handles non-printable characters
        LEFT({{ NonPrintableCharsHandling('IN_CRS_TOPIC_DESCRLONG') }}, 254) AS CRS_TOPIC_DESCRLONG,
        CRSE_REPEATABLE,
        UNITS_REPEAT_LIMIT, -- Consider handling default values if necessary
        CRSE_REPEAT_LIMIT  -- Consider handling default values if necessary
    FROM
        init_crse_topics
)

SELECT
    *
FROM
    final