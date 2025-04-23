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
        CAST(SUBSTRING(A.DESCRLONG, 1, 254) AS VARCHAR(254)) AS IN_CRS_TOPIC_DESCRLONG
    FROM
    {{ source('raw_campus_solutions_source','ps_crse_topics') }} AS A
),

second_crse_topics AS (
    SELECT
        CRSE_ID,
        CRSE_EFFDT,
        CRS_TOPIC_ID, -- Consider handling default values if necessary
        DESCRSHORT AS CRS_TOPIC_DESCRSHORT,
        DESCRFORMAL AS CRS_TOPIC_DESCRFORMAL,
        -- Assuming `NonPrintableCharsHandling` is a dbt macro or function that handles non-printable characters
        {{ NonPrintableCharsHandling('IN_CRS_TOPIC_DESCRLONG') }} AS OUT_CRS_TOPIC_DESCRLONG,
        CRSE_REPEATABLE,
        UNITS_REPEAT_LIMIT, -- Consider handling default values if necessary
        CRSE_REPEAT_LIMIT  -- Consider handling default values if necessary
    FROM
        init_crse_topics
),

final AS (
    SELECT
        CRSE_ID,
        CRSE_EFFDT,
        CRS_TOPIC_ID, -- Consider handling default values if necessary
        CRS_TOPIC_DESCRSHORT,
        CRS_TOPIC_DESCRFORMAL,
        CAST(OUT_CRS_TOPIC_DESCRLONG AS VARCHAR(254)) AS CRSE_TOPIC_DESCRLONG,
        CRSE_REPEATABLE,
        UNITS_REPEAT_LIMIT, -- Consider handling default values if necessary
        CRSE_REPEAT_LIMIT  -- Consider handling default values if necessary
    FROM
        second_crse_topics
)

SELECT
    *
FROM
    final