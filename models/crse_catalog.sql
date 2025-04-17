WITH course_data AS (
    SELECT
        A.CRSE_ID,
        to_date(date_format(A.EFFDT, 'dd-MMM-yyyy'), 'dd-MMM-yyyy') as CRSE_EFFDT,
        A.EFF_STATUS AS CRSE_EFF_STATUS,
        A.DESCR AS CRSE_DESCR,
        A.COURSE_TITLE_LONG,
        A.M_CA_ADV_PREQ_DESC AS ADV_PREQ_DESCR254,
        A.CONSENT,
        A.ALLOW_MULT_ENROLL,
        A.GRADING_BASIS,
        A.UNITS_MINIMUM,
        A.UNITS_MAXIMUM,
        A.UNITS_ACAD_PROG,
        A.UNITS_FINAID_PROG,
        A.CRSE_REPEATABLE,
        A.UNITS_REPEAT_LIMIT,
        A.CRSE_REPEAT_LIMIT,
        A.RQMNT_DESIGNTN
    FROM
        raw.campus_solutions.ps_crse_catalog A
),

rqmnt_desig_descrshort_lookup AS (
    SELECT 
        RQMNT_DESIGNTN,
        DESCRSHORT
    FROM
        raw.campus_solutions.ps_rqmnt_desig_tbl RQMNTDESIG
    WHERE
        RQMNTDESIG.EFFDT = (
            SELECT MAX(RQMNTDESIG2.EFFDT)
            FROM raw.campus_solutions.ps_rqmnt_desig_tbl RQMNTDESIG2
            WHERE 
                RQMNTDESIG.RQMNT_DESIGNTN = RQMNTDESIG2.RQMNT_DESIGNTN 
                AND RQMNTDESIG2.EFFDT <= CURRENT_DATE
        )
),

consent_descrshort_lookup AS (
    SELECT
        FIELDVALUE AS CONSENT,
        XLATSHORTNAME AS CONSENT_DESCRSHORT
    FROM 
        raw.campus_solutions.psxlatitem LOOKCONSENT
    WHERE
        LOOKCONSENT.FIELDNAME = 'CONSENT'
        AND LOOKCONSENT.EFFDT = (
            SELECT MAX(LOOKCONSENT2.EFFDT)
            FROM raw.campus_solutions.psxlatitem LOOKCONSENT2
            WHERE 
                LOOKCONSENT2.FIELDNAME = LOOKCONSENT.FIELDNAME
                AND LOOKCONSENT2.FIELDVALUE = LOOKCONSENT.FIELDVALUE
                AND LOOKCONSENT2.EFFDT <= CURRENT_DATE
        )
),

grading_basis_descr_lookup AS (
    SELECT
        FIELDVALUE AS GRADING_BASIS,
        XLATLONGNAME AS GRADING_BASIS_DESCR,
        XLATSHORTNAME AS GRADING_BASIS_DESCRSHORT
    FROM
        raw.campus_solutions.psxlatitem LOOKGRADINGBASIS
    WHERE
        LOOKGRADINGBASIS.FIELDNAME = 'GRADING_BASIS'
        AND LOOKGRADINGBASIS.EFFDT = (
            SELECT MAX(LOOKGRADINGBASIS2.EFFDT)
            FROM raw.campus_solutions.psxlatitem LOOKGRADINGBASIS2
            WHERE
                LOOKGRADINGBASIS2.FIELDNAME = LOOKGRADINGBASIS.FIELDNAME
                AND LOOKGRADINGBASIS2.FIELDVALUE = LOOKGRADINGBASIS.FIELDVALUE
                AND LOOKGRADINGBASIS2.EFFDT <= CURRENT_DATE
        )
),

final AS (
    SELECT
        cd.*,
        rddl.DESCRSHORT AS RQMNT_DESIGNTN_DESCRSHORT,
        cdl.CONSENT_DESCRSHORT AS CONSENT_DESCRSHORT,
        gb.GRADING_BASIS_DESCRSHORT AS GRADING_BASIS_DESCRSHORT,
        gb.GRADING_BASIS_DESCR AS GRADING_BASIS_DESCR
    FROM
        course_data cd
    LEFT JOIN
        rqmnt_desig_descrshort_lookup rddl
    ON
        cd.RQMNT_DESIGNTN = rddl.RQMNT_DESIGNTN
    LEFT JOIN
        consent_descrshort_lookup cdl
    ON
        cd.CONSENT = cdl.CONSENT
    LEFT JOIN
        grading_basis_descr_lookup gb
    ON
        cd.GRADING_BASIS = gb.GRADING_BASIS
)
    
SELECT * 
FROM final