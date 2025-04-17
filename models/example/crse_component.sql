with component_descr as (
    select
        LOOKCOMPONENT.FIELDVALUE as COMPONENT_ID,
        LOOKCOMPONENT.XLATSHORTNAME as DESCRSHORT
    from
        di_sandbox.bronze_cs.bronze_psxlatitem LOOKCOMPONENT
    where
        LOOKCOMPONENT.FIELDNAME = 'SSR_COMPONENT'
        and LOOKCOMPONENT.EFFDT = (
            select max(LOOKCOMPONENT2.EFFDT)
            from di_sandbox.bronze_cs.bronze_psxlatitem LOOKCOMPONENT2
            where
                LOOKCOMPONENT2.FIELDNAME = LOOKCOMPONENT.FIELDNAME
                and LOOKCOMPONENT2.FIELDVALUE = LOOKCOMPONENT.FIELDVALUE
                and LOOKCOMPONENT2.EFFDT <= current_date
        )
),

final as (
    select
        cast(A.CRSE_ID as string) as CRSE_ID,
        -- First format the date to a string and then parse it to a date with the format "dd-MMM-yyyy"
        to_date(date_format(A.EFFDT, 'dd-MMM-yyyy'), 'dd-MMM-yyyy') as CRSE_EFFDT,
        cast(A.SSR_COMPONENT as string) as CRSE_COMPONENT,
        COALESCE(
            cast(comp_descr.DESCRSHORT as string),
            ''
        ) as CRSE_COMPONENT_DESCRSHORT,
        cast(A.OPTIONAL_SECTION as string) as OPTIONAL_SECTION
    from 
        di_sandbox.bronze_cs.bronze_ps_crse_component A
    left join
        component_descr comp_descr
    on
        A.SSR_COMPONENT = comp_descr.COMPONENT_ID
)

select * from final