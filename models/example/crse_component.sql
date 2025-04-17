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
        A.CRSE_ID,
        date_format(A.EFFDT, 'dd-MMM-yyyy') as CRSE_EFFDT,
        A.SSR_COMPONENT,
        A.OPTIONAL_SECTION,
        COALESCE(comp_descr.DESCRSHORT, '') as CRSE_COMPONENT_DESCRSHORT
    from 
        di_sandbox.bronze_cs.bronze_ps_crse_component A
    left join
        component_descr comp_descr
    on
        A.SSR_COMPONENT = comp_descr.COMPONENT_ID
)

select * from final