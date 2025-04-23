with max_effs as (
    select 
        CRSE_ATTR,
        MAX(EFFDT) as EFFDT
    from 
        raw.campus_solutions.PS_CRSE_ATTR_TBL
    where 
        EFFDT <= current_date
    group by CRSE_ATTR
),

crse_attr_descr as (
    select 
        A.CRSE_ATTR,
        A.DESCR
    from 
        raw.campus_solutions.PS_CRSE_ATTR_TBL A
    inner join 
        max_effs M on A.CRSE_ATTR = M.CRSE_ATTR and A.EFFDT = M.EFFDT
),

crse_attr_descrshort as (
    select 
        A.CRSE_ATTR,
        A.DESCRSHORT
    from 
        raw.campus_solutions.PS_CRSE_ATTR_TBL A
    inner join 
        max_effs M on A.CRSE_ATTR = M.CRSE_ATTR and A.EFFDT = M.EFFDT
),

crse_attr_value_descr as (
    select
        B.CRSE_ATTR,
        B.CRSE_ATTR_VALUE,
        B.DESCR 
    from
        raw.campus_solutions.PS_CRSE_ATTR_VALUE B
    where 
        B.EFFDT = (
            select max(B2.EFFDT) 
            from raw.campus_solutions.PS_CRSE_ATTR_VALUE B2 
            where B2.CRSE_ATTR_VALUE = B.CRSE_ATTR_VALUE 
            and B2.CRSE_ATTR = B.CRSE_ATTR
            and B2.EFFDT <= current_date
        )
),

final as (
    select
        A.CRSE_ID,
        to_date(date_format(A.EFFDT, 'dd-MMM-yyyy'), 'dd-MMM-yyyy') as CRSE_EFFDT,
        A.CRSE_ATTR,
        D1.DESCR as CRSE_ATTR_DESCR,
        D2.DESCRSHORT as CRSE_ATTR_DESCRSHORT,
        A.CRSE_ATTR_VALUE,
        V.DESCR as CRSE_ATTR_VALUE_DESCR
    from 
        raw.campus_solutions.PS_CRSE_ATTRIBUTES A
    left join 
        crse_attr_descr D1 on A.CRSE_ATTR = D1.CRSE_ATTR
    left join 
        crse_attr_descrshort D2 on A.CRSE_ATTR = D2.CRSE_ATTR
    left join 
        crse_attr_value_descr V on A.CRSE_ATTR = V.CRSE_ATTR 
        and A.CRSE_ATTR_VALUE = V.CRSE_ATTR_VALUE
)

select * from final
