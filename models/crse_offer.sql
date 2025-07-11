{{ config(materialized='table') }}

with
-- Main Course Offer Table
src as (
    select
    co.CRSE_ID,
    to_date(date_format(co.EFFDT, 'yyyy-MM-dd'), 'yyyy-MM-dd') as EFFDT, 
    date_format(co.EFFDT, 'dd-MMM-yyyy') as CRSE_EFFDT_STR,
    co.CRSE_OFFER_NBR,
    ltrim(co.CATALOG_NBR) as CATALOG_NBR,
    co.COURSE_APPROVED,
    co.SUBJECT,
    co.ACAD_CAREER,
    co.CAMPUS,
    co.ACAD_GROUP,
    co.ACAD_ORG as DEPTID,
    co.CIP_CODE,
    co.HEGIS_CODE,
    co.RQRMNT_GROUP
from raw.campus_solutions.ps_crse_offer co
),
subject_descr as (
    select
        s.SUBJECT,
        s.DESCR as SUBJECT_DESCR
    from raw.campus_solutions.ps_subject_tbl s
    where s.INSTITUTION = 'UMICH'
    and s.EFFDT = (
        select max(s2.EFFDT)
        from raw.campus_solutions.ps_subject_tbl s2
        where s2.SUBJECT = s.SUBJECT
            and s2.INSTITUTION = 'UMICH'
            and s2.EFFDT <= CURRENT_DATE
    )
),
acad_career_descr as (
    select
        ac.ACAD_CAREER,
        ac.DESCR as ACAD_CAREER_DESCR,
        ac.DESCRSHORT as ACAD_CAREER_DESCRSHORT
    from raw.campus_solutions.ps_acad_car_tbl ac
    where ac.EFFDT = (
        select max(ac2.EFFDT)
        from raw.campus_solutions.ps_acad_car_tbl ac2
        where ac2.ACAD_CAREER = ac.ACAD_CAREER
            and ac2.EFFDT <= CURRENT_DATE
    )
),
acad_group_descr as (
    select
        ag.ACAD_GROUP,
        ag.DESCR as ACAD_GROUP_DESCR,
        ag.DESCRSHORT as ACAD_GROUP_DESCRSHORT
    from raw.campus_solutions.ps_acad_group_tbl ag
    where ag.EFFDT = (
        select max(ag2.EFFDT)
        from raw.campus_solutions.ps_acad_group_tbl ag2
        where ag2.ACAD_GROUP = ag.ACAD_GROUP
            and ag2.EFFDT <= CURRENT_DATE
    )
),
deptid_descr as (
    select
        ao.ACAD_ORG as DEPTID,
        ao.DESCRSHORT as DEPT_DESCRSHORT,
        ao.DESCRFORMAL as DEPT_DESCRFORMAL
    from raw.campus_solutions.ps_acad_org_tbl ao
    where ao.INSTITUTION = 'UMICH'
    and ao.EFFDT = (
        select max(ao2.EFFDT)
        from raw.campus_solutions.ps_acad_org_tbl ao2
        where ao2.ACAD_ORG = ao.ACAD_ORG
        and ao2.INSTITUTION = 'UMICH'
            and ao2.EFFDT <= CURRENT_DATE
    )
),
cip_code_descr as (
    select
        cc.CIP_CODE,
        cc.DESCR as CIP_DESCR,
        cc.DESCR254 as CIP_DESCR254
    from raw.campus_solutions.ps_cip_code_tbl cc
    where cc.EFFDT = (
        select max(cc2.EFFDT)
        from raw.campus_solutions.ps_cip_code_tbl cc2
        where cc2.CIP_CODE = cc.CIP_CODE
            and cc2.EFFDT <= CURRENT_DATE
    )
),
hegis_code_descr as (
    select
        hc.HEGIS_CODE,
        hc.DESCR as HEGIS_DESCR,
        hc.DESCR60 as HEGIS_DESCR60
    from raw.campus_solutions.ps_hegis_code_tbl hc
    where hc.EFFDT = (
        select max(hc2.EFFDT)
        from raw.campus_solutions.ps_hegis_code_tbl hc2
        where hc2.HEGIS_CODE = hc.HEGIS_CODE
            and hc2.EFFDT <= CURRENT_DATE
    )
),
rqrmnt_group_descr_ranked as (
    select
        rq.RQRMNT_GROUP,
        rq.SAA_DESCR80 as RQRMNT_GROUP_DESCR80,
        left(rq.DESCRLONG, 254) as RQRMNT_GROUP_DESCR254,
        to_date(date_format(rq.EFFDT, 'yyyy-MM-dd'), 'yyyy-MM-dd') as EFFDT,
        src.EFFDT as SRC_EFFDT,
        row_number() over (
          partition by rq.RQRMNT_GROUP, src.EFFDT
          order by rq.EFFDT desc
        ) as rn
    from raw.campus_solutions.ps_rq_grp_tbl rq
    join src
      on rq.RQRMNT_GROUP = src.RQRMNT_GROUP
      and rq.EFFDT <= src.EFFDT
),

-- Pick only most recent (max EFFDT <= src.EFFDT) per group for each course offer row
rqrmnt_group_descr as (
    select 
        RQRMNT_GROUP,
        RQRMNT_GROUP_DESCR80,
        RQRMNT_GROUP_DESCR254,
        SRC_EFFDT
    from rqrmnt_group_descr_ranked
    where rn = 1
),
  
final as 
(
select
    s.CRSE_ID,
    s.EFFDT,
    s.CRSE_EFFDT_STR,
    s.CRSE_OFFER_NBR,
    s.CATALOG_NBR,
    s.COURSE_APPROVED,
    s.SUBJECT,
    subj.SUBJECT_DESCR,
    s.ACAD_CAREER,
    acd.ACAD_CAREER_DESCR,
    acd.ACAD_CAREER_DESCRSHORT,
    s.CAMPUS,
    s.ACAD_GROUP,
    agd.ACAD_GROUP_DESCR,
    agd.ACAD_GROUP_DESCRSHORT,
    s.DEPTID,
    dtd.DEPT_DESCRSHORT,
    dtd.DEPT_DESCRFORMAL,
    s.CIP_CODE,
    ccd.CIP_DESCR,
    ccd.CIP_DESCR254,
    s.HEGIS_CODE,
    hcd.HEGIS_DESCR,
    hcd.HEGIS_DESCR60,
    s.RQRMNT_GROUP,
    rqd.RQRMNT_GROUP_DESCR80,
    rqd.RQRMNT_GROUP_DESCR254
from src s
left join subject_descr subj
    on s.SUBJECT = subj.SUBJECT
left join acad_career_descr acd
    on s.ACAD_CAREER = acd.ACAD_CAREER
left join acad_group_descr agd
    on s.ACAD_GROUP = agd.ACAD_GROUP
left join deptid_descr dtd
    on s.DEPTID = dtd.DEPTID
left join cip_code_descr ccd
    on s.CIP_CODE = ccd.CIP_CODE
left join hegis_code_descr hcd
    on s.HEGIS_CODE = hcd.HEGIS_CODE
left join rqrmnt_group_descr rqd
    on s.RQRMNT_GROUP = rqd.RQRMNT_GROUP
order by s.SUBJECT
)

select * 
from final