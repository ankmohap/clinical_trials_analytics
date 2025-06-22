with raw as (

  select
     load_time_utc,
    raw_json:protocolSection:identificationModule:nctId::string            as nct_id,
    raw_json:protocolSection:identificationModule:briefTitle::string       as brief_title,
    raw_json:protocolSection:identificationModule:officialTitle::string    as official_title,
    raw_json:protocolSection:sponsorCollaboratorsModule:leadSponsor:name::string as sponsor_name,
    raw_json:protocolSection:sponsorCollaboratorsModule:leadSponsor:class::string as sponsor_class,
    raw_json:protocolSection:statusModule:overallStatus::string            as status,
    raw_json:protocolSection:statusModule:studyFirstSubmitDate::date       as first_submit_date,
    raw_json:protocolSection:designModule:phases[0]::string                as phase_1,
    raw_json:protocolSection:designModule:phases[1]::string                as phase_2,
    raw_json:protocolSection:designModule:studyType::string                as study_type

from {{ source('ctgov', 'raw_ctgov__studies') }},
       lateral flatten(input => raw_json) study

)
select * from raw

