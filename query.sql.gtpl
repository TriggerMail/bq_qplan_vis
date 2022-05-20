SELECT project_id, job_id, total_slot_ms, job_stages
FROM `{{ sanitize .Project }}`.`{{ sanitize .Region }}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE job_id = '{{ sanitize .JobID }}' AND
creation_time BETWEEN
    TIMESTAMP('{{ dt_format .StartTime }}') AND
    TIMESTAMP('{{ dt_format .EndTime }}')
LIMIT 1