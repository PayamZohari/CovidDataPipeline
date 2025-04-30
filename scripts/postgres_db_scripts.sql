CREATE TABLE IF NOT EXISTS public.covid_cases
(
    date date,
    state text COLLATE pg_catalog."default",
    county text COLLATE pg_catalog."default",
    new_cases integer,
    new_deaths integer
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.covid_cases
    OWNER to airflow;