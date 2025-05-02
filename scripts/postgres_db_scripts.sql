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


-- Create indexes for efficient querying
CREATE INDEX idx_covid_cases_date ON public.covid_cases(date);
CREATE INDEX idx_covid_cases_state ON public.covid_cases(state);
CREATE INDEX idx_covid_cases_county ON public.covid_cases(county);