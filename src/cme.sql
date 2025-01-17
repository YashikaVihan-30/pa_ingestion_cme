-- create continuing_medical_education (cme) table
CREATE TABLE public.continuing_medical_education(
    transaction_id varchar(255) NOT NULL,
    therapeutic_area varchar(255),
    sponsor varchar(255),
    cme_name text,
    topic text,
    "year" date,
    cme_url varchar,
    created_at timestamptz NOT null default CURRENT_TIMESTAMP,
    updated_at timestamptz NOT null default CURRENT_TIMESTAMP,
    CONSTRAINT unique_transaction_id PRIMARY KEY (transaction_id));