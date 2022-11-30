DROP TABLE IF EXISTS loan;
CREATE TABLE loan
(
    id text,
    status character varying(255) COLLATE pg_catalog."default",
    contact_id character varying(255) COLLATE pg_catalog."default",
    created_at character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT loan_pkey PRIMARY KEY (id)
);