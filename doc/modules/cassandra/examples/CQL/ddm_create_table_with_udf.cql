CREATE FUNCTION redact(input text)
   CALLED ON NULL INPUT
   RETURNS text
   LANGUAGE java
   AS 'return "redacted";';

CREATE TABLE patients (
   id timeuuid PRIMARY KEY,
   name text MASKED WITH redact(),
   birth date
);
