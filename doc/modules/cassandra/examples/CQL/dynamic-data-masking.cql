// tag::table-create-ddm[]
CREATE TABLE patients (
    id timeuuid PRIMARY KEY,
    name text MASKED WITH mask_inner(1, null),
    birth date MASKED WITH mask_default()
 );
// end::table-create-ddm[]

// tag::table-create-ddm-udt[]
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
// end::table-create-ddm-udt[]

// tag::table-alter-ddm[]
ALTER TABLE patients ALTER name 
  MASKED WITH mask_default();
// end::table-alter-ddm[]

// tag::table-alter-ddm-drop-mask[]
ALTER TABLE patients ALTER name 
  DROP MASKED;
// end::table-alter-ddm-drop-mask[]

// tag::users-grant-ddm[]
CREATE USER privileged WITH PASSWORD 'xyz';
GRANT SELECT ON TABLE patients TO privileged;
GRANT UNMASK ON TABLE patients TO privileged;

CREATE USER unprivileged WITH PASSWORD 'xyz';
GRANT SELECT ON TABLE patients TO unprivileged;
// end::users-grant-ddm[]

// tag::users-revoke-ddm[]
REVOKE UNMASK ON TABLE patients 
  FROM privileged;
// end::users-revoke-ddm[]

// tag::data-insert-ddm[]
INSERT INTO patients(id, name, birth) VALUES (now(), 'alice', '1984-01-02');
INSERT INTO patients(id, name, birth) VALUES (now(), 'bob', '1982-02-03');
// end::data-insert-ddm[]

// tag::select-with-masked-column[]
SELECT name, birth FROM patients;

//  name  | birth
// -------+------------
//  a**** | 1970-01-01
//    b** | 1970-01-01
// end::select-with-masked-column[]

// tag::select-without-unmask-permission[]
LOGIN unprivileged
SELECT name, birth FROM patients;

//  name  | birth
// -------+------------
//  a**** | 1970-01-01
//    b** | 1970-01-01
// end::select-without-unmask-permission[]

// tag::select-with-unmask-permission[]
LOGIN privileged
SELECT name, birth FROM patients;

//  name  | birth
// -------+------------
//  alice | 1984-01-02
//    bob | 1982-02-03
// end::select-with-unmask-permission[]

// tag::select-without-select-masked[]
CREATE USER untrusted_user WITH PASSWORD 'xyz';
GRANT SELECT ON TABLE patients TO untrusted_user;
LOGIN untrusted_user
SELECT name, birth FROM patients WHERE name = 'Alice' ALLOW FILTERING;

// Unauthorized: Error from server: code=2100 [Unauthorized] message="User untrusted_user has no UNMASK nor SELECT_UNMASK permission on table k.patients"
// end::select-without-select-masked[]

// tag::select-with-select-masked[]
CREATE USER trusted_user WITH PASSWORD 'xyz';
GRANT SELECT, SELECT_MASKED ON TABLE patients TO trusted_user;
LOGIN trusted_user
SELECT name, birth FROM patients WHERE name = 'Alice' ALLOW FILTERING;

//  name  | birth
// -------+------------
//  a**** | 1970-01-01
// end::select-with-select-masked[]