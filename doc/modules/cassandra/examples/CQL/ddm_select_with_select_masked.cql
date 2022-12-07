CREATE USER trusted_user WITH PASSWORD 'xyz';
GRANT SELECT, SELECT_MASKED ON TABLE patients TO trusted_user;
LOGIN trusted_user
SELECT name, birth FROM patients WHERE name = 'Alice' ALLOW FILTERING;

//  name  | birth
// -------+------------
//  a**** | 1970-01-01
