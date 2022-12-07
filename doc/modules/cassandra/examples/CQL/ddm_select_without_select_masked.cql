CREATE USER untrusted_user WITH PASSWORD 'xyz';
GRANT SELECT ON TABLE patients TO untrusted_user;
LOGIN untrusted_user
SELECT name, birth FROM patients WHERE name = 'Alice' ALLOW FILTERING;

// Unauthorized: Error from server: code=2100 [Unauthorized] message="User untrusted_user has no UNMASK nor SELECT_UNMASK permission on table k.patients"
