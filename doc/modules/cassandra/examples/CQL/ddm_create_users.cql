CREATE USER privileged WITH PASSWORD 'xyz';
GRANT SELECT ON TABLE patients TO privileged;
GRANT UNMASK ON TABLE patients TO privileged;

CREATE USER unprivileged WITH PASSWORD 'xyz';
GRANT SELECT ON TABLE patients TO unprivileged;
