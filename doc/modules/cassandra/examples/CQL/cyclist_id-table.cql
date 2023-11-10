SOURCE 'keyspace-create-simple.cql';
SOURCE 'keyspace-use.cql';

DROP TABLE IF EXISTS cycling.cyclist_id;

// Find a cyclist's id given lastname and firstname
// Another CREATE TABLE using COMPOSITE PARTITION KEY
// INDEX ALSO GOOD FOR THIS TABLE

// tag::comp_pk[]
CREATE TABLE IF NOT EXISTS cycling.cyclist_id (
  lastname text,
  firstname text,
  age int,
  id UUID,
  PRIMARY KEY ((lastname, firstname), age)
);
// end::comp_pk[]

// tag::altercdc[]
ALTER TABLE cycling.cyclist_id
WITH CDC = false;
// end::altercdc[]

INSERT INTO cycling.cyclist_id (
  lastname, firstname, age, id
) VALUES (
  'EENKHOORN', 'Pascal', 18, ffdfa2a7-5fc6-49a7-bfdc-3fcdcfdd7156
);

INSERT INTO cycling.cyclist_id (
  lastname, firstname, age, id
) VALUES (
  'WELTEN', 'Bram', 18, 18f471bf-f631-4bc4-a9a2-d6f6cf5ea503
);

INSERT INTO cycling.cyclist_id (
  lastname, firstname, age, id
) VALUES (
  'COSTA', 'Adrien', 17, 15a116fc-b833-4da6-ab9a-4a7775752836
);

// tag::resetid[]
UPDATE cycling.cyclist_id
SET id = UUID()
WHERE lastname = 'WELTEN'
  AND firstname = 'Bram'
  AND age = 18
IF EXISTS;
// end::resetid[]

// tag::ifid[]
UPDATE cycling.cyclist_id
SET id = UUID()
WHERE lastname = 'WELTEN'
  AND firstname = 'Bram'
  AND age = 18
IF id = 18f471bf-f631-4bc4-a9a2-d6f6cf5ea503;
// end::ifid[]

// tag::ifnull[]
UPDATE cycling.cyclist_id
SET id = UUID()
WHERE lastname = 'Smith'
  AND firstname = 'Joe'
  AND age = 22
IF id = NULL;
// end::ifnull[]

// tag::select_with_in_and_equals[]
SELECT *
FROM cycling.cyclist_id
WHERE lastname = 'EENKHOORN'
  AND firstname = 'Pascal'
  AND age IN (17, 18);
// end::select_with_in_and_equals[]

