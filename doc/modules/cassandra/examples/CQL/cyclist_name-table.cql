SOURCE 'keyspace-create-simple.cql';
SOURCE 'keyspace-use.cql';

// tag::droptble[]
DROP TABLE IF EXISTS cycling.cyclist_name;
// end::droptble[]

// tag::pk_at_start[]
USE cycling;
CREATE TABLE cyclist_name (
  id UUID PRIMARY KEY,
  lastname text,
  firstname text
);
// end::pk_at_start[]

DROP TABLE IF EXISTS cycling.cyclist_name;

// tag::pk_at_end[]
USE cycling;
CREATE TABLE cycling.cyclist_name (
  id UUID,
  lastname text,
  firstname text,
  PRIMARY KEY (id)
);
// end::pk_at_end[]

DROP TABLE IF EXISTS cycling.cyclist_name;

// tag::keyspace_included[]
CREATE TABLE cycling.cyclist_name (
  id UUID PRIMARY KEY,
  lastname text,
  firstname text
);
// end::keyspace_included[]

DROP TABLE IF EXISTS cycling.cyclist_name;

// Create a table with a simple partition key
// tag::simple[]
CREATE TABLE IF NOT EXISTS cycling.cyclist_name (
  id UUID PRIMARY KEY,
  lastname text,
  firstname text
);
// end::simple[]

// tag::uuid[]
INSERT INTO cycling.cyclist_name (
  id
) VALUES (
  uuid()
);
// end::uuid[]

// tag::batch[]
BEGIN BATCH

  INSERT INTO cycling.cyclist_name (
    id, lastname, firstname
  ) VALUES (
    6d5f1663-89c0-45fc-8cfd-60a373b01622,'HOSKINS', 'Melissa'
  );

  INSERT INTO cycling.cyclist_name (
    id, lastname, firstname
  ) VALUES (
    38ab64b6-26cc-4de9-ab28-c257cf011659,'FERNANDES', 'Marcia'
  );

  INSERT INTO cycling.cyclist_name (
    id, lastname, firstname
  ) VALUES (
    9011d3be-d35c-4a8d-83f7-a3c543789ee7,'NIEWIADOMA', 'Katarzyna'
  );

  INSERT INTO cycling.cyclist_name (
    id, lastname, firstname
  ) VALUES (
    95addc4c-459e-4ed7-b4b5-472f19a67995,'ADRIAN', 'Vera'
  );

APPLY BATCH;
// end::batch[]

TRUNCATE cycling.cyclist_name;

// Insert data with TTL and timestamp
// tag::insertstampttl[]
INSERT INTO cycling.cyclist_name (
  id, lastname, firstname
) VALUES (
  6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47, 'KRUIKSWIJK', 'Steven'
)
USING TTL 86400
AND TIMESTAMP 123456789;
// end::insertstampttl[]

// tag::insertifnotexists[]
INSERT INTO cycling.cyclist_name (
  id, lastname, firstname
) VALUES (
  c4b65263-fe58-4846-83e8-f0e1c13d518f, 'RATTO', 'Rissella'
)
IF NOT EXISTS;
// end::insertifnotexists[]


DROP INDEX IF EXISTS fn_prefix;

// tag::fn_prefix[]
CREATE CUSTOM INDEX IF NOT EXISTS fn_prefix
ON cycling.cyclist_name (firstname)
USING 'org.apache.cassandra.index.sasi.SASIIndex';
// end::fn_prefix[]

TRUNCATE cycling.cyclist_name;

// Insert 6 rows of cyclists
INSERT INTO cycling.cyclist_name (id, lastname, firstname) VALUES (5b6962dd-3f90-4c93-8f61-eabfa4a803e2, 'VOS','Marianne');
INSERT INTO cycling.cyclist_name (id, lastname, firstname) VALUES (e7cd5752-bc0d-4157-a80f-7523add8dbcd, 'VAN DER BREGGEN','Anna');
INSERT INTO cycling.cyclist_name (id, lastname, firstname) VALUES (e7ae5cf3-d358-4d99-b900-85902fda9bb0, 'FRAME','Alex');
INSERT INTO cycling.cyclist_name (id, lastname, firstname) VALUES (220844bf-4860-49d6-9a4b-6b5d3a79cbfb, 'TIRALONGO','Paolo');
INSERT INTO cycling.cyclist_name (id, lastname, firstname) VALUES (6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47, 'KRUIKSWIJK','Steven');
INSERT INTO cycling.cyclist_name (id, lastname, firstname) VALUES (fb372533-eb95-4bb4-8685-6ef61e994caa, 'MATTHEWS', 'Michael');

// View output
CAPTURE 'select_all_from_cycling_initial.results';
// tag::select_initial[]
SELECT *
FROM cycling.cyclist_name;
// end::select_initial[]
CAPTURE OFF;

CAPTURE 'select_marianne_from_cyclist_name.results';
// tag::select_Marianne[]
SELECT *
FROM cycling.cyclist_name
WHERE firstname = 'Marianne';
// end::select_Marianne[]
CAPTURE OFF;

CAPTURE 'select_like_M_from_cyclist_name.results';
// tag::select_like_M_start[]
SELECT *
FROM cycling.cyclist_name
WHERE firstname LIKE 'M%';
// end::select_like_M_start[]
CAPTURE OFF;

// tag::no_match_upper[]
SELECT * FROM cycling.cyclist_name WHERE firstname = 'MARIANNE';
// end::no_match_upper[]

// tag::no_match_lower[]
SELECT * FROM cycling.cyclist_name WHERE firstname LIKE 'm%';
// end::no_match_lower[]

// tag::no_match_equality[]
SELECT * FROM cycling.cyclist_name WHERE firstname = 'M%';
SELECT * FROM cycling.cyclist_name WHERE firstname = '%M';
SELECT * FROM cycling.cyclist_name WHERE firstname = '%M%';
SELECT * FROM cycling.cyclist_name WHERE firstname = 'm%';
// end::no_match_equality[]

DROP INDEX IF EXISTS fn_contains;

// tag::fn_contains[]
CREATE CUSTOM INDEX IF NOT EXISTS fn_contains
ON cycling.cyclist_name (firstname)
USING 'org.apache.cassandra.index.sasi.SASIIndex'
WITH OPTIONS = { 'mode': 'CONTAINS' };
// end::fn_contains[]

// These examples generate an error if they are run before the fn_contains index
// is created
// tag::like[]
SELECT * FROM cycling.cyclist_name WHERE firstname LIKE '%m%';
SELECT * FROM cycling.cyclist_name WHERE firstname LIKE '%m%' ALLOW FILTERING;
SELECT * FROM cycling.cyclist_name WHERE firstname LIKE '%M%';
// end::like[]

CAPTURE 'select_marianne_allow_filtering_from_cyclist_name.results';
// tag::select_marianne_allow_filtering[]
SELECT *
FROM cycling.cyclist_name
WHERE firstname = 'Marianne'
ALLOW FILTERING;
// end::select_marianne_allow_filtering[]
CAPTURE OFF;

CAPTURE 'select_like_M_partial_from_cyclist_name.results';
// tag::select_like_M_partial[]
SELECT *
FROM cycling.cyclist_name
WHERE firstname LIKE '%M%';
// end::select_like_M_partial[]
CAPTURE OFF;

CAPTURE 'select_arianne_from_cyclist_name.results';
// tag::select_arianne[]
SELECT *
FROM cycling.cyclist_name
WHERE firstname LIKE '%arianne';
// end::select_arianne[]
CAPTURE OFF;

CAPTURE 'select_arian_from_cyclist_name.results';
// tag::select_arian_name[]
SELECT *
FROM cycling.cyclist_name
WHERE firstname LIKE '%arian%';
// end::select_arian_name[]
CAPTURE OFF;

CAPTURE 'select_mar_from_cyclist_name.results';
// tag::select_greater_Mar[]
SELECT *
FROM cycling.cyclist_name
WHERE firstname > 'Mar'
ALLOW FILTERING;
// end::select_greater_Mar[]
CAPTURE OFF;

// These examples do not return a match when they are run before the
// fn_suffix_allcase index is created
// tag::no_match_partial[]
SELECT * FROM cycling.cyclist_name WHERE firstname = 'MariAnne' ALLOW FILTERING;
SELECT * FROM cycling.cyclist_name WHERE firstname LIKE '%m%';
SELECT * FROM cycling.cyclist_name WHERE firstname LIKE '%M';
SELECT * FROM cycling.cyclist_name WHERE firstname LIKE 'm%';
// end::no_match_partial[]

DROP INDEX IF EXISTS fn_suffix_allcase;

// tag::fn_suffix_allcase[]
CREATE CUSTOM INDEX IF NOT EXISTS fn_suffix_allcase
ON cycling.cyclist_name (firstname)
USING 'org.apache.cassandra.index.sasi.SASIIndex'
WITH OPTIONS = {
  'mode': 'CONTAINS',
  'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer',
  'case_sensitive': 'false'
};
// end::fn_suffix_allcase[]

CAPTURE 'select_like_m_lower_from_cyclist_name.results';
// tag::select_like_m_lower[]
SELECT *
FROM cycling.cyclist_name
WHERE firstname LIKE '%m%';
// end::select_like_m_lower[]
CAPTURE OFF;

// tag::alter[]
ALTER TABLE cycling.cyclist_name
ADD age int;

UPDATE cycling.cyclist_name
SET age = 23
WHERE id = 5b6962dd-3f90-4c93-8f61-eabfa4a803e2;

INSERT INTO cycling.cyclist_name (
  id, age, firstname, lastname
) VALUES (
  8566eb59-07df-43b1-a21b-666a3c08c08a, 18, 'Marianne', 'DAAE'
);
// end::alter[]

CAPTURE 'select_name_and_age_filter_from_cyclist_name.results';
// tag::select_name_and_age_filter[]
SELECT *
FROM cycling.cyclist_name
WHERE firstname = 'Marianne'
  AND age > 20
ALLOW FILTERING;
// end::select_name_and_age_filter[]
CAPTURE OFF;

// Undo the changes in the alter section
DELETE FROM cycling.cyclist_name
WHERE id = 8566eb59-07df-43b1-a21b-666a3c08c08a;
ALTER TABLE cycling.cyclist_name
DROP age;

// Delete data in specified columns from a row, changes cyclist Alex Frame to null in name columns
// tag::deletecolumndata[]
DELETE firstname, lastname
FROM cycling.cyclist_name
WHERE id = e7ae5cf3-d358-4d99-b900-85902fda9bb0;
// end::deletecolumndata[]

// View output
CAPTURE 'cyclist_name_delete_firstname_and_lastname.results';
SELECT *
FROM cycling.cyclist_name;
CAPTURE OFF;

// Delete an entire row, deletes cyclist Alex Frame altogether
CAPTURE 'cyclist_name_delete_applied_true.results';
// tag::deleteentirerow[]
DELETE FROM cycling.cyclist_name
WHERE id = e7ae5cf3-d358-4d99-b900-85902fda9bb0
IF EXISTS;
// end::deleteentirerow[]
CAPTURE OFF;

// View output
CAPTURE 'cyclist_name_delete_entire_row.results';
SELECT *
FROM cycling.cyclist_name;
CAPTURE OFF;

// Delete a row based on a static column condition, attempts to delete cyclist Michael Matthews but lastname does not match
CAPTURE 'cyclist_name_delete_condition.results';
// tag::deletecondition[]
DELETE FROM cycling.cyclist_name
WHERE id = fb372533-eb95-4bb4-8685-6ef61e994caa
IF firstname = 'Michael'
  AND lastname = 'Smith';
// end::deletecondition[]
CAPTURE OFF;

// View output
SELECT *
FROM cycling.cyclist_name;

// Delete a row with a WHERE clause, deletes cyclist Steven Kruikswijk
// tag::deleterowhere[]
DELETE FROM cycling.cyclist_name
WHERE id = 6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47;
// end::deleterowhere[]

// View output
CAPTURE 'cyclist_name_delete_row_where.results';
SELECT *
FROM cycling.cyclist_name;
CAPTURE OFF;

// Delete rows using an IN clause on a primary key, deletes cyclists Marianne and Paolo
// tag::deletein[]
DELETE FROM cycling.cyclist_name
WHERE id IN (
  5b6962dd-3f90-4c93-8f61-eabfa4a803e2, 220844bf-4860-49d6-9a4b-6b5d3a79cbfb
);
// end::deletein[]

// View output
CAPTURE 'cyclist_name_delete_in.results';
SELECT *
FROM cycling.cyclist_name;
CAPTURE OFF;

// tag::delete_lastname[]
DELETE lastname
FROM cycling.cyclist_name
WHERE id = c7fceba0-c141-4207-9494-a29f9809de6f;
// end::delete_lastname[]

// Reinsert data
INSERT INTO cycling.cyclist_name (id, lastname, firstname) VALUES (5b6962dd-3f90-4c93-8f61-eabfa4a803e2, 'VOS','Marianne');
INSERT INTO cycling.cyclist_name (id, lastname, firstname) VALUES (e7cd5752-bc0d-4157-a80f-7523add8dbcd, 'VAN DER BREGGEN','Anna');
INSERT INTO cycling.cyclist_name (id, lastname, firstname) VALUES (e7ae5cf3-d358-4d99-b900-85902fda9bb0, 'FRAME','Alex');
INSERT INTO cycling.cyclist_name (id, lastname, firstname) VALUES (220844bf-4860-49d6-9a4b-6b5d3a79cbfb, 'TIRALONGO','Paolo');
INSERT INTO cycling.cyclist_name (id, lastname, firstname) VALUES (6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47, 'KRUIKSWIJK','Steven');
INSERT INTO cycling.cyclist_name (id, lastname, firstname) VALUES (fb372533-eb95-4bb4-8685-6ef61e994caa, 'MATTHEWS', 'Michael');

// tag::comment[]
ALTER TABLE cycling.cyclist_name
ADD comment text;
// end::comment[]

// tag::update_set_comment[]
UPDATE cycling.cyclist_name
SET comment = 'Rides hard, gets along with others, a real winner'
WHERE id = fb372533-eb95-4bb4-8685-6ef61e994caa;

UPDATE cycling.cyclist_name
SET comment = 'Rides fast, does not get along with others, a real dude'
WHERE id = 5b6962dd-3f90-4c93-8f61-eabfa4a803e2;
// end::update_set_comment[]

DROP INDEX IF EXISTS cycling.stdanalyzer_idx;

// tag::stdanalyzer_idx[]
CREATE CUSTOM INDEX IF NOT EXISTS stdanalyzer_idx
ON cycling.cyclist_name (comment) USING 'org.apache.cassandra.index.sasi.SASIIndex'
WITH OPTIONS = {
  'mode': 'CONTAINS',
  'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer',
  'analyzed': 'true',
  'tokenization_skip_stop_words': 'and, the, or',
  'tokenization_enable_stemming': 'true',
  'tokenization_normalize_lowercase': 'true',
  'tokenization_locale': 'en'
};
// end::stdanalyzer_idx[]

SOURCE 'cyclist_name-queries.cql';

// Undo UPDATE statements
UPDATE cycling.cyclist_name
SET comment = NULL
WHERE id = fb372533-eb95-4bb4-8685-6ef61e994caa;

UPDATE cycling.cyclist_name
SET comment = NULL
WHERE id = 5b6962dd-3f90-4c93-8f61-eabfa4a803e2;

// tag::update[]
UPDATE cycling.cyclist_name
SET comment = 'Rides hard, gets along with others, a real winner'
WHERE id = fb372533-eb95-4bb4-8685-6ef61e994caa
IF EXISTS;
// end::update[]

// tag::multcolumns[]
UPDATE cycling.cyclist_name
SET
  firstname = 'Marianne',
  lastname = 'VOS'
WHERE id = 88b8fd18-b1ed-4e96-bf79-4280797cba80;
// end::multcolumns[]

// tag::records[]
UPDATE cycling.cyclist_name
SET firstname = NULL
WHERE id IN (
  5b6962dd-3f90-4c93-8f61-eabfa4a803e2,
  fb372533-eb95-4bb4-8685-6ef61e994caa
);
// end::records[]

// tag::upsert[]
UPDATE cycling.cyclist_name
SET
  firstname = 'Anna',
  lastname = 'VAN DER BREGGEN'
WHERE id = e7cd5752-bc0d-4157-a80f-7523add8dbcd;
// end::upsert[]

// tag::if[]
UPDATE cycling.cyclist_name
SET comment = 'Rides hard, gets along with others, a real winner'
WHERE id = fb372533-eb95-4bb4-8685-6ef61e994caa
IF comment = NULL;
// end::if[]

// tag::fail[]
UPDATE cycling.cyclist_name
SET comment = 'Rides hard, gets along with others, a real winner'
WHERE id = fb372533-eb95-4bb4-8685-6ef61e994cac
IF comment = 'Rides hard, gets along with others, a real winner';
// end::fail[]

// tag::count_lastname[]
SELECT COUNT(lastname)
FROM cycling.cyclist_name;
// end::count_lastname[]

// tag::count_rows[]
SELECT COUNT(*)
FROM cycling.cyclist_name;
// end::count_rows[]

// tag::insert_Roxxane[]
INSERT INTO cycling.cyclist_name (
  id, lastname, firstname
) VALUES (
  4647f6d3-7bd2-4085-8d6c-1229351b5498, 'KNETEMANN', 'Roxxane'
)
IF NOT EXISTS;
// end::insert_Roxxane[]

// tag::update_Roxxane[]
UPDATE cycling.cyclist_name
SET firstname = 'Roxane'
WHERE id = 4647f6d3-7bd2-4085-8d6c-1229351b5498
IF firstname = 'Roxxane';
// end::update_Roxxane[]

DELETE FROM cycling.cyclist_name
WHERE id = 4647f6d3-7bd2-4085-8d6c-1229351b5498;

