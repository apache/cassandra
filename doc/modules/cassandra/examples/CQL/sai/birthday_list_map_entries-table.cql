SOURCE '0_create_keyspace.cql';

// tag::drop[]
DROP TABLE IF EXISTS cycling.birthday_list;
// end::drop[]

DROP INDEX IF EXISTS cycling.blist_idx;
DROP INDEX IF EXISTS cycling.blist_values_idx;

/* Map entries and regular index map fields example */

// tag::blisttable[]
CREATE TABLE IF NOT EXISTS cycling.birthday_list (
  cyclist_name text PRIMARY KEY, 
  blist map<text, text>
);
// end::blisttable[]

// Create index on map ENTRIES
// tag::entriesidx[]
CREATE INDEX IF NOT EXISTS blist_idx
ON cycling.birthday_list ( ENTRIES(blist) );
// end::entriesidx[]

// Create index on map VALUES
// tag::mapvaluesidx[]
CREATE INDEX IF NOT EXISTS blist_values_idx
ON cycling.birthday_list ( VALUES(blist) );
// end::mapvaluesidx[]

// tag::insertentries[]
INSERT INTO cycling.birthday_list (
  cyclist_name, blist
) VALUES (
  'Allan DAVIS', { 'age':'35', 'bday':'27/07/1980', 'nation':'AUSTRALIA' }
);

INSERT INTO cycling.birthday_list (
  cyclist_name, blist
) VALUES (
  'Claudio VANDELLI', { 'age':'54', 'bday':'27/07/1961', 'nation':'ITALY' }
);

INSERT INTO cycling.birthday_list (
  cyclist_name, blist
) VALUES (
  'Laurence BOURQUE', { 'age':'23', 'bday':'27/07/1992', 'nation':'CANADA' }
);

INSERT INTO cycling.birthday_list (
  cyclist_name, blist
) VALUES (
  'Claudio HEINEN', { 'age':'23', 'bday':'27/07/1992', 'nation':'GERMANY' }
);

INSERT INTO cycling.birthday_list (
  cyclist_name, blist
) VALUES (
  'Luc HAGENAARS', { 'age':'28', 'bday':'27/07/1987', 'nation':'NETHERLANDS' }
);

INSERT INTO cycling.birthday_list (
  cyclist_name, blist
) VALUES (
  'Toine POELS', { 'age':'52', 'bday':'27/07/1963', 'nation':'NETHERLANDS' }
);
// end::insertentries[]

// Query entries - find cyclist same age
CAPTURE 'select_from_birthday_list_where_age_23.result';
// tag::ageentryquery[]
SELECT *
FROM cycling.birthday_list
WHERE blist[ 'age' ] = '23';
// end::ageentryquery[]
CAPTURE OFF;

// Query entries - find cyclist same nation map entry
CAPTURE 'select_from_birthday_list_where_nation_netherlands.result';
// tag::nationentryquery[]
SELECT *
FROM cycling.birthday_list
WHERE blist[ 'nation' ] = 'NETHERLANDS';
// end::nationentryquery[]
CAPTURE OFF;

// Query entries - find cyclist same nation with map VALUES CONTAINS
CAPTURE 'select_from_birthday_list_where_nation_netherlands_2.result';
// tag::nationvaluesquery[]
SELECT *
FROM cycling.birthday_list
WHERE blist CONTAINS 'NETHERLANDS';
// end::nationvaluesquery[]
CAPTURE OFF;

// tag::dropentriesindex[]
DROP INDEX IF EXISTS cycling.blist_idx;
// end::dropentriesindex[]
