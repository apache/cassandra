SOURCE '0_create_keyspace.cql';

DROP TABLE IF EXISTS cycling.cyclist_races;

// Find all races for a particular cyclist
// CREATE TYPE - User-Defined Type, race
// CREATE TABLE WITH LIST, SIMPLE PRIMARY KEY
SOURCE 'race-type.cql';

// tag::frozenlist[]
CREATE TABLE IF NOT EXISTS cycling.cyclist_races (
  id UUID PRIMARY KEY,
  lastname text,
  firstname text,
  races list<FROZEN <race>>
);
// end::frozenlist[]

// tag::insertFL[]
INSERT INTO cycling.cyclist_races (
  id,
  lastname,
  firstname,
  races
) VALUES (
  5b6962dd-3f90-4c93-8f61-eabfa4a803e2,
  'VOS',
  'Marianne',
  [ {
       race_title:'Rabobank 7-Dorpenomloop Aalburg',
       race_date:'2015-05-09',
       race_time:'02:58:33'},
     {
       race_title:'Ronde van Gelderland',
       race_date:'2015-04-19',
       race_time:'03:22:23'
     }
  ]
);
// end::insertFL[]

INSERT INTO cycling.cyclist_races (
  id, lastname, firstname, races
) VALUES (
  e7cd5752-bc0d-4157-a80f-7523add8dbcd, 'VAN DER BREGGEN', 'Anna', [ {race_title:'Festival Luxembourgeois du cyclisme feminin Elsy Jacobs - Prologue - Garnich > Garnich',race_date:'2015-05-01',race_time:'08:13:00'},{race_title:'Festival Luxembourgeois du cyclisme feminin Elsy Jacobs - Stage 2 - Garnich > Garnich',race_date:'2015-05-02',race_time:'02:41:52'},{race_title:'Festival Luxembourgeois du cyclisme feminin Elsy Jacobs - Stage 3 - Mamer > Mamer',race_date:'2015-05-03',race_time:'02:31:24'} ]
);

// tag::add[]
ALTER TABLE cycling.cyclist_races 
ADD manager UUID;
// end::add[]

// tag::list[]
ALTER TABLE cycling.cyclist_races 
ADD completed list<text>;
// end::list[]

// tag::drop[]
ALTER TABLE cycling.cyclist_races 
DROP manager;
// end::drop[]
