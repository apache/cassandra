SOURCE '0_create_keyspace.cql';

DROP TABLE IF EXISTS cycling.cyclist_teams;

// Create a table with a map
// tag::mapColumn[]
CREATE TABLE IF NOT EXISTS cycling.cyclist_teams (
  id uuid PRIMARY KEY,
  firstname text,
  lastname text,
  teams map<int, text>
);
// end::mapColumn[]

DROP INDEX IF EXISTS cycling.team_year_idx;

// Create an index on a map key to find all cyclist/team combos for a year 
// tag::keysidx[]
CREATE INDEX IF NOT EXISTS team_year_keys_idx
ON cycling.cyclist_teams ( KEYS (teams) );
// end::keysidx[]

// Create an index on a map key to find all cyclist/team combos for a year 
// tag::valuesidx[]
CREATE INDEX IF NOT EXISTS team_year_values_idx
ON cycling.cyclist_teams ( VALUES (teams) );
// end::valuesidx[]

// Create an index on a map key to find all cyclist/team combos for a year 
// tag::entriesidx[]
CREATE INDEX IF NOT EXISTS team_year_entries_idx
ON cycling.cyclist_teams ( ENTRIES (teams) );
// end::entriesidx[]

// Insert team data into map for cyclist Vos
// tag::insertmapdata[]
INSERT INTO cycling.cyclist_teams (
  id, firstname, lastname, teams
) VALUES (
  5b6962dd-3f90-4c93-8f61-eabfa4a803e2, 
  'Marianne',
  'VOS', 
  {
    2015 : 'Rabobank-Liv Woman Cycling Team', 
    2014 : 'Rabobank-Liv Woman Cycling Team'
  }
);
// end::insertmapdata[]
 
// View data
CAPTURE 'select_all_from_cyclist_teams.result';
// tag::select[]
SELECT *
FROM cycling.cyclist_teams;
// end::select[]
CAPTURE OFF;

// Delete an element from the map
// tag::deletemapdata[]
DELETE teams[2014]
FROM cycling.cyclist_teams
WHERE id = 5b6962dd-3f90-4c93-8f61-eabfa4a803e2;
// end::deletemapdata[]

// View data again, 2014 team gone
SELECT *
FROM cycling.cyclist_teams;

// Insert more cyclists
INSERT INTO cycling.cyclist_teams (
  id,firstname,lastname,teams
) VALUES (
  cb07baad-eac8-4f65-b28a-bddc06a0de23,
  'Elizabeth',
  'ARMITSTEAD',
  {
    2015 : 'Boels:Dolmans Cycling Team',
    2014 : 'Boels:Dolmans Cycling Team',
    2013 : 'Boels:Dolmans Cycling Team',
    2012 : 'AA Drink - Leontien.nl',
    2011 : 'Team Garmin - Cervelo'
  }
);

// View data
SELECT *
FROM cycling.cyclist_teams;

// Query for KEY year 2015
CAPTURE 'cyclist_team-queries.result';
// tag::queryindexkey[]
SELECT *
FROM cycling.cyclist_teams
WHERE teams CONTAINS KEY 2015;
// end::queryindexkey[]
CAPTURE OFF;

// tag::update_DSB[]
UPDATE cycling.cyclist_teams
SET teams = teams + { 2009 : 'DSB Bank - Nederland bloeit' }
WHERE id = 5b6962dd-3f90-4c93-8f61-eabfa4a803e2;
// end::update_DSB[]

// tag::update_Ballast[]
UPDATE cycling.cyclist_teams
SET teams[2006] = 'Team DSB - Ballast Nedam'
WHERE id = 5b6962dd-3f90-4c93-8f61-eabfa4a803e2;
// end::update_Ballast[]

// tag::delete_teams[]
DELETE teams[2009]
FROM cycling.cyclist_teams
WHERE id=e7cd5752-bc0d-4157-a80f-7523add8dbcd;
// end::delete_teams[]

// tag::update_set[]
UPDATE cycling.cyclist_teams
SET teams = teams - { 2013, 2014 }
WHERE id = e7cd5752-bc0d-4157-a80f-7523add8dbcd;
// end::update_set[]
