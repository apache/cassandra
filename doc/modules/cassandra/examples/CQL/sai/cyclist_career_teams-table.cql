SOURCE '0_create_keyspace.cql';

DROP TABLE IF EXISTS cycling.cyclist_career_teams;

// Find all teams that a cyclist has been a member of
// CREATE TABLE WITH SET
// tag::setColumn[]
CREATE TABLE IF NOT EXISTS cycling.cyclist_career_teams (
  id UUID PRIMARY KEY,
  lastname text,
  teams set<text>
);
// end::setColumn[]

DROP INDEX IF EXISTS cycling.teams_idx;

// Create an index on the set collection
// tag::createidxset[]
CREATE INDEX IF NOT EXISTS teams_idx
ON cycling.cyclist_career_teams (teams);
// end::createidxset[]

// tag::insertdatasetvos[]
INSERT INTO cycling.cyclist_career_teams (
  id, lastname, teams
 ) VALUES (
  5b6962dd-3f90-4c93-8f61-eabfa4a803e2, 
  'VOS', 
  {
    'Rabobank-Liv Woman Cycling Team',
    'Rabobank-Liv Giant',
    'Rabobank Women Team',
    'Nederland bloeit'
  }
);
// end::insertdatasetvos[]

INSERT INTO cycling.cyclist_career_teams (id,lastname,teams) VALUES (e7cd5752-bc0d-4157-a80f-7523add8dbcd, 'VAN DER BREGGEN', { 'Rabobank-Liv Woman Cycling Team','Sengers Ladies Cycling Team','Team Flexpoint' } );
INSERT INTO cycling.cyclist_career_teams (id,lastname,teams) VALUES (cb07baad-eac8-4f65-b28a-bddc06a0de23, 'ARMITSTEAD', { 'Boels-Dolmans Cycling Team','AA Drink - Leontien.nl','Team Garmin - Cervelo' } );
INSERT INTO cycling.cyclist_career_teams (id,lastname,teams) VALUES (1c9ebc13-1eab-4ad5-be87-dce433216d40, 'BRAND', { 'Rabobank-Liv Woman Cycling Team','Rabobank-Liv Giant','AA Drink - Leontien.nl','Leontien.nl' } );

CAPTURE 'select_all_from_cyclist_career_teams.result';
// tag::select[]
SELECT *
FROM cycling.cyclist_career_teams;
// end::select[]
CAPTURE OFF;

// tag::add[]
UPDATE cycling.cyclist_career_teams
SET teams = teams + {'Team DSB - Ballast Nedam'} 
WHERE id = 5b6962dd-3f90-4c93-8f61-eabfa4a803e2;
// end::add[]

// tag::remove[]
UPDATE cycling.cyclist_career_teams
SET teams = teams - {'DSB Bank Nederland bloeit'} 
WHERE id = 5b6962dd-3f90-4c93-8f61-eabfa4a803e2;
// end::remove[]

// tag::clear[]
UPDATE cycling.cyclist_career_teams
SET teams = {} 
WHERE id = 5b6962dd-3f90-4c93-8f61-eabfa4a803e2;

DELETE teams
FROM cycling.cyclist_career_teams
WHERE id = 5b6962dd-3f90-4c93-8f61-eabfa4a803e2;
// end::clear[]

CAPTURE 'select_from_cyclist_career_teams_empty.result';
// tag::selectwithpartitionkey[]
SELECT id, lastname, teams 
FROM cycling.cyclist_career_teams 
WHERE id = 5b6962dd-3f90-4c93-8f61-eabfa4a803e2;
// end::selectwithpartitionkey[]
CAPTURE OFF;

CAPTURE 'select_all_from_cyclist_career_teams_contains_value.result';
// tag::select_with_contains_value[]
SELECT *
FROM cycling.cyclist_career_teams
WHERE teams CONTAINS 'Rabobank-Liv Giant';
// end::select_with_contains_value[]
CAPTURE OFF;

// tag::select_with_contains_key[]
SELECT *
FROM cycling.cyclist_career_teams
WHERE teams CONTAINS 'Rabobank-Liv Giant';
// end::select_with_contains_key[]
