SOURCE '0_create_keyspace.cql';

DROP TABLE IF EXISTS cycling.rank_by_year_and_name;

// Store race information by year and race name using a COMPOSITE PARTITION KEY

// tag::use_and_compositepk[]
USE cycling;
CREATE TABLE rank_by_year_and_name ( 
  race_year int, 
  race_name text, 
  cyclist_name text, 
  rank int, 
  PRIMARY KEY ((race_year, race_name), rank) 
);
// end::use_and_compositepk[]

DROP TABLE IF EXISTS cycling.rank_by_year_and_name;

// tag::compositepk[]
CREATE TABLE IF NOT EXISTS cycling.rank_by_year_and_name (
  race_year int,
  race_name text,
  cyclist_name text,
  rank int,
  PRIMARY KEY ((race_year, race_name), rank)
);
// end::compositepk[]

// tag::dropindex[]
DROP INDEX IF EXISTS cycling.rank_idx;
// end::dropindex[]

// tag::createindex[]
CREATE INDEX IF NOT EXISTS rank_idx
ON cycling.rank_by_year_and_name (rank);
// end::createindex[]

// tag::drop_race_year_idx[]
DROP INDEX IF EXISTS cycling.race_year_idx;
// end::drop_race_year_idx[]

// tag::create_race_year_idx[]
CREATE INDEX IF NOT EXISTS race_year_idx ON
cycling.rank_by_year_and_name (race_year);
// end::create_race_year_idx[]

INSERT INTO cycling.rank_by_year_and_name (race_year, race_name, cyclist_name, rank) VALUES (2015, 'Tour of Japan - Stage 4 - Minami > Shinshu', 'Benjamin PRADES', 1);
INSERT INTO cycling.rank_by_year_and_name (race_year, race_name, cyclist_name, rank) VALUES (2015, 'Tour of Japan - Stage 4 - Minami > Shinshu', 'Adam PHELAN', 2);
INSERT INTO cycling.rank_by_year_and_name (race_year, race_name, cyclist_name, rank) VALUES (2015, 'Tour of Japan - Stage 4 - Minami > Shinshu', 'Thomas LEBAS', 3);
INSERT INTO cycling.rank_by_year_and_name (race_year, race_name, cyclist_name, rank) VALUES (2014, 'Tour of Japan - Stage 4 - Minami > Shinshu', 'Benjamin PRADES', 3);
INSERT INTO cycling.rank_by_year_and_name (race_year, race_name, cyclist_name, rank) VALUES (2014, 'Tour of Japan - Stage 4 - Minami > Shinshu', 'Daniel MARTIN', 1);
INSERT INTO cycling.rank_by_year_and_name (race_year, race_name, cyclist_name, rank) VALUES (2014, 'Tour of Japan - Stage 4 - Minami > Shinshu', 'Johan Esteban CHAVES', 2);
INSERT INTO cycling.rank_by_year_and_name (race_year, race_name, cyclist_name, rank) VALUES (2015, 'Giro d''Italia - Stage 11 - Forli > Imola', 'Ilnur ZAKARIN', 1);
INSERT INTO cycling.rank_by_year_and_name (race_year, race_name, cyclist_name, rank) VALUES (2015, 'Giro d''Italia - Stage 11 - Forli > Imola', 'Carlos BETANCUR', 2);
INSERT INTO cycling.rank_by_year_and_name (race_year, race_name, cyclist_name, rank) VALUES (2014, '4th Tour of Beijing', 'Phillippe GILBERT', 1);
INSERT INTO cycling.rank_by_year_and_name (race_year, race_name, cyclist_name, rank) VALUES (2014, '4th Tour of Beijing', 'Daniel MARTIN', 2);
INSERT INTO cycling.rank_by_year_and_name (race_year, race_name, cyclist_name, rank) VALUES (2014, '4th Tour of Beijing', 'Johan Esteban CHAVES', 3);
INSERT INTO cycling.rank_by_year_and_name (race_year, race_name, cyclist_name, rank) VALUES (2014, '4th Tour of Beijing', 'Phillippe GILBERT', 1);
INSERT INTO cycling.rank_by_year_and_name (race_year, race_name, cyclist_name, rank) VALUES (2014, '4th Tour of Beijing', 'Daniel MARTIN', 2);
INSERT INTO cycling.rank_by_year_and_name (race_year, race_name, cyclist_name, rank) VALUES (2014, '4th Tour of Beijing', 'Johan Esteban CHAVES', 3);

// Show all inserted data
CAPTURE 'select_all_from_rank_by_year_and_name.result';
SELECT *
FROM cycling.rank_by_year_and_name;
CAPTURE OFF;

CAPTURE 'select_all_from_rank_by_year_and_name_Japan_2014.result';
// tag::select_with_name_and_year[]
SELECT *
FROM cycling.rank_by_year_and_name
WHERE race_year = 2014
  AND race_name  = 'Tour of Japan - Stage 4 - Minami > Shinshu';
// end::select_with_name_and_year[]
CAPTURE OFF;

// tag::sepstatementswithand[]
SELECT
  rank,
  cyclist_name AS name
FROM cycling.rank_by_year_and_name
WHERE "race_name"  = 'Tour of Japan - Stage 4 - Minami > Shinshu' 
  AND race_year = 2014;
// end::sepstatementswithand[]

// tag::columnalias[]
CAPTURE 'select_best_rank_from_rank_by_year_and_name.result';
SELECT
  MIN(rank) AS best_rank,
  cyclist_name
FROM cycling.rank_by_year_and_name
WHERE "race_name" = 'Tour of Japan - Stage 4 - Minami > Shinshu' 
  AND race_year = 2014;
// end::columnalias[]
CAPTURE OFF;

// The following query generates a warning, which is normal
// tag::countrows[]
SELECT COUNT(*)
FROM cycling.rank_by_year_and_name;
// end::countrows[]

// tag::limitrows[]
SELECT cyclist_name
FROM cycling.rank_by_year_and_name
LIMIT 50000;
// end::limitrows[]

// Query by partition
CAPTURE 'select_all_from_rank_by_year_and_name_partition_limit.result';
// tag::partlimit[]
SELECT * 
FROM cycling.rank_by_year_and_name 
PER PARTITION LIMIT 2;
// end::partlimit[]
CAPTURE OFF;

// Select rank - filter on a clustering column 
CAPTURE 'select_all_from_rank_by_year_and_name_rank_1.result';
// tag::selectrank[]
SELECT *
FROM cycling.rank_by_year_and_name
WHERE rank = 1;
// end::selectrank[]
CAPTURE OFF;

CAPTURE 'select_all_from_rank_by_year_and_name_2014.result';
// tag::select_with_year[]
SELECT *
FROM cycling.rank_by_year_and_name
WHERE race_year = 2014;
// end::select_with_year[]
CAPTURE OFF;
