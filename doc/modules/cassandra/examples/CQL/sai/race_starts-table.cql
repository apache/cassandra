SOURCE '0_create_keyspace.cql';

DROP TABLE IF EXISTS cycling.race_starts;

// create a table with a frozen list of ints
// tag::frozenlist[]
CREATE TABLE IF NOT EXISTS cycling.race_starts (
  cyclist_name text PRIMARY KEY,
  rnumbers FROZEN<LIST<int>>
);
// end::frozenlist[]
    
// View created table
DESCRIBE TABLE cycling.race_starts;

DROP INDEX IF EXISTS cycling.rnumbers_idx;

// Create index
// tag::fullindex[]
CREATE INDEX IF NOT EXISTS rnumbers_idx
ON cycling.race_starts ( FULL(rnumbers) );
// end::fullindex[]

// Insert data
INSERT INTO cycling.race_starts (
  cyclist_name, rnumbers
) VALUES (
  'John DEGENKOLB', [39, 7, 14]
);

// Select data with WHERE clause
CAPTURE 'race_starts-queries.result';
// tag::selectrnumbers[]
SELECT *
FROM cycling.race_starts
WHERE rnumbers = [39, 7, 14];
// end::selectrnumbers[]
CAPTURE OFF;
