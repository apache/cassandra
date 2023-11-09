// Find cyclists that fit a particular category

SOURCE 'keyspace-create-simple.cql';
SOURCE 'keyspace-use.cql';

// CREATE TABLE CLUSTERING ORDER, PRIMARY KEY: PARTITION KEY + 1 CLUSTERING COLUMN, SIMPLE WHERE QUERY

DROP TABLE IF EXISTS cycling.cyclist_category;

// tag::use_and_cpartitionkey[]
USE cycling;
CREATE TABLE IF NOT EXISTS cyclist_category (
  category text,
  points int,
  id UUID,
  lastname text,
  PRIMARY KEY (category, points)
)
WITH CLUSTERING ORDER BY (points DESC);
// end::use_and_cpartitionkey[]

DROP TABLE IF EXISTS cycling.cyclist_category;

// tag::cpartitionkey[]
CREATE TABLE IF NOT EXISTS cycling.cyclist_category (
  category text,
  points int,
  id UUID,
  lastname text,
  PRIMARY KEY (category, points)
)
WITH CLUSTERING ORDER BY (points DESC);
// end::cpartitionkey[]

INSERT INTO cycling.cyclist_category (category, points, id, lastname) VALUES ('GC',1269,220844bf-4860-49d6-9a4b-6b5d3a79cbfb,'TIRALONGO');
INSERT INTO cycling.cyclist_category (category, points, id, lastname) VALUES ('One-day-races',367,220844bf-4860-49d6-9a4b-6b5d3a79cbfb,'TIRALONGO');
INSERT INTO cycling.cyclist_category (category, points, id, lastname) VALUES ('Time-trial',182,220844bf-4860-49d6-9a4b-6b5d3a79cbfb,'TIRALONGO');
INSERT INTO cycling.cyclist_category (category, points, id, lastname) VALUES ('Sprint',0,220844bf-4860-49d6-9a4b-6b5d3a79cbfb,'TIRALONGO');
INSERT INTO cycling.cyclist_category (category, points, id, lastname) VALUES ('GC',1324,6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47,'KRUIJSWIJK');
INSERT INTO cycling.cyclist_category (category, points, id, lastname) VALUES ('One-day-races',198,6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47,'KRUIJSWIJK');
INSERT INTO cycling.cyclist_category (category, points, id, lastname) VALUES ('Sprint',39,6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47,'KRUIJSWIJK');
INSERT INTO cycling.cyclist_category (category, points, id, lastname) VALUES ('Time-trial',3,6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47,'KRUIJSWIJK');

// tag::rows[]
RESTRICT ROWS ON cycling.cyclist_category USING category;
// end::rows[]

CAPTURE 'select_all_from_cyclist_category_order_by_descending_points.results';
// tag::select_compound_primary_key[]
SELECT *
FROM cycling.cyclist_category
WHERE category = 'Time-trial'
ORDER BY points DESC;
// end::select_compound_primary_key[]
CAPTURE OFF;

// Add JSON rows

// tag::insert_SUTHERLAND[]
INSERT INTO cycling.cyclist_category JSON '{
  "category" : "GC",
  "points" : 780,
  "id" : "829aa84a-4bba-411f-a4fb-38167a987cda",
  "lastname" : "SUTHERLAND"
}';
// end::insert_SUTHERLAND[]

// tag::clear_lastname[]
INSERT INTO cycling.cyclist_category JSON '{
  "category" : "Sprint",
  "points" : 780,
  "id" : "829aa84a-4bba-411f-a4fb-38167a987cda"
}';
// end::clear_lastname[]

// tag::unset[]
INSERT INTO cycling.cyclist_category JSON '{
  "category" : "Sprint",
  "points" : 780,
  "id" : "829aa84a-4bba-411f-a4fb-38167a987cda"
}'
DEFAULT UNSET;
// end::unset[]

// tag::set_points[]
INSERT INTO cycling.cyclist_category JSON '{
  "category" : "Sprint",
  "points" : 700,
  "id" : "829aa84a-4bba-411f-a4fb-38167a987cda"
}';
// end::set_points[]

CAPTURE 'select_all_from_cyclist_category.results';
// tag::select_all_rows[]
SELECT *
FROM cycling.cyclist_category;
// end::select_all_rows[]
CAPTURE OFF;

// tag::copy[]
COPY cycling.cyclist_category
FROM 'cyclist_category.csv'
WITH DELIMITER = '|'
  AND HEADER = TRUE;
// end::copy[]

// Truncate table
TRUNCATE TABLE cycling.cyclist_category;

// Re-insert data
INSERT INTO cycling.cyclist_category (category, points, id, lastname) VALUES ('GC',1269,220844bf-4860-49d6-9a4b-6b5d3a79cbfb,'TIRALONGO');
INSERT INTO cycling.cyclist_category (category, points, id, lastname) VALUES ('One-day-races',367,220844bf-4860-49d6-9a4b-6b5d3a79cbfb,'TIRALONGO');
INSERT INTO cycling.cyclist_category (category, points, id, lastname) VALUES ('Time-trial',182,220844bf-4860-49d6-9a4b-6b5d3a79cbfb,'TIRALONGO');
INSERT INTO cycling.cyclist_category (category, points, id, lastname) VALUES ('Sprint',0,220844bf-4860-49d6-9a4b-6b5d3a79cbfb,'TIRALONGO');
INSERT INTO cycling.cyclist_category (category, points, id, lastname) VALUES ('GC',1324,6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47,'KRUIJSWIJK');
INSERT INTO cycling.cyclist_category (category, points, id, lastname) VALUES ('One-day-races',198,6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47,'KRUIJSWIJK');
INSERT INTO cycling.cyclist_category (category, points, id, lastname) VALUES ('Sprint',39,6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47,'KRUIJSWIJK');
INSERT INTO cycling.cyclist_category (category, points, id, lastname) VALUES ('Time-trial',3,6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47,'KRUIJSWIJK');
