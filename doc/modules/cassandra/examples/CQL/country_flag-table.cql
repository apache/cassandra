// CREATE TABLE WITH STATIC COLUMN, example uses an integer to identify flag, but it could be a blob
SOURCE 'keyspace-create-simple.cql';
SOURCE 'keyspace-use.cql';

DROP TABLE IF EXISTS cycling.country_flag;

// tag::staticColumn[]
CREATE TABLE IF NOT EXISTS cycling.country_flag (
  country text,
  cyclist_name text,
  flag int STATIC,
  PRIMARY KEY (country, cyclist_name)
);
// end::staticColumn[]

// tag::insert[]
INSERT INTO cycling.country_flag (
  country, cyclist_name, flag
) VALUES (
  'Belgium', 'Jacques', 1
);

INSERT INTO cycling.country_flag (
  country, cyclist_name
) VALUES (
  'Belgium', 'Andre'
);

INSERT INTO cycling.country_flag (
  country, cyclist_name, flag
) VALUES (
  'France', 'Andre', 2
);

INSERT INTO cycling.country_flag (
  country, cyclist_name, flag
) VALUES (
  'France', 'George', 3
);
// end::insert[]

CAPTURE 'select_initial_from_country_flag.results';
// Query to view inserted data
// tag::select[]
SELECT *
FROM cycling.country_flag;
// end::select[]
CAPTURE OFF;

// Truncate data without dropping the table
// tag::truncate[]
TRUNCATE cycling.country_flag;
// end::truncate[]

// Query again to view data truncated from table
// tag::emptytable[]
SELECT *
FROM cycling.country_flag;
// end::emptytable[]

// Insert data again
INSERT INTO cycling.country_flag (
  country, cyclist_name, flag
) VALUES (
  'Belgium', 'Jacques', 1
);

INSERT INTO cycling.country_flag (
  country, cyclist_name
) VALUES (
  'Belgium', 'Andre'
);

INSERT INTO cycling.country_flag (
  country, cyclist_name, flag
) VALUES (
  'France', 'Andre', 2
);

INSERT INTO cycling.country_flag (
  country, cyclist_name, flag
) VALUES (
  'France', 'George', 3
);