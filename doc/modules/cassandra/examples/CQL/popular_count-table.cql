SOURCE 'keyspace-create-simple.cql';
SOURCE 'keyspace-use.cql';

DROP TABLE IF EXISTS cycling.popular_count;

// tag::create[]
CREATE TABLE IF NOT EXISTS cycling.popular_count (
  id UUID PRIMARY KEY,
  popularity counter
);
// end::create[]

// tag::batch[]
BEGIN COUNTER BATCH

  UPDATE cycling.popular_count
  SET popularity = popularity + 1
  WHERE id = 6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47;

  UPDATE cycling.popular_count
  SET popularity = popularity + 125
  WHERE id = 6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47;

  UPDATE cycling.popular_count
  SET popularity = popularity - 64
  WHERE id = 6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47;

APPLY BATCH;
// end::batch[]

// tag::count[]
UPDATE cycling.popular_count
SET popularity = popularity + 2
WHERE id = 6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47;
// end::count[]

CAPTURE 'select_all_from_popular_count.results';
// tag::select[]
SELECT *
FROM cycling.popular_count;
// end::select[]
CAPTURE OFF;