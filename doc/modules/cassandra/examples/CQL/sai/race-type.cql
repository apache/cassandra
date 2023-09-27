SOURCE '0_create_keyspace.cql';

// tag::drop[]
DROP TYPE IF EXISTS cycling.race;
// end::drop[]

// tag::racetype[]
CREATE TYPE IF NOT EXISTS cycling.race (
  race_title text,
  race_date timestamp,
  race_time text
);
// end::racetype[]
