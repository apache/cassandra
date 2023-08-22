SOURCE '0_create_keyspace.cql';

DROP TABLE IF EXISTS cycling.cyclist_semi_pro;

// tag::createCyclistSemiProTable[]
CREATE TABLE IF NOT EXISTS cycling.cyclist_semi_pro (
  id int, 
  firstname text, 
  lastname text, 
  age int, 
  affiliation text,
  country text,
  registration date,
  PRIMARY KEY (id));
// end::createCyclistSemiProTable[]
