SOURCE 'keyspace-create-simple.cql';
SOURCE 'keyspace-use.cql';

DROP TABLE IF EXISTS cycling.race_times;

// tag::TABLE[]
CREATE TABLE IF NOT EXISTS cycling.race_times (
  race_name text,
  race_time time,
  finish_time duration,
  race_date date,
  cyclist_name text,
  PRIMARY KEY (race_date, race_name, cyclist_name)
);
// end::TABLE[]

// tag::DATA[]
INSERT INTO cycling.race_times (
  race_name, cyclist_name, race_time, race_date
) VALUES (
  '17th Santos Tour Down Under', 'Rohan DENNIS', '19:15:18', '2017-04-14'
);

INSERT INTO cycling.race_times (
  race_name, cyclist_name, race_time, race_date
) VALUES (
  '17th Santos Tour Down Under', 'Richie PORTE', '19:15:20', '2017-04-14'
);

INSERT INTO cycling.race_times (
  race_name, cyclist_name, race_time, race_date
) VALUES (
  '17th Santos Tour Down Under', 'Cadel EVANS', '19:15:38', '2017-04-14'
);

INSERT INTO cycling.race_times (
  race_name, cyclist_name, race_time, race_date
) VALUES (
  '17th Santos Tour Down Under', 'Tom DUMOULIN', '19:15:40', '2017-04-14'
);
// end::DATA[]

// tag::rename[]
ALTER TABLE cycling.race_times 
RENAME race_date TO date;
// end::rename[]

ALTER TABLE cycling.race_times 
RENAME date TO race_date;
