SOURCE 'keyspace-create-simple.cql';
SOURCE 'keyspace-use.cql';

DROP TABLE IF EXISTS cycling.race_winners;

SOURCE 'user-defined-type-create-fullname.cql';

// tag::usetype[]
CREATE TABLE IF NOT EXISTS cycling.race_winners (
  cyclist_name FROZEN<fullname>,
  race_name text,
  race_position int,
  PRIMARY KEY (race_name, race_position)
);
// end::usetype[]

// tag::insert[]
INSERT INTO cycling.race_winners (
  race_name, race_position,
  cyclist_name
) VALUES (
  'National Championships South Africa WJ-ITT (CN)', 1,
  { firstname:'Frances', lastname:'DU TOUT' }
);
// end::insert[]

INSERT INTO cycling.race_winners (
  race_name, race_position,
  cyclist_name
) VALUES (
  'National Championships South Africa WJ-ITT (CN)', 2,
  { firstname:'Lynette', lastname:'BENSON' }
);

INSERT INTO cycling.race_winners (
  race_name, race_position,
  cyclist_name
) VALUES (
  'National Championships South Africa WJ-ITT (CN)', 3,
  { firstname:'Anja', lastname:'GERBER' }
);

INSERT INTO cycling.race_winners (
  race_name, race_position,
  cyclist_name
) VALUES (
  'National Championships South Africa WJ-ITT (CN)', 4,
  { firstname:'Ame', lastname:'VENTER' }
);

INSERT INTO cycling.race_winners (
  race_name, race_position,
  cyclist_name
) VALUES (
  'National Championships South Africa WJ-ITT (CN)', 5,
  { firstname:'Danielle', lastname:'VAN NIEKERK' }
);

// tag::comment_hyphens[]
SELECT * FROM cycling.race_winners; -- End of line comment
// end::comment_hyphens[]

// tag::comment_slashes[]
SELECT * FROM cycling.race_winners; // End of line comment
// end::comment_slashes[]

// tag::comment_span[]
/* This is a
   comment that spans multiple
   lines */
SELECT * FROM cycling.race_winners;
// end::comment_span[]

