SOURCE 'keyspace-create-simple.cql';
SOURCE 'keyspace-use.cql';

DROP TABLE IF EXISTS cycling.cyclist_base;

// tag::cyclist_base[]
CREATE TABLE IF NOT EXISTS cycling.cyclist_base (
  cid UUID PRIMARY KEY,
  name text,
  age int,
  birthday date,
  country text
);
// end::cyclist_base[]

// tag::data[]
INSERT INTO cycling.cyclist_base (
  cid,name,age,birthday,country
) VALUES (
  e7ae5cf3-d358-4d99-b900-85902fda9bb0,'Alex FRAME', 22, '1993-06-18', 'New Zealand'
);

INSERT INTO cycling.cyclist_base (
  cid,name,age,birthday,country
) VALUES (
  220844bf-4860-49d6-9a4b-6b5d3a79cbfb,'Paolo TIRALONGO', 38, '1977-07-08', 'Italy'
);

INSERT INTO cycling.cyclist_base (
  cid,name,age,birthday,country
) VALUES (
  6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47,'Steven KRUIKSWIJK', 28, '1987-06-07', 'Netherlands'
);

INSERT INTO cycling.cyclist_base (
  cid,name,age,birthday,country
) VALUES (
  ffdfa2a7-5fc6-49a7-bfdc-3fcdcfdd7156,'Pascal EENKHOORN', 18, '1997-02-08', 'Netherlands'
);

INSERT INTO cycling.cyclist_base (
  cid,name,age,birthday,country
) VALUES (
  18f471bf-f631-4bc4-a9a2-d6f6cf5ea503,'Bram WELTEN', 18, '1997-03-29', 'Netherlands'
);

INSERT INTO cycling.cyclist_base (
  cid,name,age,birthday,country
) VALUES (
  15a116fc-b833-4da6-ab9a-4a7775752836,'Adrien COSTA', 18, '1997-08-19', 'United States'
);

INSERT INTO cycling.cyclist_base (
  cid,name,age,birthday,country
) VALUES (
  862cc51f-00a1-4d5a-976b-a359cab7300e,'Joakim BUKDAL', 20, '1994-09-04', 'Denmark'
);

INSERT INTO cycling.cyclist_base (
  cid,name,age,birthday,country
) VALUES (
  c9c9c484-5e4a-4542-8203-8d047a01b8a8,'Cristian EGIDIO', 27, '1987-09-04', 'Brazil'
);
// end::data[]

CAPTURE 'select_all_from_cyclist_base.results';
SELECT *
FROM cycling.cyclist_base;
CAPTURE OFF;

// tag::alt[]
ALTER TABLE cycling.cyclist_base
WITH comment = 'basic cyclist information';
// end::alt[]

// tag::specr[]
ALTER TABLE cycling.cyclist_base
WITH speculative_retry = '95percentile';
// end::specr[]

// tag::milli[]
ALTER TABLE cycling.cyclist_base
WITH speculative_retry = '10ms';
// end::milli[]