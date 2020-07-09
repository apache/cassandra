// Error: cannot include both v1 and v2 in the primary key as both are not in the base table primary key

CREATE MATERIALIZED VIEW mv1 AS
   SELECT * FROM t 
   WHERE k IS NOT NULL AND c1 IS NOT NULL AND c2 IS NOT NULL AND v1 IS NOT NULL
   PRIMARY KEY (v1, v2, k, c1, c2);

// Error: must include k in the primary as it's a base table primary key column

CREATE MATERIALIZED VIEW mv1 AS
   SELECT * FROM t 
   WHERE c1 IS NOT NULL AND c2 IS NOT NULL
   PRIMARY KEY (c1, c2);
