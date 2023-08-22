SOURCE '0_create_keyspace.cql';

// tag::cyclistSelectDateRange[]
SELECT * FROM cycling.cyclist_semi_pro WHERE registration > '2010-01-01' AND registration < '2015-12-31' LIMIT 10;
// end::cyclistSelectDateRange[]
