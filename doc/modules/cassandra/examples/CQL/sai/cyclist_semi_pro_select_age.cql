SOURCE '0_create_keyspace.cql';

// tag::cyclistSelectAge[]
SELECT * FROM cycling.cyclist_semi_pro WHERE age <= 23;
// end::cyclistSelectAge[]
