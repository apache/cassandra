SOURCE '0_create_keyspace.cql';

// tag::cyclistSelectSpecificRider[]
SELECT * FROM cycling.cyclist_semi_pro WHERE lastname = 'Eppinger';
// end::cyclistSelectSpecificRider[]
