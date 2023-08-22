SOURCE '0_create_keyspace.cql';

// tag::cyclistSelectCountry[]
SELECT * FROM cycling.cyclist_semi_pro WHERE country = 'GBR';
// end::cyclistSelectCountry[]
