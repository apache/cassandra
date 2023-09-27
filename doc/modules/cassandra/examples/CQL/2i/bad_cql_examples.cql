// This file contains CQL statements that generate errors.
// This file can be run after the cycling keyspace is set up.

// InvalidRequest: Error from server: code=2200 [Invalid query]
// message=“Altering of types is not allowed”
ALTER TABLE cycling.cyclist_alt_stats ALTER favorite_color TYPE text;

// tag::select_all_from_cyclist_alt_stats_error[]
SELECT *
FROM cycling.cyclist_alt_stats
WHERE birthday = '1982-01-29'
  AND nationality = 'Russia';
// end::select_all_from_cyclist_alt_stats_error[]
