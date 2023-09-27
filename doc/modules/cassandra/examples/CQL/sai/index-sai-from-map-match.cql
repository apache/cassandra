// Create an index on a map key to find all cyclist/team combos for a year 
// tag::keysidx[]
CREATE INDEX IF NOT EXISTS team_year_keys_idx
ON cycling.cyclist_teams ( KEYS (teams) );
// end::keysidx[]

// Create an index on a map key to find all cyclist/team combos for a year 
// tag::valuesidx[]
CREATE INDEX IF NOT EXISTS team_year_values_idx
ON cycling.cyclist_teams ( VALUES (teams) );
// end::valuesidx[]

// Create an index on a map key to find all cyclist/team combos for a year 
// tag::entriesidx[]
CREATE INDEX IF NOT EXISTS team_year_entries_idx
ON cycling.cyclist_teams ( ENTRIES (teams) );
// end::entriesidx[]