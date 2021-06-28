CREATE MATERIALIZED VIEW monkeySpecies_by_population AS
   SELECT * FROM monkeySpecies
   WHERE population IS NOT NULL AND species IS NOT NULL
   PRIMARY KEY (population, species)
   WITH comment='Allow query by population instead of species';
