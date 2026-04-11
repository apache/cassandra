SOURCE '0_create_keyspace.cql';

DROP INDEX IF EXISTS cycling.lastname_sai_idx;
DROP INDEX IF EXISTS cycling.age_sai_idx;
DROP INDEX IF EXISTS cycling.country_sai_idx;
DROP INDEX IF EXISTS cycling.registration_sai_idx;

// tag::createQuickStartIndices[]
CREATE INDEX lastname_sai_idx ON cycling.cyclist_semi_pro (lastname) 
USING 'sai' 
WITH OPTIONS = {'case_sensitive': 'false', 'normalize': 'true', 'ascii': 'true'}; 

CREATE INDEX age_sai_idx ON cycling.cyclist_semi_pro (age) 
USING 'sai';

CREATE INDEX country_sai_idx ON cycling.cyclist_semi_pro (country) 
USING 'sai'
WITH OPTIONS = {'case_sensitive': 'false', 'normalize': 'true', 'ascii': 'true'}; 

CREATE INDEX registration_sai_idx ON cycling.cyclist_semi_pro (registration) 
USING 'sai'; 
// end::createQuickStartIndices[]
