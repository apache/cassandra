SOURCE '0_create_keyspace.cql';

DROP INDEX IF EXISTS cycling.lastname_sai_idx;
DROP INDEX IF EXISTS cycling.age_sai_idx;
DROP INDEX IF EXISTS cycling.country_sai_idx;
DROP INDEX IF EXISTS cycling.registration_sai_idx;

// tag::createQuickStartIndices[]
CREATE CUSTOM INDEX lastname_sai_idx ON cycling.cyclist_semi_pro (lastname) 
USING 'StorageAttachedIndex' 
WITH OPTIONS = {'case_sensitive': 'false', 'normalize': 'true', 'ascii': 'true'}; 

CREATE CUSTOM INDEX age_sai_idx ON cycling.cyclist_semi_pro (age) 
USING 'StorageAttachedIndex';

CREATE CUSTOM INDEX country_sai_idx ON cycling.cyclist_semi_pro (country) 
USING 'StorageAttachedIndex'
WITH OPTIONS = {'case_sensitive': 'false', 'normalize': 'true', 'ascii': 'true'}; 

CREATE CUSTOM INDEX registration_sai_idx ON cycling.cyclist_semi_pro (registration) 
USING 'StorageAttachedIndex'; 
// end::createQuickStartIndices[]
