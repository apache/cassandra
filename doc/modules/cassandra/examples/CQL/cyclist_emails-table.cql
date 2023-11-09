SOURCE 'keyspace-create-simple.cql';
SOURCE 'keyspace-use.cql';

DROP TABLE IF EXISTS cycling.cyclist_emails;

// Create a table with a known table id
// tag::ID[]
CREATE TABLE IF NOT EXISTS cycling.cyclist_emails (
  userid text PRIMARY KEY,
  id UUID,
  emails set<text>
)
WITH ID = '1bb7516e-b140-11e8-96f8-529269fb1459';
// end::ID[]

// Drop table
DROP TABLE IF EXISTS cycling.cyclist_emails;

// Create table again with a known table id
// tag::recreateID[]
CREATE TABLE IF NOT EXISTS cycling.cyclist_emails (
  userid text PRIMARY KEY,
  id UUID,
  emails set<text>
)
WITH ID = '1bb7516e-b140-11e8-96f8-529269fb1459';
// end::recreateID[]

// Retrieve the table id
// tag::select_id_from_system_schema[]
SELECT id
FROM system_schema.tables
WHERE keyspace_name = 'cycling'
  AND table_name = 'cyclist_emails';
// end::select_id_from_system_schema[]
