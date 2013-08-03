Cassandra Trigger's Example:
=========================

InvertedIndex class will create a inverted index of 
RowKey:ColumnName:Value to Value:ColumnName:RowKey

NOTE: This example is limited to append-only workloads, 
	  doesn't delete indexes on deletes. 

Installation:
============
change directory to <cassandra_src_dir>/examples/triggers
run "ant jar"
Copy build/trigger-example.jar to <cassandra_conf>/triggers/
Copy conf/* to <cassandra_home>/conf/
Create column family configured in InvertedIndex.properties 
    Example: Keyspace1.InvertedIndex as configured in InvertedIndex.properties
Configure trigger on the table.
    Example: CREATE TRIGGER test1 ON "Keyspace1"."Standard1"
                 USING 'org.apache.cassandra.triggers.InvertedIndex';
Start inserting data to the column family which has the triggers. 
