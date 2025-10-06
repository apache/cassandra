package org.apache.cassandra.distributed.test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests BETWEEN operator respects SQL semantics:
 * - Normal BETWEEN (low <= high) returns rows.
 * - Inverted BETWEEN (low > high) returns no rows.
 */
public class BetweenInversionTest
{
    @Test
    public void testBetweenInversion() throws Throwable
    {
        // Start a 1-node in-JVM cluster
        try (Cluster cluster = Cluster.build(1).start())
        {
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication = "
                               + "{'class':'SimpleStrategy','replication_factor':1}");
            cluster.schemaChange("CREATE TABLE ks.t1 (pk int PRIMARY KEY, val text)");

            // Insert two rows
            cluster.coordinator(1).execute("INSERT INTO ks.t1 (pk,val) VALUES (1,'a')", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("INSERT INTO ks.t1 (pk,val) VALUES (2,'b')", ConsistencyLevel.ALL);

            // Normal BETWEEN: should return 2 rows
            Object[][] rows = cluster.coordinator(1)
                                    .execute("SELECT * FROM ks.t1 WHERE pk BETWEEN 1 AND 2", ConsistencyLevel.ALL);
            assertEquals(2, rows.length);

            // Inverted BETWEEN: should return 0 rows
            Object[][] inverted = cluster.coordinator(1)
                                         .execute("SELECT * FROM ks.t1 WHERE pk BETWEEN 2 AND 1", ConsistencyLevel.ALL);
            assertEquals(0, inverted.length);
        }
    }
}
