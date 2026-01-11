package org.apache.cassandra.distributed.test;

import org.junit.Test;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.junit.Assert.assertEquals;

/**
 * Tests that the BETWEEN operator respects SQL semantics:
 * - Normal BETWEEN (low <= high) returns rows.
 * - Inverted BETWEEN (low > high) returns no rows (empty result).
 */
public class BetweenInversionTest extends TestBaseImpl
{
    @Test
    public void testBetweenInversion() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy','replication_factor':1}");
            cluster.schemaChange("CREATE TABLE ks.t1 (pk int PRIMARY KEY, val text)");

            cluster.coordinator(1).execute("INSERT INTO ks.t1 (pk,val) VALUES (1,'a')", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("INSERT INTO ks.t1 (pk,val) VALUES (2,'b')", ConsistencyLevel.ALL);

            Object[][] rows = cluster.coordinator(1)
                                    .execute("SELECT * FROM ks.t1 WHERE pk BETWEEN 1 AND 2 ALLOW FILTERING", ConsistencyLevel.ALL);
            assertEquals(2, rows.length);

            Object[][] inverted = cluster.coordinator(1)
                                         .execute("SELECT * FROM ks.t1 WHERE pk BETWEEN 2 AND 1 ALLOW FILTERING", ConsistencyLevel.ALL);
            assertEquals(0, inverted.length);
        }
    }
}