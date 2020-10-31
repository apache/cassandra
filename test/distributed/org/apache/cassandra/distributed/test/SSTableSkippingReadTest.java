/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.test;

import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertEquals;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;

public class SSTableSkippingReadTest extends TestBaseImpl
{
    @Test
    public void skippedSSTableWithPartitionDeletionTest() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY(pk, ck))"));
            // insert a partition tombstone on node 1, the deletion timestamp should end up being the sstable's minTimestamp
            cluster.get(1).executeInternal(withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 1 WHERE pk = 0"));
            // and a row from a different partition, to provide the sstable's min/max clustering
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (1, 1, 1) USING TIMESTAMP 2"));
            cluster.get(1).flush(KEYSPACE);
            // expect a single sstable, where minTimestamp equals the timestamp of the partition delete
            cluster.get(1).runOnInstance(() -> {
                Set<SSTableReader> sstables = Keyspace.open(KEYSPACE)
                                                      .getColumnFamilyStore("tbl")
                                                      .getLiveSSTables();
                assertEquals("Expected a single sstable, but found " + sstables.size(), 1, sstables.size());
                long minTimestamp = sstables.iterator().next().getMinTimestamp();
                assertEquals("Expected min timestamp of 1, but was " + minTimestamp, 1, minTimestamp);
            });

            // on node 2, add a row for the deleted partition with an older timestamp than the deletion so it should be shadowed
            cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (0, 10, 10) USING TIMESTAMP 0"));


            Object[][] rows = cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk=0 AND ck > 5"), ALL);
            assertEquals("Expected 0 rows, but found " + rows.length, 0, rows.length);
        }
    }

    @Test
    public void skippedSSTableWithPartitionDeletionShadowingDataOnAnotherNode() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY(pk, ck))"));
            // insert a partition tombstone on node 1, the deletion timestamp should end up being the sstable's minTimestamp
            cluster.get(1).executeInternal(withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 1 WHERE pk = 0"));
            // and a row from a different partition, to provide the sstable's min/max clustering
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (1, 1, 1) USING TIMESTAMP 1"));
            cluster.get(1).flush(KEYSPACE);
            // sstable 1 has minTimestamp == maxTimestamp == 1 and is skipped due to its min/max clusterings. Now we
            // insert a row which is not shadowed by the partition delete and flush to a second sstable. Importantly,
            // this sstable's minTimestamp is > than the maxTimestamp of the first sstable. This would cause the first
            // sstable not to be reincluded in the merge input, but we can't really make that decision as we don't
            // know what data and/or tombstones are present on other nodes
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (0, 6, 6) USING TIMESTAMP 2"));
            cluster.get(1).flush(KEYSPACE);

            // on node 2, add a row for the deleted partition with an older timestamp than the deletion so it should be shadowed
            cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (0, 10, 10) USING TIMESTAMP 0"));

            Object[][] rows = cluster.coordinator(1)
                                     .execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk=0 AND ck > 5"), ALL);
            // we expect that the row from node 2 (0, 10, 10) was shadowed by the partition delete, but the row from
            // node 1 (0, 6, 6) was not.
            assertRows(rows, new Object[]{ 0, 6, 6 });
        }
    }

    @Test
    public void skippedSSTableWithPartitionDeletionShadowingDataOnAnotherNode2() throws Throwable
    {
        // don't not add skipped sstables back just because the partition delete ts is < the local min ts

        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY(pk, ck))"));
            // insert a partition tombstone on node 1, the deletion timestamp should end up being the sstable's minTimestamp
            cluster.get(1).executeInternal(withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 1 WHERE pk = 0"));
            // and a row from a different partition, to provide the sstable's min/max clustering
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (1, 1, 1) USING TIMESTAMP 3"));
            cluster.get(1).flush(KEYSPACE);
            // sstable 1 has minTimestamp == maxTimestamp == 1 and is skipped due to its min/max clusterings. Now we
            // insert a row which is not shadowed by the partition delete and flush to a second sstable. The first sstable
            // has a maxTimestamp > than the min timestamp of all sstables, so it is a candidate for reinclusion to the
            // merge. Hoever, the second sstable's minTimestamp is > than the partition delete. This would  cause the
            // first sstable not to be reincluded in the merge input, but we can't really make that decision as we don't
            // know what data and/or tombstones are present on other nodes
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (0, 6, 6) USING TIMESTAMP 2"));
            cluster.get(1).flush(KEYSPACE);

            // on node 2, add a row for the deleted partition with an older timestamp than the deletion so it should be shadowed
            cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (0, 10, 10) USING TIMESTAMP 0"));

            Object[][] rows = cluster.coordinator(1)
                                     .execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk=0 AND ck > 5"), ALL);
            // we expect that the row from node 2 (0, 10, 10) was shadowed by the partition delete, but the row from
            // node 1 (0, 6, 6) was not.
            assertRows(rows, new Object[]{ 0, 6, 6 });
        }
    }
}
