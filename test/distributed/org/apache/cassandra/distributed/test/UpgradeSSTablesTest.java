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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.LogAction;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public class UpgradeSSTablesTest extends TestBaseImpl
{
    @Test
    public void rewriteSSTablesTest() throws Throwable
    {
        try (ICluster<IInvokableInstance> cluster = builder().withNodes(1).withDataDirCount(1).start())
        {
            for (String compressionBefore : new String[]{ "{'class' : 'LZ4Compressor', 'chunk_length_in_kb' : 32}", "{'enabled': 'false'}" })
            {
                for (String command : new String[]{ "upgradesstables", "recompress_sstables" })
                {
                    cluster.schemaChange(withKeyspace("DROP KEYSPACE IF EXISTS %s"));
                    cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"));

                    cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v text, PRIMARY KEY (pk, ck)) " +
                                                      "WITH compression = " + compressionBefore));
                    cluster.get(1).acceptsOnInstance((String ks) -> {
                        Keyspace.open(ks).getColumnFamilyStore("tbl").disableAutoCompaction();
                    }).accept(KEYSPACE);

                    String blob = "blob";
                    for (int i = 0; i < 6; i++)
                        blob += blob;

                    for (int i = 0; i < 100; i++)
                    {
                        cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (?,?,?)"),
                                                       ConsistencyLevel.QUORUM, i, i, blob);
                    }
                    cluster.get(1).nodetool("flush", KEYSPACE, "tbl");

                    Assert.assertEquals(0, cluster.get(1).nodetool("upgradesstables", "-a", KEYSPACE, "tbl"));
                    cluster.schemaChange(withKeyspace("ALTER TABLE %s.tbl WITH compression = {'class' : 'LZ4Compressor', 'chunk_length_in_kb' : 128};"));

                    Thread.sleep(2000); // Make sure timestamp will be different even with 1-second resolution.

                    long maxSoFar = cluster.get(1).appliesOnInstance((String ks) -> {
                        long maxTs = -1;
                        ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore("tbl");
                        cfs.disableAutoCompaction();
                        for (SSTableReader tbl : cfs.getLiveSSTables())
                        {
                            maxTs = Math.max(maxTs, tbl.getCreationTimeFor(Component.DATA));
                        }
                        return maxTs;
                    }).apply(KEYSPACE);

                    for (int i = 100; i < 200; i++)
                    {
                        cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (?,?,?)"),
                                                       ConsistencyLevel.QUORUM, i, i, blob);
                    }
                    cluster.get(1).nodetool("flush", KEYSPACE, "tbl");

                    LogAction logAction = cluster.get(1).logs();
                    logAction.mark();

                    long expectedCount = cluster.get(1).appliesOnInstance((String ks, Long maxTs) -> {
                        long count = 0;
                        long skipped = 0;
                        Set<SSTableReader> liveSSTables = Keyspace.open(ks).getColumnFamilyStore("tbl").getLiveSSTables();
                        assert liveSSTables.size() == 2 : String.format("Expected 2 sstables, but got " + liveSSTables.size());
                        for (SSTableReader tbl : liveSSTables)
                        {
                            if (tbl.getCreationTimeFor(Component.DATA) <= maxTs)
                                count++;
                            else
                                skipped++;
                        }
                        assert skipped > 0;
                        return count;
                    }).apply(KEYSPACE, maxSoFar);

                    if (command.equals("upgradesstables"))
                        Assert.assertEquals(0, cluster.get(1).nodetool("upgradesstables", "-a", "-t", Long.toString(maxSoFar), KEYSPACE, "tbl"));
                    else
                        Assert.assertEquals(0, cluster.get(1).nodetool("recompress_sstables", KEYSPACE, "tbl"));

                    Assert.assertFalse(logAction.grep(String.format("%d sstables to", expectedCount)).getResult().isEmpty());
                }
            }
        }
    }
}
