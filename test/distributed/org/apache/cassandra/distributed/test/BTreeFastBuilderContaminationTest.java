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

import java.util.List;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.shared.ShutdownException;
import org.apache.cassandra.net.Verb;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.fail;

public class BTreeFastBuilderContaminationTest extends TestBaseImpl
{
    // 4200 columns * ~18 bytes/name > 64KB large-message threshold
    // → READ_REQ deserialized on SEPWorker threads, not Netty event loop
    private static final int NUM_WIDE_COLUMNS = 4200;

    // Small-message scenario: both READ_REQ and MUTATION_REQ stay under 64KB
    // → deserialized on Netty event loop threads
    private static final int NUM_SMALL_SOURCE_COLUMNS = 150; // >31 to trigger FastBuilder overflow
    private static final int NUM_SMALL_VICTIM_COLUMNS = 2000;

    private static final int NUM_PARTITIONS = 200;
    private static final int NUM_DELETE_PARTITIONS = 300;

    // Verify CASSANDRA-21216/CASSANDRA-21260 fix: stale ColumnMetadata from a failed
    // READ_REQ deserialization must not leak into a Row BTree during mutation, which can
    // cause ClassCastException. Source table is wide (~4200 columns) so READ_REQ exceeds 
    // 64KB, meaning it is deserialized on SEPWorker. Victim table is narrow — without the 
    // fix, corruption can happen via BTree.updateLeaves() during mutation execution on 
    // the same SEPWorker thread (SharedExecutorPool threads hop between stages).
    @Test
    public void testSchemaDisagreementCorruptsPartitionViaFastBuilder() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(2)
                                             .withConfig(config -> {
                                                 config.with(NETWORK, GOSSIP);
                                                 config.set("concurrent_reads", 2);
                                                 config.set("concurrent_writes", 2);
                                                 config.set("read_request_timeout_in_ms", 5000L);
                                                 config.set("write_request_timeout_in_ms", 5000L);
                                             })
                                             .start()))
        {
            createWideSourceTable(cluster);

            cluster.schemaChange(withKeyspace(
                "CREATE TABLE %s.victim (pk int, ck int, v text, PRIMARY KEY (pk, ck))"));

            cluster.coordinator(1).execute(
                withKeyspace("INSERT INTO %s.source (pk, src_wide_col_0000) VALUES (1, 42)"), ALL);

            for (int pk = 0; pk < NUM_PARTITIONS; pk++)
                cluster.get(2).executeInternal(withKeyspace(
                    "INSERT INTO %s.victim (pk, ck, v) VALUES (" + pk + ", 1, 'seed')"));

            createSchemaDisagreement(cluster);
            poisonFastBuilder(cluster);

            for (int pk = 0; pk < NUM_PARTITIONS; pk++)
            {
                try
                {
                    cluster.coordinator(1).execute(withKeyspace(
                        "INSERT INTO %s.victim (pk, ck, v) VALUES (" + pk + ", 2, 'probe')"), ALL);
                }
                catch (Exception e)
                {
                    if (rootCauseIs(e, ClassCastException.class))
                        fail("ClassCastException from corrupted partition BTree (CASSANDRA-21216): " + e.getMessage());
                }
            }

            for (int pk = 0; pk < NUM_PARTITIONS; pk++)
            {
                try
                {
                    cluster.coordinator(1).execute(withKeyspace(
                        "SELECT * FROM %s.victim WHERE pk = " + pk), ALL);
                }
                catch (Exception e)
                {
                    if (rootCauseIs(e, ClassCastException.class))
                        fail("ClassCastException from corrupted partition BTree (CASSANDRA-21216): " + e.getMessage());
                }
            }

            try
            {
                cluster.get(2).flush(KEYSPACE);
            }
            catch (Exception e)
            {
                if (rootCauseIs(e, ClassCastException.class))
                    fail("ClassCastException from corrupted partition BTree (CASSANDRA-21216): " + e.getMessage());
            }
        }
        catch (ShutdownException e)
        {
            if (rootCauseIs(e, ClassCastException.class))
                fail("ClassCastException from corrupted partition BTree during shutdown (CASSANDRA-21216): " + e.getMessage());
            throw e;
        }
    }

    // Verify CASSANDRA-21260 fix: SSTable header must not be contaminated via small messages
    // on the Netty event loop.
    // Source: 150 columns (>31 → FastBuilder overflow) but only ~3KB → small message.
    // Victim: 2000 columns, but partition DELETE has empty updatedColumns → tiny message.
    // Both deserialized on the same Netty event loop thread (channel-to-EventLoop binding).
    // Without the fix, the poisoned FastBuilder is reused for the victim's SerializationHeader
    // deserialization.
    @Test
    public void testSmallMessageContaminatesSSTableHeaderViaNettyEventLoop() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(2)
                                             .withConfig(config -> {
                                                 config.with(NETWORK, GOSSIP);
                                                 config.set("read_request_timeout_in_ms", 5000L);
                                                 config.set("write_request_timeout_in_ms", 5000L);
                                             })
                                             .start()))
        {
            createTable(cluster, "source", NUM_SMALL_SOURCE_COLUMNS, "src_col");
            createTable(cluster, "victim", NUM_SMALL_VICTIM_COLUMNS, "vic_col");

            createSchemaDisagreement(cluster);
            poisonFastBuilder(cluster);

            // Partition deletions to the victim table. Despite the victim having 2000 columns,
            // a partition-level DELETE has empty updatedColumns (no column operations), so
            // the MUTATION_REQ is tiny. It is deserialized on the same Netty event loop thread
            // that handled the failed READ_REQ. The poisoned FastBuilder's stale savedBuffer
            // is drained even though 0 new columns are added — build() calls propagateOverflow()
            // when hasOverflow() is true from the previous use.
            int batchSize = NUM_DELETE_PARTITIONS / 5;
            for (int round = 0; round < 5; round++)
            {
                if (round > 0)
                    poisonFastBuilder(cluster);

                for (int pk = round * batchSize; pk < (round + 1) * batchSize; pk++)
                {
                    try
                    {
                        cluster.coordinator(1).execute(withKeyspace(
                            "DELETE FROM %s.victim WHERE pk = " + pk), ALL);
                    }
                    catch (Exception ignored)
                    {
                    }
                }
            }

            cluster.get(2).flush(KEYSPACE);

            List<String> foreignColumns = cluster.get(2).callOnInstance(() -> {
                java.util.List<String> result = new java.util.ArrayList<>();
                org.apache.cassandra.db.ColumnFamilyStore cfs =
                    org.apache.cassandra.db.ColumnFamilyStore.getIfExists(KEYSPACE, "victim");
                if (cfs == null)
                    return result;
                org.apache.cassandra.schema.TableMetadata metadata = cfs.metadata.get();
                for (org.apache.cassandra.io.sstable.format.SSTableReader sstable : cfs.getLiveSSTables())
                {
                    try
                    {
                        org.apache.cassandra.db.SerializationHeader.Component header =
                            (org.apache.cassandra.db.SerializationHeader.Component)
                                sstable.descriptor.getMetadataSerializer()
                                       .deserialize(sstable.descriptor,
                                                    org.apache.cassandra.io.sstable.metadata.MetadataType.HEADER);
                        result.addAll(getUnknownColumns(header, metadata));
                    }
                    catch (Exception e)
                    {
                        result.add("ERROR reading header: " + e.getMessage());
                    }
                }
                return result;
            });

            if (!foreignColumns.isEmpty())
                fail("SSTable header contamination detected (CASSANDRA-21260): foreign columns "
                     + "found in victim's SSTable header: " + foreignColumns);
        }
    }

    private void createTable(Cluster cluster, String tableName, int numColumns, String columnPrefix)
    {
        StringBuilder ddl = new StringBuilder(
            withKeyspace("CREATE TABLE %s." + tableName + " (pk int PRIMARY KEY"));
        for (int i = 0; i < numColumns; i++)
            ddl.append(String.format(", %s_%04d int", columnPrefix, i));
        ddl.append(')');
        cluster.schemaChange(ddl.toString());
    }

    // Wide source table: 4200 columns * ~18 bytes/name > 64KB large-message threshold
    private void createWideSourceTable(Cluster cluster)
    {
        createTable(cluster, "source", NUM_WIDE_COLUMNS, "src_wide_col");
    }

    private void createSchemaDisagreement(Cluster cluster)
    {
        cluster.filters().verbs(Verb.TCM_REPLICATION.id).from(1).to(2).drop();
        cluster.filters().verbs(Verb.TCM_FETCH_CMS_LOG_RSP.id).from(1).to(2).drop();
        cluster.filters().verbs(Verb.TCM_FETCH_PEER_LOG_RSP.id).from(1).to(2).drop();

        cluster.get(1).schemaChangeInternal(
            withKeyspace("ALTER TABLE %s.source ADD zzz_new_col text"));
    }

    // Trigger a failed READ_REQ on node2 (schema disagreement), poisoning the
    // deserializing thread's FastBuilder with stale savedBuffer/savedNextKey.
    private void poisonFastBuilder(Cluster cluster)
    {
        try
        {
            cluster.coordinator(1).execute(
                withKeyspace("SELECT * FROM %s.source WHERE pk = 1"), ALL);
        }
        catch (Exception e)
        {
            // Expected: schema disagreement causes unknown column exception on node2
        }
    }

    // Check for columns in an SSTable header that don't belong to the table's schema.
    private static java.util.List<String> getUnknownColumns(
        org.apache.cassandra.db.SerializationHeader.Component header,
        org.apache.cassandra.schema.TableMetadata metadata)
    {
        java.util.List<String> unknownColumns = new java.util.ArrayList<>();
        java.util.Map<java.nio.ByteBuffer, org.apache.cassandra.db.marshal.AbstractType<?>>[] maps =
            new java.util.Map[] { header.getStaticColumns(), header.getRegularColumns() };
        boolean[] isStatic = { true, false };
        for (int i = 0; i < maps.length; i++)
        {
            for (java.nio.ByteBuffer name : maps[i].keySet())
            {
                org.apache.cassandra.schema.ColumnMetadata column = metadata.getColumn(name);
                if (column == null || column.isStatic() != isStatic[i])
                {
                    column = metadata.getDroppedColumn(name, isStatic[i]);
                    if (column == null)
                    {
                        unknownColumns.add(org.apache.cassandra.db.marshal.UTF8Type.instance.getString(name));
                    }
                }
            }
        }
        return unknownColumns;
    }

    private static boolean rootCauseIs(Throwable t, Class<? extends Throwable> type)
    {
        while (t != null)
        {
            if (type.isInstance(t))
                return true;
            for (Throwable suppressed : t.getSuppressed())
            {
                if (rootCauseIs(suppressed, type))
                    return true;
            }
            t = t.getCause();
        }
        return false;
    }

}
