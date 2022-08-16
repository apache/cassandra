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

package org.apache.cassandra.distributed.test.accord;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;

import accord.txn.Txn;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.QueryResults;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.AccordTxnBuilder;
import org.apache.cassandra.service.accord.db.AccordData;

import static org.apache.cassandra.service.accord.db.AccordUpdate.UpdatePredicate.Type.EQUAL;
import static org.apache.cassandra.service.accord.db.AccordUpdate.UpdatePredicate.Type.NOT_EXISTS;

public class AccordIntegrationTest extends TestBaseImpl
{
    private static void assertRow(Cluster cluster, String query, int k, int c, int v)
    {
        SimpleQueryResult result = cluster.coordinator(1).executeWithResult(query, ConsistencyLevel.QUORUM);
        AssertUtils.assertRows(result, QueryResults.builder()
                                                   .row(k, c, v)
                                                   .build());
    }

    @Test
    public void testQuery() throws Throwable
    {
        String keyspace = "ks" + System.currentTimeMillis();
        try (Cluster cluster = init(Cluster.build(2).withConfig(c -> c.set("write_request_timeout_in_ms", TimeUnit.MINUTES.toMillis(10))).start()))
        {
            cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE " + keyspace + ".tbl (k int, c int, v int, primary key (k, c))");
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.createEpochFromConfigUnsafe()));
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.setCacheSize(0)));

            cluster.get(1).runOnInstance(() -> execute(txn()
                                                       .withRead("SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0")
                                                       .withWrite("INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (0, 0, 1)")
                                                       .withCondition(keyspace, "tbl", 0, 0, NOT_EXISTS)));

            awaitAsyncApply(cluster);
            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0", 0, 0, 1);

            // row exists now, so tx should no-op
            cluster.get(1).runOnInstance(() -> execute(txn()
                                                       .withRead("SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0")
                                                       .withWrite("INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (0, 0, 2)")
                                                       .withCondition(keyspace, "tbl", 0, 0, NOT_EXISTS)));

            awaitAsyncApply(cluster);
            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0", 0, 0, 1);
        }
    }

    @Test
    public void multiKeyMultiQuery() throws Throwable
    {
        String keyspace = "ks" + System.currentTimeMillis();

        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE " + keyspace + ".tbl (k int, c int, v int, primary key (k, c))");
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.createEpochFromConfigUnsafe()));

            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.setCacheSize(0)));

            cluster.get(1).runOnInstance(() -> execute(txn()
                                                       .withRead("SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0")
                                                       .withRead("SELECT * FROM " + keyspace + ".tbl WHERE k=1 AND c=0")
                                                       .withWrite("INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (0, 0, 0)")
                                                       .withWrite("INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (1, 0, 0)")
                                                       .withCondition(keyspace, "tbl", 0, 0, NOT_EXISTS)));

            cluster.get(1).runOnInstance(() -> execute(txn()
                                                       .withRead("SELECT * FROM " + keyspace + ".tbl WHERE k=1 AND c=0")
                                                       .withRead("SELECT * FROM " + keyspace + ".tbl WHERE k=2 AND c=0")
                                                       .withWrite("INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (1, 0, 1)")
                                                       .withWrite("INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (2, 0, 1)")
                                                       .withCondition(keyspace, "tbl", 1, 0, "v", EQUAL, 0)));

            awaitAsyncApply(cluster);
            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0", 0, 0, 0);
            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=1 AND c=0", 1, 0, 1);
            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=2 AND c=0", 2, 0, 1);
        }
    }

    @Test
    public void testRecovery() throws Throwable
    {
        String keyspace = "ks" + System.currentTimeMillis();
        try (Cluster cluster = init(Cluster.build(2).withConfig(c -> c.set("write_request_timeout_in_ms", TimeUnit.MINUTES.toMillis(10))).start()))
        {
            cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE " + keyspace + ".tbl (k int, c int, v int, primary key (k, c))");
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.createEpochFromConfigUnsafe()));
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance.setCacheSize(0)));

            IMessageFilters.Filter lostApply = cluster.filters().verbs(Verb.ACCORD_APPLY_REQ.id).drop();
            IMessageFilters.Filter lostCommit = cluster.filters().verbs(Verb.ACCORD_COMMIT_REQ.id).to(2).drop();

            cluster.get(1).runOnInstance(() -> execute(txn()
                                                       .withRead("SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0")
                                                       .withWrite("INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (0, 0, 1)")
                                                       .withCondition(keyspace, "tbl", 0, 0, NOT_EXISTS)));

            lostApply.off();
            lostCommit.off();

            // query again, this should trigger recovery
            cluster.get(1).runOnInstance(() -> execute(txn()
                                                       .withRead("SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0")
                                                       .withWrite("UPDATE " + keyspace + ".tbl SET v=? WHERE k=? AND c=?", 2, 0, 0)
                                                       .withCondition(keyspace, "tbl", 0, 0, "v", EQUAL, 1)));

            awaitAsyncApply(cluster);

            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0", 0, 0, 1);

            cluster.get(1).runOnInstance(() -> execute(txn()
                                                       .withRead("SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0")
                                                       .withWrite("INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (0, 0, 2)")
                                                       .withCondition(keyspace, "tbl", 0, 0, NOT_EXISTS)));

            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0", 0, 0, 1);
        }
    }

    private static void awaitAsyncApply(Cluster cluster)
    {
        //TODO figure out a way to block so tests can be stable
        // I tried to check the stores but you have to be in the correct threads and we attempt to load from table but this isn't updated
        //TODO can't send PreAccept as we don't know the Keys (empty tx)
//        cluster.get(1).runOnInstance(() -> execute(txn())); // TODO why does this hang?

        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
    }

    private static AccordTxnBuilder txn()
    {
        return new AccordTxnBuilder();
    }

    private static SimpleQueryResult execute(AccordTxnBuilder builder)
    {
        return execute(builder.build());
    }

    private static SimpleQueryResult execute(Txn txn)
    {
        try
        {
            AccordData result = (AccordData) AccordService.instance.node.coordinate(txn).get();
            Assert.assertNotNull(result);
            QueryResults.Builder builder = QueryResults.builder();
            boolean addedHeader = false;
            for (FilteredPartition partition : result)
            {
                //TODO lot of this is copy/paste from SelectStatement...
                TableMetadata metadata = partition.metadata();
                if (!addedHeader)
                {
                    builder.columns(StreamSupport.stream(Spliterators.spliteratorUnknownSize(metadata.allColumnsInSelectOrder(), Spliterator.ORDERED), false)
                                                 .map(cm -> cm.name.toString())
                                                 .toArray(String[]::new));
                    addedHeader = true;
                }
                ByteBuffer[] keyComponents = SelectStatement.getComponents(metadata, partition.partitionKey());
                for (Row row : partition)
                    append(metadata, keyComponents, row, builder);
            }
            return builder.build();
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new AssertionError(e);
        }
    }

    private static void append(TableMetadata metadata, ByteBuffer[] keyComponents, Row row, QueryResults.Builder builder)
    {
        Object[] buffer = new Object[Iterators.size(metadata.allColumnsInSelectOrder())];
        Clustering<?> clustering = row.clustering();
        Iterator<ColumnMetadata> it = metadata.allColumnsInSelectOrder();
        int idx = 0;
        while (it.hasNext())
        {
            ColumnMetadata column = it.next();
            switch (column.kind)
            {
                case PARTITION_KEY:
                    buffer[idx++] = column.type.compose(keyComponents[column.position()]);
                    break;
                case CLUSTERING:
                    buffer[idx++] = column.type.compose(clustering.bufferAt(column.position()));
                    break;
                case REGULAR:
                {
                    if (column.isComplex())
                    {
                        throw new UnsupportedOperationException("Ill implement complex later..");
                    }
                    else
                    {
                        //TODO deletes
                        buffer[idx++] = column.type.compose(row.getCell(column).buffer());
                    }
                }
                break;
//                case STATIC:
                default: throw new IllegalArgumentException("Unsupported kind: " + column.kind);
            }
        }
        builder.row(buffer);
    }
}
