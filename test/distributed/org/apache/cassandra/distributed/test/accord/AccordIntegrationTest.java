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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.coordinate.Preempted;
import accord.local.Status;
import accord.messages.Commit;
import accord.primitives.Keys;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.QueryResults;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.util.QueryResultUtil;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.AccordTxnBuilder;
import org.apache.cassandra.service.accord.db.AccordData;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FailingConsumer;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.service.accord.db.AccordUpdate.UpdatePredicate.Type.EQUAL;
import static org.apache.cassandra.service.accord.db.AccordUpdate.UpdatePredicate.Type.NOT_EXISTS;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

//TODO there are too many new clusters, this will cause Metaspace issues.  Once Schema and topology are integrated, can switch
// to a shared cluster with isolated tables
public class AccordIntegrationTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(AccordIntegrationTest.class);

    private static final String keyspace = "ks";

    private static void assertRow(Cluster cluster, String query, int k, int c, int v)
    {
        SimpleQueryResult result = cluster.coordinator(1).executeWithResult(query, ConsistencyLevel.QUORUM);
        AssertUtils.assertRows(result, QueryResults.builder()
                                                   .row(k, c, v)
                                                   .build());
    }

    private static void test(FailingConsumer<Cluster> fn) throws IOException
    {
        try (Cluster cluster = createCluster())
        {
            cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE " + keyspace + ".tbl (k int, c int, v int, primary key (k, c))");
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance().createEpochFromConfigUnsafe()));
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance().setCacheSize(0)));

            fn.accept(cluster);
        }
    }

    private static Cluster createCluster() throws IOException
    {
        // need to up the timeout else tests get flaky
        // disable vnode for now, but should enable before trunk
        return init(Cluster.build(2).withoutVNodes().withConfig(c -> c.with(Feature.NETWORK).set("write_request_timeout_in_ms", TimeUnit.SECONDS.toMillis(10))).start());
    }

    @Test
    public void testQuery() throws IOException
    {
        test(cluster -> {
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
        });
    }

    @Test
    public void multiKeyMultiQuery() throws IOException
    {
        test(cluster -> {
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
        });
    }

    @Test
    public void testRecovery() throws IOException
    {
        test(cluster -> {
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
                                                       .withCondition(keyspace, "tbl", 0, 0, "v", EQUAL, 1),
                                                       true));

            awaitAsyncApply(cluster);

            //TODO why is this flakey?  Also why does .close hang on Accord?
            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0", 0, 0, 2);

            cluster.get(1).runOnInstance(() -> execute(txn()
                                                       .withRead("SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0")
                                                       .withWrite("INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (0, 0, 3)")
                                                       .withCondition(keyspace, "tbl", 0, 0, NOT_EXISTS)));
            awaitAsyncApply(cluster);

            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0", 0, 0, 2);
        });
    }

    @Test
    public void multipleShards() throws IOException, TimeoutException
    {
        // can't reuse test() due to it using "int" for pk; this test needs "blob"
        try (Cluster cluster = createCluster())
        {
            cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 1}");
            cluster.schemaChange("CREATE TABLE " + keyspace + ".tbl (k blob, c int, v int, primary key (k, c))");
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance().createEpochFromConfigUnsafe()));
            cluster.forEach(node -> node.runOnInstance(() -> AccordService.instance().setCacheSize(0)));

            List<String> tokens = cluster.stream()
                                         .flatMap(i -> StreamSupport.stream(Splitter.on(",").split(i.config().getString("initial_token")).spliterator(), false))
                                         .collect(Collectors.toList());
            // needs to be byte[] because ByteBuffer isn't serializable so can't pass into the coordinator ClassLoader
            List<byte[]> keys = tokens.stream().map(t -> (Murmur3Partitioner.LongToken) Murmur3Partitioner.instance.getTokenFactory().fromString(t))
                                      .map(Murmur3Partitioner.LongToken::keyForToken)
                                      .map(ByteBufferUtil::getArray)
                                      .collect(Collectors.toList());

            cluster.get(1).runOnInstance(() -> {
                AccordTxnBuilder txn = txn();
                int i = 0;
                for (byte[] data : keys)
                {
                    ByteBuffer key = ByteBuffer.wrap(data);
                    txn = txn.withRead("SELECT * FROM " + keyspace + ".tbl WHERE k=? and c=0", key)
                             .withWrite("INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (?, 0, ?)", key, i++)
                             .withCondition(keyspace, "tbl", key, 0, NOT_EXISTS);
                }
                Keys keySet = txn.build().keys();
                Topologies topology = AccordService.instance().node.topology().withUnsyncedEpochs(keySet, 1);
                // currently we don't detect out-of-bounds read/write, so need this logic to validate we reach different
                // shards
                Assertions.assertThat(topology.totalShards()).isEqualTo(2);
                execute(txn);
            });

            awaitAsyncApply(cluster);

            SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT * FROM " + keyspace + ".tbl", ConsistencyLevel.ALL);
            QueryResults.Builder expected = QueryResults.builder()
                                                        .columns("k", "c", "v");
            for (int i = 0; i < keys.size(); i++)
                expected.row(ByteBuffer.wrap(keys.get(i)), 0, i);
            AssertUtils.assertRows(result, expected.build());
        }
    }

    @Test
    public void testLostCommitReadTriggersFallbackRead() throws IOException
    {
        test(cluster -> {
            // its expected that the required Read will happen reguardless of if this fails to return a read
            cluster.filters().verbs(Verb.ACCORD_COMMIT_REQ.id).messagesMatching((from, to, iMessage) -> cluster.get(from).callOnInstance(() -> {
                Message<?> msg = Instance.deserializeMessage(iMessage);
                if (msg.payload instanceof Commit)
                    return ((Commit) msg.payload).read != null;
                return false;
            })).drop();

            cluster.get(1).runOnInstance(() -> execute(txn()
                                                       .withRead("SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0")
                                                       .withWrite("INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (0, 0, 1)")
                                                       .withCondition(keyspace, "tbl", 0, 0, NOT_EXISTS)));

            awaitAsyncApply(cluster);

            // recovery happened
            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0", 0, 0, 1);
        });
    }

    @Test
    public void testReadOnlyTx() throws IOException
    {
        test(cluster -> cluster.get(1).runOnInstance(() -> execute(txn().withRead("SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0"))));
    }

    @Test
    public void testWriteOnlyTx() throws IOException
    {
        test(cluster -> {
            cluster.get(1).runOnInstance(() -> execute(txn().withWrite("INSERT INTO " + keyspace + ".tbl (k, c, v) VALUES (0, 0, 1)")));

            awaitAsyncApply(cluster);

            assertRow(cluster, "SELECT * FROM " + keyspace + ".tbl WHERE k=0 AND c=0", 0, 0, 1);
        });
    }

    private static void awaitAsyncApply(Cluster cluster) throws TimeoutException
    {
        long deadlineNanos = nanoTime() + TimeUnit.SECONDS.toNanos(30);
        AtomicReference<TimeoutException> timeout = new AtomicReference<>(null);
        cluster.stream().filter(i -> !i.isShutdown()).forEach(inst -> {
            while (timeout.get() == null)
            {
                SimpleQueryResult pending = inst.executeInternalWithResult("SELECT store_generation, store_index, txn_id, status FROM system_accord.commands WHERE status < ? ALLOW FILTERING", Status.PreApplied.ordinal());
                pending = QueryResultUtil.map(pending, ImmutableMap.of(
                "txn_id", (ByteBuffer bb) -> AccordKeyspace.deserializeTimestampOrNull(bb, TxnId::new),
                "status", (Integer ordinal) -> Status.values()[ordinal]
                ));
                logger.info("[node{}] Pending:\n{}", inst.config().num(), QueryResultUtil.expand(pending));
                pending.reset();
                if (!pending.hasNext())
                    break;
                if (nanoTime() > deadlineNanos)
                {
                    pending.reset();
                    timeout.set(new TimeoutException("Timeout waiting on Accord Txn to complete; node" + inst.config().num() + " Pending:\n" + QueryResultUtil.expand(pending)));
                    break;
                }
                Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
            }
        });
        if (timeout.get() != null)
            throw timeout.get();
    }

    private static AccordTxnBuilder txn()
    {
        return new AccordTxnBuilder();
    }

    private static SimpleQueryResult execute(AccordTxnBuilder builder, boolean allowPreempted)
    {
        return execute(builder.build(), allowPreempted);
    }

    private static SimpleQueryResult execute(AccordTxnBuilder builder)
    {
        return execute(builder, false);
    }

    private static SimpleQueryResult execute(Txn txn)
    {
        return execute(txn, false);
    }

    private static SimpleQueryResult execute(Txn txn, boolean allowPreempted)
    {
        try
        {
            AccordData result = (AccordData) AccordService.instance().node.coordinate(txn).get();
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
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
        catch (ExecutionException e)
        {
            if (e.getCause() instanceof Preempted && allowPreempted)
                return null;
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
                default:
                    throw new IllegalArgumentException("Unsupported kind: " + column.kind);
            }
        }
        builder.row(buffer);
    }

//    @Test
//    public void acceptInvalidationTest()
//    {
//
//    }
//
//    @Test
//    public void applyAndCheckTest()
//    {
//
//    }
//
//    @Test
//    public void beginInvalidationTest()
//    {
//
//    }
//
//    @Test
//    public void checkStatusTest()
//    {
//
//    }
}
