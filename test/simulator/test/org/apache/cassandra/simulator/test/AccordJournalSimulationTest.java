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
package org.apache.cassandra.simulator.test;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.schema.*;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.Utils;
import accord.api.Data;
import accord.api.RoutingKey;
import accord.api.Update;
import accord.api.Write;
import accord.impl.TopologyUtils;
import accord.local.Node;
import accord.messages.PreAccept;
import accord.messages.TxnRequest;
import accord.primitives.FullKeyRoute;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.Files;
import org.apache.cassandra.journal.AsyncWriteCallback;
import org.apache.cassandra.service.accord.AccordJournal;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.txn.TxnNamedRead;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Isolated;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

public class AccordJournalSimulationTest extends SimulationTestBase
{
    @Test
    @Ignore // TODO: re-enable
    public void test() throws IOException
    {
        simulate(arr(() -> run()),
                 () -> check());
    }

    private static void run()
    {
        for (int i = 0; i < State.events; i++)
        {
            int finalI = i;
            State.executor.execute(() -> State.append(finalI));
        }

        try
        {
            State.eventsDurable.await();
            State.logger.info("All events are durable done!");
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }

        if (!State.exceptions.isEmpty())
        {
            AssertionError error = new AssertionError("Exceptions found during test");
            State.exceptions.forEach(error::addSuppressed);
            throw error;
        }

        State.journal.shutdown();
        State.logger.info("Run complete");
    }

    private static void check()
    {
        State.logger.info("Check starting");
        State.journal.start(); // to avoid a while true deadlock
        try
        {
            for (int i = 0; i < State.events; i++)
            {
                TxnRequest<?> event = State.journal.readMessage(State.toTxnId(i), AccordJournal.Type.PRE_ACCEPT, PreAccept.class);
                State.logger.info("Event {} -> {}", i, event);
                if (event == null)
                    throw new AssertionError(String.format("Unable to read event %d", i));
            }
            State.logger.info("Check complete");
        }
        finally
        {
            State.journal.shutdown();
        }
    }

    @Isolated
    public static class State
    {
        private static final Logger logger = LoggerFactory.getLogger(State.class);
        private static final String KEYSPACE = "test";

        static
        {
            Files.newGlobalInMemoryFileSystem();
            DatabaseDescriptor.clientWithDaemonConfig();
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
            DatabaseDescriptor.setAccordJournalDirectory("/journal");
            new File("/journal").createDirectoriesIfNotExists();
            DatabaseDescriptor.setCommitLogCompression(new ParameterizedClass("LZ4Compressor", ImmutableMap.of()));
            DatabaseDescriptor.setDumpHeapOnUncaughtException(false);

            // in order to do journal.read, we need all this setup first!
            Keyspace.setInitialized();
            Schema.instance.submit(SchemaTransformations.addKeyspace(KeyspaceMetadata.create(State.KEYSPACE, KeyspaceParams.simple(1)), true));
            Keyspace ks = Keyspace.open(State.KEYSPACE);
            ks.initCfCustom(ColumnFamilyStore.createColumnFamilyStore(ks, TableMetadataRef.forOfflineTools(TableMetadata.builder(State.KEYSPACE, State.KEYSPACE)
                                                                                                                        .addPartitionKeyColumn("pk", Int32Type.instance)
                                                                                                                        .build()).get(), false));

            try
            {
                CommitLog.instance.shutdownBlocking();
            }
            catch (InterruptedException e)
            {
                // ignore
            }
        }
        private static final ExecutorPlus executor = ExecutorFactory.Global.executorFactory().pooled("name", 10);
        private static final AccordJournal journal = new AccordJournal();
        private static final int events = 100;
        private static final CountDownLatch eventsWritten = CountDownLatch.newCountDownLatch(events);
        private static final CountDownLatch eventsDurable = CountDownLatch.newCountDownLatch(events);
        private static final List<Throwable> exceptions = new CopyOnWriteArrayList<>();

        static
        {
            journal.start();
        }

        public static void append(int event)
        {
            TxnRequest<?> request = toRequest(event);
            journal.appendMessage(request, executor, new AsyncWriteCallback()
            {
                @Override
                public void run()
                {
                    durable(event);
                }

                @Override
                public void onFailure(Throwable error)
                {
                    eventsDurable.decrement(); // to make sure we don't block forever
                    exceptions.add(error);
                }
            });
            eventsWritten.decrement();
            logger.info("append({}); remaining {}", event, eventsWritten.count());
        }

        private static void durable(int event)
        {
            eventsDurable.decrement();
            logger.info("durable({}); remaining {}", event, eventsDurable.count());
        }

        private static TxnRequest<?> toRequest(int event)
        {
            TxnId id = toTxnId(event);
            Ranges ranges = Ranges.of(new TokenRange(AccordRoutingKey.SentinelKey.min("system"), AccordRoutingKey.SentinelKey.max("system")));
            Topologies topologies = Utils.topologies(TopologyUtils.initialTopology(new Node.Id[] {node}, ranges, 3));
            Keys keys = Keys.of(toKey(0));
            Txn txn = new Txn.InMemory(keys, new TxnRead(new TxnNamedRead[0], keys), TxnQuery.ALL, new NoopUpdate());
            FullRoute<?> route = route();
            return new PreAccept(node, topologies, id, txn, route);
        }

        private static TxnId toTxnId(int event)
        {
            return TxnId.fromValues(1, event, 0, node);
        }

        private static PartitionKey toKey(int a)
        {
            return new PartitionKey(KEYSPACE, tableId, Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(a)));
        }

        private static final TableId tableId = TableId.fromUUID(new UUID(0, 0));
        private static final Node.Id node = new Node.Id(0);

        private static FullRoute<?> route()
        {
            return new FullKeyRoute(key, true, new RoutingKey[]{ key });
        }

        private static final RoutingKey key = new AccordRoutingKey.TokenKey("system", new Murmur3Partitioner.LongToken(42));
    }

    public static class NoopUpdate implements Update
    {
        @Override
        public Seekables<?, ?> keys()
        {
            return null;
        }

        @Override
        public Write apply(Timestamp executeAt, @Nullable Data data)
        {
            return null;
        }

        @Override
        public Update slice(Ranges ranges)
        {
            return null;
        }

        @Override
        public Update merge(Update other)
        {
            return null;
        }
    }
}
