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
package org.apache.cassandra.hints;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.utils.MoreFutures;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MockMessagingService;
import org.apache.cassandra.net.MockMessagingSpy;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;

import static org.apache.cassandra.Util.dk;
import static org.apache.cassandra.config.CassandraRelevantProperties.SKIP_REWRITING_HINTS_ON_HOST_LEFT;
import static org.apache.cassandra.net.Verb.HINT_REQ;
import static org.apache.cassandra.net.Verb.HINT_RSP;
import static org.apache.cassandra.net.MockMessagingService.verb;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HintsServiceTest
{
    private static final String KEYSPACE = "hints_service_test";
    private static final String TABLE = "table";

    private final AtomicBoolean isAlive = new AtomicBoolean(true);

    @BeforeClass
    public static void defineSchema()
    {
        SKIP_REWRITING_HINTS_ON_HOST_LEFT.setBoolean(true);
        SchemaLoader.prepareServer();
        StorageService.instance.initServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE));
    }

    @AfterClass
    public static void tearDown()
    {
        System.clearProperty(SKIP_REWRITING_HINTS_ON_HOST_LEFT.getKey());
    }

    @After
    public void cleanup()
    {
        MockMessagingService.cleanup();
    }

    @Before
    public void reinstanciateService() throws Throwable
    {
        MessagingService.instance().inboundSink.clear();
        MessagingService.instance().outboundSink.clear();

        // Hint service has to know the endpoint version to be able to send the hints. Otherwise,
        // it wouldn't be able to make a decision whether to decode (rewrap) or not.
        MessagingService.instance().versions.set(
            HintsEndpointProvider.instance.endpointForHost(StorageService.instance.getLocalHostUUID()),
            MessagingService.current_version);

        if (!HintsService.instance.isShutDown())
        {
            HintsService.instance.shutdownBlocking();
            HintsService.instance.deleteAllHints();
        }

        isAlive.set(true);

        HintsService.instance = new HintsService(e -> isAlive.get());

        HintsService.instance.startDispatch();
    }

    @Test
    public void testHintsDroppedForUnknownHost()
    {
        // pause the scheduled dispatch before writing hints
        HintsService.instance.pauseDispatch();

        long totalHints = StorageMetrics.totalHints.getCount();

        // write 100 hints on disk for host that is not part of cluster
        UUID randomHost = UUID.randomUUID();
        int numHints = 100;

        HintsStore store = writeAndFlushHints(randomHost, numHints);
        assertTrue(store.hasFiles());

        // metrics should have been updated with number of create hints
        assertEquals(totalHints + numHints, StorageMetrics.totalHints.getCount());

        // re-enable dispatching
        HintsService.instance.resumeDispatch();

        // trigger a manual dispatching on host that is not part of cluster: hints should be dropped
        HintsService.instance.dispatcherExecutor().dispatch(store);
        ConditionFactory hints_dropped = Awaitility.await("Hints dropped");
        hints_dropped.atMost(30, TimeUnit.SECONDS);
        hints_dropped.untilAsserted(() ->
                                    assertEquals(totalHints + numHints,
                                                 StorageMetrics.totalHints.getCount())
        );
    }

    @Test
    public void testDispatchHints() throws InterruptedException, ExecutionException
    {
        long cnt = StorageMetrics.totalHints.getCount();

        // create spy for hint messages
        MockMessagingSpy spy = sendHintsAndResponses(100, -1);

        // metrics should have been updated with number of create hints
        assertEquals(cnt + 100, StorageMetrics.totalHints.getCount());

        // wait until hints have been send
        spy.interceptMessageOut(100).get();
        spy.interceptNoMsg(500, TimeUnit.MILLISECONDS).get();
    }

    @Test
    public void testPauseAndResume() throws InterruptedException, ExecutionException
    {
        HintsService.instance.pauseDispatch();

        // create spy for hint messages
        MockMessagingSpy spy = sendHintsAndResponses(100, -1);

        // we should not send any hints while paused
        ListenableFuture<Boolean> noMessagesWhilePaused = spy.interceptNoMsg(15, TimeUnit.SECONDS);
        Futures.addCallback(noMessagesWhilePaused, new MoreFutures.SuccessCallback<Boolean>()
        {
            public void onSuccess(@Nullable Boolean aBoolean)
            {
                HintsService.instance.resumeDispatch();
            }
        }, MoreExecutors.directExecutor());

        Futures.allAsList(
                noMessagesWhilePaused,
                spy.interceptMessageOut(100),
                spy.interceptNoMsg(200, TimeUnit.MILLISECONDS)
        ).get();
    }

    @Test
    public void testPageRetry() throws InterruptedException, ExecutionException, TimeoutException
    {
        // create spy for hint messages, but only create responses for 5 hints
        MockMessagingSpy spy = sendHintsAndResponses(20, 5);

        Futures.allAsList(
                // the dispatcher will always send all hints within the current page
                // and only wait for the acks before going to the next page
                spy.interceptMessageOut(20),
                spy.interceptNoMsg(200, TimeUnit.MILLISECONDS),

                // next tick will trigger a retry of the same page as we only replied with 5/20 acks
                spy.interceptMessageOut(20)
        ).get();

        // marking the destination node as dead should stop sending hints
        isAlive.set(false);
        spy.interceptNoMsg(20, TimeUnit.SECONDS).get();
    }

    @Test
    public void testPageSeek() throws InterruptedException, ExecutionException
    {
        // create spy for hint messages, stop replying after 12k (should be on 3rd page)
        MockMessagingSpy spy = sendHintsAndResponses(20000, 12000);

        // At this point the dispatcher will constantly retry the page we stopped acking,
        // thus we receive the same hints from the page multiple times and in total more than
        // all written hints. Lets just consume them for a while and then pause the dispatcher.
        spy.interceptMessageOut(22000).get();
        HintsService.instance.pauseDispatch();
        Thread.sleep(1000);

        // verify that we have a dispatch offset set for the page we're currently stuck at
        HintsStore store = HintsService.instance.getCatalog().get(StorageService.instance.getLocalHostUUID());
        HintsDescriptor descriptor = store.poll();
        store.offerFirst(descriptor); // add again for cleanup during re-instanciation
        InputPosition dispatchOffset = store.getDispatchOffset(descriptor);
        assertTrue(dispatchOffset != null);
        assertTrue(((ChecksummedDataInput.Position) dispatchOffset).sourcePosition > 0);
    }

    @Test
    public void testDeleteHintsForEndpoint() throws UnknownHostException
    {
        int numHints = 10;
        TokenMetadata tokenMeta = StorageService.instance.getTokenMetadata();
        InetAddressAndPort endpointToDeleteHints = InetAddressAndPort.getByName("1.1.1.1");
        UUID hostIdToDeleteHints = UUID.randomUUID();
        tokenMeta.updateHostId(hostIdToDeleteHints, endpointToDeleteHints);
        InetAddressAndPort anotherEndpoint = InetAddressAndPort.getByName("1.1.1.2");
        UUID anotherHostId = UUID.randomUUID();
        tokenMeta.updateHostId(anotherHostId, anotherEndpoint);

        HintsStore storeToDeleteHints = writeAndFlushHints(hostIdToDeleteHints, numHints);
        assertTrue(storeToDeleteHints.hasFiles());
        HintsStore anotherStore = writeAndFlushHints(anotherHostId, numHints);
        assertTrue(anotherStore.hasFiles());

        HintsService.instance.deleteAllHintsForEndpoint(endpointToDeleteHints);
        assertFalse(storeToDeleteHints.hasFiles());
        assertTrue(anotherStore.hasFiles());
        assertTrue(HintsService.instance.getCatalog().hasFiles());

        HintsService.instance.deleteAllHints();
        assertEquals(0, HintsService.instance.getTotalHintsSize());
        assertFalse(anotherStore.hasFiles());
    }

    private MockMessagingSpy sendHintsAndResponses(int noOfHints, int noOfResponses)
    {
        // create spy for hint messages, but only create responses for noOfResponses hints
        Message<NoPayload> message = Message.internalResponse(HINT_RSP, NoPayload.noPayload);

        MockMessagingSpy spy;
        if (noOfResponses != -1)
        {
            spy = MockMessagingService.when(verb(HINT_REQ)).respondN(message, noOfResponses);
        }
        else
        {
            spy = MockMessagingService.when(verb(HINT_REQ)).respond(message);
        }

        writeHints(StorageService.instance.getLocalHostUUID(), noOfHints);
        return spy;
    }

    private HintsStore writeAndFlushHints(UUID hostId, int noOfHints)
    {
        writeHints(hostId, noOfHints);
        HintsService.instance.flushAndFsyncBlockingly(Collections.singleton(hostId));

        // close the write so hints are available for dispatching
        HintsStore store = HintsService.instance.getCatalog().get(hostId);
        store.closeWriter();

        return store;
    }

    private void writeHints(UUID hostId, int noOfHints)
    {
        // create and write noOfHints using service
        for (int i = 0; i < noOfHints; i++)
        {
            long now = System.currentTimeMillis();
            DecoratedKey dkey = dk(String.valueOf(i));
            TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
            PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, dkey).timestamp(now);
            builder.row("column0").add("val", "value0");
            Hint hint = Hint.create(builder.buildAsMutation(), now);
            HintsService.instance.write(hostId, hint);
        }
    }
}
