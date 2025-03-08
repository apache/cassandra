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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.primitives.Keys;
import accord.primitives.TxnId;
import com.datastax.driver.core.utils.MoreFutures;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.metrics.HintsServiceMetrics;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MockMessagingService;
import org.apache.cassandra.net.MockMessagingSpy;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.PreserveTimestamp;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordResult;
import org.apache.cassandra.service.accord.AccordTestUtils;
import org.apache.cassandra.service.accord.IAccordService.IAccordResult;
import org.apache.cassandra.service.accord.TimeOnlyRequestBookkeeping.LatencyRequestBookkeeping;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.service.consensus.migration.ConsensusMigrationMutationHelper;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.MockFailureDetector;

import static org.apache.cassandra.Util.spinAssertEquals;
import static org.apache.cassandra.config.CassandraRelevantProperties.HINT_DISPATCH_INTERVAL_MS;
import static org.apache.cassandra.hints.HintsTestUtil.sendHintsAndResponses;
import static org.apache.cassandra.hints.HintsTestUtil.sendHintsWithRetryDifferentSystemUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class HintsServiceTest
{
    private static final String KEYSPACE = "hints_service_test";
    private static final String TABLE = "table";

    private final MockFailureDetector failureDetector = new MockFailureDetector();
    private static TableMetadata metadata;

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        StorageService.instance.initServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                KeyspaceParams.simple(1),
                SchemaLoader.standardCFMD(KEYSPACE, TABLE));
        metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        HINT_DISPATCH_INTERVAL_MS.setLong(100);
        DatabaseDescriptor.setHintsFlushPeriodInMS(100);
    }

    @After
    public void cleanup()
    {
        MockMessagingService.cleanup();
        ConsensusMigrationMutationHelper.resetInstanceForTest();
    }

    @Before
    public void reinstanciateService() throws Throwable
    {
        MessagingService.instance().inboundSink.clear();
        MessagingService.instance().outboundSink.clear();

        if (!HintsService.instance.isShutDown())
        {
            HintsService.instance.shutdownBlocking();
            HintsService.instance.deleteAllHints();
        }

        failureDetector.isAlive = true;

        HintsService.instance = new HintsService(failureDetector);

        HintsService.instance.startDispatch();
    }

    @Test
    public void testDispatchHints() throws InterruptedException, ExecutionException
    {
        long cnt = StorageMetrics.totalHints.getCount();

        // create spy for hint messages
        MockMessagingSpy spy = sendHintsAndResponses(metadata, 100, -1);

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
        MockMessagingSpy spy = sendHintsAndResponses(metadata, 100, -1);

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
        MockMessagingSpy spy = sendHintsAndResponses(metadata, 20, 5);

        Futures.allAsList(
                // the dispatcher will always send all hints within the current page
                // and only wait for the acks before going to the next page
                spy.interceptMessageOut(20),
                spy.interceptNoMsg(200, TimeUnit.MILLISECONDS),

                // next tick will trigger a retry of the same page as we only replied with 5/20 acks
                spy.interceptMessageOut(20)
        ).get();

        // marking the destination node as dead should stop sending hints
        failureDetector.isAlive = false;
        spy.interceptNoMsg(20, TimeUnit.SECONDS).get();
    }

    @Test
    public void testPageSeek() throws InterruptedException, ExecutionException
    {
        // create spy for hint messages, stop replying after 12k (should be on 3rd page)
        MockMessagingSpy spy = sendHintsAndResponses(metadata, 20000, 12000);

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

    /*
     * Make sure that if hints from the batchlog end up needing to be executed without Accord
     * that they are turned into
     */
    @Test
    public void testHintsNeedingRehinting() throws Throwable
    {
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(metadata.id);
        long startWrites =  cfs.metric.writeLatency.latency.getCount();
        HintsService.instance = spy(HintsService.instance);
        AtomicInteger accordTxnCount = new AtomicInteger();
        ConsensusMigrationMutationHelper.replaceInstanceForTest(
            new ConsensusMigrationMutationHelper()
                {
                    int count = 0;

                    @Override
                    public <T extends IMutation> SplitMutation<T> splitMutationIntoAccordAndNormal(T mutation, ClusterMetadata cm)
                    {
                        if (count > 2)
                            return super.splitMutationIntoAccordAndNormal(mutation, cm);

                        SplitMutation split;
                        if (count % 2 == 0)
                            split = new SplitMutation(mutation, null);
                        else
                            split = new SplitMutation<>(null, mutation);
                        count++;
                        return split;
                    }

                    @Override
                    public IAccordResult<TxnResult> mutateWithAccordAsync(ClusterMetadata cm, Mutation mutation, @Nullable ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime, PreserveTimestamp preserveTimestamps)
                    {
                        accordTxnCount.incrementAndGet();
                        TxnId txnId = AccordTestUtils.txnId(42, 43, 44);
                        AccordResult<TxnResult> result = new AccordResult<>(txnId, Keys.EMPTY, new LatencyRequestBookkeeping(null), requestTime.startedAtNanos(), requestTime.startedAtNanos(), true);
                        result.accept(new TxnData(), null);
                        return result;
                    }
                });
        sendHintsWithRetryDifferentSystemUUID(metadata);
        // Two should be Accord transactions
        spinAssertEquals(2, accordTxnCount::get, 10);
        Thread.sleep(1000);
        // An attempt should be made to write to all replicas
        verify(HintsService.instance, times(1)).writeForAllReplicas(notNull());
        // And it should be written locally
        spinAssertEquals(startWrites + 1L, cfs.metric.writeLatency.latency::getCount, 10);

        // Hints that are rehinted are treated as succeeding immediately for the ACCORD_HINT_ENDPOINT
        assertEquals(3, HintsServiceMetrics.getDelayCount(HintsServiceMetrics.ACCORD_HINT_ENDPOINT));
    }
}
