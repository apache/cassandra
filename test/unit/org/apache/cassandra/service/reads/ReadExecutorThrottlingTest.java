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

package org.apache.cassandra.service.reads;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.cassandra.transport.Dispatcher;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.SimpleBuilders;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.service.reads.repair.AbstractReadRepair;

import static org.apache.cassandra.locator.ReplicaUtils.full;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ReadExecutorThrottlingTest extends AbstractReadResponseTest
{
    static EndpointsForToken targets;

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        targets = EndpointsForToken.of(Murmur3Partitioner.instance.getMinimumToken(),
                                       full(EP1),
                                       full(EP2),
                                       full(EP3)
        );
    }

    private static PartitionUpdate.Builder update(TableMetadata metadata, String key, Row... rows)
    {
        PartitionUpdate.Builder builder = new PartitionUpdate.Builder(metadata, dk(key), metadata.regularAndStaticColumns(), rows.length, false);
        for (Row row : rows)
        {
            builder.add(row);
        }
        return builder;
    }

    private static PartitionUpdate.Builder update(Row... rows)
    {
        return update(cfm, "key1", rows);
    }

    private static Row row(long timestamp, int clustering, int value)
    {
        SimpleBuilders.RowBuilder builder = new SimpleBuilders.RowBuilder(cfm, Integer.toString(clustering));
        builder.timestamp(timestamp).add("c1", Integer.toString(value));
        return builder.build();
    }

    @Test
    public void oneReplicaThrottlingDoSpeculativeRetry() throws InterruptedException
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        PartitionUpdate response = update(row(1000, 4, 4), row(1000, 5, 5)).build();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try
        {
            long speculativeRetriesDueToThrottlingBefore = cfs.metric.speculativeRetriesDueToThrottling.getCount();
            long speculativeReadRepairRetriesDueToThrottlingBefore = cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount();
            AbstractReadExecutor executor = new AbstractReadExecutor.SpeculatingReadExecutor(cfs, command,
                                                                                             new ReplicaPlan.ForTokenRead(ks, ks.getReplicationStrategy(), ConsistencyLevel.LOCAL_QUORUM, targets, targets.subList(0, 2)), Dispatcher.RequestTime.forImmediateExecution());
            CountDownLatch waitForTwoReplicas = new CountDownLatch(2);
            pool.execute(() ->
                         {
                             executor.handler.onFailure(EP1, RequestFailureReason.TRAFFIC_THROTTLED);
                             waitForTwoReplicas.countDown();
                         });
            pool.execute(() ->
                         {
                             executor.handler.onResponse(response(command, EP2, iter(response), true));
                             waitForTwoReplicas.countDown();
                         });
            waitForTwoReplicas.await();

            // Do speculative retry
            executor.maybeTryAdditionalReplicas();
            CountDownLatch waitForThirdReplica = new CountDownLatch(1);
            pool.execute(() ->
                         {
                             waitForThirdReplica.countDown();
                             executor.handler.onResponse(response(command, EP3, iter(response), true));
                         });

            waitForThirdReplica.await();

            executor.awaitResponses();
            Assert.assertTrue(executor.digestResolver.isDataPresent());
            Assert.assertTrue(executor.digestResolver.responsesMatch());
            assertEquals(speculativeRetriesDueToThrottlingBefore+1, cfs.metric.speculativeRetriesDueToThrottling.getCount());
            assertEquals(speculativeReadRepairRetriesDueToThrottlingBefore, cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount());
        }
        finally
        {
            pool.shutdown();
        }
    }

    @Test
    public void oneReplicaThrottlingNoSpeculativeRetry() throws InterruptedException
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        PartitionUpdate response = update(row(1000, 4, 4), row(1000, 5, 5)).build();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try
        {
            long speculativeRetriesDueToThrottlingBefore = cfs.metric.speculativeRetriesDueToThrottling.getCount();
            long speculativeReadRepairRetriesDueToThrottlingBefore = cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount();
            AbstractReadExecutor executor = new AbstractReadExecutor.SpeculatingReadExecutor(cfs, command,
                                                                                             new ReplicaPlan.ForTokenRead(ks, ks.getReplicationStrategy(), ConsistencyLevel.LOCAL_QUORUM, targets, targets.subList(0, 2)), Dispatcher.RequestTime.forImmediateExecution());
            CountDownLatch waitForTwoReplicas = new CountDownLatch(2);
            pool.execute(() ->
                         {
                             executor.handler.onFailure(EP1, RequestFailureReason.TRAFFIC_THROTTLED);
                             waitForTwoReplicas.countDown();
                         });
            pool.execute(() ->
                         {
                             executor.handler.onResponse(response(command, EP2, iter(response), true));
                             waitForTwoReplicas.countDown();
                         });
            waitForTwoReplicas.await();
            assertEquals(speculativeRetriesDueToThrottlingBefore, cfs.metric.speculativeRetriesDueToThrottling.getCount());
            assertEquals(speculativeReadRepairRetriesDueToThrottlingBefore, cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount());

            executor.awaitResponses();
            Assert.fail("An exception should be thrown");
        }
        catch (ReadFailureException e)
        {
            assertThat(e.getMessage(), containsString("TRAFFIC_THROTTLED from /127.0.0"));
        }
        finally
        {
            pool.shutdown();
        }
    }

    @Test
    public void twoReplicasThrottlingDoSpeculativeRetry() throws InterruptedException
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        PartitionUpdate response = update(row(1000, 4, 4), row(1000, 5, 5)).build();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try
        {
            long speculativeRetriesDueToThrottlingBefore = cfs.metric.speculativeRetriesDueToThrottling.getCount();
            long speculativeReadRepairRetriesDueToThrottlingBefore = cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount();
            AbstractReadExecutor executor = new AbstractReadExecutor.SpeculatingReadExecutor(cfs, command,
                                                                                             new ReplicaPlan.ForTokenRead(ks, ks.getReplicationStrategy(), ConsistencyLevel.LOCAL_QUORUM, targets, targets.subList(0, 2)), Dispatcher.RequestTime.forImmediateExecution());
            CountDownLatch waitForTwoReplicas = new CountDownLatch(2);
            pool.execute(() ->
                         {
                             executor.handler.onFailure(EP1, RequestFailureReason.TRAFFIC_THROTTLED);
                             waitForTwoReplicas.countDown();
                         });
            pool.execute(() ->
                         {
                             executor.handler.onResponse(response(command, EP2, iter(response), true));
                             waitForTwoReplicas.countDown();
                         });
            waitForTwoReplicas.await();

            // Do speculative retry
            executor.maybeTryAdditionalReplicas();
            CountDownLatch waitForThirdReplica = new CountDownLatch(1);
            pool.execute(() ->
                         {
                             waitForThirdReplica.countDown();
                             executor.handler.onFailure(EP3, RequestFailureReason.TRAFFIC_THROTTLED);
                         });

            waitForThirdReplica.await();

            assertEquals(speculativeRetriesDueToThrottlingBefore+1, cfs.metric.speculativeRetriesDueToThrottling.getCount());
            assertEquals(speculativeReadRepairRetriesDueToThrottlingBefore, cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount());
            executor.awaitResponses();
            Assert.fail("An exception should be thrown");
        }
        catch (ReadFailureException e)
        {
            assertThat(e.getMessage(), containsString("TRAFFIC_THROTTLED from /127.0.0"));
        }
        finally
        {
            pool.shutdown();
        }
    }

    @Test
    public void twoReplicasSomeOtherErrorDoSpeculativeRetry() throws InterruptedException
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try
        {
            long speculativeRetriesDueToThrottlingBefore = cfs.metric.speculativeRetriesDueToThrottling.getCount();
            long speculativeReadRepairRetriesDueToThrottlingBefore = cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount();
            AbstractReadExecutor executor = new AbstractReadExecutor.SpeculatingReadExecutor(cfs, command,
                                                                                             new ReplicaPlan.ForTokenRead(ks, ks.getReplicationStrategy(), ConsistencyLevel.LOCAL_QUORUM, targets, targets.subList(0, 2)), Dispatcher.RequestTime.forImmediateExecution());
            CountDownLatch waitForTwoReplicas = new CountDownLatch(2);
            pool.execute(() ->
                         {
                             executor.handler.onFailure(EP1, RequestFailureReason.TRAFFIC_THROTTLED);
                             waitForTwoReplicas.countDown();
                         });
            pool.execute(() ->
                         {
                             executor.handler.onFailure(EP2, RequestFailureReason.UNKNOWN);
                             waitForTwoReplicas.countDown();
                         });
            waitForTwoReplicas.await();

            // Do speculative retry
            executor.maybeTryAdditionalReplicas();
            CountDownLatch waitForThirdReplica = new CountDownLatch(1);
            pool.execute(() ->
                         {
                             waitForThirdReplica.countDown();
                             executor.handler.onFailure(EP3, RequestFailureReason.UNKNOWN);
                         });

            waitForThirdReplica.await();

            assertEquals(speculativeRetriesDueToThrottlingBefore+1, cfs.metric.speculativeRetriesDueToThrottling.getCount());
            assertEquals(speculativeReadRepairRetriesDueToThrottlingBefore, cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount());
            executor.awaitResponses();
            Assert.fail("An exception should be thrown");
        }
        catch (ReadFailureException e)
        {
            assertThat(e.getMessage(), containsString("TRAFFIC_THROTTLED from /127.0.0"));
        }
        finally
        {
            pool.shutdown();
        }
    }

    @Test
    public void oneReplicaSomeOtherErrorNoSpeculativeRetry() throws InterruptedException
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try
        {
            long speculativeRetriesDueToThrottlingBefore = cfs.metric.speculativeRetriesDueToThrottling.getCount();
            long speculativeReadRepairRetriesDueToThrottlingBefore = cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount();
            AbstractReadExecutor executor = new AbstractReadExecutor.SpeculatingReadExecutor(cfs, command,
                                                                                             new ReplicaPlan.ForTokenRead(ks, ks.getReplicationStrategy(), ConsistencyLevel.LOCAL_QUORUM, targets, targets.subList(0, 2)), Dispatcher.RequestTime.forImmediateExecution());
            CountDownLatch waitForOneReplica = new CountDownLatch(1);
            pool.execute(() ->
                         {
                             executor.handler.onFailure(EP1, RequestFailureReason.UNKNOWN);
                             waitForOneReplica.countDown();
                         });
            waitForOneReplica.await();

            // Do speculative retry, but it should not trigger any speculative retry
            executor.maybeTryAdditionalReplicas();
            assertEquals(speculativeRetriesDueToThrottlingBefore, cfs.metric.speculativeRetriesDueToThrottling.getCount());
            assertEquals(speculativeReadRepairRetriesDueToThrottlingBefore, cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount());
            executor.awaitResponses();
            Assert.fail("An exception should be thrown");
        }
        catch (ReadFailureException e)
        {
            assertThat(e.getMessage(), containsString("Operation failed - received 0 responses and 1 failures"));
        }
        finally
        {
            pool.shutdown();
        }
    }


    @Test
    public void twoReplicasThrottlingNoSpeculativeRetry() throws InterruptedException
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try
        {
            long speculativeRetriesDueToThrottlingBefore = cfs.metric.speculativeRetriesDueToThrottling.getCount();
            long speculativeReadRepairRetriesDueToThrottlingBefore = cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount();
            AbstractReadExecutor executor = new AbstractReadExecutor.SpeculatingReadExecutor(cfs, command,
                                                                                             new ReplicaPlan.ForTokenRead(ks, ks.getReplicationStrategy(), ConsistencyLevel.LOCAL_QUORUM, targets, targets.subList(0, 2)), Dispatcher.RequestTime.forImmediateExecution());
            CountDownLatch waitForTwoReplicas = new CountDownLatch(2);
            pool.execute(() ->
                         {
                             executor.handler.onFailure(EP1, RequestFailureReason.TRAFFIC_THROTTLED);
                             waitForTwoReplicas.countDown();
                         });
            pool.execute(() ->
                         {
                             executor.handler.onFailure(EP2, RequestFailureReason.TRAFFIC_THROTTLED);
                             waitForTwoReplicas.countDown();
                         });
            waitForTwoReplicas.await();
            assertEquals(speculativeRetriesDueToThrottlingBefore, cfs.metric.speculativeRetriesDueToThrottling.getCount());
            assertEquals(speculativeReadRepairRetriesDueToThrottlingBefore, cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount());
            executor.awaitResponses();
            Assert.fail("An exception should be thrown");
        }
        catch (ReadFailureException e)
        {
            assertThat(e.getMessage(), containsString("TRAFFIC_THROTTLED from /127.0.0"));
        }
        finally
        {
            pool.shutdown();
        }
    }

    @Test
    public void oneReplicaTimeOutNoSpeculativeRetry() throws InterruptedException
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        PartitionUpdate response = update(row(1000, 4, 4), row(1000, 5, 5)).build();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try
        {
            long speculativeRetriesDueToThrottlingBefore = cfs.metric.speculativeRetriesDueToThrottling.getCount();
            long speculativeReadRepairRetriesDueToThrottlingBefore = cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount();
            AbstractReadExecutor executor = new AbstractReadExecutor.SpeculatingReadExecutor(cfs, command,
                                                                                             new ReplicaPlan.ForTokenRead(ks, ks.getReplicationStrategy(), ConsistencyLevel.LOCAL_QUORUM, targets, targets.subList(0, 2)), Dispatcher.RequestTime.forImmediateExecution());
            CountDownLatch waitForTwoReplicas = new CountDownLatch(2);
            pool.execute(() ->
                         {
                             executor.handler.onFailure(EP1, RequestFailureReason.TIMEOUT);
                             waitForTwoReplicas.countDown();
                         });
            pool.execute(() ->
                         {
                             executor.handler.onResponse(response(command, EP2, iter(response), true));
                             waitForTwoReplicas.countDown();
                         });
            waitForTwoReplicas.await();
            assertEquals(speculativeRetriesDueToThrottlingBefore, cfs.metric.speculativeRetriesDueToThrottling.getCount());
            assertEquals(speculativeReadRepairRetriesDueToThrottlingBefore, cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount());
            executor.awaitResponses();
            Assert.fail("An exception should be thrown");
        }
        catch (ReadFailureException e)
        {
            assertThat(e.getMessage(), containsString("TIMEOUT from /127.0.0"));
        }
        finally
        {
            pool.shutdown();
        }
    }

    @Test
    public void noReplicasRespond()
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try
        {
            long speculativeRetriesDueToThrottlingBefore = cfs.metric.speculativeRetriesDueToThrottling.getCount();
            long speculativeReadRepairRetriesDueToThrottlingBefore = cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount();
            AbstractReadExecutor executor = new AbstractReadExecutor.SpeculatingReadExecutor(cfs, command,
                                                                                             new ReplicaPlan.ForTokenRead(ks, ks.getReplicationStrategy(), ConsistencyLevel.LOCAL_QUORUM, targets, targets.subList(0, 2)), Dispatcher.RequestTime.forImmediateExecution());
            assertEquals(speculativeRetriesDueToThrottlingBefore, cfs.metric.speculativeRetriesDueToThrottling.getCount());
            assertEquals(speculativeReadRepairRetriesDueToThrottlingBefore, cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount());
            executor.awaitResponses();
            Assert.fail("An exception should be thrown");
        }
        catch (ReadTimeoutException e)
        {
            assertThat(e.getMessage(), containsString("Operation timed out - received only 0 responses."));
        }
        finally
        {
            pool.shutdown();
        }
    }

    @Test
    public void onlyOneReplicaResponds() throws InterruptedException
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        PartitionUpdate response = update(row(1000, 4, 4), row(1000, 5, 5)).build();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try
        {
            long speculativeRetriesDueToThrottlingBefore = cfs.metric.speculativeRetriesDueToThrottling.getCount();
            long speculativeReadRepairRetriesDueToThrottlingBefore = cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount();
            AbstractReadExecutor executor = new AbstractReadExecutor.SpeculatingReadExecutor(cfs, command,
                                                                                             new ReplicaPlan.ForTokenRead(ks, ks.getReplicationStrategy(), ConsistencyLevel.LOCAL_QUORUM, targets, targets.subList(0, 2)), Dispatcher.RequestTime.forImmediateExecution());
            CountDownLatch waitForTwoReplicas = new CountDownLatch(1);
            pool.execute(() ->
                         {
                             executor.handler.onResponse(response(command, EP2, iter(response), true));
                             waitForTwoReplicas.countDown();
                         });
            waitForTwoReplicas.await();
            assertEquals(speculativeRetriesDueToThrottlingBefore, cfs.metric.speculativeRetriesDueToThrottling.getCount());
            assertEquals(speculativeReadRepairRetriesDueToThrottlingBefore, cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount());
            executor.awaitResponses();
            Assert.fail("An exception should be thrown");
        }
        catch (ReadTimeoutException e)
        {
            assertThat(e.getMessage(), containsString("Operation timed out - received only 1 responses."));
        }
        finally
        {
            pool.shutdown();
        }
    }

    @Test
    public void onlyOneReplicaRespondsWithThrottlingError() throws InterruptedException
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try
        {
            long speculativeRetriesDueToThrottlingBefore = cfs.metric.speculativeRetriesDueToThrottling.getCount();
            long speculativeReadRepairRetriesDueToThrottlingBefore = cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount();
            AbstractReadExecutor executor = new AbstractReadExecutor.SpeculatingReadExecutor(cfs, command,
                                                                                             new ReplicaPlan.ForTokenRead(ks, ks.getReplicationStrategy(), ConsistencyLevel.LOCAL_QUORUM, targets, targets.subList(0, 2)), Dispatcher.RequestTime.forImmediateExecution());
            CountDownLatch waitForTwoReplicas = new CountDownLatch(1);
            pool.execute(() ->
                         {
                             executor.handler.onFailure(EP1, RequestFailureReason.TRAFFIC_THROTTLED);
                             waitForTwoReplicas.countDown();
                         });
            waitForTwoReplicas.await();
            assertEquals(speculativeRetriesDueToThrottlingBefore, cfs.metric.speculativeRetriesDueToThrottling.getCount());
            assertEquals(speculativeReadRepairRetriesDueToThrottlingBefore, cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount());
            executor.awaitResponses();
            Assert.fail("An exception should be thrown");
        }
        catch (ReadFailureException e)
        {
            assertThat(e.getMessage(), containsString("RAFFIC_THROTTLED from /127.0.0"));
        }
        finally
        {
            pool.shutdown();
        }
    }

    @Test
    public void onlyOneReplicaRespondsWithUnknownError() throws InterruptedException
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try
        {
            long speculativeRetriesDueToThrottlingBefore = cfs.metric.speculativeRetriesDueToThrottling.getCount();
            long speculativeReadRepairRetriesDueToThrottlingBefore = cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount();
            AbstractReadExecutor executor = new AbstractReadExecutor.SpeculatingReadExecutor(cfs, command,
                                                                                             new ReplicaPlan.ForTokenRead(ks, ks.getReplicationStrategy(), ConsistencyLevel.LOCAL_QUORUM, targets, targets.subList(0, 2)), Dispatcher.RequestTime.forImmediateExecution());
            CountDownLatch waitForTwoReplicas = new CountDownLatch(1);
            pool.execute(() ->
                         {
                             executor.handler.onFailure(EP1, RequestFailureReason.UNKNOWN);
                             waitForTwoReplicas.countDown();
                         });
            waitForTwoReplicas.await();
            assertEquals(speculativeRetriesDueToThrottlingBefore, cfs.metric.speculativeRetriesDueToThrottling.getCount());
            assertEquals(speculativeReadRepairRetriesDueToThrottlingBefore, cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount());
            executor.awaitResponses();
            Assert.fail("An exception should be thrown");
        }
        catch (ReadFailureException e)
        {
            assertThat(e.getMessage(), containsString("UNKNOWN from /127.0.0"));
        }
        finally
        {
            pool.shutdown();
        }
    }

    @Test
    public void noThrottlingSpeculativeRetry() throws InterruptedException
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        PartitionUpdate response = update(row(1000, 4, 4), row(1000, 5, 5)).build();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try
        {
            long speculativeRetriesDueToThrottlingBefore = cfs.metric.speculativeRetriesDueToThrottling.getCount();
            long speculativeReadRepairRetriesDueToThrottlingBefore = cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount();
            AbstractReadExecutor executor = new AbstractReadExecutor.SpeculatingReadExecutor(cfs, command,
                                                                                             new ReplicaPlan.ForTokenRead(ks, ks.getReplicationStrategy(), ConsistencyLevel.LOCAL_QUORUM, targets, targets.subList(0, 2)), Dispatcher.RequestTime.forImmediateExecution());
            CountDownLatch waitForTwoReplicas = new CountDownLatch(2);
            pool.execute(() ->
                         {
                             executor.handler.onResponse(response(command, EP1, iter(response), true));
                             waitForTwoReplicas.countDown();
                         });
            pool.execute(() ->
                         {
                             executor.handler.onResponse(response(command, EP2, iter(response), true));
                             waitForTwoReplicas.countDown();
                         });
            waitForTwoReplicas.await();

            // Do speculative retry
            executor.maybeTryAdditionalReplicas();
            CountDownLatch waitForThirdReplica = new CountDownLatch(1);
            pool.execute(() ->
                         {
                             waitForThirdReplica.countDown();
                             executor.handler.onResponse(response(command, EP3, iter(response), true));
                         });

            waitForThirdReplica.await();

            executor.awaitResponses();
            Assert.assertTrue(executor.digestResolver.isDataPresent());
            Assert.assertTrue(executor.digestResolver.responsesMatch());
            assertEquals(speculativeRetriesDueToThrottlingBefore, cfs.metric.speculativeRetriesDueToThrottling.getCount());
            assertEquals(speculativeReadRepairRetriesDueToThrottlingBefore, cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount());
        }
        finally
        {
            pool.shutdown();
        }
    }

    @Test
    public void noThrottlingNoSpeculativeRetry() throws InterruptedException
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        PartitionUpdate response = update(row(1000, 4, 4), row(1000, 5, 5)).build();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try
        {
            long speculativeRetriesDueToThrottlingBefore = cfs.metric.speculativeRetriesDueToThrottling.getCount();
            long speculativeReadRepairRetriesDueToThrottlingBefore = cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount();
            AbstractReadExecutor executor = new AbstractReadExecutor.SpeculatingReadExecutor(cfs, command,
                                                                                             new ReplicaPlan.ForTokenRead(ks, ks.getReplicationStrategy(), ConsistencyLevel.LOCAL_QUORUM, targets, targets.subList(0, 2)), Dispatcher.RequestTime.forImmediateExecution());
            CountDownLatch waitForTwoReplicas = new CountDownLatch(2);
            pool.execute(() ->
                         {
                             executor.handler.onResponse(response(command, EP1, iter(response), true));
                             waitForTwoReplicas.countDown();
                         });
            pool.execute(() ->
                         {
                             executor.handler.onResponse(response(command, EP2, iter(response), true));
                             waitForTwoReplicas.countDown();
                         });
            waitForTwoReplicas.await();
            executor.awaitResponses();
            Assert.assertTrue(executor.digestResolver.isDataPresent());
            Assert.assertTrue(executor.digestResolver.responsesMatch());
            assertEquals(speculativeRetriesDueToThrottlingBefore, cfs.metric.speculativeRetriesDueToThrottling.getCount());
            assertEquals(speculativeReadRepairRetriesDueToThrottlingBefore, cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount());
        }
        finally
        {
            pool.shutdown();
        }
    }

    @Test
    public void testSpeculativeReadRepairRetrySuccess() throws InterruptedException
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        PartitionUpdate response1 = update(row(1000, 4, 4), row(1000, 5, 5)).build();
        PartitionUpdate response2 = update(row(1000, 4, 41), row(1000, 5, 51)).build();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try
        {
            long speculativeReadRepairRetriesDueToThrottlingBefore = cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount();
            AbstractReadExecutor executor = new AbstractReadExecutor.SpeculatingReadExecutor(cfs, command,
                                                                                             new ReplicaPlan.ForTokenRead(ks, ks.getReplicationStrategy(), ConsistencyLevel.LOCAL_QUORUM, targets, targets.subList(0, 2)), Dispatcher.RequestTime.forImmediateExecution());
            CountDownLatch waitForTwoReplicas = new CountDownLatch(2);
            pool.execute(() ->
                         {
                             executor.handler.onResponse(response(command, EP1, iter(response1), true));
                             waitForTwoReplicas.countDown();
                         });
            pool.execute(() ->
                         {
                             executor.handler.onResponse(response(command, EP2, iter(response2), true));
                             waitForTwoReplicas.countDown();
                         });
            waitForTwoReplicas.await();

            executor.awaitResponses();

            CountDownLatch rrLatch = new CountDownLatch(2);
            AbstractReadRepair<EndpointsForToken, ReplicaPlan.ForTokenRead> rr = (AbstractReadRepair<EndpointsForToken, ReplicaPlan.ForTokenRead>) executor.readRepair;

            pool.execute(() ->
                         {
                             rr.digestRepair.readCallback.onResponse(response(command, EP1, iter(response1), true));
                             rrLatch.countDown();
                         });
            pool.execute(() ->
                         {
                             rr.digestRepair.readCallback.onFailure(EP2, RequestFailureReason.TRAFFIC_THROTTLED);
                             rrLatch.countDown();
                         });
            rrLatch.await();


            rr.maybeSendAdditionalReads();

            CountDownLatch waitForThirdReplica = new CountDownLatch(1);
            pool.execute(() ->
                         {
                             rr.digestRepair.readCallback.onResponse(response(command, EP3, iter(response1), true));
                             waitForThirdReplica.countDown();
                         });

            waitForThirdReplica.await();
            assertEquals(speculativeReadRepairRetriesDueToThrottlingBefore+1, cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount());
            rr.awaitReads();
        }
        catch (ReadFailureException e)
        {
            Assert.fail("No exception should be thrown");
        }
        finally
        {
            pool.shutdown();
        }
    }

    @Test
    public void testSpeculativeReadRepairRetryFailre() throws InterruptedException
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        PartitionUpdate response1 = update(row(1000, 4, 4), row(1000, 5, 5)).build();
        PartitionUpdate response2 = update(row(1000, 4, 41), row(1000, 5, 51)).build();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try
        {
            long speculativeReadRepairRetriesDueToThrottlingBefore = cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount();
            AbstractReadExecutor executor = new AbstractReadExecutor.SpeculatingReadExecutor(cfs, command,
                                                                                             new ReplicaPlan.ForTokenRead(ks, ks.getReplicationStrategy(), ConsistencyLevel.LOCAL_QUORUM, targets, targets.subList(0, 2)), Dispatcher.RequestTime.forImmediateExecution());
            CountDownLatch waitForTwoReplicas = new CountDownLatch(2);
            pool.execute(() ->
                         {
                             executor.handler.onResponse(response(command, EP1, iter(response1), true));
                             waitForTwoReplicas.countDown();
                         });
            pool.execute(() ->
                         {
                             executor.handler.onResponse(response(command, EP2, iter(response2), true));
                             waitForTwoReplicas.countDown();
                         });
            waitForTwoReplicas.await();

            executor.awaitResponses();

            CountDownLatch rrLatch = new CountDownLatch(2);
            AbstractReadRepair<EndpointsForToken, ReplicaPlan.ForTokenRead> rr = (AbstractReadRepair<EndpointsForToken, ReplicaPlan.ForTokenRead>) executor.readRepair;

            pool.execute(() ->
                         {
                             rr.digestRepair.readCallback.onResponse(response(command, EP1, iter(response1), true));
                             rrLatch.countDown();
                         });
            pool.execute(() ->
                         {
                             rr.digestRepair.readCallback.onFailure(EP2, RequestFailureReason.TRAFFIC_THROTTLED);
                             rrLatch.countDown();
                         });
            rrLatch.await();


            rr.maybeSendAdditionalReads();

            CountDownLatch waitForThirdReplica = new CountDownLatch(1);
            pool.execute(() ->
                         {
                             rr.digestRepair.readCallback.onFailure(EP3, RequestFailureReason.TRAFFIC_THROTTLED);
                             waitForThirdReplica.countDown();
                         });

            waitForThirdReplica.await();

            assertEquals(speculativeReadRepairRetriesDueToThrottlingBefore+1, cfs.metric.speculativeReadRepairRetriesDueToThrottling.getCount());
            rr.awaitReads();
        }
        catch (ReadFailureException e)
        {
            assertThat(e.getMessage(), containsString("TRAFFIC_THROTTLED from /127.0.0"));
        }
        finally
        {
            pool.shutdown();
        }
    }
}
