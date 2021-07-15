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
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.locator.ReplicaPlan;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.SimpleBuilders;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.locator.ReplicaUtils.full;
import static org.apache.cassandra.locator.ReplicaUtils.trans;

public class DigestResolverTest extends AbstractReadResponseTest
{
    private static PartitionUpdate.Builder update(TableMetadata metadata, String key, Row... rows)
    {
        PartitionUpdate.Builder builder = new PartitionUpdate.Builder(metadata, dk(key), metadata.regularAndStaticColumns(), rows.length, false);
        for (Row row: rows)
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
    public void noRepairNeeded()
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        EndpointsForToken targetReplicas = EndpointsForToken.of(dk.getToken(), full(EP1), full(EP2));
        DigestResolver resolver = new DigestResolver(command, plan(ConsistencyLevel.QUORUM, targetReplicas), 0);

        PartitionUpdate response = update(row(1000, 4, 4), row(1000, 5, 5)).build();

        Assert.assertFalse(resolver.isDataPresent());
        resolver.preprocess(response(command, EP2, iter(response), true));
        resolver.preprocess(response(command, EP1, iter(response), false));
        Assert.assertTrue(resolver.isDataPresent());
        Assert.assertTrue(resolver.responsesMatch());

        assertPartitionsEqual(filter(iter(response)), resolver.getData());
    }

    /**
     * This test makes a time-boxed effort to reproduce the issue found in CASSANDRA-16807.
     */
    @Test
    public void multiThreadedNoRepairNeededReadCallback()
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        EndpointsForToken targetReplicas = EndpointsForToken.of(dk.getToken(), full(EP1), full(EP2));
        PartitionUpdate response = update(row(1000, 4, 4), row(1000, 5, 5)).build();
        ReplicaPlan.SharedForTokenRead plan = plan(ConsistencyLevel.ONE, targetReplicas);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        long endTime = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);

        try
        {
            while (System.nanoTime() < endTime)
            {
                final long startNanos = System.nanoTime();
                final DigestResolver<EndpointsForToken, ReplicaPlan.ForTokenRead> resolver = new DigestResolver<>(command, plan, startNanos);
                final ReadCallback<EndpointsForToken, ReplicaPlan.ForTokenRead> callback = new ReadCallback<>(resolver, command, plan, startNanos);
                
                final CountDownLatch startlatch = new CountDownLatch(2);

                pool.execute(() ->
                             {
                                 startlatch.countDown();
                                 waitForLatch(startlatch);
                                 callback.onResponse(response(command, EP1, iter(response), true));
                             });

                pool.execute(() ->
                             {
                                 startlatch.countDown();
                                 waitForLatch(startlatch);
                                 callback.onResponse(response(command, EP2, iter(response), true));
                             });

                callback.awaitResults();
                Assert.assertTrue(resolver.isDataPresent());
                Assert.assertTrue(resolver.responsesMatch());
            }
        }
        finally
        {
            pool.shutdown();
        }
    }

    @Test
    public void digestMismatch()
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        EndpointsForToken targetReplicas = EndpointsForToken.of(dk.getToken(), full(EP1), full(EP2));
        DigestResolver resolver = new DigestResolver(command, plan(ConsistencyLevel.QUORUM, targetReplicas), 0);

        PartitionUpdate response1 = update(row(1000, 4, 4), row(1000, 5, 5)).build();
        PartitionUpdate response2 = update(row(2000, 4, 5)).build();

        Assert.assertFalse(resolver.isDataPresent());
        resolver.preprocess(response(command, EP2, iter(response1), true));
        resolver.preprocess(response(command, EP1, iter(response2), false));
        Assert.assertTrue(resolver.isDataPresent());
        Assert.assertFalse(resolver.responsesMatch());
        Assert.assertFalse(resolver.hasTransientResponse());
    }

    /**
     * A full response and a transient response, with the transient response being a subset of the full one
     */
    @Test
    public void agreeingTransient()
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        EndpointsForToken targetReplicas = EndpointsForToken.of(dk.getToken(), full(EP1), trans(EP2));
        DigestResolver<?, ?> resolver = new DigestResolver<>(command, plan(ConsistencyLevel.QUORUM, targetReplicas), 0);

        PartitionUpdate response1 = update(row(1000, 4, 4), row(1000, 5, 5)).build();
        PartitionUpdate response2 = update(row(1000, 5, 5)).build();

        Assert.assertFalse(resolver.isDataPresent());
        resolver.preprocess(response(command, EP1, iter(response1), false));
        resolver.preprocess(response(command, EP2, iter(response2), false));
        Assert.assertTrue(resolver.isDataPresent());
        Assert.assertTrue(resolver.responsesMatch());
        Assert.assertTrue(resolver.hasTransientResponse());
    }

    /**
     * Transient responses shouldn't be classified as the single dataResponse
     */
    @Test
    public void transientResponse()
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        EndpointsForToken targetReplicas = EndpointsForToken.of(dk.getToken(), full(EP1), trans(EP2));
        DigestResolver<?, ?> resolver = new DigestResolver<>(command, plan(ConsistencyLevel.QUORUM, targetReplicas), 0);

        PartitionUpdate response2 = update(row(1000, 5, 5)).build();
        Assert.assertFalse(resolver.isDataPresent());
        Assert.assertFalse(resolver.hasTransientResponse());
        resolver.preprocess(response(command, EP2, iter(response2), false));
        Assert.assertFalse(resolver.isDataPresent());
        Assert.assertTrue(resolver.hasTransientResponse());
    }

    @Test
    public void transientResponseData()
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        EndpointsForToken targetReplicas = EndpointsForToken.of(dk.getToken(), full(EP1), full(EP2), trans(EP3));
        DigestResolver<?, ?> resolver = new DigestResolver<>(command, plan(ConsistencyLevel.QUORUM, targetReplicas), 0);

        PartitionUpdate fullResponse = update(row(1000, 1, 1)).build();
        PartitionUpdate digestResponse = update(row(1000, 1, 1)).build();
        PartitionUpdate transientResponse = update(row(1000, 2, 2)).build();
        Assert.assertFalse(resolver.isDataPresent());
        Assert.assertFalse(resolver.hasTransientResponse());
        resolver.preprocess(response(command, EP1, iter(fullResponse), false));
        Assert.assertTrue(resolver.isDataPresent());
        resolver.preprocess(response(command, EP2, iter(digestResponse), true));
        resolver.preprocess(response(command, EP3, iter(transientResponse), false));
        Assert.assertTrue(resolver.hasTransientResponse());

        assertPartitionsEqual(filter(iter(dk,
                                          row(1000, 1, 1),
                                          row(1000, 2, 2))),
                              resolver.getData());
    }

    private ReplicaPlan.SharedForTokenRead plan(ConsistencyLevel consistencyLevel, EndpointsForToken replicas)
    {
        return ReplicaPlan.shared(new ReplicaPlan.ForTokenRead(ks, ks.getReplicationStrategy(), consistencyLevel, replicas, replicas));
    }

    private void waitForLatch(CountDownLatch startlatch)
    {
        try
        {
            startlatch.await();
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }
}
