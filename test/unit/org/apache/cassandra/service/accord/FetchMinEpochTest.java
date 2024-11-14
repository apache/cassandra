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

package org.apache.cassandra.service.accord;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.RetrySpec;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.IVersionedSerializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.SimulatedMessageDelivery.Action;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.RoutingKeyKind;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.SimulatedMiniCluster;
import org.apache.cassandra.utils.SimulatedMiniCluster.Node;
import org.apache.cassandra.utils.concurrent.Future;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;
import static org.apache.cassandra.net.MessagingService.Version.VERSION_51;
import static org.apache.cassandra.utils.AccordGenerators.fromQT;
import static org.assertj.core.api.Assertions.assertThat;

public class FetchMinEpochTest
{
    static
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    private static final Gen<Gen<Action>> ACTION_DISTRIBUTION = Gens.enums().allMixedDistribution(Action.class);
    private static final List<MessagingService.Version> SUPPORTED = Stream.of(MessagingService.Version.values()).filter(v -> v.compareTo(VERSION_51) >= 0).collect(Collectors.toList());

    private static void boundedRetries(int retries)
    {
        DatabaseDescriptor.getAccord().minEpochSyncRetry.maxAttempts = new RetrySpec.MaxAttempt(retries);
    }

    @Test
    public void requestSerde()
    {
        DataOutputBuffer output = new DataOutputBuffer();
        Gen<FetchMinEpoch> gen = fromQT(CassandraGenerators.partitioners())
                                 .map(CassandraGenerators::simplify)
                                 .flatMap(partitioner ->
                                          Gens.lists(AccordGenerators.range(partitioner)
                                                                     .map(r -> (TokenRange) r))
                                              .ofSizeBetween(0, 10)
                                              .map(FetchMinEpoch::new));
        qt().forAll(gen).check(req -> {
            maybeSetPartitioner(req);
            for (MessagingService.Version version : SUPPORTED)
                IVersionedSerializers.testSerde(output, FetchMinEpoch.serializer, req, version.value);
        });
    }

    @Test
    public void responseSerde()
    {
        Gen<Long> all = Gens.longs().all();
        Gen<Long> nulls = ignore -> null;
        Gen<Long> domain = rs -> rs.nextBoolean() ? nulls.next(rs) : all.next(rs);
        DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(domain.map(FetchMinEpoch.Response::new)).check(rsp -> {
            for (MessagingService.Version version : SUPPORTED)
                IVersionedSerializers.testSerde(output, FetchMinEpoch.Response.serializer, rsp, version.value);
        });
    }

    @Test
    public void fetchOneNodeAlwaysFails()
    {
        int expectedMaxAttempts = 3;
        boundedRetries(expectedMaxAttempts);
        qt().check(rs -> {
            SimulatedMiniCluster cluster = new SimulatedMiniCluster.Builder(rs, node -> msg -> {throw new IllegalStateException();}).build();
            Node from = cluster.createNodeAndJoin();
            Node to = cluster.createNodeAndJoin();

            Future<Long> f = FetchMinEpoch.fetch(from, to.broadcastAddressAndPort(), Collections.emptySet());
            assertThat(f).isNotDone();
            cluster.processAll();
            assertThat(f).isDone();
            MessageDelivery.MaxRetriesException maxRetries = getMaxRetriesException(f);
            Assertions.assertThat(maxRetries.attempts).isEqualTo(expectedMaxAttempts);
        });
    }

    @Test
    public void fetchOneNode()
    {
        int maxRetries = 42;
        boundedRetries(maxRetries);
        qt().check(rs -> {
            long epoch = rs.nextLong(0, Long.MAX_VALUE);
            SimulatedMiniCluster cluster = new SimulatedMiniCluster.Builder(rs, node -> msg -> node.messaging().respond(new FetchMinEpoch.Response(epoch), msg)).build();
            Node from = cluster.createNodeAndJoin();
            {
                Supplier<Action> safeActionGen = actionGen(rs, maxRetries);
                from.messagingActions((self, msg, to) -> safeActionGen.get());
            }
            Node to = cluster.createNodeAndJoin();

            Future<Long> f = FetchMinEpoch.fetch(from, to.broadcastAddressAndPort(), Collections.emptySet());
            assertThat(f).isNotDone();
            cluster.processAll();
            assertThat(f).isDone();
            assertThat(f.get()).isEqualTo(epoch);
        });
    }

    @Test
    public void fetchManyNodesAllNodesFail()
    {
        int expectedMaxAttempts = 3;
        boundedRetries(expectedMaxAttempts);
        qt().check(rs -> {
            SimulatedMiniCluster cluster = new SimulatedMiniCluster.Builder(rs, node -> msg -> {throw new IllegalStateException();}).build();

            Node from = cluster.createNodeAndJoin();
            Node to1 = cluster.createNodeAndJoin();
            Node to2 = cluster.createNodeAndJoin();
            Node to3 = cluster.createNodeAndJoin();
            Node to4 = cluster.createNodeAndJoin();

            Future<Long> f = FetchMinEpoch.fetch(from, ImmutableMap.of(to1.broadcastAddressAndPort(), Collections.emptySet(),
                                                                       to2.broadcastAddressAndPort(), Collections.emptySet(),
                                                                       to3.broadcastAddressAndPort(), Collections.emptySet(),
                                                                       to4.broadcastAddressAndPort(), Collections.emptySet()));
            assertThat(f).isNotDone();
            cluster.processAll();
            assertThat(f).isDone();
            assertThat(f.get()).isNull();
        });
    }

    @Test
    public void fetchManyNodes()
    {
        boundedRetries(Integer.MAX_VALUE); // networking should be unbounded, but the actions should be bounded
        int maxRetries = 3;
        qt().check(rs -> {
            Map<Integer, Long> nodeToEpoch = new HashMap<>();
            Long min = null;
            for (int i = 2; i < 6; i++)
            {
                Long epoch = rs.nextBoolean() ? null : rs.nextLong();
                nodeToEpoch.put(i, epoch);
                if (min == null)        min = epoch;
                else if (epoch != null) min = Math.min(min, epoch);
            }

            SimulatedMiniCluster cluster = new SimulatedMiniCluster.Builder(rs, node -> msg -> node.messaging().respond(new FetchMinEpoch.Response(nodeToEpoch.get(node.id().id())), msg)).build();

            Node from = cluster.createNodeAndJoin();
            Node to1 = cluster.createNodeAndJoin();
            Node to2 = cluster.createNodeAndJoin();
            Node to3 = cluster.createNodeAndJoin();
            Node to4 = cluster.createNodeAndJoin();
            Map<InetAddressAndPort, Supplier<Action>> nodeToActions = ImmutableMap.of(to1.broadcastAddressAndPort(), actionGen(rs, maxRetries),
                                                                                      to2.broadcastAddressAndPort(), actionGen(rs, maxRetries),
                                                                                      to3.broadcastAddressAndPort(), actionGen(rs, maxRetries),
                                                                                      to4.broadcastAddressAndPort(), actionGen(rs, maxRetries));
            from.messagingActions((self, msg, to) -> nodeToActions.get(to).get());

            Future<Long> f = FetchMinEpoch.fetch(from, ImmutableMap.of(to1.broadcastAddressAndPort(), Collections.emptySet(),
                                                                       to2.broadcastAddressAndPort(), Collections.emptySet(),
                                                                       to3.broadcastAddressAndPort(), Collections.emptySet(),
                                                                       to4.broadcastAddressAndPort(), Collections.emptySet()));
            assertThat(f).isNotDone();
            cluster.processAll();
            assertThat(f).isDone();
            assertThat(f.get()).isEqualTo(min);
        });
    }

    private static Supplier<Action> actionGen(RandomSource rs, int maxRetries)
    {
        RandomSource actionSource = rs.fork();
        Gen<Action> actionGen = ACTION_DISTRIBUTION.next(actionSource);
        // it is very possible that DELIVER is very rare, which will cause the test to run for a long time and could fail in CI,
        // when a long history of non-DELIVER is seen, start to force DELIVER to bound the amount of processing in the test
        Gen<Action> safeActionGen = new Gen<>()
        {
            private int notDelivers = 0;
            @Override
            public Action next(RandomSource rng)
            {
                if (notDelivers > maxRetries - 1)
                    return Action.DELIVER;
                Action action = actionGen.next(rng);
                if (action == Action.DELIVER) notDelivers = 0;
                else notDelivers++;
                return action;
            }
        };
        return safeActionGen.asSupplier(actionSource);
    }

    private static void maybeSetPartitioner(FetchMinEpoch req)
    {
        IPartitioner partitioner = null;
        for (TokenRange r : req.ranges)
        {
            IPartitioner rangePartitioner = null;
            if (r.start().kindOfRoutingKey() != RoutingKeyKind.SENTINEL)
                rangePartitioner = r.start().token().getPartitioner();
            if (rangePartitioner == null && r.end().kindOfRoutingKey() != RoutingKeyKind.SENTINEL)
                rangePartitioner = r.end().token().getPartitioner();
            if (rangePartitioner == null)
                continue;
            if (partitioner == null)
            {
                partitioner = rangePartitioner;
            }
            else
            {
                Assertions.assertThat(rangePartitioner).isEqualTo(partitioner);
            }
        }
        if (partitioner != null)
            DatabaseDescriptor.setPartitionerUnsafe(partitioner);
    }

    private static MessageDelivery.MaxRetriesException getMaxRetriesException(Future<Long> f) throws InterruptedException, ExecutionException
    {
        MessageDelivery.MaxRetriesException maxRetries;
        try
        {
            f.get();
            Assert.fail("Future should have failed");
            throw new AssertionError("Unreachable");
        }
        catch (ExecutionException e)
        {
            if (e.getCause() instanceof MessageDelivery.MaxRetriesException)
            {
                maxRetries = (MessageDelivery.MaxRetriesException) e.getCause();
            }
            else
            {
                throw e;
            }
        }
        return maxRetries;
    }
}