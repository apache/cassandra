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

package org.apache.cassandra.distributed.test.topology;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceInitializer;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.util.Coordinators;
import org.apache.cassandra.distributed.util.QueryResultUtil;
import org.apache.cassandra.distributed.util.byterewrite.StatusChangeListener;
import org.apache.cassandra.distributed.util.byterewrite.StatusChangeListener.Hooks;
import org.apache.cassandra.distributed.util.byterewrite.Undead;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.AssertionUtils;
import org.apache.cassandra.utils.FBUtilities;

import static net.bytebuddy.matcher.ElementMatchers.named;

public abstract class DecommissionAvoidTimeouts extends TestBaseImpl
{
    public static final int DECOM_NODE = 6;

    @Test
    public void test() throws IOException
    {
        try (Cluster cluster = Cluster.build(8)
                                      .withRacks(2, 4)
                                      .withInstanceInitializer(new BB())
                                      .withConfig(c -> c.with(Feature.GOSSIP)
                                                        .set("transfer_hints_on_decommission", false)
                                                        .set("severity_during_decommission", 10000D)
                                                        .set("dynamic_snitch_badness_threshold", 0))
                                      .start())
        {
            // failure happens in PendingRangeCalculatorService.update, so the keyspace is being removed
            cluster.setUncaughtExceptionsFilter((ignore, throwable) -> !"Unknown keyspace system_distributed".equals(throwable.getMessage()));

            fixDistributedSchemas(cluster);
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3, 'datacenter2': 3}");
            String table = KEYSPACE + ".tbl";
            cluster.schemaChange("CREATE TABLE " + table + " (pk blob PRIMARY KEY)");

            List<IInvokableInstance> dc1 = cluster.get(1, 2, 3, 4);
            List<IInvokableInstance> dc2 = cluster.get(5, 6, 7, 8);
            IInvokableInstance toDecom = dc2.get(1);
            List<Murmur3Partitioner.LongToken> tokens = ClusterUtils.getLocalTokens(toDecom).stream().map(t -> new Murmur3Partitioner.LongToken(Long.parseLong(t))).collect(Collectors.toList());

            for (Murmur3Partitioner.LongToken token : tokens)
            {
                ByteBuffer key = Murmur3Partitioner.LongToken.keyForToken(token);

                toDecom.coordinator().execute("INSERT INTO " + table + "(pk) VALUES (?)", ConsistencyLevel.EACH_QUORUM, key);
            }

            CompletableFuture<Void> nodetool = CompletableFuture.runAsync(() -> toDecom.nodetoolResult("decommission").asserts().success());

            Hooks statusHooks = StatusChangeListener.hooks(DECOM_NODE);
            statusHooks.leaving.awaitAndEnter();
            // make sure all nodes see the severity change
            ClusterUtils.awaitGossipStateMatch(cluster, cluster.get(DECOM_NODE), ApplicationState.SEVERITY);
            cluster.forEach(i -> i.runOnInstance(() -> ((DynamicEndpointSnitch) DatabaseDescriptor.getEndpointSnitch()).updateScores()));

            statusHooks.leave.await();
            cluster.filters().verbs(Verb.GOSSIP_DIGEST_SYN.id).drop();
            statusHooks.leave.enter();

            nodetool.join();

            List<String> failures = new ArrayList<>();
            String query = getQuery(table);
            for (Murmur3Partitioner.LongToken token : tokens)
            {
                ByteBuffer key = Murmur3Partitioner.LongToken.keyForToken(token);

                for (IInvokableInstance i : dc1)
                {
                    for (ConsistencyLevel cl : levels())
                    {
                        try
                        {
                            Coordinators.withTracing(i.coordinator(), query, cl, key);
                        }
                        catch (Coordinators.WithTraceException e)
                        {
                            Throwable cause = e.getCause();
                            if (AssertionUtils.isInstanceof(WriteTimeoutException.class).matches(cause) || AssertionUtils.isInstanceof(ReadTimeoutException.class).matches(cause))
                            {
                                List<String> traceMesssages = Arrays.asList("Sending mutation to remote replica",
                                                                            "reading data from",
                                                                            "reading digest from");
                                SimpleQueryResult filtered = QueryResultUtil.query(e.trace)
                                                                            .select("activity")
                                                                            .filter(row -> traceMesssages.stream().anyMatch(row.getString("activity")::startsWith))
                                                                            .build();
                                InetAddressAndPort decomeNode = BB.address((byte) DECOM_NODE);
                                while (filtered.hasNext())
                                {
                                    String log = filtered.next().getString("activity");
                                    if (log.contains(decomeNode.toString()))
                                        failures.add("Failure with node" + i.config().num() + ", cl=" + cl + ";\n\t" + cause.getMessage() + ";\n\tTrace activity=" + log);
                                }
                            }
                            else
                            {
                                throw e;
                            }
                        }
                    }
                }
            }
            if (!failures.isEmpty()) throw new AssertionError(String.join("\n", failures));

            // since only one tests exists per file, shutdown without blocking so .close does not timeout
            try
            {
                FBUtilities.waitOnFutures(cluster.stream().map(IInstance::shutdown).collect(Collectors.toList()),
                                          1, TimeUnit.MINUTES);
            }
            catch (Exception e)
            {
                // ignore
            }
        }
    }

    protected abstract String getQuery(String table);

    private static Collection<ConsistencyLevel> levels()
    {
        return EnumSet.of(ConsistencyLevel.QUORUM, ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM,
                          ConsistencyLevel.ONE);
    }

    public static class BB implements IInstanceInitializer, AutoCloseable
    {
        @Override
        public void initialise(ClassLoader cl, ThreadGroup group, int node, int generation)
        {
            Undead.install(cl);
            new ByteBuddy().rebase(DynamicEndpointSnitch.class)
                           .method(named("sortedByProximity")).intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);

            if (node != DECOM_NODE) return;
            StatusChangeListener.install(cl, node, StatusChangeListener.Status.LEAVING, StatusChangeListener.Status.LEAVE);
        }

        @Override
        public void close() throws Exception
        {
            Undead.close();
            StatusChangeListener.close();
        }

        public static  <C extends ReplicaCollection<? extends C>> C sortedByProximity(final InetAddressAndPort address, C replicas, @SuperCall Callable<C> real) throws Exception
        {
            C result = real.call();
            if (result.size() > 1)
            {
                InetAddressAndPort decom = address((byte) DECOM_NODE);
                if (result.endpoints().contains(decom))
                {
                    if (DynamicEndpointSnitch.getSeverity(decom) != 0)
                    {
                        Replica last = result.get(result.size() - 1);
                        if (!last.endpoint().equals(decom))
                            throw new AssertionError("Expected endpoint " + decom + " to be the last replica, but found " + last.endpoint() + "; " + result);
                    }
                }
            }
            return result;
        }

        private static InetAddressAndPort address(byte num)
        {
            try
            {
                return InetAddressAndPort.getByAddressOverrideDefaults(InetAddress.getByAddress(new byte[]{ 127, 0, 0, num }), 7012);
            }
            catch (UnknownHostException e)
            {
                throw new AssertionError(e);
            }
        }
    }
}
