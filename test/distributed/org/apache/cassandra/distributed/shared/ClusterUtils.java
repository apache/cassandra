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

package org.apache.cassandra.distributed.shared;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.impl.InstanceConfig;
import org.apache.cassandra.distributed.impl.TestChangeListener;
import org.apache.cassandra.distributed.test.log.TestProcessor;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Commit;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.ReplicaGroups;
import org.apache.cassandra.utils.Isolated;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.apache.cassandra.config.CassandraRelevantProperties.BOOTSTRAP_SCHEMA_DELAY_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.BROADCAST_INTERVAL_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.REPLACE_ADDRESS_FIRST_BOOT;
import static org.apache.cassandra.config.CassandraRelevantProperties.RING_DELAY;
import static org.apache.cassandra.distributed.impl.TestEndpointCache.toCassandraInetAddressAndPort;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Utilities for working with jvm-dtest clusters.
 *
 * This class is marked as Isolated as it relies on lambdas, which are in a package that is marked as shared, so need to
 * tell jvm-dtest to not share this class.
 *
 * This class should never be called from within the cluster, always in the App ClassLoader.
 */
@Isolated
public class ClusterUtils
{
    private static final Logger logger = LoggerFactory.getLogger(ClusterUtils.class);
    /**
     * Start the instance with the given System Properties, after the instance has started, the properties will be cleared.
     */
    public static <I extends IInstance> I start(I inst, Consumer<WithProperties> fn)
    {
        return start(inst, (ignore, prop) -> fn.accept(prop));
    }

    /**
     * Start the instance with the given System Properties, after the instance has started, the properties will be cleared.
     */
    public static <I extends IInstance> I start(I inst, BiConsumer<I, WithProperties> fn)
    {
        try (WithProperties properties = new WithProperties())
        {
            fn.accept(inst, properties);
            inst.startup();
            return inst;
        }
    }

    /**
     * Stop an instance in a blocking manner.
     *
     * The main difference between this and {@link IInstance#shutdown()} is that the wait on the future will catch
     * the exceptions and throw as runtime.
     */
    public static void stopUnchecked(IInstance i)
    {
        Futures.getUnchecked(i.shutdown());
    }

    /**
     * Stops an instance abruptly.  This is done by blocking all messages to/from so all other instances are unable
     * to communicate, then stopping the instance gracefully.
     *
     * The assumption is that hard stopping inbound and outbound messages will apear to the cluster as if the instance
     * was stopped via kill -9; this does not hold true if the instance is restarted as it knows it was properly shutdown.
     *
     * @param cluster to filter messages to
     * @param inst to shut down
     */
    public static <I extends IInstance> void stopAbrupt(ICluster<I> cluster, I inst)
    {
        // block all messages to/from the node going down to make sure a clean shutdown doesn't happen
        IMessageFilters.Filter to = cluster.filters().allVerbs().to(inst.config().num()).drop();
        IMessageFilters.Filter from = cluster.filters().allVerbs().from(inst.config().num()).drop();
        try
        {
            stopUnchecked(inst);
        }
        finally
        {
            from.off();
            to.off();
        }
    }

    /**
     * Stop all the instances in the cluster.  This function is differe than {@link ICluster#close()} as it doesn't
     * clean up the cluster state, it only stops all the instances.
     */
    public static <I extends IInstance> void stopAll(ICluster<I> cluster)
    {
        cluster.stream().forEach(ClusterUtils::stopUnchecked);
    }

    /**
     * Create a new instance and add it to the cluster, without starting it.
     *
     * @param cluster to add to
     * @param other config to copy from
     * @param fn function to add to the config before starting
     * @param <I> instance type
     * @return the instance added
     */
    public static <I extends IInstance> I addInstance(AbstractCluster<I> cluster,
                                                      IInstanceConfig other,
                                                      Consumer<IInstanceConfig> fn)
    {
        return addInstance(cluster, other.localDatacenter(), other.localRack(), fn);
    }

    /**
     * Create a new instance and add it to the cluster, without starting it.
     *
     * @param cluster to add to
     * @param dc the instance should be in
     * @param rack the instance should be in
     * @param <I> instance type
     * @return the instance added
     */
    public static <I extends IInstance> I addInstance(AbstractCluster<I> cluster,
                                                      String dc, String rack)
    {
        return addInstance(cluster, dc, rack, ignore -> {});
    }

    /**
     * Create a new instance and add it to the cluster, without starting it.
     *
     * @param cluster to add to
     * @param dc the instance should be in
     * @param rack the instance should be in
     * @param fn function to add to the config before starting
     * @param <I> instance type
     * @return the instance added
     */
    public static <I extends IInstance> I addInstance(AbstractCluster<I> cluster,
                                                      String dc, String rack,
                                                      Consumer<IInstanceConfig> fn)
    {
        Objects.requireNonNull(dc, "dc");
        Objects.requireNonNull(rack, "rack");

        InstanceConfig config = cluster.newInstanceConfig();
        //TODO adding new instances should be cleaner, currently requires you create the cluster with all
        // instances known about (at least to NetworkTopology and TokenStategy)
        // this is very hidden, so should be more explicit
        config.networkTopology().put(config.broadcastAddress(), NetworkTopology.dcAndRack(dc, rack));

        fn.accept(config);

        return cluster.bootstrap(config);
    }

    /**
     * Create and start a new instance that replaces an existing instance.
     *
     * The instance will be in the same datacenter and rack as the existing instance.
     *
     * @param cluster to add to
     * @param toReplace instance to replace
     * @param <I> instance type
     * @return the instance added
     */
    public static <I extends IInstance> I replaceHostAndStart(AbstractCluster<I> cluster, I toReplace)
    {
        return replaceHostAndStart(cluster, toReplace, ignore -> {});
    }

    /**
     * Create and start a new instance that replaces an existing instance.
     *
     * The instance will be in the same datacenter and rack as the existing instance.
     *
     * @param cluster to add to
     * @param toReplace instance to replace
     * @param fn lambda to add additional properties
     * @param <I> instance type
     * @return the instance added
     */
    public static <I extends IInstance> I replaceHostAndStart(AbstractCluster<I> cluster,
                                                              I toReplace,
                                                              Consumer<WithProperties> fn)
    {
        return replaceHostAndStart(cluster, toReplace, (ignore, prop) -> fn.accept(prop));
    }

    /**
     * Create and start a new instance that replaces an existing instance.
     *
     * The instance will be in the same datacenter and rack as the existing instance.
     *
     * @param cluster to add to
     * @param toReplace instance to replace
     * @param fn lambda to add additional properties or modify instance
     * @param <I> instance type
     * @return the instance added
     */
    public static <I extends IInstance> I replaceHostAndStart(AbstractCluster<I> cluster,
                                                              I toReplace,
                                                              BiConsumer<I, WithProperties> fn)
    {
        IInstanceConfig toReplaceConf = toReplace.config();
        I inst = addInstance(cluster, toReplaceConf, c -> c.set("auto_bootstrap", true)
                                                           .set("progress_barrier_min_consistency_level", ConsistencyLevel.ONE));
        return startHostReplacement(toReplace, inst, fn);

    }

    /**
     * Start a instance with the properties needed to perform a host replacement.
     *
     * @param toReplace instance to replace
     * @param inst      to start
     * @param fn        lambda to add additional properties or modify instance
     * @param <I>       instance type
     * @return inst
     */
    public static <I extends IInstance> I startHostReplacement(I toReplace, I inst, BiConsumer<I, WithProperties> fn)
    {
        return start(inst, properties -> {
            // lower this so the replacement waits less time
            properties.set(BROADCAST_INTERVAL_MS, Long.toString(TimeUnit.SECONDS.toMillis(30)));
            // default is 30s, lowering as it should be faster
            properties.set(RING_DELAY, Long.toString(TimeUnit.SECONDS.toMillis(10)));
            properties.set(BOOTSTRAP_SCHEMA_DELAY_MS, TimeUnit.SECONDS.toMillis(10));

            // state which node to replace
            InetSocketAddress address = toReplace.config().broadcastAddress();
            // when port isn't defined we use the default port, but in jvm-dtest the port might change!
            properties.set(REPLACE_ADDRESS_FIRST_BOOT, address.getAddress().getHostAddress() + ":" + address.getPort());

            fn.accept(inst, properties);
        });
    }

    /**
     * Calls TokenMap#tokens(), returning as a list of strings.
     */
    public static List<String> getTokenMetadataTokens(IInvokableInstance inst)
    {
        return inst.callOnInstance(() ->
                                   ClusterMetadata.current().tokenMap.tokens()
                                                                     .stream()
                                                                     .map(Object::toString)
                                                                     .collect(Collectors.toList()));
    }

    public static Collection<String> getLocalTokens(IInvokableInstance inst)
    {
        return inst.callOnInstance(() -> {
            List<String> tokens = new ArrayList<>();

            for (Token t : ClusterMetadata.current().tokenMap.tokens(ClusterMetadata.current().myNodeId()))
                tokens.add(t.getTokenValue().toString());
            return tokens;
        });
    }

    public static List<String> getPeerDirectoryDebugStrings(IInvokableInstance inst)
    {
        String s = inst.callOnInstance(() -> ClusterMetadata.current().directory.toDebugString());
        return Arrays.asList(s.split("\n"));
    }

    public static List<String> getTokenMapDebugStrings(IInvokableInstance inst)
    {
        String s = inst.callOnInstance(() -> ClusterMetadata.current().tokenMap.toDebugString());
        return Arrays.asList(s.split("\n"));
    }

    public static void logTokenMapDebugString(IInvokableInstance inst)
    {
        inst.runOnInstance(() -> ClusterMetadata.current().tokenMap.logDebugString());
    }

    @SuppressWarnings("rawtypes")
    public static Map<String, List[]> getDataPlacementDebugInfo(IInvokableInstance inst)
    {
        return inst.callOnInstance(() -> getPlacementDebugInfo(ClusterMetadataService.instance()));
    }


    // not pretty, but this is for testing. For each keyspace, includes 2—element array of List<Replica>.
    // Element 0 is the read replicas for the keyspace, element 1 is the write replicas.
    @VisibleForTesting
    @SuppressWarnings("rawtypes")
    public static Map<String, List[]> getPlacementDebugInfo(ClusterMetadataService metadataService)
    {
        ClusterMetadata metadata = metadataService.metadata();
        Map<String, List[]> byKeyspace = new HashMap<>();
        for (KeyspaceMetadata keyspace : metadata.schema.getKeyspaces())
        {
            List[] placements = new List[2];
            placements[0] = metadata.placements.get(keyspace.params.replication).reads.toReplicaStringList();
            placements[1] = metadata.placements.get(keyspace.params.replication).writes.toReplicaStringList();
            byKeyspace.put(keyspace.name, placements);
        }
        return byKeyspace;
    }

    public static void logDataPlacementDebugString(IInvokableInstance inst, boolean byEndpoint)
    {
        inst.runOnInstance(() -> logPlacementDebugString(ClusterMetadataService.instance(), byEndpoint));
    }

    public static void logPlacementDebugString(ClusterMetadataService metadataService, boolean byEndpoint)
    {
        ClusterMetadata metadata = metadataService.metadata();
        List<String> keyspaces = new ArrayList<>();
        for (KeyspaceMetadata keyspace : metadata.schema.getKeyspaces())
        {
            StringBuilder builder = new StringBuilder();
            builder.append("'keyspace' { 'name':").append(keyspace.name).append("', ");
            builder.append("'reads':['");
            ReplicaGroups placement = metadata.placements.get(keyspace.params.replication).reads;
            builder.append(byEndpoint ? placement.toStringByEndpoint() : placement.toString());
            builder.append("'], 'writes':['");
            placement = metadata.placements.get(keyspace.params.replication).writes;
            builder.append(byEndpoint ? placement.toStringByEndpoint() : placement.toString());
            builder.append("']}");
            keyspaces.add(builder.toString());
        }
        String debug = String.join("\n", keyspaces);
        logger.debug(debug);
    }

    public static <I extends IInstance> void runAndWaitForLogs(Runnable r, String waitString, AbstractCluster<I> cluster) throws TimeoutException
    {
        runAndWaitForLogs(r, waitString, cluster.stream().toArray(IInstance[]::new));
    }

    public static void runAndWaitForLogs(Runnable r, String waitString, IInstance...instances) throws TimeoutException
    {
        long [] marks = new long[instances.length];
        for (int i = 0; i < instances.length; i++)
            marks[i] = instances[i].logs().mark();
        r.run();
        for (int i = 0; i < instances.length; i++)
            instances[i].logs().watchFor(marks[i], waitString);
    }

    public static Epoch getClusterMetadataVersion(IInvokableInstance inst)
    {
        return decode(inst.callOnInstance(() -> encode(ClusterMetadata.current().epoch)));
    }

    public static long encode(Epoch epoch)
    {
        return epoch.getEpoch();
    }

    public static Epoch decode(long periodEpoch)
    {
        return Epoch.create(periodEpoch);
    }

    public static void waitForCMSToQuiesce(ICluster<IInvokableInstance> cluster, IInvokableInstance leader, int...ignored)
    {
        ClusterUtils.waitForCMSToQuiesce(cluster, getClusterMetadataVersion(leader), ignored);
    }

    public static void dropAllEntriesBeginningAt(IInvokableInstance instance, Epoch epoch)
    {
        instance.runOnInstance(() -> ClusterMetadataService.instance().log().addFilter(e -> e.epoch.isEqualOrAfter(epoch)));
    }

    public static void clearEntryFilters(IInvokableInstance instance)
    {
        instance.runOnInstance(() -> ClusterMetadataService.instance().log().clearFilters());
    }

    public static Callable<Void> pauseBeforeEnacting(IInvokableInstance instance, Epoch epoch)
    {
        return pauseBeforeEnacting(instance, epoch, 10, TimeUnit.SECONDS);
    }

    protected static Callable<Void> pauseBeforeEnacting(IInvokableInstance instance,
                                                        Epoch epoch,
                                                        long wait,
                                                        TimeUnit waitUnit)
    {
        return instance.callOnInstance(() -> {
            TestChangeListener listener = TestChangeListener.instance;
            AsyncPromise<?> promise = new AsyncPromise<>();
            listener.pauseBefore(epoch, () -> promise.setSuccess(null));
            return () -> {
                try
                {
                    promise.get(wait, waitUnit);
                    return null;
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            };
        });
    }

    public static Callable<Void> pauseAfterEnacting(IInvokableInstance instance, Epoch epoch)
    {
        return pauseAfterEnacting(instance, epoch, 10, TimeUnit.SECONDS);
    }

    protected static Callable<Void> pauseAfterEnacting(IInvokableInstance instance,
                                                       Epoch epoch,
                                                       long wait,
                                                       TimeUnit waitUnit)
    {
        return instance.callOnInstance(() -> {
            TestChangeListener listener = TestChangeListener.instance;
            AsyncPromise<?> promise = new AsyncPromise<>();
            listener.pauseAfter(epoch, () -> promise.setSuccess(null));
            return () -> {
                try
                {
                    promise.get(wait, waitUnit);
                    return null;
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            };
        });
    }

    public static Callable<Epoch> pauseBeforeCommit(IInvokableInstance cmsInstance, SerializablePredicate<Transformation> predicate)
    {
        Callable<Long> remoteCallable = cmsInstance.callOnInstance(() -> {
            TestProcessor processor = (TestProcessor) ((ClusterMetadataService.SwitchableProcessor) ClusterMetadataService.instance().processor()).delegate();
            AsyncPromise<Epoch> promise = new AsyncPromise<>();
            processor.pauseIf(predicate, () -> promise.setSuccess(ClusterMetadata.current().epoch));
            return () -> {
                try
                {
                    return promise.get(30, TimeUnit.SECONDS).getEpoch();
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            };
        });
        return () -> Epoch.create(remoteCallable.call());

    }

    public static Callable<Epoch> getSequenceAfterCommit(IInvokableInstance cmsInstance,
                                                         SerializableBiPredicate<Transformation, Commit.Result> predicate)
    {
        Callable<Long> remoteCallable = cmsInstance.callOnInstance(() -> {
            TestProcessor processor = (TestProcessor) ((ClusterMetadataService.SwitchableProcessor) ClusterMetadataService.instance().processor()).delegate();

            AsyncPromise<Epoch> promise = new AsyncPromise<>();
            processor.registerCommitPredicate((event, result) -> {
                if (predicate.test(event, result))
                {
                    promise.setSuccess(result.success().logState.latestEpoch());
                    return true;
                }

                return false;
            });
            return () -> {
                try
                {
                    return promise.get(30, TimeUnit.SECONDS).getEpoch();
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            };
        });

        return () -> Epoch.create(remoteCallable.call());
    }

    public static void unpauseCommits(IInvokableInstance instance)
    {
        if (instance.isShutdown())
            return;
        instance.runOnInstance(() -> {
            TestProcessor processor = (TestProcessor) ((ClusterMetadataService.SwitchableProcessor) ClusterMetadataService.instance().processor()).delegate();
            processor.unpause();
        });
    }

    public static void unpauseEnactment(IInvokableInstance instance)
    {
        instance.runOnInstance(() -> TestChangeListener.instance.unpause());
    }

    public static boolean isMigrating(IInvokableInstance instance)
    {
        return instance.callOnInstance(() -> ClusterMetadataService.instance().isMigrating());
    }

    public static interface SerializablePredicate<T> extends Predicate<T>, Serializable
    {}

    public static interface SerializableBiPredicate<T1, T2> extends BiPredicate<T1, T2>, Serializable {}

    private static class ClusterMetadataVersion
    {
        public final int node;
        public final Epoch epoch;

        private ClusterMetadataVersion(int node, Epoch epoch)
        {
            this.node = node;
            this.epoch = epoch;
        }

        public String toString()
        {
            return "Version{" +
                   "node=" + node +
                   ", epoch=" + epoch +
                   '}';
        }
    }

    public static void waitForCMSToQuiesce(ICluster<IInvokableInstance> cluster, int[] cmsNodes)
    {
        // first step; find the largest epoch
        waitForCMSToQuiesce(cluster, maxEpoch(cluster, cmsNodes));
    }

    private static Epoch maxEpoch(ICluster<IInvokableInstance> cluster, int[] cmsNodes)
    {
        Epoch max = null;
        for (int id : cmsNodes)
        {
            IInvokableInstance inst = cluster.get(id);
            if (inst.isShutdown()) continue;
            Epoch version = getClusterMetadataVersion(inst);
            if (max == null || version.getEpoch() > max.getEpoch())
                max = version;
        }
        if (max == null)
            throw new AssertionError("Unable to find max epoch from " + cmsNodes);
        return max;
    }

    public static void waitForCMSToQuiesce(ICluster<IInvokableInstance> cluster, Epoch awaitedEpoch, int...ignored)
    {
        List<ClusterMetadataVersion> notMatching = new ArrayList<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline)
        {
            notMatching.clear();
            for (int j = 1; j <= cluster.size(); j++)
            {
                boolean skip = false;
                for (int ignore : ignored)
                    if (ignore == j)
                        skip = true;

                if (skip)
                    continue;

                if (cluster.get(j).isShutdown())
                    continue;
                Epoch version = getClusterMetadataVersion(cluster.get(j));
                if (version.getEpoch() < awaitedEpoch.getEpoch())
                    notMatching.add(new ClusterMetadataVersion(j, version));
            }
            if (notMatching.isEmpty())
                return;

            sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
        }
        throw new AssertionError(String.format("Some instances have not reached schema agreement with the leader. Awaited %s; diverging nodes: %s. ", awaitedEpoch, notMatching));
    }

    public static Epoch getCurrentEpoch(IInvokableInstance inst)
    {
        return decode(inst.callOnInstance(() -> encode(ClusterMetadata.current().epoch)));
    }

    public static Epoch getNextEpoch(IInvokableInstance inst)
    {
        return decode(inst.callOnInstance(() -> encode(ClusterMetadata.current().nextEpoch())));
    }

    public static Epoch snapshotClusterMetadata(IInvokableInstance inst)
    {
        return decode(inst.callOnInstance(() -> {
            ClusterMetadata snapshotted = ClusterMetadataService.instance().triggerSnapshot();
            return encode(snapshotted.epoch);
        }));
    }

    public static Map<String, Epoch> getPeerEpochs(IInvokableInstance requester)
    {
        Map<String, Long> map = requester.callOnInstance(() -> {
            ImmutableList<InetAddressAndPort> peers = ClusterMetadata.current().directory.allAddresses();
            CountDownLatch latch = CountDownLatch.newCountDownLatch(peers.size());
            Map<String, Long> epochs = new ConcurrentHashMap<>(peers.size());
            peers.forEach(peer -> {
                Message<Epoch> request = Message.out(Verb.TCM_CURRENT_EPOCH_REQ, ClusterMetadata.current().epoch);
                RequestCallback<Epoch> callback = response -> {
                    epochs.put(peer.toString(), encode(response.payload));
                    latch.decrement();
                };
                MessagingService.instance().sendWithCallback(request, peer, callback);
            });
            latch.awaitUninterruptibly();
            return epochs;
        });
        return map.entrySet()
                  .stream()
                  .collect(Collectors.toMap(Map.Entry::getKey, e -> decode(e.getValue())));
    }

    public static Set<String> getCMSMembers(IInvokableInstance inst)
    {
        return inst.callOnInstance(() -> ClusterMetadata.current()
                                                        .fullCMSMembers()
                                                        .stream()
                                                        .map(InetSocketAddress::getAddress)
                                                        .map(Object::toString)
                                                        .collect(Collectors.toSet()));
    }

    public static boolean decommission(IInvokableInstance leaving)
    {
        return leaving.callOnInstance(() -> {
            try
            {
                StorageService.instance.decommission(true);
                return true;
            }
            catch (Exception e)
            {
                e.printStackTrace();
                return false;
            }
        });
    }

    public static NodeId getNodeId(IInvokableInstance target)
    {
        return new NodeId(getNodeId(target, target));
    }

    public static int getNodeId(IInvokableInstance target, IInvokableInstance executor)
    {
        InetSocketAddress targetAddress = target.config().broadcastAddress();
        return executor.callOnInstance(() -> {
            try
            {
                return ClusterMetadata.current().directory.peerId(toCassandraInetAddressAndPort(targetAddress)).id();
            }
            catch (Exception e)
            {
                e.printStackTrace();
                return null;
            }
        });
    }

    public static boolean cancelInProgressSequences(IInvokableInstance executor)
    {
        return cancelInProgressSequences(getNodeId(executor), executor);
    }

    public static boolean cancelInProgressSequences(NodeId nodeId, IInvokableInstance executor)
    {
        int id = nodeId.id();
        return executor.callOnInstance(() -> {
            try
            {

                StorageService.instance.cancelInProgressSequences(new NodeId(id));
                return true;
            }
            catch (Exception e)
            {
                e.printStackTrace();
                return false;
            }
        });
    }

    /**
     * Get the ring from the perspective of the instance.
     */
    public static List<RingInstanceDetails> ring(IInstance inst)
    {
        NodeToolResult results = inst.nodetoolResult("ring");
        results.asserts().success();
        return parseRing(results.getStdout());
    }

    /**
     * Make sure the target instance is in the ring.
     *
     * @param instance instance to check on
     * @param expectedInRing instance expected in the ring
     * @return the ring (if target is present)
     */
    public static List<RingInstanceDetails> assertInRing(IInstance instance, IInstance expectedInRing)
    {
        String targetAddress = getBroadcastAddressHostString(expectedInRing);
        List<RingInstanceDetails> ring = ring(instance);
        Optional<RingInstanceDetails> match = ring.stream().filter(d -> d.address.equals(targetAddress)).findFirst();
        assertThat(match).as("Not expected to find %s but was found", targetAddress).isPresent();
        return ring;
    }

    /**
     * Make sure the target instance's gossip state matches on the source instance
     *
     * @param instance instance to check on
     * @param expectedInRing instance expected in the ring
     * @param state expected gossip state
     * @return the ring (if target is present and has expected state)
     */
    public static List<RingInstanceDetails> assertRingState(IInstance instance, IInstance expectedInRing, String state)
    {
        String targetAddress = getBroadcastAddressHostString(expectedInRing);
        List<RingInstanceDetails> ring = ring(instance);
        List<RingInstanceDetails> match = ring.stream()
                                              .filter(d -> d.address.equals(targetAddress))
                                              .collect(Collectors.toList());
        assertThat(match)
        .isNotEmpty()
        .as("State was expected to be %s but was not", state)
        .anyMatch(r -> r.state.equals(state));
        return ring;
    }

    /**
     * Make sure the target instance is NOT in the ring.
     *
     * @param instance instance to check on
     * @param expectedInRing instance not expected in the ring
     * @return the ring (if target is not present)
     */
    public static List<RingInstanceDetails> assertNotInRing(IInstance instance, IInstance expectedInRing)
    {
        String targetAddress = getBroadcastAddressHostString(expectedInRing);
        List<RingInstanceDetails> ring = ring(instance);
        Optional<RingInstanceDetails> match = ring.stream().filter(d -> d.address.equals(targetAddress)).findFirst();
        Assert.assertEquals("Not expected to find " + targetAddress + " but was found", Optional.empty(), match);
        return ring;
    }

    private static List<RingInstanceDetails> awaitRing(IInstance src, String errorMessage, Predicate<List<RingInstanceDetails>> fn)
    {
        List<RingInstanceDetails> ring = null;
        for (int i = 0; i < 100; i++)
        {
            ring = ring(src);
            if (fn.test(ring))
            {
                return ring;
            }
            sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
        throw new AssertionError(errorMessage + "\n" + ring);
    }

    /**
     * Wait for the target to be in the ring as seen by the source instance.
     *
     * @param instance instance to check on
     * @param expectedInRing instance to wait for
     * @return the ring
     */
    public static List<RingInstanceDetails> awaitRingJoin(IInstance instance, IInstance expectedInRing)
    {
        return awaitRingJoin(instance, expectedInRing.broadcastAddress().getAddress().getHostAddress());
    }

    /**
     * Wait for the target to be in the ring as seen by the source instance.
     *
     * @param instance instance to check on
     * @param expectedInRing instance address to wait for
     * @return the ring
     */
    public static List<RingInstanceDetails> awaitRingJoin(IInstance instance, String expectedInRing)
    {
        return awaitRing(instance, "Node " + expectedInRing + " did not join the ring...", ring -> {
            Optional<RingInstanceDetails> match = ring.stream().filter(d -> d.address.equals(expectedInRing)).findFirst();
            if (match.isPresent())
            {
                RingInstanceDetails details = match.get();
                return details.status.equals("Up") && details.state.equals("Normal");
            }
            return false;
        });
    }

    /**
     * Wait for the ring to only have instances that are Up and Normal.
     *
     * @param src instance to check on
     * @return the ring
     */
    public static List<RingInstanceDetails> awaitRingHealthy(IInstance src)
    {
        return awaitRing(src, "Timeout waiting for ring to become healthy",
                         ring ->
                         ring.stream().allMatch(ClusterUtils::isRingInstanceDetailsHealthy));
    }

    /**
     * Wait for the ring to have the target instance with the provided status.
     *
     * @param instance instance to check on
     * @param expectedInRing to look for
     * @param status expected
     * @return the ring
     */
    public static List<RingInstanceDetails> awaitRingStatus(IInstance instance, IInstance expectedInRing, String status)
    {
        return awaitInstanceMatching(instance, expectedInRing, d -> d.status.equals(status),
                                     "Timeout waiting for " + expectedInRing + " to have status " + status);
    }

    /**
     * Wait for the ring to have the target instance with the provided state.
     *
     * @param instance instance to check on
     * @param expectedInRing to look for
     * @param state expected
     * @return the ring
     */
    public static List<RingInstanceDetails> awaitRingState(IInstance instance, IInstance expectedInRing, String state)
    {
        return awaitInstanceMatching(instance, expectedInRing, d -> d.state.equals(state),
                                     "Timeout waiting for " + expectedInRing + " to have state " + state);
    }

    private static List<RingInstanceDetails> awaitInstanceMatching(IInstance instance,
                                                                   IInstance expectedInRing,
                                                                   Predicate<RingInstanceDetails> predicate,
                                                                   String errorMessage)
    {
        return awaitRing(instance,
                         errorMessage,
                         ring -> ring.stream()
                                     .filter(d -> d.address.equals(getBroadcastAddressHostString(expectedInRing)))
                                     .anyMatch(predicate));
    }

    /**
     * Make sure the ring is only the expected instances.  The source instance may not be in the ring, so this function
     * only relies on the expectedInsts param.
     *
     * @param instance instance to check on
     * @param expectedInRing expected instances in the ring
     * @return the ring (if condition is true)
     */
    public static List<RingInstanceDetails> assertRingIs(IInstance instance, IInstance... expectedInRing)
    {
        return assertRingIs(instance, Arrays.asList(expectedInRing));
    }

    /**
     * Make sure the ring is only the expected instances.  The source instance may not be in the ring, so this function
     * only relies on the expectedInsts param.
     *
     * @param instance instance to check on
     * @param expectedInRing expected instances in the ring
     * @return the ring (if condition is true)
     */
    public static List<RingInstanceDetails> assertRingIs(IInstance instance, Collection<? extends IInstance> expectedInRing)
    {
        Set<String> expectedRingAddresses = expectedInRing.stream()
                                                         .map(i -> i.config().broadcastAddress().getAddress().getHostAddress())
                                                         .collect(Collectors.toSet());
        return assertRingIs(instance, expectedRingAddresses);
    }

    /**
     * Make sure the ring is only the expected instances.  The source instance may not be in the ring, so this function
     * only relies on the expectedInsts param.
     *
     * @param instance instance to check on
     * @param expectedRingAddresses expected instances addresses in the ring
     * @return the ring (if condition is true)
     */
    public static List<RingInstanceDetails> assertRingIs(IInstance instance, Set<String> expectedRingAddresses)
    {
        List<RingInstanceDetails> ring = ring(instance);
        Set<String> ringAddresses = ring.stream().map(d -> d.address).collect(Collectors.toSet());
        assertThat(ringAddresses)
        .as("Ring addreses did not match for instance %s", instance)
        .isEqualTo(expectedRingAddresses);
        return ring;
    }

    private static boolean isRingInstanceDetailsHealthy(RingInstanceDetails details)
    {
        return details.status.equals("Up") && details.state.equals("Normal");
    }

    private static List<RingInstanceDetails> parseRing(String str)
    {
        // 127.0.0.3  rack0       Up     Normal  46.21 KB        100.00%             -1
        // /127.0.0.1:7012  Unknown     ?      Normal  ?               100.00%             -3074457345618258603
        Pattern pattern = Pattern.compile("^(/?[0-9.:]+)\\s+(\\w+|\\?)\\s+(\\w+|\\?)\\s+(\\w+|\\?).*?(-?\\d+)\\s*$");
        List<RingInstanceDetails> details = new ArrayList<>();
        String[] lines = str.split("\n");
        for (String line : lines)
        {
            Matcher matcher = pattern.matcher(line);
            if (!matcher.find())
            {
                continue;
            }
            details.add(new RingInstanceDetails(matcher.group(1), matcher.group(2), matcher.group(3), matcher.group(4), matcher.group(5)));
        }

        return details;
    }

    private static Map<String, Map<String, String>> awaitGossip(IInstance src, String errorMessage, Predicate<Map<String, Map<String, String>>> fn)
    {
        Map<String, Map<String, String>> gossip = null;
        for (int i = 0; i < 100; i++)
        {
            gossip = gossipInfo(src);
            if (fn.test(gossip))
            {
                return gossip;
            }
            sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
        throw new AssertionError(errorMessage + "\n" + gossip);
    }

    /**
     * Wait for the target instance to have the desired status. Target status is checked via string contains so works
     * with 'NORMAL' but also can check tokens or full state.
     *
     * @param instance instance to check on
     * @param expectedInGossip instance to wait for
     * @param targetStatus for the instance
     * @return gossip info
     */
    public static Map<String, Map<String, String>> awaitGossipStatus(IInstance instance, IInstance expectedInGossip, String targetStatus)
    {
        return awaitGossip(instance, "Node " + expectedInGossip + " did not match state " + targetStatus, gossip -> {
            Map<String, String> state = gossip.get(getBroadcastAddressString(expectedInGossip));
            if (state == null)
                return false;
            String status = state.get("STATUS_WITH_PORT");
            if (status == null)
                status = state.get("STATUS");
            if (status == null)
                return targetStatus == null;
            return status.contains(targetStatus);
        });
    }

    public static void awaitGossipSchemaMatch(ICluster<? extends  IInstance> cluster)
    {
        cluster.forEach(ClusterUtils::awaitGossipSchemaMatch);
    }

    public static void awaitGossipSchemaMatch(IInstance instance)
    {
        if (!instance.config().has(Feature.GOSSIP))
        {
            // when gosisp isn't enabled, don't bother waiting on gossip to settle...
            return;
        }
        awaitGossip(instance, "Schema IDs did not match", all -> {
            String current = null;
            for (Map.Entry<String, Map<String, String>> e : all.entrySet())
            {
                Map<String, String> state = e.getValue();
                // has the instance joined?
                String status = state.get(ApplicationState.STATUS_WITH_PORT.name());
                if (status == null)
                    status = state.get(ApplicationState.STATUS.name());
                if (status == null || !status.contains(VersionedValue.STATUS_NORMAL))
                    continue; // ignore instances not joined yet
                String schema = state.get("SCHEMA");
                if (schema == null)
                    throw new AssertionError("Unable to find schema for " + e.getKey() + "; status was " + status);
                schema = schema.split(":")[1];

                if (current == null)
                {
                    current = schema;
                }
                else if (!current.equals(schema))
                {
                    return false;
                }
            }
            return true;
        });
    }

    public static void awaitGossipStateMatch(ICluster<? extends  IInstance> cluster, IInstance expectedInGossip, ApplicationState key)
    {
        Set<String> matches = null;
        for (int i = 0; i < 100; i++)
        {
            matches = cluster.stream().map(ClusterUtils::gossipInfo)
                             .map(gi -> Objects.requireNonNull(gi.get(getBroadcastAddressString(expectedInGossip))))
                             .map(m -> m.get(key.name()))
                             .collect(Collectors.toSet());
            if (matches.isEmpty() || matches.size() == 1)
                return;
            sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
        throw new AssertionError("Expected ApplicationState." + key + " to match, but saw " + matches);
    }

    /**
     * Get the gossip information from the node.  Currently only address, generation, and heartbeat are returned
     *
     * @param inst to check on
     * @return gossip info
     */
    public static Map<String, Map<String, String>> gossipInfo(IInstance inst)
    {
        NodeToolResult results = inst.nodetoolResult("gossipinfo");
        results.asserts().success();
        return parseGossipInfo(results.getStdout());
    }

    /**
     * Make sure the gossip info for the specific target has the expected generation and heartbeat
     *
     * @param instance to check on
     * @param expectedInGossip instance to check for
     * @param expectedGeneration expected generation
     * @param expectedHeartbeat expected heartbeat
     */
    public static void assertGossipInfo(IInstance instance,
                                        InetSocketAddress expectedInGossip, int expectedGeneration, int expectedHeartbeat)
    {
        String targetAddress = expectedInGossip.getAddress().toString();
        Map<String, Map<String, String>> gossipInfo = gossipInfo(instance);
        Map<String, String> gossipState = gossipInfo.get(targetAddress);
        if (gossipState == null)
            throw new NullPointerException("Unable to find gossip info for " + targetAddress + "; gossip info = " + gossipInfo);
        Assert.assertEquals(Long.toString(expectedGeneration), gossipState.get("generation"));
        Assert.assertEquals(Long.toString(expectedHeartbeat), gossipState.get("heartbeat")); //TODO do we really mix these two?
    }

    private static Map<String, Map<String, String>> parseGossipInfo(String str)
    {
        Map<String, Map<String, String>> map = new HashMap<>();
        String[] lines = str.split("\n");
        String currentInstance = null;
        for (String line : lines)
        {
            if (line.startsWith("/"))
            {
                // start of new instance
                currentInstance = line;
                continue;
            }
            Objects.requireNonNull(currentInstance);
            String[] kv = line.trim().split(":", 2);
            assert kv.length == 2 : "When splitting line '" + line + "' expected 2 parts but not true";
            Map<String, String> state = map.computeIfAbsent(currentInstance, ignore -> new HashMap<>());
            state.put(kv[0], kv[1]);
        }

        return map;
    }

    /**
     * Get the tokens assigned to the instance via config.  This method does not work if the instance has learned
     * or generated its tokens.
     *
     * @param instance to get tokens from
     * @return non-empty list of tokens
     */
    public static List<String> getTokens(IInstance instance)
    {
        IInstanceConfig conf = instance.config();
        int numTokens = conf.getInt("num_tokens");
        Assert.assertEquals("Only single token is supported", 1, numTokens);
        String token = conf.getString("initial_token");
        Assert.assertNotNull("initial_token was not found", token);
        return Arrays.asList(token);
    }

    /**
     * Get the number of tokens for the instance via config.
     *
     * @param instance to get token count from
     * @return number of tokens
     */
    public static int getTokenCount(IInvokableInstance instance)
    {
        return instance.config().getInt("num_tokens");
    }

    /**
     * Get all data directories for the given instance.
     *
     * @param instance to get data directories for
     * @return data directories
     */
    public static List<File> getDataDirectories(IInstance instance)
    {
        IInstanceConfig conf = instance.config();
        // this isn't safe as it assumes the implementation of InstanceConfig
        // might need to get smarter... some day...
        String[] ds = (String[]) conf.get("data_file_directories");
        List<File> files = new ArrayList<>(ds.length);
        for (int i = 0; i < ds.length; i++)
            files.add(new File(ds[i]));
        return files;
    }

    /**
     * Get the commit log directory for the given instance.
     *
     * @param instance to get the commit log directory for
     * @return commit log directory
     */
    public static File getCommitLogDirectory(IInstance instance)
    {
        IInstanceConfig conf = instance.config();
        // this isn't safe as it assumes the implementation of InstanceConfig
        // might need to get smarter... some day...
        String d = (String) conf.get("commitlog_directory");
        return new File(d);
    }

    /**
     * Get the hints directory for the given instance.
     *
     * @param instance to get the hints directory for
     * @return hints directory
     */
    public static File getHintsDirectory(IInstance instance)
    {
        IInstanceConfig conf = instance.config();
        // this isn't safe as it assumes the implementation of InstanceConfig
        // might need to get smarter... some day...
        String d = (String) conf.get("hints_directory");
        return new File(d);
    }

    /**
     * Get the saved caches directory for the given instance.
     *
     * @param instance to get the saved caches directory for
     * @return saved caches directory
     */
    public static File getSavedCachesDirectory(IInstance instance)
    {
        IInstanceConfig conf = instance.config();
        // this isn't safe as it assumes the implementation of InstanceConfig
        // might need to get smarter... some day...
        String d = (String) conf.get("saved_caches_directory");
        return new File(d);
    }

    /**
     * Get all writable directories for the given instance.
     *
     * @param instance to get directories for
     * @return all writable directories
     */
    public static List<File> getDirectories(IInstance instance)
    {
        List<File> out = new ArrayList<>();
        out.addAll(getDataDirectories(instance));
        out.add(getCommitLogDirectory(instance));
        out.add(getHintsDirectory(instance));
        out.add(getSavedCachesDirectory(instance));
        return out;
    }

    /**
     * Gets the name of the Partitioner for the given instance.
     *
     * @param instance to get partitioner from
     * @return partitioner name
     */
    public static String getPartitionerName(IInstance instance)
    {
        return (String) instance.config().get("partitioner");
    }

    /**
     * Changes the instance's address to the new address.  This method should only be called while the instance is
     * down, else has undefined behavior.
     *
     * @param instance to update address for
     * @param address to set
     */
    public static void updateAddress(IInstance instance, String address)
    {
        updateAddress(instance.config(), address);
    }

    /**
     * Changes the instance's address to the new address.  This method should only be called while the instance is
     * down, else has undefined behavior.
     *
     * @param conf to update address for
     * @param address to set
     */
    private static void updateAddress(IInstanceConfig conf, String address)
    {
        InetSocketAddress previous = conf.broadcastAddress();

        for (String key : Arrays.asList("broadcast_address", "listen_address", "broadcast_rpc_address", "rpc_address"))
            conf.set(key, address);

        // InstanceConfig caches InetSocketAddress -> InetAddressAndPort
        // this causes issues as startup now ignores config, so force reset it to pull from conf.
        ((InstanceConfig) conf).unsetBroadcastAddressAndPort(); //TODO remove the need to null out the cache...

        //TODO NetworkTopology class isn't flexible and doesn't handle adding/removing nodes well...
        // it also uses a HashMap which makes the class not thread safe... so mutating AFTER starting nodes
        // are a risk
        if (!conf.broadcastAddress().equals(previous))
        {
            conf.networkTopology().put(conf.broadcastAddress(), NetworkTopology.dcAndRack(conf.localDatacenter(), conf.localRack()));
            try
            {
                Field field = NetworkTopology.class.getDeclaredField("map");
                field.setAccessible(true);
                Map<InetSocketAddress, NetworkTopology.DcAndRack> map = (Map<InetSocketAddress, NetworkTopology.DcAndRack>) field.get(conf.networkTopology());
                map.remove(previous);
            }
            catch (NoSuchFieldException | IllegalAccessException e)
            {
                throw new AssertionError(e);
            }
        }
    }

    /**
     * Get the broadcast address host address only (ex. 127.0.0.1)
     */
    private static String getBroadcastAddressHostString(IInstance target)
    {
        return target.config().broadcastAddress().getAddress().getHostAddress();
    }

    /**
     * Get the broadcast address in host:port format (ex. 127.0.0.1:7190)
     */
    public static String getBroadcastAddressHostWithPortString(IInstance target)
    {
        InetSocketAddress address = target.config().broadcastAddress();
        return address.getAddress().getHostAddress() + ":" + address.getPort();
    }

    /**
     * @return the native address in host:port format (ex. 127.0.0.1:9042)
     */
    public static InetSocketAddress getNativeInetSocketAddress(IInstance target)
    {
        return new InetSocketAddress(target.config().broadcastAddress().getAddress(),
                                     getIntConfig(target.config(), "native_transport_port", 9042));
    }

    /**
     * Get the broadcast address InetAddess string (ex. localhost/127.0.0.1 or /127.0.0.1)
     */
    private static String getBroadcastAddressString(IInstance target)
    {
        return target.config().broadcastAddress().getAddress().toString();
    }

    /**
     * Tries to return the integer configuration from the {@code config}, fallsback to {@code defaultValue}
     * when it fails to retrieve the value.
     *
     * @param config       the config instance
     * @param configName   the name of the configuration
     * @param defaultValue the default value
     * @return the integer value from the configuration, or the default value when it fails to retrieve it
     */
    public static int getIntConfig(IInstanceConfig config, String configName, int defaultValue)
    {
        try
        {
            return config.getInt(configName);
        }
        catch (NullPointerException npe)
        {
            return defaultValue;
        }
    }

    public static final class RingInstanceDetails
    {
        private final String address;
        private final String rack;
        private final String status;
        private final String state;
        private final String token;

        private RingInstanceDetails(String address, String rack, String status, String state, String token)
        {
            this.address = address;
            this.rack = rack;
            this.status = status;
            this.state = state;
            this.token = token;
        }

        public String getAddress()
        {
            return address;
        }

        public String getRack()
        {
            return rack;
        }

        public String getStatus()
        {
            return status;
        }

        public String getState()
        {
            return state;
        }

        public String getToken()
        {
            return token;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RingInstanceDetails that = (RingInstanceDetails) o;
            return Objects.equals(address, that.address) &&
                   Objects.equals(rack, that.rack) &&
                   Objects.equals(status, that.status) &&
                   Objects.equals(state, that.state) &&
                   Objects.equals(token, that.token);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(address, rack, status, state, token);
        }

        public String toString()
        {
            return Arrays.asList(address, rack, status, state, token).toString();
        }
    }

    public static void preventSystemExit()
    {
        System.setSecurityManager(new PreventSystemExit());
    }
}

