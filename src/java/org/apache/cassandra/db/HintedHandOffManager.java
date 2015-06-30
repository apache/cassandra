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
package org.apache.cassandra.db;

import java.io.DataInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.concurrent.JMXEnabledScheduledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.NIODataInputStream;
import org.apache.cassandra.metrics.HintedHandoffMetrics;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.*;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.cliffc.high_scale_lib.NonBlockingHashSet;

/**
 * The hint schema looks like this:
 *
 * CREATE TABLE hints (
 *   target_id uuid,
 *   hint_id timeuuid,
 *   message_version int,
 *   mutation blob,
 *   PRIMARY KEY (target_id, hint_id, message_version)
 * ) WITH COMPACT STORAGE;
 *
 * Thus, for each node in the cluster we treat its uuid as the partition key; each hint is a logical row
 * (physical composite column) containing the mutation to replay and associated metadata.
 *
 * When FailureDetector signals that a node that was down is back up, we page through
 * the hinted mutations and send them over one at a time, waiting for
 * hinted_handoff_throttle_delay in between each.
 *
 * deliverHints is also exposed to JMX so it can be run manually if FD ever misses
 * its cue somehow.
 */

public class HintedHandOffManager implements HintedHandOffManagerMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=HintedHandoffManager";
    public static final HintedHandOffManager instance = new HintedHandOffManager();

    private static final Logger logger = LoggerFactory.getLogger(HintedHandOffManager.class);

    private static final int MAX_SIMULTANEOUSLY_REPLAYED_HINTS = 128;
    private static final int LARGE_NUMBER = 65536; // 64k nodes ought to be enough for anybody.

    public final HintedHandoffMetrics metrics = new HintedHandoffMetrics();

    private volatile boolean hintedHandOffPaused = false;

    static final int maxHintTTL = Integer.parseInt(System.getProperty("cassandra.maxHintTTL", String.valueOf(Integer.MAX_VALUE)));

    private final NonBlockingHashSet<InetAddress> queuedDeliveries = new NonBlockingHashSet<>();

    private final JMXEnabledScheduledThreadPoolExecutor executor =
        new JMXEnabledScheduledThreadPoolExecutor(
            DatabaseDescriptor.getMaxHintsThread(),
            new NamedThreadFactory("HintedHandoff", Thread.MIN_PRIORITY),
            "internal");

    private final ColumnFamilyStore hintStore = Keyspace.open(SystemKeyspace.NAME).getColumnFamilyStore(SystemKeyspace.HINTS);

    private static final ColumnDefinition hintColumn = SystemKeyspace.Hints.compactValueColumn();

    /**
     * Returns a mutation representing a Hint to be sent to <code>targetId</code>
     * as soon as it becomes available again.
     */
    public Mutation hintFor(Mutation mutation, long now, int ttl, UUID targetId)
    {
        assert ttl > 0;

        InetAddress endpoint = StorageService.instance.getTokenMetadata().getEndpointForHostId(targetId);
        // during tests we may not have a matching endpoint, but this would be unexpected in real clusters
        if (endpoint != null)
            metrics.incrCreatedHints(endpoint);
        else
            logger.warn("Unable to find matching endpoint for target {} when storing a hint", targetId);

        UUID hintId = UUIDGen.getTimeUUID();
        // serialize the hint with id and version as a composite column name

        DecoratedKey key = StorageService.getPartitioner().decorateKey(UUIDType.instance.decompose(targetId));

        Clustering clustering = SystemKeyspace.Hints.comparator.make(hintId, MessagingService.current_version);
        ByteBuffer value = ByteBuffer.wrap(FBUtilities.serialize(mutation, Mutation.serializer, MessagingService.current_version));
        Cell cell = BufferCell.expiring(hintColumn, now, ttl, FBUtilities.nowInSeconds(), value);

        return new Mutation(PartitionUpdate.singleRowUpdate(SystemKeyspace.Hints, key, ArrayBackedRow.singleCellRow(clustering, cell)));
    }

    /*
     * determine the TTL for the hint Mutation
     * this is set at the smallest GCGraceSeconds for any of the CFs in the RM
     * this ensures that deletes aren't "undone" by delivery of an old hint
     */
    public static int calculateHintTTL(Mutation mutation)
    {
        int ttl = maxHintTTL;
        for (PartitionUpdate upd : mutation.getPartitionUpdates())
            ttl = Math.min(ttl, upd.metadata().getGcGraceSeconds());
        return ttl;
    }

    public void start()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        logger.debug("Created HHOM instance, registered MBean.");

        Runnable runnable = new Runnable()
        {
            public void run()
            {
                scheduleAllDeliveries();
                metrics.log();
            }
        };
        executor.scheduleWithFixedDelay(runnable, 10, 10, TimeUnit.MINUTES);
    }

    private static void deleteHint(ByteBuffer tokenBytes, Clustering clustering, long timestamp)
    {
        DecoratedKey dk =  StorageService.getPartitioner().decorateKey(tokenBytes);
        Cell cell = BufferCell.tombstone(hintColumn, timestamp, FBUtilities.nowInSeconds());
        PartitionUpdate upd = PartitionUpdate.singleRowUpdate(SystemKeyspace.Hints, dk, ArrayBackedRow.singleCellRow(clustering, cell));
        new Mutation(upd).applyUnsafe(); // don't bother with commitlog since we're going to flush as soon as we're done with delivery
    }

    public void deleteHintsForEndpoint(final String ipOrHostname)
    {
        try
        {
            InetAddress endpoint = InetAddress.getByName(ipOrHostname);
            deleteHintsForEndpoint(endpoint);
        }
        catch (UnknownHostException e)
        {
            logger.warn("Unable to find {}, not a hostname or ipaddr of a node", ipOrHostname);
            throw new RuntimeException(e);
        }
    }

    public void deleteHintsForEndpoint(final InetAddress endpoint)
    {
        if (!StorageService.instance.getTokenMetadata().isMember(endpoint))
            return;
        UUID hostId = StorageService.instance.getTokenMetadata().getHostId(endpoint);
        DecoratedKey dk =  StorageService.getPartitioner().decorateKey(ByteBuffer.wrap(UUIDGen.decompose(hostId)));
        final Mutation mutation = new Mutation(PartitionUpdate.fullPartitionDelete(SystemKeyspace.Hints, dk, System.currentTimeMillis(), FBUtilities.nowInSeconds()));

        // execute asynchronously to avoid blocking caller (which may be processing gossip)
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                try
                {
                    logger.info("Deleting any stored hints for {}", endpoint);
                    mutation.apply();
                    hintStore.forceBlockingFlush();
                    compact();
                }
                catch (Exception e)
                {
                    JVMStabilityInspector.inspectThrowable(e);
                    logger.warn("Could not delete hints for {}: {}", endpoint, e);
                }
            }
        };
        executor.submit(runnable);
    }

    //foobar
    public void truncateAllHints() throws ExecutionException, InterruptedException
    {
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                try
                {
                    logger.info("Truncating all stored hints.");
                    Keyspace.open(SystemKeyspace.NAME).getColumnFamilyStore(SystemKeyspace.HINTS).truncateBlocking();
                }
                catch (Exception e)
                {
                    logger.warn("Could not truncate all hints.", e);
                }
            }
        };
        executor.submit(runnable).get();

    }

    @VisibleForTesting
    protected synchronized void compact()
    {
        ArrayList<Descriptor> descriptors = new ArrayList<>();
        for (SSTable sstable : hintStore.getTracker().getUncompacting())
            descriptors.add(sstable.descriptor);

        if (descriptors.isEmpty())
            return;

        try
        {
            CompactionManager.instance.submitUserDefined(hintStore, descriptors, (int) (System.currentTimeMillis() / 1000)).get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    private int waitForSchemaAgreement(InetAddress endpoint) throws TimeoutException
    {
        Gossiper gossiper = Gossiper.instance;
        int waited = 0;
        // first, wait for schema to be gossiped.
        while (gossiper.getEndpointStateForEndpoint(endpoint) != null && gossiper.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.SCHEMA) == null)
        {
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            waited += 1000;
            if (waited > 2 * StorageService.RING_DELAY)
                throw new TimeoutException("Didin't receive gossiped schema from " + endpoint + " in " + 2 * StorageService.RING_DELAY + "ms");
        }
        if (gossiper.getEndpointStateForEndpoint(endpoint) == null)
            throw new TimeoutException("Node " + endpoint + " vanished while waiting for agreement");
        waited = 0;
        // then wait for the correct schema version.
        // usually we use DD.getDefsVersion, which checks the local schema uuid as stored in the system keyspace.
        // here we check the one in gossip instead; this serves as a canary to warn us if we introduce a bug that
        // causes the two to diverge (see CASSANDRA-2946)
        while (gossiper.getEndpointStateForEndpoint(endpoint) != null && !gossiper.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.SCHEMA).value.equals(
                gossiper.getEndpointStateForEndpoint(FBUtilities.getBroadcastAddress()).getApplicationState(ApplicationState.SCHEMA).value))
        {
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            waited += 1000;
            if (waited > 2 * StorageService.RING_DELAY)
                throw new TimeoutException("Could not reach schema agreement with " + endpoint + " in " + 2 * StorageService.RING_DELAY + "ms");
        }
        if (gossiper.getEndpointStateForEndpoint(endpoint) == null)
            throw new TimeoutException("Node " + endpoint + " vanished while waiting for agreement");
        logger.debug("schema for {} matches local schema", endpoint);
        return waited;
    }

    private void deliverHintsToEndpoint(InetAddress endpoint)
    {
        if (hintStore.isEmpty())
            return; // nothing to do, don't confuse users by logging a no-op handoff

        // check if hints delivery has been paused
        if (hintedHandOffPaused)
        {
            logger.debug("Hints delivery process is paused, aborting");
            return;
        }

        logger.debug("Checking remote({}) schema before delivering hints", endpoint);
        try
        {
            waitForSchemaAgreement(endpoint);
        }
        catch (TimeoutException e)
        {
            return;
        }

        if (!FailureDetector.instance.isAlive(endpoint))
        {
            logger.debug("Endpoint {} died before hint delivery, aborting", endpoint);
            return;
        }

        doDeliverHintsToEndpoint(endpoint);

        // Flush all the tombstones to disk
        hintStore.forceBlockingFlush();
    }

    private boolean checkDelivered(InetAddress endpoint, List<WriteResponseHandler<Mutation>> handlers, AtomicInteger rowsReplayed)
    {
        for (WriteResponseHandler<Mutation> handler : handlers)
        {
            try
            {
                handler.get();
            }
            catch (WriteTimeoutException e)
            {
                logger.info("Failed replaying hints to {}; aborting ({} delivered), error : {}",
                            endpoint, rowsReplayed, e.getMessage());
                return false;
            }
        }
        return true;
    }

    /*
     * 1. Get the key of the endpoint we need to handoff
     * 2. For each column, deserialize the mutation and send it to the endpoint
     * 3. Delete the column if the write was successful
     * 4. Force a flush
     */
    private void doDeliverHintsToEndpoint(InetAddress endpoint)
    {
        // find the hints for the node using its token.
        UUID hostId = Gossiper.instance.getHostId(endpoint);
        logger.info("Started hinted handoff for host: {} with IP: {}", hostId, endpoint);
        final ByteBuffer hostIdBytes = ByteBuffer.wrap(UUIDGen.decompose(hostId));
        DecoratedKey epkey =  StorageService.getPartitioner().decorateKey(hostIdBytes);

        final AtomicInteger rowsReplayed = new AtomicInteger(0);

        // rate limit is in bytes per second. Uses Double.MAX_VALUE if disabled (set to 0 in cassandra.yaml).
        // max rate is scaled by the number of nodes in the cluster (CASSANDRA-5272).
        int throttleInKB = DatabaseDescriptor.getHintedHandoffThrottleInKB()
                           / (StorageService.instance.getTokenMetadata().getAllEndpoints().size() - 1);
        RateLimiter rateLimiter = RateLimiter.create(throttleInKB == 0 ? Double.MAX_VALUE : throttleInKB * 1024);

        int nowInSec = FBUtilities.nowInSeconds();
        try (OpOrder.Group op = hintStore.readOrdering.start();
             RowIterator iter = UnfilteredRowIterators.filter(SinglePartitionReadCommand.fullPartitionRead(SystemKeyspace.Hints, nowInSec, epkey).queryMemtableAndDisk(hintStore, op), nowInSec))
        {
            List<WriteResponseHandler<Mutation>> responseHandlers = Lists.newArrayList();

            while (iter.hasNext())
            {
                // check if node is still alive and we should continue delivery process
                if (!FailureDetector.instance.isAlive(endpoint))
                {
                    logger.info("Endpoint {} died during hint delivery; aborting ({} delivered)", endpoint, rowsReplayed);
                    return;
                }

                // check if hints delivery has been paused during the process
                if (hintedHandOffPaused)
                {
                    logger.debug("Hints delivery process is paused, aborting");
                    return;
                }

                // Wait regularly on the endpoint acknowledgment. If we timeout on it, the endpoint is probably dead so stop delivery
                if (responseHandlers.size() > MAX_SIMULTANEOUSLY_REPLAYED_HINTS && !checkDelivered(endpoint, responseHandlers, rowsReplayed))
                    return;

                final Row hint = iter.next();
                int version = Int32Type.instance.compose(hint.clustering().get(1));
                Cell cell = hint.getCell(hintColumn);

                final long timestamp = cell.timestamp();
                DataInputPlus in = new NIODataInputStream(cell.value(), true);
                Mutation mutation;
                try
                {
                    mutation = Mutation.serializer.deserialize(in, version);
                }
                catch (UnknownColumnFamilyException e)
                {
                    logger.debug("Skipping delivery of hint for deleted table", e);
                    deleteHint(hostIdBytes, hint.clustering(), timestamp);
                    continue;
                }
                catch (IOException e)
                {
                    throw new AssertionError(e);
                }

                for (UUID cfId : mutation.getColumnFamilyIds())
                {
                    if (timestamp <= SystemKeyspace.getTruncatedAt(cfId))
                    {
                        logger.debug("Skipping delivery of hint for truncated table {}", cfId);
                        mutation = mutation.without(cfId);
                    }
                }

                if (mutation.isEmpty())
                {
                    deleteHint(hostIdBytes, hint.clustering(), timestamp);
                    continue;
                }

                MessageOut<Mutation> message = mutation.createMessage();
                rateLimiter.acquire(message.serializedSize(MessagingService.current_version));
                Runnable callback = new Runnable()
                {
                    public void run()
                    {
                        rowsReplayed.incrementAndGet();
                        deleteHint(hostIdBytes, hint.clustering(), timestamp);
                    }
                };
                WriteResponseHandler<Mutation> responseHandler = new WriteResponseHandler<>(endpoint, WriteType.SIMPLE, callback);
                MessagingService.instance().sendRR(message, endpoint, responseHandler, false);
                responseHandlers.add(responseHandler);
            }

            // Wait on the last handlers
            if (checkDelivered(endpoint, responseHandlers, rowsReplayed))
                logger.info("Finished hinted handoff of {} rows to endpoint {}", rowsReplayed, endpoint);
        }
    }

    /**
     * Attempt delivery to any node for which we have hints.  Necessary since we can generate hints even for
     * nodes which are never officially down/failed.
     */
    private void scheduleAllDeliveries()
    {
        logger.debug("Started scheduleAllDeliveries");

        // Force a major compaction to get rid of the tombstones and expired hints. Do it once, before we schedule any
        // individual replay, to avoid N - 1 redundant individual compactions (when N is the number of nodes with hints
        // to deliver to).
        compact();

        ReadCommand cmd = new PartitionRangeReadCommand(hintStore.metadata,
                                                        FBUtilities.nowInSeconds(),
                                                        ColumnFilter.all(hintStore.metadata),
                                                        RowFilter.NONE,
                                                        DataLimits.cqlLimits(Integer.MAX_VALUE, 1),
                                                        DataRange.allData(StorageService.getPartitioner()));

        try (ReadOrderGroup orderGroup = cmd.startOrderGroup(); UnfilteredPartitionIterator iter = cmd.executeLocally(orderGroup))
        {
            while (iter.hasNext())
            {
                try (UnfilteredRowIterator partition = iter.next())
                {
                    UUID hostId = UUIDGen.getUUID(partition.partitionKey().getKey());
                    InetAddress target = StorageService.instance.getTokenMetadata().getEndpointForHostId(hostId);
                    // token may have since been removed (in which case we have just read back a tombstone)
                    if (target != null)
                        scheduleHintDelivery(target, false);
                }
            }
        }

        logger.debug("Finished scheduleAllDeliveries");
    }

    /*
     * This method is used to deliver hints to a particular endpoint.
     * When we learn that some endpoint is back up we deliver the data
     * to him via an event driven mechanism.
    */
    public void scheduleHintDelivery(final InetAddress to, final boolean precompact)
    {
        // We should not deliver hints to the same host in 2 different threads
        if (!queuedDeliveries.add(to))
            return;

        logger.debug("Scheduling delivery of Hints to {}", to);

        executor.execute(new Runnable()
        {
            public void run()
            {
                try
                {
                    // If it's an individual node hint replay (triggered by Gossip or via JMX), and not the global scheduled replay
                    // (every 10 minutes), force a major compaction to get rid of the tombstones and expired hints.
                    if (precompact)
                        compact();

                    deliverHintsToEndpoint(to);
                }
                finally
                {
                    queuedDeliveries.remove(to);
                }
            }
        });
    }

    public void scheduleHintDelivery(String to) throws UnknownHostException
    {
        scheduleHintDelivery(InetAddress.getByName(to), true);
    }

    public void pauseHintsDelivery(boolean b)
    {
        hintedHandOffPaused = b;
    }

    public List<String> listEndpointsPendingHints()
    {
        Token.TokenFactory tokenFactory = StorageService.getPartitioner().getTokenFactory();

        // Extract the keys as strings to be reported.
        LinkedList<String> result = new LinkedList<>();
        ReadCommand cmd = PartitionRangeReadCommand.allDataRead(SystemKeyspace.Hints, FBUtilities.nowInSeconds());
        try (ReadOrderGroup orderGroup = cmd.startOrderGroup(); UnfilteredPartitionIterator iter = cmd.executeLocally(orderGroup))
        {
            while (iter.hasNext())
            {
                try (UnfilteredRowIterator partition = iter.next())
                {
                    // We don't delete by range on the hints table, so we don't have to worry about the
                    // iterator returning only range tombstone marker
                    if (partition.hasNext())
                        result.addFirst(tokenFactory.toString(partition.partitionKey().getToken()));
                }
            }
        }
        return result;
    }
}
