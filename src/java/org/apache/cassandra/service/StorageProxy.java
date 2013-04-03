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
package org.apache.cassandra.service;

import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.base.Function;
import com.google.common.collect.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.FastByteArrayOutputStream;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.metrics.ClientRequestMetrics;
import org.apache.cassandra.net.*;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class StorageProxy implements StorageProxyMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=StorageProxy";
    private static final Logger logger = LoggerFactory.getLogger(StorageProxy.class);
    private static final boolean OPTIMIZE_LOCAL_REQUESTS = true; // set to false to test messagingservice path on single node

    public static final String UNREACHABLE = "UNREACHABLE";

    private static final WritePerformer standardWritePerformer;
    private static final WritePerformer counterWritePerformer;
    private static final WritePerformer counterWriteOnCoordinatorPerformer;

    public static final StorageProxy instance = new StorageProxy();

    private static volatile int maxHintsInProgress = 1024 * FBUtilities.getAvailableProcessors();
    private static final AtomicInteger totalHintsInProgress = new AtomicInteger();
    private static final Map<InetAddress, AtomicInteger> hintsInProgress = new MapMaker().concurrencyLevel(1).makeComputingMap(new Function<InetAddress, AtomicInteger>()
    {
        public AtomicInteger apply(InetAddress inetAddress)
        {
            return new AtomicInteger(0);
        }
    });
    private static final AtomicLong totalHints = new AtomicLong();
    private static final ClientRequestMetrics readMetrics = new ClientRequestMetrics("Read");
    private static final ClientRequestMetrics rangeMetrics = new ClientRequestMetrics("RangeSlice");
    private static final ClientRequestMetrics writeMetrics = new ClientRequestMetrics("Write");

    private StorageProxy() {}

    static
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(new StorageProxy(), new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        standardWritePerformer = new WritePerformer()
        {
            public void apply(IMutation mutation,
                              Iterable<InetAddress> targets,
                              AbstractWriteResponseHandler responseHandler,
                              String localDataCenter,
                              ConsistencyLevel consistency_level)
            throws IOException, OverloadedException
            {
                assert mutation instanceof RowMutation;
                sendToHintedEndpoints((RowMutation) mutation, targets, responseHandler, localDataCenter, consistency_level);
            }
        };

        /*
         * We execute counter writes in 2 places: either directly in the coordinator node if it is a replica, or
         * in CounterMutationVerbHandler on a replica othewise. The write must be executed on the MUTATION stage
         * but on the latter case, the verb handler already run on the MUTATION stage, so we must not execute the
         * underlying on the stage otherwise we risk a deadlock. Hence two different performer.
         */
        counterWritePerformer = new WritePerformer()
        {
            public void apply(IMutation mutation,
                              Iterable<InetAddress> targets,
                              AbstractWriteResponseHandler responseHandler,
                              String localDataCenter,
                              ConsistencyLevel consistency_level)
            throws IOException
            {
                if (logger.isTraceEnabled())
                    logger.trace("insert writing local & replicate " + mutation.toString(true));

                Runnable runnable = counterWriteTask(mutation, targets, responseHandler, localDataCenter, consistency_level);
                runnable.run();
            }
        };

        counterWriteOnCoordinatorPerformer = new WritePerformer()
        {
            public void apply(IMutation mutation,
                              Iterable<InetAddress> targets,
                              AbstractWriteResponseHandler responseHandler,
                              String localDataCenter,
                              ConsistencyLevel consistency_level)
            throws IOException
            {
                if (logger.isTraceEnabled())
                    logger.trace("insert writing local & replicate " + mutation.toString(true));

                Runnable runnable = counterWriteTask(mutation, targets, responseHandler, localDataCenter, consistency_level);
                StageManager.getStage(Stage.MUTATION).execute(runnable);
            }
        };
    }

    /**
     * Use this method to have these Mutations applied
     * across all replicas. This method will take care
     * of the possibility of a replica being down and hint
     * the data across to some other replica.
     *
     * @param mutations the mutations to be applied across the replicas
     * @param consistency_level the consistency level for the operation
     */
    public static void mutate(Collection<? extends IMutation> mutations, ConsistencyLevel consistency_level)
    throws UnavailableException, OverloadedException, WriteTimeoutException
    {
        Tracing.trace("Determining replicas for mutation");
        logger.trace("Mutations/ConsistencyLevel are {}/{}", mutations, consistency_level);
        final String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());

        long startTime = System.nanoTime();
        List<AbstractWriteResponseHandler> responseHandlers = new ArrayList<AbstractWriteResponseHandler>(mutations.size());

        IMutation mostRecentMutation = null;
        try
        {
            for (IMutation mutation : mutations)
            {
                mostRecentMutation = mutation;
                if (mutation instanceof CounterMutation)
                {
                    responseHandlers.add(mutateCounter((CounterMutation)mutation, localDataCenter));
                }
                else
                {
                    WriteType wt = mutations.size() <= 1 ? WriteType.SIMPLE : WriteType.UNLOGGED_BATCH;
                    responseHandlers.add(performWrite(mutation, consistency_level, localDataCenter, standardWritePerformer, null, wt));
                }
            }

            // wait for writes.  throws TimeoutException if necessary
            for (AbstractWriteResponseHandler responseHandler : responseHandlers)
            {
                responseHandler.get();
            }

        }
        catch (WriteTimeoutException ex)
        {
            writeMetrics.timeouts.mark();
            ClientRequestMetrics.writeTimeouts.inc();
            if (logger.isDebugEnabled())
            {
                List<String> mstrings = new ArrayList<String>(mutations.size());
                for (IMutation mutation : mutations)
                    mstrings.add(mutation.toString(true));
                logger.debug("Write timeout {} for one (or more) of: {}", ex.toString(), mstrings);
            }
            Tracing.trace("Write timeout");
            throw ex;
        }
        catch (UnavailableException e)
        {
            writeMetrics.unavailables.mark();
            ClientRequestMetrics.writeUnavailables.inc();
            Tracing.trace("Unavailable");
            throw e;
        }
        catch (OverloadedException e)
        {
            ClientRequestMetrics.writeUnavailables.inc();
            Tracing.trace("Overloaded");
            throw e;
        }
        catch (IOException e)
        {
            assert mostRecentMutation != null;
            throw new RuntimeException("error writing key " + ByteBufferUtil.bytesToHex(mostRecentMutation.key()), e);
        }
        finally
        {
            writeMetrics.addNano(System.nanoTime() - startTime);
        }
    }

    /**
     * See mutate. Adds additional steps before and after writing a batch.
     * Before writing the batch (but after doing availability check against the FD for the row replicas):
     *      write the entire batch to a batchlog elsewhere in the cluster.
     * After: remove the batchlog entry (after writing hints for the batch rows, if necessary).
     *
     * @param mutations the RowMutations to be applied across the replicas
     * @param consistency_level the consistency level for the operation
     */
    public static void mutateAtomically(Collection<RowMutation> mutations, ConsistencyLevel consistency_level)
    throws UnavailableException, OverloadedException, WriteTimeoutException
    {
        Tracing.trace("Determining replicas for atomic batch");
        long startTime = System.nanoTime();
        logger.trace("Mutations/ConsistencyLevel are {}/{}", mutations, consistency_level);

        List<WriteResponseHandlerWrapper> wrappers = new ArrayList<WriteResponseHandlerWrapper>(mutations.size());
        String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());

        try
        {
            // add a handler for each mutation - includes checking availability, but doesn't initiate any writes, yet
            for (RowMutation mutation : mutations)
            {
                WriteResponseHandlerWrapper wrapper = wrapResponseHandler(mutation, consistency_level, WriteType.BATCH);
                // exit early if we can't fulfill the CL at this time.
                wrapper.handler.assureSufficientLiveNodes();
                wrappers.add(wrapper);
            }

            // write to the batchlog
            Collection<InetAddress> batchlogEndpoints = getBatchlogEndpoints(localDataCenter);
            UUID batchUUID = UUID.randomUUID();
            syncWriteToBatchlog(mutations, batchlogEndpoints, batchUUID);

            // now actually perform the writes and wait for them to complete
            syncWriteBatchedMutations(wrappers, localDataCenter, consistency_level);

            // remove the batchlog entries asynchronously
            asyncRemoveFromBatchlog(batchlogEndpoints, batchUUID);
        }
        catch (UnavailableException e)
        {
            writeMetrics.unavailables.mark();
            ClientRequestMetrics.writeUnavailables.inc();
            Tracing.trace("Unavailable");
            throw e;
        }
        catch (WriteTimeoutException e)
        {
            writeMetrics.timeouts.mark();
            ClientRequestMetrics.writeTimeouts.inc();
            Tracing.trace("Write timeout");
            throw e;
        }
        finally
        {
            writeMetrics.addNano(System.nanoTime() - startTime);
        }
    }

    private static void syncWriteToBatchlog(Collection<RowMutation> mutations, Collection<InetAddress> endpoints, UUID uuid)
    throws WriteTimeoutException
    {
        RowMutation rm = BatchlogManager.getBatchlogMutationFor(mutations, uuid);
        AbstractWriteResponseHandler handler = new WriteResponseHandler(endpoints,
                                                                        Collections.<InetAddress>emptyList(),
                                                                        ConsistencyLevel.ONE,
                                                                        Table.open(Table.SYSTEM_KS),
                                                                        null,
                                                                        WriteType.BATCH_LOG);
        updateBatchlog(rm, endpoints, handler);
        handler.get();
    }

    private static void asyncRemoveFromBatchlog(Collection<InetAddress> endpoints, UUID uuid)
    {
        RowMutation rm = new RowMutation(Table.SYSTEM_KS, UUIDType.instance.decompose(uuid));
        rm.delete(new QueryPath(SystemTable.BATCHLOG_CF), FBUtilities.timestampMicros());
        AbstractWriteResponseHandler handler = new WriteResponseHandler(endpoints,
                                                                        Collections.<InetAddress>emptyList(),
                                                                        ConsistencyLevel.ANY,
                                                                        Table.open(Table.SYSTEM_KS),
                                                                        null,
                                                                        WriteType.SIMPLE);
        updateBatchlog(rm, endpoints, handler);
    }

    private static void updateBatchlog(RowMutation rm, Collection<InetAddress> endpoints, AbstractWriteResponseHandler handler)
    {
        if (endpoints.contains(FBUtilities.getBroadcastAddress()))
        {
            assert endpoints.size() == 1;
            insertLocal(rm, handler);
        }
        else
        {
            sendMessagesToOneDC(rm.createMessage(), endpoints, true, handler);
        }
    }

    private static void syncWriteBatchedMutations(List<WriteResponseHandlerWrapper> wrappers,
                                                  String localDataCenter,
                                                  ConsistencyLevel consistencyLevel)
    throws WriteTimeoutException, OverloadedException
    {
        for (WriteResponseHandlerWrapper wrapper : wrappers)
        {
            Iterable<InetAddress> endpoints = Iterables.concat(wrapper.handler.naturalEndpoints, wrapper.handler.pendingEndpoints);
            sendToHintedEndpoints(wrapper.mutation, endpoints, wrapper.handler, localDataCenter, consistencyLevel);
        }

        for (WriteResponseHandlerWrapper wrapper : wrappers)
        {
            wrapper.handler.get();
        }
    }

    /**
     * Perform the write of a mutation given a WritePerformer.
     * Gather the list of write endpoints, apply locally and/or forward the mutation to
     * said write endpoint (deletaged to the actual WritePerformer) and wait for the
     * responses based on consistency level.
     *
     * @param mutation the mutation to be applied
     * @param consistency_level the consistency level for the write operation
     * @param performer the WritePerformer in charge of appliying the mutation
     * given the list of write endpoints (either standardWritePerformer for
     * standard writes or counterWritePerformer for counter writes).
     * @param callback an optional callback to be run if and when the write is
     * successful.
     */
    public static AbstractWriteResponseHandler performWrite(IMutation mutation,
                                                            ConsistencyLevel consistency_level,
                                                            String localDataCenter,
                                                            WritePerformer performer,
                                                            Runnable callback,
                                                            WriteType writeType)
    throws UnavailableException, OverloadedException, IOException
    {
        String table = mutation.getTable();
        AbstractReplicationStrategy rs = Table.open(table).getReplicationStrategy();

        Token tk = StorageService.getPartitioner().getToken(mutation.key());
        List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(table, tk);
        Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, table);

        AbstractWriteResponseHandler responseHandler = rs.getWriteResponseHandler(naturalEndpoints, pendingEndpoints, consistency_level, callback, writeType);

        // exit early if we can't fulfill the CL at this time
        responseHandler.assureSufficientLiveNodes();

        performer.apply(mutation, Iterables.concat(naturalEndpoints, pendingEndpoints), responseHandler, localDataCenter, consistency_level);
        return responseHandler;
    }

    // same as above except does not initiate writes (but does perfrom availability checks).
    private static WriteResponseHandlerWrapper wrapResponseHandler(RowMutation mutation, ConsistencyLevel consistency_level, WriteType writeType)
    {
        AbstractReplicationStrategy rs = Table.open(mutation.getTable()).getReplicationStrategy();
        String table = mutation.getTable();
        Token tk = StorageService.getPartitioner().getToken(mutation.key());
        List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(table, tk);
        Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, table);
        AbstractWriteResponseHandler responseHandler = rs.getWriteResponseHandler(naturalEndpoints, pendingEndpoints, consistency_level, null, writeType);
        return new WriteResponseHandlerWrapper(responseHandler, mutation);
    }

    // used by atomic_batch_mutate to decouple availability check from the write itself, caches consistency level and endpoints.
    private static class WriteResponseHandlerWrapper
    {
        final AbstractWriteResponseHandler handler;
        final RowMutation mutation;

        WriteResponseHandlerWrapper(AbstractWriteResponseHandler handler, RowMutation mutation)
        {
            this.handler = handler;
            this.mutation = mutation;
        }
    }

    /*
     * Replicas are picked manually:
     * - replicas should be alive according to the failure detector
     * - replicas should be in the local datacenter
     * - choose min(2, number of qualifying candiates above)
     * - allow the local node to be the only replica only if it's a single-node cluster
     */
    private static Collection<InetAddress> getBatchlogEndpoints(String localDataCenter) throws UnavailableException
    {
        // will include every known node in the DC, including localhost.
        TokenMetadata.Topology topology = StorageService.instance.getTokenMetadata().cloneOnlyTokenMap().getTopology();
        Collection<InetAddress> localMembers = topology.getDatacenterEndpoints().get(localDataCenter);

        // special case for single-node datacenters
        if (localMembers.size() == 1)
            return localMembers;

        // not a single-node cluster - don't count the local node.
        localMembers.remove(FBUtilities.getBroadcastAddress());

        // include only alive nodes
        List<InetAddress> candidates = new ArrayList<InetAddress>(localMembers.size());
        for (InetAddress member : localMembers)
        {
            if (FailureDetector.instance.isAlive(member))
                candidates.add(member);
        }

        if (candidates.isEmpty())
            throw new UnavailableException(ConsistencyLevel.ONE, 1, 0);

        if (candidates.size() > 2)
        {
            IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
            snitch.sortByProximity(FBUtilities.getBroadcastAddress(), candidates);
            candidates = candidates.subList(0, 2);
        }

        return candidates;
    }

    /**
     * Send the mutations to the right targets, write it locally if it corresponds or writes a hint when the node
     * is not available.
     *
     * Note about hints:
     *
     * | Hinted Handoff | Consist. Level |
     * | on             |       >=1      | --> wait for hints. We DO NOT notify the handler with handler.response() for hints;
     * | on             |       ANY      | --> wait for hints. Responses count towards consistency.
     * | off            |       >=1      | --> DO NOT fire hints. And DO NOT wait for them to complete.
     * | off            |       ANY      | --> DO NOT fire hints. And DO NOT wait for them to complete.
     *
     * @throws TimeoutException if the hints cannot be written/enqueued
     */
    public static void sendToHintedEndpoints(final RowMutation rm,
                                             Iterable<InetAddress> targets,
                                             AbstractWriteResponseHandler responseHandler,
                                             String localDataCenter,
                                             ConsistencyLevel consistency_level)
    throws OverloadedException
    {
        // Multimap that holds onto all the messages and addresses meant for a specific datacenter
        Map<String, Multimap<MessageOut, InetAddress>> dcMessages = new HashMap<String, Multimap<MessageOut, InetAddress>>();

        for (InetAddress destination : targets)
        {
            // avoid OOMing due to excess hints.  we need to do this check even for "live" nodes, since we can
            // still generate hints for those if it's overloaded or simply dead but not yet known-to-be-dead.
            // The idea is that if we have over maxHintsInProgress hints in flight, this is probably due to
            // a small number of nodes causing problems, so we should avoid shutting down writes completely to
            // healthy nodes.  Any node with no hintsInProgress is considered healthy.
            if (totalHintsInProgress.get() > maxHintsInProgress
                && (hintsInProgress.get(destination).get() > 0 && shouldHint(destination)))
            {
                throw new OverloadedException("Too many in flight hints: " + totalHintsInProgress.get());
            }

            if (FailureDetector.instance.isAlive(destination))
            {
                if (destination.equals(FBUtilities.getBroadcastAddress()) && OPTIMIZE_LOCAL_REQUESTS)
                {
                    insertLocal(rm, responseHandler);
                }
                else
                {
                    // belongs on a different server
                    if (logger.isTraceEnabled())
                        logger.trace("insert writing key " + ByteBufferUtil.bytesToHex(rm.key()) + " to " + destination);

                    String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(destination);
                    Multimap<MessageOut, InetAddress> messages = dcMessages.get(dc);
                    if (messages == null)
                    {
                        messages = HashMultimap.create();
                        dcMessages.put(dc, messages);
                    }

                    messages.put(rm.createMessage(), destination);
                }
            }
            else
            {
                if (!shouldHint(destination))
                    continue;

                // Schedule a local hint
                submitHint(rm, destination, responseHandler, consistency_level);
            }
        }

        sendMessages(localDataCenter, dcMessages, responseHandler);
    }

    public static Future<Void> submitHint(final RowMutation mutation,
                                          final InetAddress target,
                                          final AbstractWriteResponseHandler responseHandler,
                                          final ConsistencyLevel consistencyLevel)
    {
        // local write that time out should be handled by LocalMutationRunnable
        assert !target.equals(FBUtilities.getBroadcastAddress()) : target;

        HintRunnable runnable = new HintRunnable(target)
        {
            public void runMayThrow() throws IOException
            {
                logger.debug("Adding hint for {}", target);

                writeHintForMutation(mutation, target);
                // Notify the handler only for CL == ANY
                if (responseHandler != null && consistencyLevel == ConsistencyLevel.ANY)
                    responseHandler.response(null);
            }
        };

        return submitHint(runnable);
    }

    private static Future<Void> submitHint(HintRunnable runnable)
    {
        totalHintsInProgress.incrementAndGet();
        hintsInProgress.get(runnable.target).incrementAndGet();
        return (Future<Void>) StageManager.getStage(Stage.MUTATION).submit(runnable);
    }

    public static void writeHintForMutation(RowMutation mutation, InetAddress target) throws IOException
    {
        UUID hostId = StorageService.instance.getTokenMetadata().getHostId(target);
        if ((hostId == null) && (MessagingService.instance().getVersion(target) < MessagingService.VERSION_12))
        {
            logger.warn("Unable to store hint for host with missing ID, {} (old node?)", target.toString());
            return;
        }
        assert hostId != null : "Missing host ID for " + target.getHostAddress();
        RowMutation hintedMutation = RowMutation.hintFor(mutation, hostId);
        hintedMutation.apply();

        totalHints.incrementAndGet();
    }

    /**
     * for each datacenter, send a message to one node to relay the write to other replicas
     */
    private static void sendMessages(String localDataCenter, Map<String, Multimap<MessageOut, InetAddress>> dcMessages, AbstractWriteResponseHandler handler)
    {
        for (Map.Entry<String, Multimap<MessageOut, InetAddress>> entry: dcMessages.entrySet())
        {
            boolean isLocalDC = entry.getKey().equals(localDataCenter);
            for (Map.Entry<MessageOut, Collection<InetAddress>> messages: entry.getValue().asMap().entrySet())
            {
                MessageOut message = messages.getKey();
                Collection<InetAddress> targets = messages.getValue();
                // a single message object is used for unhinted writes, so clean out any forwards
                // from previous loop iterations
                message = message.withHeaderRemoved(RowMutation.FORWARD_TO);
                sendMessagesToOneDC(message, targets, isLocalDC, handler);
            }
        }
    }

    private static void sendMessagesToOneDC(MessageOut message, Collection<InetAddress> targets, boolean localDC, AbstractWriteResponseHandler handler)
    {
        try
        {
            sendMessagesToOneDCInternal(message, targets, localDC, handler);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static void sendMessagesToOneDCInternal(MessageOut message, Collection<InetAddress> targets, boolean localDC, AbstractWriteResponseHandler handler) throws IOException
    {
        Iterator<InetAddress> iter = targets.iterator();
        InetAddress target = iter.next();

        // direct writes to local DC or old Cassandra versions
        if (localDC || MessagingService.instance().getVersion(target) < MessagingService.VERSION_11)
        {
            // yes, the loop and non-loop code here are the same; this is clunky but we want to avoid
            // creating a second iterator since we already have a perfectly good one
            MessagingService.instance().sendRR(message, target, handler);
            while (iter.hasNext())
            {
                target = iter.next();
                MessagingService.instance().sendRR(message, target, handler);
            }
            return;
        }

        // Add all the other destinations of the same message as a FORWARD_HEADER entry
        FastByteArrayOutputStream bos = new FastByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        dos.writeInt(targets.size() - 1);
        while (iter.hasNext())
        {
            InetAddress destination = iter.next();
            CompactEndpointSerializationHelper.serialize(destination, dos);
            String id = MessagingService.instance().addCallback(handler, message, destination, message.getTimeout());
            dos.writeUTF(id);
            logger.trace("Adding FWD message to {}@{}", id, destination);
        }
        message = message.withParameter(RowMutation.FORWARD_TO, bos.toByteArray());
        // send the combined message + forward headers
        String id = MessagingService.instance().sendRR(message, target, handler);
        logger.trace("Sending message to {}@{}", id, target);
    }

    private static void insertLocal(final RowMutation rm, final AbstractWriteResponseHandler responseHandler)
    {
        if (logger.isTraceEnabled())
            logger.trace("insert writing local " + rm.toString(true));

        Runnable runnable = new DroppableRunnable(MessagingService.Verb.MUTATION)
        {
            public void runMayThrow() throws IOException
            {
                rm.apply();
                responseHandler.response(null);
            }
        };
        StageManager.getStage(Stage.MUTATION).execute(runnable);
    }

    /**
     * Handle counter mutation on the coordinator host.
     *
     * A counter mutation needs to first be applied to a replica (that we'll call the leader for the mutation) before being
     * replicated to the other endpoint. To achieve so, there is two case:
     *   1) the coordinator host is a replica: we proceed to applying the update locally and replicate throug
     *   applyCounterMutationOnCoordinator
     *   2) the coordinator is not a replica: we forward the (counter)mutation to a chosen replica (that will proceed through
     *   applyCounterMutationOnLeader upon receive) and wait for its acknowledgment.
     *
     * Implementation note: We check if we can fulfill the CL on the coordinator host even if he is not a replica to allow
     * quicker response and because the WriteResponseHandlers don't make it easy to send back an error. We also always gather
     * the write latencies at the coordinator node to make gathering point similar to the case of standard writes.
     */
    public static AbstractWriteResponseHandler mutateCounter(CounterMutation cm, String localDataCenter) throws UnavailableException, OverloadedException, IOException
    {
        InetAddress endpoint = findSuitableEndpoint(cm.getTable(), cm.key(), localDataCenter, cm.consistency());

        if (endpoint.equals(FBUtilities.getBroadcastAddress()))
        {
            return applyCounterMutationOnCoordinator(cm, localDataCenter);
        }
        else
        {
            // Exit now if we can't fulfill the CL here instead of forwarding to the leader replica
            String table = cm.getTable();
            AbstractReplicationStrategy rs = Table.open(table).getReplicationStrategy();
            Token tk = StorageService.getPartitioner().getToken(cm.key());
            List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(table, tk);
            Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, table);

            rs.getWriteResponseHandler(naturalEndpoints, pendingEndpoints, cm.consistency(), null, WriteType.COUNTER).assureSufficientLiveNodes();

            // Forward the actual update to the chosen leader replica
            AbstractWriteResponseHandler responseHandler = new WriteResponseHandler(endpoint, WriteType.COUNTER);

            if (logger.isTraceEnabled())
                logger.trace("forwarding counter update of key " + ByteBufferUtil.bytesToHex(cm.key()) + " to " + endpoint);
            MessagingService.instance().sendRR(cm.makeMutationMessage(), endpoint, responseHandler);
            return responseHandler;
        }
    }

    /**
     * Find a suitable replica as leader for counter update.
     * For now, we pick a random replica in the local DC (or ask the snitch if
     * there is no replica alive in the local DC).
     * TODO: if we track the latency of the counter writes (which makes sense
     * contrarily to standard writes since there is a read involved), we could
     * trust the dynamic snitch entirely, which may be a better solution. It
     * is unclear we want to mix those latencies with read latencies, so this
     * may be a bit involved.
     */
    private static InetAddress findSuitableEndpoint(String tableName, ByteBuffer key, String localDataCenter, ConsistencyLevel cl) throws UnavailableException
    {
        Table table = Table.open(tableName);
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        List<InetAddress> endpoints = StorageService.instance.getLiveNaturalEndpoints(table, key);
        if (endpoints.isEmpty())
            // TODO have a way to compute the consistency level
            throw new UnavailableException(cl, cl.blockFor(table), 0);

        List<InetAddress> localEndpoints = new ArrayList<InetAddress>();
        for (InetAddress endpoint : endpoints)
        {
            if (snitch.getDatacenter(endpoint).equals(localDataCenter))
                localEndpoints.add(endpoint);
        }
        if (localEndpoints.isEmpty())
        {
            // No endpoint in local DC, pick the closest endpoint according to the snitch
            snitch.sortByProximity(FBUtilities.getBroadcastAddress(), endpoints);
            return endpoints.get(0);
        }
        else
        {
            return localEndpoints.get(FBUtilities.threadLocalRandom().nextInt(localEndpoints.size()));
        }
    }

    // Must be called on a replica of the mutation. This replica becomes the
    // leader of this mutation.
    public static AbstractWriteResponseHandler applyCounterMutationOnLeader(CounterMutation cm, String localDataCenter, Runnable callback)
    throws UnavailableException, IOException, OverloadedException
    {
        return performWrite(cm, cm.consistency(), localDataCenter, counterWritePerformer, callback, WriteType.COUNTER);
    }

    // Same as applyCounterMutationOnLeader but must with the difference that it use the MUTATION stage to execute the write (while
    // applyCounterMutationOnLeader assumes it is on the MUTATION stage already)
    public static AbstractWriteResponseHandler applyCounterMutationOnCoordinator(CounterMutation cm, String localDataCenter)
    throws UnavailableException, IOException, OverloadedException
    {
        return performWrite(cm, cm.consistency(), localDataCenter, counterWriteOnCoordinatorPerformer, null, WriteType.COUNTER);
    }

    private static Runnable counterWriteTask(final IMutation mutation,
                                             final Iterable<InetAddress> targets,
                                             final AbstractWriteResponseHandler responseHandler,
                                             final String localDataCenter,
                                             final ConsistencyLevel consistency_level)
    {
        return new LocalMutationRunnable()
        {
            public void runMayThrow() throws IOException
            {
                assert mutation instanceof CounterMutation;
                final CounterMutation cm = (CounterMutation) mutation;

                // apply mutation
                cm.apply();
                responseHandler.response(null);

                // then send to replicas, if any
                final Set<InetAddress> remotes = Sets.difference(ImmutableSet.copyOf(targets), ImmutableSet.of(FBUtilities.getBroadcastAddress()));
                if (cm.shouldReplicateOnWrite() && !remotes.isEmpty())
                {
                    // We do the replication on another stage because it involves a read (see CM.makeReplicationMutation)
                    // and we want to avoid blocking too much the MUTATION stage
                    StageManager.getStage(Stage.REPLICATE_ON_WRITE).execute(new DroppableRunnable(MessagingService.Verb.READ)
                    {
                        public void runMayThrow() throws OverloadedException
                        {
                            // send mutation to other replica
                            sendToHintedEndpoints(cm.makeReplicationMutation(), remotes, responseHandler, localDataCenter, consistency_level);
                        }
                    });
                }
            }
        };
    }

    private static boolean systemTableQuery(List<ReadCommand> cmds)
    {
        for (ReadCommand cmd : cmds)
            if (!cmd.table.equals(Table.SYSTEM_KS))
                return false;
        return true;
    }

    /**
     * Performs the actual reading of a row out of the StorageService, fetching
     * a specific set of column names from a given column family.
     */
    public static List<Row> read(List<ReadCommand> commands, ConsistencyLevel consistency_level)
    throws UnavailableException, IsBootstrappingException, ReadTimeoutException
    {
        if (StorageService.instance.isBootstrapMode() && !systemTableQuery(commands))
        {
            readMetrics.unavailables.mark();
            ClientRequestMetrics.readUnavailables.inc();
            throw new IsBootstrappingException();
        }
        long startTime = System.nanoTime();
        List<Row> rows = null;
        try
        {
            rows = fetchRows(commands, consistency_level);
        }
        catch (UnavailableException e)
        {
            readMetrics.unavailables.mark();
            ClientRequestMetrics.readUnavailables.inc();
            throw e;
        }
        catch (ReadTimeoutException e)
        {
            readMetrics.timeouts.mark();
            ClientRequestMetrics.readTimeouts.inc();
            throw e;
        }
        finally
        {
            readMetrics.addNano(System.nanoTime() - startTime);
        }
        return rows;
    }

    /**
     * This function executes local and remote reads, and blocks for the results:
     *
     * 1. Get the replica locations, sorted by response time according to the snitch
     * 2. Send a data request to the closest replica, and digest requests to either
     *    a) all the replicas, if read repair is enabled
     *    b) the closest R-1 replicas, where R is the number required to satisfy the ConsistencyLevel
     * 3. Wait for a response from R replicas
     * 4. If the digests (if any) match the data return the data
     * 5. else carry out read repair by getting data from all the nodes.
     */
    private static List<Row> fetchRows(List<ReadCommand> initialCommands, ConsistencyLevel consistency_level)
    throws UnavailableException, ReadTimeoutException
    {
        List<Row> rows = new ArrayList<Row>(initialCommands.size());
        List<ReadCommand> commandsToRetry = Collections.emptyList();

        do
        {
            List<ReadCommand> commands = commandsToRetry.isEmpty() ? initialCommands : commandsToRetry;
            ReadCallback<ReadResponse, Row>[] readCallbacks = new ReadCallback[commands.size()];

            if (!commandsToRetry.isEmpty())
                logger.debug("Retrying {} commands", commandsToRetry.size());

            // send out read requests
            for (int i = 0; i < commands.size(); i++)
            {
                ReadCommand command = commands.get(i);
                Table table = Table.open(command.getKeyspace());
                assert !command.isDigestQuery();
                logger.trace("Command/ConsistencyLevel is {}/{}", command, consistency_level);

                List<InetAddress> endpoints = getLiveSortedEndpoints(table, command.key);
                CFMetaData cfm = Schema.instance.getCFMetaData(command.getKeyspace(), command.getColumnFamilyName());
                endpoints = consistency_level.filterForQuery(table, endpoints, cfm.newReadRepairDecision());

                RowDigestResolver resolver = new RowDigestResolver(command.table, command.key);
                ReadCallback<ReadResponse, Row> handler = new ReadCallback(resolver, consistency_level, command, endpoints);
                handler.assureSufficientLiveNodes();
                assert !endpoints.isEmpty();
                readCallbacks[i] = handler;

                // The data-request message is sent to dataPoint, the node that will actually get the data for us
                InetAddress dataPoint = endpoints.get(0);
                if (dataPoint.equals(FBUtilities.getBroadcastAddress()) && OPTIMIZE_LOCAL_REQUESTS)
                {
                    logger.trace("reading data locally");
                    StageManager.getStage(Stage.READ).execute(new LocalReadRunnable(command, handler));
                }
                else
                {
                    logger.trace("reading data from {}", dataPoint);
                    MessagingService.instance().sendRR(command.createMessage(), dataPoint, handler);
                }

                if (endpoints.size() == 1)
                    continue;

                // send the other endpoints a digest request
                ReadCommand digestCommand = command.copy();
                digestCommand.setDigestQuery(true);
                MessageOut message = null;
                for (InetAddress digestPoint : endpoints.subList(1, endpoints.size()))
                {
                    if (digestPoint.equals(FBUtilities.getBroadcastAddress()) && OPTIMIZE_LOCAL_REQUESTS)
                    {
                        logger.trace("reading digest locally");
                        StageManager.getStage(Stage.READ).execute(new LocalReadRunnable(digestCommand, handler));
                    }
                    else
                    {
                        logger.trace("reading digest from {}", digestPoint);
                        // (We lazy-construct the digest Message object since it may not be necessary if we
                        // are doing a local digest read, or no digest reads at all.)
                        if (message == null)
                            message = digestCommand.createMessage();
                        MessagingService.instance().sendRR(message, digestPoint, handler);
                    }
                }
            }

            // read results and make a second pass for any digest mismatches
            List<ReadCommand> repairCommands = null;
            List<ReadCallback<ReadResponse, Row>> repairResponseHandlers = null;
            for (int i = 0; i < commands.size(); i++)
            {
                ReadCallback<ReadResponse, Row> handler = readCallbacks[i];
                ReadCommand command = commands.get(i);
                try
                {
                    Row row = handler.get();
                    if (row != null)
                    {
                        command.maybeTrim(row);
                        rows.add(row);
                    }
                }
                catch (ReadTimeoutException ex)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Read timeout: {}", ex.toString());
                    throw ex;
                }
                catch (DigestMismatchException ex)
                {
                    logger.debug("Digest mismatch: {}", ex.toString());
                    // Do a full data read to resolve the correct response (and repair node that need be)
                    RowDataResolver resolver = new RowDataResolver(command.table, command.key, command.filter());
                    ReadCallback<ReadResponse, Row> repairHandler = handler.withNewResolver(resolver);

                    if (repairCommands == null)
                    {
                        repairCommands = new ArrayList<ReadCommand>();
                        repairResponseHandlers = new ArrayList<ReadCallback<ReadResponse, Row>>();
                    }
                    repairCommands.add(command);
                    repairResponseHandlers.add(repairHandler);

                    for (InetAddress endpoint : handler.endpoints)
                    {
                        MessageOut<ReadCommand> message = command.createMessage();
                        MessagingService.instance().sendRR(message, endpoint, repairHandler);
                    }
                }
            }

            if (commandsToRetry != Collections.EMPTY_LIST)
                commandsToRetry.clear();

            // read the results for the digest mismatch retries
            if (repairResponseHandlers != null)
            {
                for (int i = 0; i < repairCommands.size(); i++)
                {
                    ReadCommand command = repairCommands.get(i);
                    ReadCallback<ReadResponse, Row> handler = repairResponseHandlers.get(i);

                    Row row;
                    try
                    {
                        row = handler.get();
                    }
                    catch (DigestMismatchException e)
                    {
                        throw new AssertionError(e); // full data requested from each node here, no digests should be sent
                    }

                    RowDataResolver resolver = (RowDataResolver)handler.resolver;
                    try
                    {
                        // wait for the repair writes to be acknowledged, to minimize impact on any replica that's
                        // behind on writes in case the out-of-sync row is read multiple times in quick succession
                        FBUtilities.waitOnFutures(resolver.repairResults, DatabaseDescriptor.getWriteRpcTimeout());
                    }
                    catch (TimeoutException e)
                    {
                        int blockFor = consistency_level.blockFor(Table.open(command.getKeyspace()));
                        throw new ReadTimeoutException(consistency_level, blockFor, blockFor, true);
                    }

                    // retry any potential short reads
                    ReadCommand retryCommand = command.maybeGenerateRetryCommand(resolver, row);
                    if (retryCommand != null)
                    {
                        logger.debug("Issuing retry for read command");
                        if (commandsToRetry == Collections.EMPTY_LIST)
                            commandsToRetry = new ArrayList<ReadCommand>();
                        commandsToRetry.add(retryCommand);
                        continue;
                    }

                    if (row != null)
                    {
                        command.maybeTrim(row);
                        rows.add(row);
                    }
                }
            }
        } while (!commandsToRetry.isEmpty());

        return rows;
    }

    static class LocalReadRunnable extends DroppableRunnable
    {
        private final ReadCommand command;
        private final ReadCallback<ReadResponse, Row> handler;
        private final long start = System.currentTimeMillis();

        LocalReadRunnable(ReadCommand command, ReadCallback<ReadResponse, Row> handler)
        {
            super(MessagingService.Verb.READ);
            this.command = command;
            this.handler = handler;
        }

        protected void runMayThrow()
        {
            logger.trace("LocalReadRunnable reading {}", command);

            Table table = Table.open(command.table);
            Row r = command.getRow(table);
            ReadResponse result = ReadVerbHandler.getResponse(command, r);
            MessagingService.instance().addLatency(FBUtilities.getBroadcastAddress(), System.currentTimeMillis() - start);
            handler.response(result);
        }
    }

    static class LocalRangeSliceRunnable extends DroppableRunnable
    {
        private final RangeSliceCommand command;
        private final ReadCallback<RangeSliceReply, Iterable<Row>> handler;
        private final long start = System.currentTimeMillis();

        LocalRangeSliceRunnable(RangeSliceCommand command, ReadCallback<RangeSliceReply, Iterable<Row>> handler)
        {
            super(MessagingService.Verb.READ);
            this.command = command;
            this.handler = handler;
        }

        protected void runMayThrow() throws ExecutionException, InterruptedException
        {
            logger.trace("LocalReadRunnable reading {}", command);

            RangeSliceReply result = new RangeSliceReply(RangeSliceVerbHandler.executeLocally(command));
            MessagingService.instance().addLatency(FBUtilities.getBroadcastAddress(), System.currentTimeMillis() - start);
            handler.response(result);
        }
    }

    private static List<InetAddress> getLiveSortedEndpoints(Table table, ByteBuffer key)
    {
        return getLiveSortedEndpoints(table, StorageService.instance.getPartitioner().decorateKey(key));
    }

    private static List<InetAddress> getLiveSortedEndpoints(Table table, RingPosition pos)
    {
        List<InetAddress> liveEndpoints = StorageService.instance.getLiveNaturalEndpoints(table, pos);
        DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getBroadcastAddress(), liveEndpoints);
        return liveEndpoints;
    }

    private static List<InetAddress> intersection(List<InetAddress> l1, List<InetAddress> l2)
    {
        // Note: we don't use Guava Sets.intersection() for 3 reasons:
        //   1) retainAll would be inefficient if l1 and l2 are large but in practice both are the replicas for a range and
        //   so will be very small (< RF). In that case, retainAll is in fact more efficient.
        //   2) we do ultimately need a list so converting everything to sets don't make sense
        //   3) l1 and l2 are sorted by proximity. The use of retainAll  maintain that sorting in the result, while using sets wouldn't.
        List<InetAddress> inter = new ArrayList<InetAddress>(l1);
        inter.retainAll(l2);
        return inter;
    }

    public static List<Row> getRangeSlice(RangeSliceCommand command, ConsistencyLevel consistency_level)
    throws UnavailableException, ReadTimeoutException
    {
        Tracing.trace("Determining replicas to query");
        logger.trace("Command/ConsistencyLevel is {}/{}", command.toString(), consistency_level);
        long startTime = System.nanoTime();

        Table table = Table.open(command.keyspace);
        List<Row> rows;
        // now scan until we have enough results
        try
        {
            IDiskAtomFilter commandPredicate = command.predicate;

            int cql3RowCount = 0;
            rows = new ArrayList<Row>();
            List<AbstractBounds<RowPosition>> ranges = getRestrictedRanges(command.range);
            int i = 0;
            AbstractBounds<RowPosition> nextRange = null;
            List<InetAddress> nextEndpoints = null;
            List<InetAddress> nextFilteredEndpoints = null;
            while (i < ranges.size())
            {
                AbstractBounds<RowPosition> range = nextRange == null
                                                  ? ranges.get(i)
                                                  : nextRange;
                List<InetAddress> liveEndpoints = nextEndpoints == null
                                                ? getLiveSortedEndpoints(table, range.right)
                                                : nextEndpoints;
                List<InetAddress> filteredEndpoints = nextFilteredEndpoints == null
                                                    ? consistency_level.filterForQuery(table, liveEndpoints)
                                                    : nextFilteredEndpoints;
                ++i;

                // getRestrictedRange has broken the queried range into per-[vnode] token ranges, but this doesn't take
                // the replication factor into account. If the intersection of live endpoints for 2 consecutive ranges
                // still meets the CL requirements, then we can merge both ranges into the same RangeSliceCommand.
                while (i < ranges.size())
                {
                    nextRange = ranges.get(i);
                    nextEndpoints = getLiveSortedEndpoints(table, nextRange.right);
                    nextFilteredEndpoints = consistency_level.filterForQuery(table, nextEndpoints);

                    /*
                     * If the current range right is the min token, we should stop merging because CFS.getRangeSlice
                     * don't know how to deal with a wrapping range.
                     * Note: it would be slightly more efficient to have CFS.getRangeSlice on the destination nodes unwraps
                     * the range if necessary and deal with it. However, we can't start sending wrapped range without breaking
                     * wire compatibility, so It's likely easier not to bother;
                     */
                    if (range.right.isMinimum())
                        break;

                    List<InetAddress> merged = intersection(liveEndpoints, nextEndpoints);

                    // Check if there is enough endpoint for the merge to be possible.
                    if (!consistency_level.isSufficientLiveNodes(table, merged))
                        break;

                    List<InetAddress> filteredMerged = consistency_level.filterForQuery(table, merged);

                    // Estimate whether merging will be a win or not
                    if (!DatabaseDescriptor.getEndpointSnitch().isWorthMergingForRangeQuery(filteredMerged, filteredEndpoints, nextFilteredEndpoints))
                        break;

                    // If we get there, merge this range and the next one
                    range = range.withNewRight(nextRange.right);
                    liveEndpoints = merged;
                    filteredEndpoints = filteredMerged;
                    ++i;
                }

                RangeSliceCommand nodeCmd = new RangeSliceCommand(command.keyspace,
                                                                  command.column_family,
                                                                  command.super_column,
                                                                  commandPredicate,
                                                                  range,
                                                                  command.row_filter,
                                                                  command.maxResults,
                                                                  command.countCQL3Rows,
                                                                  command.isPaging);

                // collect replies and resolve according to consistency level
                RangeSliceResponseResolver resolver = new RangeSliceResponseResolver(nodeCmd.keyspace);
                ReadCallback<RangeSliceReply, Iterable<Row>> handler = new ReadCallback(resolver, consistency_level, nodeCmd, filteredEndpoints);
                handler.assureSufficientLiveNodes();
                resolver.setSources(filteredEndpoints);
                if (filteredEndpoints.size() == 1
                    && filteredEndpoints.get(0).equals(FBUtilities.getBroadcastAddress())
                    && OPTIMIZE_LOCAL_REQUESTS)
                {
                    logger.trace("reading data locally");
                    StageManager.getStage(Stage.READ).execute(new LocalRangeSliceRunnable(nodeCmd, handler));
                }
                else
                {
                    MessageOut<RangeSliceCommand> message = nodeCmd.createMessage();
                    for (InetAddress endpoint : filteredEndpoints)
                    {
                        MessagingService.instance().sendRR(message, endpoint, handler);
                        logger.trace("reading {} from {}", nodeCmd, endpoint);
                    }
                }

                try
                {
                    for (Row row : handler.get())
                    {
                        rows.add(row);
                        if (nodeCmd.countCQL3Rows)
                            cql3RowCount += row.getLiveCount(commandPredicate);
                        logger.trace("range slices read {}", row.key);
                    }
                    FBUtilities.waitOnFutures(resolver.repairResults, DatabaseDescriptor.getWriteRpcTimeout());
                }
                catch (TimeoutException ex)
                {
                    logger.debug("Range slice timeout: {}", ex.toString());
                    // We actually got all response at that point
                    int blockFor = consistency_level.blockFor(table);
                    throw new ReadTimeoutException(consistency_level, blockFor, blockFor, true);
                }
                catch (DigestMismatchException e)
                {
                    throw new AssertionError(e); // no digests in range slices yet
                }

                // if we're done, great, otherwise, move to the next range
                int count = nodeCmd.countCQL3Rows ? cql3RowCount : rows.size();
                if (count >= nodeCmd.maxResults)
                    break;

                // if we are paging and already got some rows, reset the column filter predicate,
                // so we start iterating the next row from the first column
                if (!rows.isEmpty() && command.isPaging)
                {
                    // We only allow paging with a slice filter (doesn't make sense otherwise anyway)
                    assert commandPredicate instanceof SliceQueryFilter;
                    commandPredicate = ((SliceQueryFilter)commandPredicate).withUpdatedSlices(ColumnSlice.ALL_COLUMNS_ARRAY);
                }
            }
        }
        finally
        {
            rangeMetrics.addNano(System.nanoTime() - startTime);
        }
        return trim(command, rows);
    }

    private static List<Row> trim(RangeSliceCommand command, List<Row> rows)
    {
        // When countCQL3Rows, we let the caller trim the result.
        if (command.countCQL3Rows)
            return rows;
        else
            return rows.size() > command.maxResults ? rows.subList(0, command.maxResults) : rows;
    }

    /**
     * initiate a request/response session with each live node to check whether or not everybody is using the same
     * migration id. This is useful for determining if a schema change has propagated through the cluster. Disagreement
     * is assumed if any node fails to respond.
     */
    public static Map<String, List<String>> describeSchemaVersions()
    {
        final String myVersion = Schema.instance.getVersion().toString();
        final Map<InetAddress, UUID> versions = new ConcurrentHashMap<InetAddress, UUID>();
        final Set<InetAddress> liveHosts = Gossiper.instance.getLiveMembers();
        final CountDownLatch latch = new CountDownLatch(liveHosts.size());

        IAsyncCallback<UUID> cb = new IAsyncCallback<UUID>()
        {
            public void response(MessageIn<UUID> message)
            {
                // record the response from the remote node.
                logger.trace("Received schema check response from {}", message.from.getHostAddress());
                versions.put(message.from, message.payload);
                latch.countDown();
            }

            public boolean isLatencyForSnitch()
            {
                return false;
            }
        };
        // an empty message acts as a request to the SchemaCheckVerbHandler.
        MessageOut message = new MessageOut(MessagingService.Verb.SCHEMA_CHECK);
        for (InetAddress endpoint : liveHosts)
            MessagingService.instance().sendRR(message, endpoint, cb);

        try
        {
            // wait for as long as possible. timeout-1s if possible.
            latch.await(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError("This latch shouldn't have been interrupted.");
        }

        logger.trace("My version is {}", myVersion);

        // maps versions to hosts that are on that version.
        Map<String, List<String>> results = new HashMap<String, List<String>>();
        Iterable<InetAddress> allHosts = Iterables.concat(Gossiper.instance.getLiveMembers(), Gossiper.instance.getUnreachableMembers());
        for (InetAddress host : allHosts)
        {
            UUID version = versions.get(host);
            String stringVersion = version == null ? UNREACHABLE : version.toString();
            List<String> hosts = results.get(stringVersion);
            if (hosts == null)
            {
                hosts = new ArrayList<String>();
                results.put(stringVersion, hosts);
            }
            hosts.add(host.getHostAddress());
        }

        // we're done: the results map is ready to return to the client.  the rest is just debug logging:
        if (results.get(UNREACHABLE) != null)
            logger.debug("Hosts not in agreement. Didn't get a response from everybody: {}", StringUtils.join(results.get(UNREACHABLE), ","));
        for (Map.Entry<String, List<String>> entry : results.entrySet())
        {
            // check for version disagreement. log the hosts that don't agree.
            if (entry.getKey().equals(UNREACHABLE) || entry.getKey().equals(myVersion))
                continue;
            for (String host : entry.getValue())
                logger.debug("{} disagrees ({})", host, entry.getKey());
        }
        if (results.size() == 1)
            logger.debug("Schemas are in agreement.");

        return results;
    }

    /**
     * Compute all ranges we're going to query, in sorted order. Nodes can be replica destinations for many ranges,
     * so we need to restrict each scan to the specific range we want, or else we'd get duplicate results.
     */
    static <T extends RingPosition> List<AbstractBounds<T>> getRestrictedRanges(final AbstractBounds<T> queryRange)
    {
        // special case for bounds containing exactly 1 (non-minimum) token
        if (queryRange instanceof Bounds && queryRange.left.equals(queryRange.right) && !queryRange.left.isMinimum(StorageService.getPartitioner()))
        {
            logger.trace("restricted single token match for query {}", queryRange);
            return Collections.singletonList(queryRange);
        }

        TokenMetadata tokenMetadata = StorageService.instance.getTokenMetadata();

        List<AbstractBounds<T>> ranges = new ArrayList<AbstractBounds<T>>();
        // divide the queryRange into pieces delimited by the ring and minimum tokens
        Iterator<Token> ringIter = TokenMetadata.ringIterator(tokenMetadata.sortedTokens(), queryRange.left.getToken(), true);
        AbstractBounds<T> remainder = queryRange;
        while (ringIter.hasNext())
        {
            /*
             * remainder can be a range/bounds of token _or_ keys and we want to split it with a token:
             *   - if remainder is tokens, then we'll just split using the provided token.
             *   - if remainder is keys, we want to split using token.upperBoundKey. For instance, if remainder
             *     is [DK(10, 'foo'), DK(20, 'bar')], and we have 3 nodes with tokens 0, 15, 30. We want to
             *     split remainder to A=[DK(10, 'foo'), 15] and B=(15, DK(20, 'bar')]. But since we can't mix
             *     tokens and keys at the same time in a range, we uses 15.upperBoundKey() to have A include all
             *     keys having 15 as token and B include none of those (since that is what our node owns).
             * asSplitValue() abstracts that choice.
             */
            Token upperBoundToken = ringIter.next();
            T upperBound = (T)upperBoundToken.upperBound(queryRange.left.getClass());
            if (!remainder.left.equals(upperBound) && !remainder.contains(upperBound))
                // no more splits
                break;
            Pair<AbstractBounds<T>,AbstractBounds<T>> splits = remainder.split(upperBound);
            if (splits == null)
                continue;

            ranges.add(splits.left);
            remainder = splits.right;
        }
        ranges.add(remainder);
        if (logger.isDebugEnabled())
            logger.trace("restricted ranges for query {} are {}", queryRange, ranges);

        return ranges;
    }

    public long getReadOperations()
    {
        return readMetrics.latency.count();
    }

    public long getTotalReadLatencyMicros()
    {
        return readMetrics.totalLatency.count();
    }

    public double getRecentReadLatencyMicros()
    {
        return readMetrics.getRecentLatency();
    }

    public long[] getTotalReadLatencyHistogramMicros()
    {
        return readMetrics.totalLatencyHistogram.getBuckets(false);
    }

    public long[] getRecentReadLatencyHistogramMicros()
    {
        return readMetrics.recentLatencyHistogram.getBuckets(true);
    }

    public long getRangeOperations()
    {
        return rangeMetrics.latency.count();
    }

    public long getTotalRangeLatencyMicros()
    {
        return rangeMetrics.totalLatency.count();
    }

    public double getRecentRangeLatencyMicros()
    {
        return rangeMetrics.getRecentLatency();
    }

    public long[] getTotalRangeLatencyHistogramMicros()
    {
        return rangeMetrics.totalLatencyHistogram.getBuckets(false);
    }

    public long[] getRecentRangeLatencyHistogramMicros()
    {
        return rangeMetrics.recentLatencyHistogram.getBuckets(true);
    }

    public long getWriteOperations()
    {
        return writeMetrics.latency.count();
    }

    public long getTotalWriteLatencyMicros()
    {
        return writeMetrics.totalLatency.count();
    }

    public double getRecentWriteLatencyMicros()
    {
        return writeMetrics.getRecentLatency();
    }

    public long[] getTotalWriteLatencyHistogramMicros()
    {
        return writeMetrics.totalLatencyHistogram.getBuckets(false);
    }

    public long[] getRecentWriteLatencyHistogramMicros()
    {
        return writeMetrics.recentLatencyHistogram.getBuckets(true);
    }

    public boolean getHintedHandoffEnabled()
    {
        return DatabaseDescriptor.hintedHandoffEnabled();
    }

    public void setHintedHandoffEnabled(boolean b)
    {
        DatabaseDescriptor.setHintedHandoffEnabled(b);
    }

    public int getMaxHintWindow()
    {
        return DatabaseDescriptor.getMaxHintWindow();
    }

    public void setMaxHintWindow(int ms)
    {
        DatabaseDescriptor.setMaxHintWindow(ms);
    }

    public static boolean shouldHint(InetAddress ep)
    {
        if (!DatabaseDescriptor.hintedHandoffEnabled())
        {
            HintedHandOffManager.instance.metrics.incrPastWindow(ep);
            return false;
        }

        boolean hintWindowExpired = Gossiper.instance.getEndpointDowntime(ep) > DatabaseDescriptor.getMaxHintWindow();
        if (hintWindowExpired)
        {
            HintedHandOffManager.instance.metrics.incrPastWindow(ep);
            logger.trace("not hinting {} which has been down {}ms", ep, Gossiper.instance.getEndpointDowntime(ep));
        }
        return !hintWindowExpired;
    }

    /**
     * Performs the truncate operatoin, which effectively deletes all data from
     * the column family cfname
     * @param keyspace
     * @param cfname
     * @throws UnavailableException If some of the hosts in the ring are down.
     * @throws TimeoutException
     * @throws IOException
     */
    public static void truncateBlocking(String keyspace, String cfname) throws UnavailableException, TimeoutException, IOException
    {
        logger.debug("Starting a blocking truncate operation on keyspace {}, CF ", keyspace, cfname);
        if (isAnyHostDown())
        {
            logger.info("Cannot perform truncate, some hosts are down");
            // Since the truncate operation is so aggressive and is typically only
            // invoked by an admin, for simplicity we require that all nodes are up
            // to perform the operation.
            int liveMembers = Gossiper.instance.getLiveMembers().size();
            throw new UnavailableException(ConsistencyLevel.ALL, liveMembers + Gossiper.instance.getUnreachableMembers().size(), liveMembers);
        }

        Set<InetAddress> allEndpoints = Gossiper.instance.getLiveMembers();
        int blockFor = allEndpoints.size();
        final TruncateResponseHandler responseHandler = new TruncateResponseHandler(blockFor);

        // Send out the truncate calls and track the responses with the callbacks.
        logger.trace("Starting to send truncate messages to hosts {}", allEndpoints);
        final Truncation truncation = new Truncation(keyspace, cfname);
        MessageOut<Truncation> message = truncation.createMessage();
        for (InetAddress endpoint : allEndpoints)
            MessagingService.instance().sendRR(message, endpoint, responseHandler);

        // Wait for all
        logger.trace("Sent all truncate messages, now waiting for {} responses", blockFor);
        responseHandler.get();
    }

    /**
     * Asks the gossiper if there are any nodes that are currently down.
     * @return true if the gossiper thinks all nodes are up.
     */
    private static boolean isAnyHostDown()
    {
        return !Gossiper.instance.getUnreachableMembers().isEmpty();
    }

    public interface WritePerformer
    {
        public void apply(IMutation mutation, Iterable<InetAddress> targets, AbstractWriteResponseHandler responseHandler, String localDataCenter, ConsistencyLevel consistency_level) throws IOException, OverloadedException;
    }

    /**
     * A Runnable that aborts if it doesn't start running before it times out
     */
    private static abstract class DroppableRunnable implements Runnable
    {
        private final long constructionTime = System.currentTimeMillis();
        private final MessagingService.Verb verb;

        public DroppableRunnable(MessagingService.Verb verb)
        {
            this.verb = verb;
        }

        public final void run()
        {
            if (System.currentTimeMillis() > constructionTime + DatabaseDescriptor.getTimeout(verb))
            {
                MessagingService.instance().incrementDroppedMessages(verb);
                return;
            }

            try
            {
                runMayThrow();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        abstract protected void runMayThrow() throws Exception;
    }

    /**
     * Like DroppableRunnable, but if it aborts, it will rerun (on the mutation stage) after
     * marking itself as a hint in progress so that the hint backpressure mechanism can function.
     */
    private static abstract class LocalMutationRunnable implements Runnable
    {
        private final long constructionTime = System.currentTimeMillis();

        public final void run()
        {
            if (System.currentTimeMillis() > constructionTime + DatabaseDescriptor.getTimeout(MessagingService.Verb.MUTATION))
            {
                MessagingService.instance().incrementDroppedMessages(MessagingService.Verb.MUTATION);
                HintRunnable runnable = new HintRunnable(FBUtilities.getBroadcastAddress())
                {
                    protected void runMayThrow() throws Exception
                    {
                        LocalMutationRunnable.this.runMayThrow();
                    }
                };
                submitHint(runnable);
                return;
            }

            try
            {
                runMayThrow();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        abstract protected void runMayThrow() throws Exception;
    }

    /**
     * HintRunnable will decrease totalHintsInProgress and targetHints when finished.
     * It is the caller's responsibility to increment them initially.
     */
    private abstract static class HintRunnable implements Runnable
    {
        public final InetAddress target;

        protected HintRunnable(InetAddress target)
        {
            this.target = target;
        }

        public void run()
        {
            try
            {
                runMayThrow();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            finally
            {
                totalHintsInProgress.decrementAndGet();
                hintsInProgress.get(target).decrementAndGet();
            }
        }

        abstract protected void runMayThrow() throws Exception;
    }

    public long getTotalHints()
    {
        return totalHints.get();
    }

    public int getMaxHintsInProgress()
    {
        return maxHintsInProgress;
    }

    public void setMaxHintsInProgress(int qs)
    {
        maxHintsInProgress = qs;
    }

    public int getHintsInProgress()
    {
        return totalHintsInProgress.get();
    }

    public void verifyNoHintsInProgress()
    {
        if (getHintsInProgress() > 0)
            logger.warn("Some hints were not written before shutdown.  This is not supposed to happen.  You should (a) run repair, and (b) file a bug report");
    }

    public Long getRpcTimeout() { return DatabaseDescriptor.getRpcTimeout(); }
    public void setRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setRpcTimeout(timeoutInMillis); }

    public Long getReadRpcTimeout() { return DatabaseDescriptor.getReadRpcTimeout(); }
    public void setReadRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setReadRpcTimeout(timeoutInMillis); }

    public Long getWriteRpcTimeout() { return DatabaseDescriptor.getWriteRpcTimeout(); }
    public void setWriteRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setWriteRpcTimeout(timeoutInMillis); }

    public Long getRangeRpcTimeout() { return DatabaseDescriptor.getRangeRpcTimeout(); }
    public void setRangeRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setRangeRpcTimeout(timeoutInMillis); }

    public Long getTruncateRpcTimeout() { return DatabaseDescriptor.getTruncateRpcTimeout(); }
    public void setTruncateRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setTruncateRpcTimeout(timeoutInMillis); }
}
