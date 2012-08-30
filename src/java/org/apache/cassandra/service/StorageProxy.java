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
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Multimap;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.FastByteArrayOutputStream;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.metrics.ClientRequestMetrics;
import org.apache.cassandra.net.*;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.*;

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

    private static volatile int maxHintsInProgress = 1024 * Runtime.getRuntime().availableProcessors();
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
                              Collection<InetAddress> targets,
                              IWriteResponseHandler responseHandler,
                              String localDataCenter,
                              ConsistencyLevel consistency_level)
            throws IOException, UnavailableException
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
                              Collection<InetAddress> targets,
                              IWriteResponseHandler responseHandler,
                              String localDataCenter,
                              ConsistencyLevel consistency_level)
            throws IOException
            {
                if (logger.isDebugEnabled())
                    logger.debug("insert writing local & replicate " + mutation.toString(true));

                Runnable runnable = counterWriteTask(mutation, targets, responseHandler, localDataCenter, consistency_level);
                runnable.run();
            }
        };

        counterWriteOnCoordinatorPerformer = new WritePerformer()
        {
            public void apply(IMutation mutation,
                              Collection<InetAddress> targets,
                              IWriteResponseHandler responseHandler,
                              String localDataCenter,
                              ConsistencyLevel consistency_level)
            throws IOException
            {
                if (logger.isDebugEnabled())
                    logger.debug("insert writing local & replicate " + mutation.toString(true));

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
    public static void mutate(List<? extends IMutation> mutations, ConsistencyLevel consistency_level) throws UnavailableException, TimedOutException
    {
        logger.debug("Mutations/ConsistencyLevel are {}/{}", mutations, consistency_level);
        final String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());

        long startTime = System.nanoTime();
        List<IWriteResponseHandler> responseHandlers = new ArrayList<IWriteResponseHandler>(mutations.size());

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
                    responseHandlers.add(performWrite(mutation, consistency_level, localDataCenter, standardWritePerformer));
                }
            }

            // wait for writes.  throws TimeoutException if necessary
            for (IWriteResponseHandler responseHandler : responseHandlers)
            {
                responseHandler.get();
            }

        }
        catch (TimedOutException ex)
        {
            writeMetrics.timeouts.mark();
            ClientRequestMetrics.writeTimeouts.inc();
            if (logger.isDebugEnabled())
            {
                List<String> mstrings = new ArrayList<String>(mutations.size());
                for (IMutation mutation : mutations)
                    mstrings.add(mutation.toString(true));
                logger.debug("Write timeout {} for one (or more) of: ", ex.toString(), mstrings);
            }
            throw ex;
        }
        catch (UnavailableException e)
        {
            writeMetrics.unavailables.mark();
            ClientRequestMetrics.writeUnavailables.inc();
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
     */
    public static IWriteResponseHandler performWrite(IMutation mutation,
                                                     ConsistencyLevel consistency_level,
                                                     String localDataCenter,
                                                     WritePerformer performer)
    throws UnavailableException, IOException
    {
        String table = mutation.getTable();
        AbstractReplicationStrategy rs = Table.open(table).getReplicationStrategy();

        Collection<InetAddress> writeEndpoints = getWriteEndpoints(table, mutation.key());

        IWriteResponseHandler responseHandler = rs.getWriteResponseHandler(writeEndpoints, consistency_level);

        // exit early if we can't fulfill the CL at this time
        responseHandler.assureSufficientLiveNodes();

        performer.apply(mutation, writeEndpoints, responseHandler, localDataCenter, consistency_level);
        return responseHandler;
    }

    private static Collection<InetAddress> getWriteEndpoints(String table, ByteBuffer key)
    {
        StorageService ss = StorageService.instance;
        Token tk = StorageService.getPartitioner().getToken(key);
        List<InetAddress> naturalEndpoints = ss.getNaturalEndpoints(table, tk);
        return ss.getTokenMetadata().getWriteEndpoints(tk, table, naturalEndpoints);
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
                                             Collection<InetAddress> targets,
                                             IWriteResponseHandler responseHandler,
                                             String localDataCenter,
                                             ConsistencyLevel consistency_level)
    throws IOException, UnavailableException
    {
        // Multimap that holds onto all the messages and addresses meant for a specific datacenter
        Map<String, Multimap<MessageOut, InetAddress>> dcMessages = new HashMap<String, Multimap<MessageOut, InetAddress>>(targets.size());

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
                throw new UnavailableException();
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
                    if (logger.isDebugEnabled())
                        logger.debug("insert writing key " + ByteBufferUtil.bytesToHex(rm.key()) + " to " + destination);

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
                scheduleLocalHint(rm, destination, responseHandler, consistency_level);
            }
        }

        sendMessages(localDataCenter, dcMessages, responseHandler);
    }

    public static Future<Void> scheduleLocalHint(final RowMutation mutation,
                                                 final InetAddress target,
                                                 final IWriteResponseHandler responseHandler,
                                                 final ConsistencyLevel consistencyLevel)
    {
        // Hint of itself doesn't make sense.
        assert !target.equals(FBUtilities.getBroadcastAddress()) : target;
        totalHintsInProgress.incrementAndGet();
        final AtomicInteger targetHints = hintsInProgress.get(target);
        targetHints.incrementAndGet();

        Runnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws IOException
            {
                if (logger.isDebugEnabled())
                    logger.debug("Adding hint for " + target);

                try
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

                    // Notify the handler only for CL == ANY
                    if (responseHandler != null && consistencyLevel == ConsistencyLevel.ANY)
                        responseHandler.response(null);
                }
                finally
                {
                    totalHintsInProgress.decrementAndGet();
                    targetHints.decrementAndGet();
                }
            }
        };

        return (Future<Void>) StageManager.getStage(Stage.MUTATION).submit(runnable);
    }

    /**
     * for each datacenter, send a message to one node to relay the write to other replicas
     */
    private static void sendMessages(String localDataCenter, Map<String, Multimap<MessageOut, InetAddress>> dcMessages, IWriteResponseHandler handler)
    throws IOException
    {
        for (Map.Entry<String, Multimap<MessageOut, InetAddress>> entry: dcMessages.entrySet())
        {
            String dataCenter = entry.getKey();

            // send the messages corresponding to this datacenter
            for (Map.Entry<MessageOut, Collection<InetAddress>> messages: entry.getValue().asMap().entrySet())
            {
                MessageOut message = messages.getKey();
                // a single message object is used for unhinted writes, so clean out any forwards
                // from previous loop iterations
                message = message.withHeaderRemoved(RowMutation.FORWARD_TO);
                Iterator<InetAddress> iter = messages.getValue().iterator();
                InetAddress target = iter.next();

                // direct writes to local DC or old Cassadra versions
                if (dataCenter.equals(localDataCenter) || MessagingService.instance().getVersion(target) < MessagingService.VERSION_11)
                {
                    // yes, the loop and non-loop code here are the same; this is clunky but we want to avoid
                    // creating a second iterator since we already have a perfectly good one
                    MessagingService.instance().sendRR(message, target, handler);
                    while (iter.hasNext())
                    {
                        target = iter.next();
                        MessagingService.instance().sendRR(message, target, handler);
                    }
                    continue;
                }

                // Add all the other destinations of the same message as a FORWARD_HEADER entry
                FastByteArrayOutputStream bos = new FastByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(bos);
                dos.writeInt(messages.getValue().size() - 1);
                while (iter.hasNext())
                {
                    InetAddress destination = iter.next();
                    CompactEndpointSerializationHelper.serialize(destination, dos);
                    String id = MessagingService.instance().addCallback(handler, message, destination, message.getTimeout());
                    dos.writeUTF(id);
                    if (logger.isDebugEnabled())
                        logger.debug("Adding FWD message to: " + destination + " with ID " + id);
                }
                message = message.withParameter(RowMutation.FORWARD_TO, bos.toByteArray());
                // send the combined message + forward headers
                String id = MessagingService.instance().sendRR(message, target, handler);
                if (logger.isDebugEnabled())
                    logger.debug("Sending message to: " + target + " with ID " + id);
            }
        }
    }

    private static void insertLocal(final RowMutation rm, final IWriteResponseHandler responseHandler)
    {
        if (logger.isDebugEnabled())
            logger.debug("insert writing local " + rm.toString(true));
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
    public static IWriteResponseHandler mutateCounter(CounterMutation cm, String localDataCenter) throws UnavailableException, IOException
    {
        InetAddress endpoint = findSuitableEndpoint(cm.getTable(), cm.key(), localDataCenter);

        if (endpoint.equals(FBUtilities.getBroadcastAddress()))
        {
            return applyCounterMutationOnCoordinator(cm, localDataCenter);
        }
        else
        {
            // Exit now if we can't fulfill the CL here instead of forwarding to the leader replica
            String table = cm.getTable();
            AbstractReplicationStrategy rs = Table.open(table).getReplicationStrategy();
            Collection<InetAddress> writeEndpoints = getWriteEndpoints(table, cm.key());

            rs.getWriteResponseHandler(writeEndpoints, cm.consistency()).assureSufficientLiveNodes();

            // Forward the actual update to the chosen leader replica
            IWriteResponseHandler responseHandler = WriteResponseHandler.create(endpoint);

            if (logger.isDebugEnabled())
                logger.debug("forwarding counter update of key " + ByteBufferUtil.bytesToHex(cm.key()) + " to " + endpoint);
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
    private static InetAddress findSuitableEndpoint(String table, ByteBuffer key, String localDataCenter) throws UnavailableException
    {
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        List<InetAddress> endpoints = StorageService.instance.getLiveNaturalEndpoints(table, key);
        if (endpoints.isEmpty())
            throw new UnavailableException();

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
    public static IWriteResponseHandler applyCounterMutationOnLeader(CounterMutation cm, String localDataCenter) throws UnavailableException, IOException
    {
        return performWrite(cm, cm.consistency(), localDataCenter, counterWritePerformer);
    }

    // Same as applyCounterMutationOnLeader but must with the difference that it use the MUTATION stage to execute the write (while
    // applyCounterMutationOnLeader assumes it is on the MUTATION stage already)
    public static IWriteResponseHandler applyCounterMutationOnCoordinator(CounterMutation cm, String localDataCenter) throws UnavailableException, IOException
    {
        return performWrite(cm, cm.consistency(), localDataCenter, counterWriteOnCoordinatorPerformer);
    }

    private static Runnable counterWriteTask(final IMutation mutation,
                                             final Collection<InetAddress> targets,
                                             final IWriteResponseHandler responseHandler,
                                             final String localDataCenter,
                                             final ConsistencyLevel consistency_level)
    {
        return new DroppableRunnable(MessagingService.Verb.MUTATION)
        {
            public void runMayThrow() throws IOException
            {
                assert mutation instanceof CounterMutation;
                final CounterMutation cm = (CounterMutation) mutation;

                // apply mutation
                cm.apply();
                responseHandler.response(null);

                // then send to replicas, if any
                targets.remove(FBUtilities.getBroadcastAddress());
                if (cm.shouldReplicateOnWrite() && !targets.isEmpty())
                {
                    // We do the replication on another stage because it involves a read (see CM.makeReplicationMutation)
                    // and we want to avoid blocking too much the MUTATION stage
                    StageManager.getStage(Stage.REPLICATE_ON_WRITE).execute(new DroppableRunnable(MessagingService.Verb.READ)
                    {
                        public void runMayThrow() throws IOException, TimeoutException, UnavailableException
                        {
                            // send mutation to other replica
                            sendToHintedEndpoints(cm.makeReplicationMutation(), targets, responseHandler, localDataCenter, consistency_level);
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
            throws IOException, UnavailableException, TimeoutException, InvalidRequestException
    {
        if (StorageService.instance.isBootstrapMode() && !systemTableQuery(commands))
        {
            readMetrics.unavailables.mark();
            ClientRequestMetrics.readUnavailables.inc();
            throw new UnavailableException();
        }
        long startTime = System.nanoTime();
        List<Row> rows;
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
        catch (TimeoutException e)
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
    private static List<Row> fetchRows(List<ReadCommand> initialCommands, ConsistencyLevel consistency_level) throws IOException, UnavailableException, TimeoutException
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
                assert !command.isDigestQuery();
                logger.debug("Command/ConsistencyLevel is {}/{}", command, consistency_level);

                List<InetAddress> endpoints = StorageService.instance.getLiveNaturalEndpoints(command.table,
                                                                                              command.key);
                DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getBroadcastAddress(), endpoints);

                RowDigestResolver resolver = new RowDigestResolver(command.table, command.key);
                ReadCallback<ReadResponse, Row> handler = getReadCallback(resolver, command, consistency_level, endpoints);
                handler.assureSufficientLiveNodes();
                assert !handler.endpoints.isEmpty();
                readCallbacks[i] = handler;

                // The data-request message is sent to dataPoint, the node that will actually get the data for us
                InetAddress dataPoint = handler.endpoints.get(0);
                if (dataPoint.equals(FBUtilities.getBroadcastAddress()) && OPTIMIZE_LOCAL_REQUESTS)
                {
                    logger.debug("reading data locally");
                    StageManager.getStage(Stage.READ).execute(new LocalReadRunnable(command, handler));
                }
                else
                {
                    logger.debug("reading data from {}", dataPoint);
                    MessagingService.instance().sendRR(command.createMessage(), dataPoint, handler);
                }

                if (handler.endpoints.size() == 1)
                    continue;

                // send the other endpoints a digest request
                ReadCommand digestCommand = command.copy();
                digestCommand.setDigestQuery(true);
                MessageOut message = null;
                for (InetAddress digestPoint : handler.endpoints.subList(1, handler.endpoints.size()))
                {
                    if (digestPoint.equals(FBUtilities.getBroadcastAddress()))
                    {
                        logger.debug("reading digest locally");
                        StageManager.getStage(Stage.READ).execute(new LocalReadRunnable(digestCommand, handler));
                    }
                    else
                    {
                        logger.debug("reading digest from {}", digestPoint);
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
            List<RepairCallback> repairResponseHandlers = null;
            for (int i = 0; i < commands.size(); i++)
            {
                ReadCallback<ReadResponse, Row> handler = readCallbacks[i];
                ReadCommand command = commands.get(i);
                try
                {
                    long startTime2 = System.currentTimeMillis();
                    Row row = handler.get();
                    if (row != null)
                    {
                        command.maybeTrim(row);
                        rows.add(row);
                    }

                    if (logger.isDebugEnabled())
                        logger.debug("Read: " + (System.currentTimeMillis() - startTime2) + " ms.");
                }
                catch (TimeoutException ex)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Read timeout: {}", ex.toString());
                    throw ex;
                }
                catch (DigestMismatchException ex)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Digest mismatch: {}", ex.toString());
                    RowRepairResolver resolver = new RowRepairResolver(command.table, command.key);
                    RepairCallback repairHandler = new RepairCallback(resolver, handler.endpoints);

                    if (repairCommands == null)
                    {
                        repairCommands = new ArrayList<ReadCommand>();
                        repairResponseHandlers = new ArrayList<RepairCallback>();
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
                    RepairCallback handler = repairResponseHandlers.get(i);
                    // wait for the repair writes to be acknowledged, to minimize impact on any replica that's
                    // behind on writes in case the out-of-sync row is read multiple times in quick succession
                    FBUtilities.waitOnFutures(handler.resolver.repairResults, DatabaseDescriptor.getWriteRpcTimeout());

                    Row row;
                    try
                    {
                        row = handler.get();
                    }
                    catch (DigestMismatchException e)
                    {
                        throw new AssertionError(e); // full data requested from each node here, no digests should be sent
                    }

                    ReadCommand retryCommand = command.maybeGenerateRetryCommand(handler, row);
                    if (retryCommand != null)
                    {
                        logger.debug("issuing retry for read command");
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

        protected void runMayThrow() throws IOException
        {
            if (logger.isDebugEnabled())
                logger.debug("LocalReadRunnable reading " + command);

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
            if (logger.isDebugEnabled())
                logger.debug("LocalReadRunnable reading " + command);

            RangeSliceReply result = new RangeSliceReply(RangeSliceVerbHandler.executeLocally(command));
            MessagingService.instance().addLatency(FBUtilities.getBroadcastAddress(), System.currentTimeMillis() - start);
            handler.response(result);
        }
    }

    static <TMessage, TResolved> ReadCallback<TMessage, TResolved> getReadCallback(IResponseResolver<TMessage, TResolved> resolver, IReadCommand command, ConsistencyLevel consistencyLevel, List<InetAddress> endpoints)
    {
        if (consistencyLevel == ConsistencyLevel.LOCAL_QUORUM || consistencyLevel == ConsistencyLevel.EACH_QUORUM)
        {
            return new DatacenterReadCallback(resolver, consistencyLevel, command, endpoints);
        }
        return new ReadCallback(resolver, consistencyLevel, command, endpoints);
    }

    public static List<Row> getRangeSlice(RangeSliceCommand command, ConsistencyLevel consistency_level)
    throws IOException, UnavailableException, TimeoutException
    {
        if (logger.isDebugEnabled())
            logger.debug("Command/ConsistencyLevel is {}/{}", command.toString(), consistency_level);
        long startTime = System.nanoTime();
        List<Row> rows;
        // now scan until we have enough results
        try
        {
            int columnsCount = 0;
            rows = new ArrayList<Row>();
            List<AbstractBounds<RowPosition>> ranges = getRestrictedRanges(command.range);
            for (AbstractBounds<RowPosition> range : ranges)
            {
                RangeSliceCommand nodeCmd = new RangeSliceCommand(command.keyspace,
                                                                  command.column_family,
                                                                  command.super_column,
                                                                  command.predicate,
                                                                  range,
                                                                  command.row_filter,
                                                                  command.maxResults,
                                                                  command.maxIsColumns,
                                                                  command.isPaging);

                List<InetAddress> liveEndpoints = StorageService.instance.getLiveNaturalEndpoints(nodeCmd.keyspace, range.right);
                DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getBroadcastAddress(), liveEndpoints);

                // collect replies and resolve according to consistency level
                RangeSliceResponseResolver resolver = new RangeSliceResponseResolver(nodeCmd.keyspace);
                ReadCallback<RangeSliceReply, Iterable<Row>> handler = getReadCallback(resolver, nodeCmd, consistency_level, liveEndpoints);
                handler.assureSufficientLiveNodes();
                resolver.setSources(handler.endpoints);
                if (handler.endpoints.size() == 1 && handler.endpoints.get(0).equals(FBUtilities.getBroadcastAddress()))
                {
                    logger.debug("reading data locally");
                    StageManager.getStage(Stage.READ).execute(new LocalRangeSliceRunnable(nodeCmd, handler));
                }
                else
                {
                    MessageOut<RangeSliceCommand> message = nodeCmd.createMessage();
                    for (InetAddress endpoint : handler.endpoints)
                    {
                        MessagingService.instance().sendRR(message, endpoint, handler);
                        if (logger.isDebugEnabled())
                            logger.debug("reading " + nodeCmd + " from " + endpoint);
                    }
                }

                try
                {
                    for (Row row : handler.get())
                    {
                        rows.add(row);
                        columnsCount += row.getLiveColumnCount();
                        logger.debug("range slices read {}", row.key);
                    }
                    FBUtilities.waitOnFutures(resolver.repairResults, DatabaseDescriptor.getWriteRpcTimeout());
                }
                catch (TimeoutException ex)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Range slice timeout: {}", ex.toString());
                    throw ex;
                }
                catch (DigestMismatchException e)
                {
                    throw new AssertionError(e); // no digests in range slices yet
                }

                // if we're done, great, otherwise, move to the next range
                int count = nodeCmd.maxIsColumns ? columnsCount : rows.size();
                if (count >= nodeCmd.maxResults)
                    break;
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
        // When maxIsColumns, we let the caller trim the result.
        if (command.maxIsColumns)
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
                logger.debug("Received schema check response from {}", message.from.getHostAddress());
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

        logger.debug("My version is {}", myVersion);

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
            if (logger.isDebugEnabled())
                logger.debug("restricted single token match for query {}", queryRange);
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
            logger.debug("restricted ranges for query {} are {}", queryRange, ranges);

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
            return false;

        boolean hintWindowExpired = Gossiper.instance.getEndpointDowntime(ep) > DatabaseDescriptor.getMaxHintWindow();
        if (hintWindowExpired)
            logger.debug("not hinting {} which has been down {}ms", ep, Gossiper.instance.getEndpointDowntime(ep));
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
            throw new UnavailableException();
        }

        Set<InetAddress> allEndpoints = Gossiper.instance.getLiveMembers();
        int blockFor = allEndpoints.size();
        final TruncateResponseHandler responseHandler = new TruncateResponseHandler(blockFor);

        // Send out the truncate calls and track the responses with the callbacks.
        logger.debug("Starting to send truncate messages to hosts {}", allEndpoints);
        final Truncation truncation = new Truncation(keyspace, cfname);
        MessageOut<Truncation> message = truncation.createMessage();
        for (InetAddress endpoint : allEndpoints)
            MessagingService.instance().sendRR(message, endpoint, responseHandler);

        // Wait for all
        logger.debug("Sent all truncate messages, now waiting for {} responses", blockFor);
        responseHandler.get();
        logger.debug("truncate done");
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
        public void apply(IMutation mutation, Collection<InetAddress> targets, IWriteResponseHandler responseHandler, String localDataCenter, ConsistencyLevel consistency_level) throws IOException, UnavailableException;
    }

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
