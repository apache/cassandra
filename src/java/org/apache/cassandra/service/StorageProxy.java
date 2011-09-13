/**
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

import java.io.*;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import org.apache.cassandra.net.CachingMessageProducer;
import org.apache.cassandra.net.MessageProducer;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.UnavailableException;

public class StorageProxy implements StorageProxyMBean
{
    private static final Logger logger = LoggerFactory.getLogger(StorageProxy.class);

    // mbean stuff
    private static final LatencyTracker readStats = new LatencyTracker();
    private static final LatencyTracker rangeStats = new LatencyTracker();
    private static final LatencyTracker writeStats = new LatencyTracker();
    private static boolean hintedHandoffEnabled = DatabaseDescriptor.hintedHandoffEnabled();
    private static int maxHintWindow = DatabaseDescriptor.getMaxHintWindow();
    public static final String UNREACHABLE = "UNREACHABLE";

    private static final WritePerformer standardWritePerformer;
    private static final WritePerformer counterWritePerformer;
    private static final WritePerformer counterWriteOnCoordinatorPerformer;

    public static final StorageProxy instance = new StorageProxy();

    private StorageProxy() {}

    static
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(new StorageProxy(), new ObjectName("org.apache.cassandra.db:type=StorageProxy"));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        standardWritePerformer = new WritePerformer()
        {
            public void apply(IMutation mutation, Multimap<InetAddress, InetAddress> hintedEndpoints, IWriteResponseHandler responseHandler, String localDataCenter, ConsistencyLevel consistency_level) throws IOException
            {
                assert mutation instanceof RowMutation;
                sendToHintedEndpoints((RowMutation) mutation, hintedEndpoints, responseHandler, localDataCenter, consistency_level);
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
            public void apply(IMutation mutation, Multimap<InetAddress, InetAddress> hintedEndpoints, IWriteResponseHandler responseHandler, String localDataCenter, ConsistencyLevel consistency_level) throws IOException
            {
                if (logger.isDebugEnabled())
                    logger.debug("insert writing local & replicate " + mutation.toString(true));

                Runnable runnable = counterWriteTask(mutation, hintedEndpoints, responseHandler, localDataCenter, consistency_level);
                runnable.run();
            }
        };

        counterWriteOnCoordinatorPerformer = new WritePerformer()
        {
            public void apply(IMutation mutation, Multimap<InetAddress, InetAddress> hintedEndpoints, IWriteResponseHandler responseHandler, String localDataCenter, ConsistencyLevel consistency_level) throws IOException
            {
                if (logger.isDebugEnabled())
                    logger.debug("insert writing local & replicate " + mutation.toString(true));

                Runnable runnable = counterWriteTask(mutation, hintedEndpoints, responseHandler, localDataCenter, consistency_level);
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
    public static void mutate(List<? extends IMutation> mutations, ConsistencyLevel consistency_level) throws UnavailableException, TimeoutException
    {
        final String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getLocalAddress());

        long startTime = System.nanoTime();
        List<IWriteResponseHandler> responseHandlers = new ArrayList<IWriteResponseHandler>();

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
            // wait for writes.  throws timeoutexception if necessary
            for (IWriteResponseHandler responseHandler : responseHandlers)
            {
                responseHandler.get();
            }
        }
        catch (TimeoutException ex)
        {
            if (logger.isDebugEnabled())
            {
                List<String> mstrings = new ArrayList<String>();
                for (IMutation mutation : mutations)
                    mstrings.add(mutation.toString(true));
                logger.debug("Write timeout {} for one (or more) of: ", ex.toString(), mstrings);
            }
            throw ex;
        }
        catch (IOException e)
        {
            assert mostRecentMutation != null;
            throw new RuntimeException("error writing key " + ByteBufferUtil.bytesToHex(mostRecentMutation.key()), e);
        }
        finally
        {
            writeStats.addNano(System.nanoTime() - startTime);
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
    public static IWriteResponseHandler performWrite(IMutation mutation, ConsistencyLevel consistency_level, String localDataCenter, WritePerformer performer) throws UnavailableException, TimeoutException, IOException
    {
        String table = mutation.getTable();
        AbstractReplicationStrategy rs = Table.open(table).getReplicationStrategy();

        Collection<InetAddress> writeEndpoints = getWriteEndpoints(table, mutation.key());
        Multimap<InetAddress, InetAddress> hintedEndpoints = rs.getHintedEndpoints(writeEndpoints);

        IWriteResponseHandler responseHandler = rs.getWriteResponseHandler(writeEndpoints, hintedEndpoints, consistency_level);

        // exit early if we can't fulfill the CL at this time
        responseHandler.assureSufficientLiveNodes();

        performer.apply(mutation, hintedEndpoints, responseHandler, localDataCenter, consistency_level);
        return responseHandler;
    }

    private static Collection<InetAddress> getWriteEndpoints(String table, ByteBuffer key)
    {
        StorageService ss = StorageService.instance;
        List<InetAddress> naturalEndpoints = ss.getNaturalEndpoints(table, key);
        return ss.getTokenMetadata().getWriteEndpoints(StorageService.getPartitioner().getToken(key), table, naturalEndpoints);
    }

    private static void sendToHintedEndpoints(final RowMutation rm, Multimap<InetAddress, InetAddress> hintedEndpoints, IWriteResponseHandler responseHandler, String localDataCenter, ConsistencyLevel consistency_level)
    throws IOException
    {
        // Multimap that holds onto all the messages and addresses meant for a specific datacenter
        Map<String, Multimap<Message, InetAddress>> dcMessages = new HashMap<String, Multimap<Message, InetAddress>>(hintedEndpoints.size());
        MessageProducer producer = new CachingMessageProducer(rm);

        for (Map.Entry<InetAddress, Collection<InetAddress>> entry : hintedEndpoints.asMap().entrySet())
        {
            InetAddress destination = entry.getKey();
            Collection<InetAddress> targets = entry.getValue();

            String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(destination);

            if (targets.size() == 1 && targets.iterator().next().equals(destination))
            {
                // unhinted writes
                if (destination.equals(FBUtilities.getLocalAddress()))
                {
                    insertLocal(rm, responseHandler);
                }
                else
                {
                    // belongs on a different server
                    if (logger.isDebugEnabled())
                        logger.debug("insert writing key " + ByteBufferUtil.bytesToHex(rm.key()) + " to " + destination);

                    Multimap<Message, InetAddress> messages = dcMessages.get(dc);
                    if (messages == null)
                    {
                       messages = HashMultimap.create();
                       dcMessages.put(dc, messages);
                    }

                    messages.put(producer.getMessage(Gossiper.instance.getVersion(destination)), destination);
                }
            }
            else
            {
                // hinted messages are unique, so there is no point to adding a hop by forwarding via another node.
                // thus, we use sendRR/sendOneWay directly here.
                Message hintedMessage = rm.getMessage(Gossiper.instance.getVersion(destination));
                for (InetAddress target : targets)
                {
                    if (!target.equals(destination))
                    {
                        addHintHeader(hintedMessage, target);
                        if (logger.isDebugEnabled())
                            logger.debug("insert writing key " + ByteBufferUtil.bytesToHex(rm.key()) + " to " + destination + " for " + target);
                    }
                }
                // non-destination hints are part of the callback and count towards consistency only under CL.ANY
                if (targets.contains(destination) || consistency_level == ConsistencyLevel.ANY)
                    MessagingService.instance().sendRR(hintedMessage, destination, responseHandler);
                else
                    MessagingService.instance().sendOneWay(hintedMessage, destination);
            }
        }
        sendMessages(localDataCenter, dcMessages, responseHandler);
    }

    /**
     * for each datacenter, send a message to one node to relay the write to other replicas
     */
    private static void sendMessages(String localDataCenter, Map<String, Multimap<Message, InetAddress>> dcMessages, IWriteResponseHandler handler)
    throws IOException
    {
        for (Map.Entry<String, Multimap<Message, InetAddress>> entry: dcMessages.entrySet())
        {
            String dataCenter = entry.getKey();

            // send the messages corresponding to this datacenter
            for (Map.Entry<Message, Collection<InetAddress>> messages: entry.getValue().asMap().entrySet())
            {
                Message message = messages.getKey();
                // a single message object is used for unhinted writes, so clean out any forwards
                // from previous loop iterations
                message.removeHeader(RowMutation.FORWARD_HEADER);

                if (dataCenter.equals(localDataCenter) || StorageService.instance.useEfficientCrossDCWrites())
                {
                    // direct writes to local DC or old Cassadra versions
                    for (InetAddress destination : messages.getValue())
                        MessagingService.instance().sendRR(message, destination, handler);
                }
                else
                {
                    // Non-local DC. First endpoint in list is the destination for this group
                    Iterator<InetAddress> iter = messages.getValue().iterator();
                    InetAddress target = iter.next();
                    // Add all the other destinations of the same message as a header in the primary message.
                    while (iter.hasNext())
                    {
                        InetAddress destination = iter.next();
                        // group all nodes in this DC as forward headers on the primary message
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        DataOutputStream dos = new DataOutputStream(bos);

                        // append to older addresses
                        byte[] previousHints = message.getHeader(RowMutation.FORWARD_HEADER);
                        if (previousHints != null)
                            dos.write(previousHints);

                        dos.write(destination.getAddress());
                        message.setHeader(RowMutation.FORWARD_HEADER, bos.toByteArray());
                    }
                    // send the combined message + forward headers
                    MessagingService.instance().sendRR(message, target, handler);
                }
            }
        }
    }

    private static void addHintHeader(Message message, InetAddress target) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        byte[] previousHints = message.getHeader(RowMutation.HINT);
        if (previousHints != null)
        {
            dos.write(previousHints);
        }
        ByteBufferUtil.writeWithShortLength(ByteBufferUtil.bytes(target.getHostAddress()), dos);
        message.setHeader(RowMutation.HINT, bos.toByteArray());
    }

    private static void insertLocal(final RowMutation rm, final IWriteResponseHandler responseHandler)
    {
        if (logger.isDebugEnabled())
            logger.debug("insert writing local " + rm.toString(true));
        Runnable runnable = new DroppableRunnable(StorageService.Verb.MUTATION)
        {
            public void runMayThrow() throws IOException
            {
                rm.localCopy().apply();
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
    public static IWriteResponseHandler mutateCounter(CounterMutation cm, String localDataCenter) throws UnavailableException, TimeoutException, IOException
    {
        InetAddress endpoint = findSuitableEndpoint(cm.getTable(), cm.key(), localDataCenter);

        if (endpoint.equals(FBUtilities.getLocalAddress()))
        {
            return applyCounterMutationOnCoordinator(cm, localDataCenter);
        }
        else
        {
            // Exit now if we can't fulfill the CL here instead of forwarding to the leader replica
            String table = cm.getTable();
            AbstractReplicationStrategy rs = Table.open(table).getReplicationStrategy();
            Collection<InetAddress> writeEndpoints = getWriteEndpoints(table, cm.key());
            Multimap<InetAddress, InetAddress> hintedEndpoints = rs.getHintedEndpoints(writeEndpoints);
            rs.getWriteResponseHandler(writeEndpoints, hintedEndpoints, cm.consistency()).assureSufficientLiveNodes();

            // Forward the actual update to the chosen leader replica
            IWriteResponseHandler responseHandler = WriteResponseHandler.create(endpoint);

            Message message = cm.makeMutationMessage(Gossiper.instance.getVersion(endpoint));
            if (logger.isDebugEnabled())
                logger.debug("forwarding counter update of key " + ByteBufferUtil.bytesToHex(cm.key()) + " to " + endpoint);
            MessagingService.instance().sendRR(message, endpoint, responseHandler);
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
            snitch.sortByProximity(FBUtilities.getLocalAddress(), endpoints);
            return endpoints.get(0);
        }
        else
        {
            return localEndpoints.get(FBUtilities.threadLocalRandom().nextInt(localEndpoints.size()));
        }
    }



    // Must be called on a replica of the mutation. This replica becomes the
    // leader of this mutation.
    public static IWriteResponseHandler applyCounterMutationOnLeader(CounterMutation cm, String localDataCenter) throws UnavailableException, TimeoutException, IOException
    {
        return performWrite(cm, cm.consistency(), localDataCenter, counterWritePerformer);
    }

    // Same as applyCounterMutationOnLeader but must with the difference that it use the MUTATION stage to execute the write (while
    // applyCounterMutationOnLeader assumes it is on the MUTATION stage already)
    public static IWriteResponseHandler applyCounterMutationOnCoordinator(CounterMutation cm, String localDataCenter) throws UnavailableException, TimeoutException, IOException
    {
        return performWrite(cm, cm.consistency(), localDataCenter, counterWriteOnCoordinatorPerformer);
    }

    private static Runnable counterWriteTask(final IMutation mutation, final Multimap<InetAddress, InetAddress> hintedEndpoints, final IWriteResponseHandler responseHandler, final String localDataCenter, final ConsistencyLevel consistency_level)
    {
        return new DroppableRunnable(StorageService.Verb.MUTATION)
        {
            public void runMayThrow() throws IOException
            {
                assert mutation instanceof CounterMutation;
                final CounterMutation cm = (CounterMutation) mutation;

                // apply mutation
                cm.apply();
                responseHandler.response(null);

                // then send to replicas, if any
                InetAddress local = FBUtilities.getLocalAddress();
                hintedEndpoints.remove(local, local);
                if (cm.shouldReplicateOnWrite() && !hintedEndpoints.isEmpty())
                {
                    // We do the replication on another stage because it involves a read (see CM.makeReplicationMutation)
                    // and we want to avoid blocking too much the MUTATION stage
                    StageManager.getStage(Stage.REPLICATE_ON_WRITE).execute(new DroppableRunnable(StorageService.Verb.READ)
                    {
                        public void runMayThrow() throws IOException
                        {
                            // send mutation to other replica
                            sendToHintedEndpoints(cm.makeReplicationMutation(), hintedEndpoints, responseHandler, localDataCenter, consistency_level);
                        }
                    });
                }
            }
        };
    }

    /**
     * Performs the actual reading of a row out of the StorageService, fetching
     * a specific set of column names from a given column family.
     */
    public static List<Row> read(List<ReadCommand> commands, ConsistencyLevel consistency_level)
            throws IOException, UnavailableException, TimeoutException, InvalidRequestException
    {
        if (StorageService.instance.isBootstrapMode())
            throw new UnavailableException();
        long startTime = System.nanoTime();
        List<Row> rows;
        try
        {
            rows = fetchRows(commands, consistency_level);
        }
        finally
        {
            readStats.addNano(System.nanoTime() - startTime);
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
    private static List<Row> fetchRows(List<ReadCommand> commands, ConsistencyLevel consistency_level) throws IOException, UnavailableException, TimeoutException
    {
        List<ReadCallback<Row>> readCallbacks = new ArrayList<ReadCallback<Row>>();
        List<Row> rows = new ArrayList<Row>();

        // send out read requests
        for (ReadCommand command: commands)
        {
            assert !command.isDigestQuery();
            logger.debug("Command/ConsistencyLevel is {}/{}", command, consistency_level);

            List<InetAddress> endpoints = StorageService.instance.getLiveNaturalEndpoints(command.table, command.key);
            DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getLocalAddress(), endpoints);

            RowDigestResolver resolver = new RowDigestResolver(command.table, command.key);
            ReadCallback<Row> handler = getReadCallback(resolver, command, consistency_level, endpoints);
            handler.assureSufficientLiveNodes();
            assert !handler.endpoints.isEmpty();
            readCallbacks.add(handler);

            // The data-request message is sent to dataPoint, the node that will actually get the data for us
            InetAddress dataPoint = handler.endpoints.get(0);
            if (dataPoint.equals(FBUtilities.getLocalAddress()))
            {
                logger.debug("reading data locally");
                StageManager.getStage(Stage.READ).execute(new LocalReadRunnable(command, handler));
            }
            else
            {
                logger.debug("reading data from {}", dataPoint);
                MessagingService.instance().sendRR(command, dataPoint, handler);
            }

            if (handler.endpoints.size() == 1)
                continue;

            // send the other endpoints a digest request
            ReadCommand digestCommand = command.copy();
            digestCommand.setDigestQuery(true);
            MessageProducer producer = null;
            for (InetAddress digestPoint : handler.endpoints.subList(1, handler.endpoints.size()))
            {
                if (digestPoint.equals(FBUtilities.getLocalAddress()))
                {
                    logger.debug("reading digest locally");
                    StageManager.getStage(Stage.READ).execute(new LocalReadRunnable(digestCommand, handler));
                }
                else
                {
                    logger.debug("reading digest from {}", digestPoint);
                    // (We lazy-construct the digest Message object since it may not be necessary if we
                    // are doing a local digest read, or no digest reads at all.)
                    if (producer == null)
                        producer = new CachingMessageProducer(digestCommand);
                    MessagingService.instance().sendRR(producer, digestPoint, handler);
                }
            }
        }

        // read results and make a second pass for any digest mismatches
        List<RepairCallback<Row>> repairResponseHandlers = null;
        for (int i = 0; i < commands.size(); i++)
        {
            ReadCallback<Row> handler = readCallbacks.get(i);
            Row row;
            ReadCommand command = commands.get(i);
            try
            {
                long startTime2 = System.currentTimeMillis();
                row = handler.get(); // CL.ONE is special cased here to ignore digests even if some have arrived
                if (row != null)
                    rows.add(row);

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
                RepairCallback<Row> repairHandler = new RepairCallback<Row>(resolver, handler.endpoints);
                MessageProducer producer = new CachingMessageProducer(command);
                for (InetAddress endpoint : handler.endpoints)
                    MessagingService.instance().sendRR(producer, endpoint, repairHandler);

                if (repairResponseHandlers == null)
                    repairResponseHandlers = new ArrayList<RepairCallback<Row>>();
                repairResponseHandlers.add(repairHandler);
            }
        }

        // read the results for the digest mismatch retries
        if (repairResponseHandlers != null)
        {
            for (RepairCallback<Row> handler : repairResponseHandlers)
            {
                try
                {
                    Row row = handler.get();
                    if (row != null)
                        rows.add(row);
                }
                catch (DigestMismatchException e)
                {
                    throw new AssertionError(e); // full data requested from each node here, no digests should be sent
                }
            }
        }

        return rows;
    }

    static class LocalReadRunnable extends DroppableRunnable
    {
        private final ReadCommand command;
        private final ReadCallback<Row> handler;
        private final long start = System.currentTimeMillis();

        LocalReadRunnable(ReadCommand command, ReadCallback<Row> handler)
        {
            super(StorageService.Verb.READ);
            this.command = command;
            this.handler = handler;
        }

        protected void runMayThrow() throws IOException
        {
            if (logger.isDebugEnabled())
                logger.debug("LocalReadRunnable reading " + command);

            Table table = Table.open(command.table);
            ReadResponse result = ReadVerbHandler.getResponse(command, command.getRow(table));
            MessagingService.instance().addLatency(FBUtilities.getLocalAddress(), System.currentTimeMillis() - start);
            handler.response(result);
        }
    }

    static <T> ReadCallback<T> getReadCallback(IResponseResolver<T> resolver, IReadCommand command, ConsistencyLevel consistencyLevel, List<InetAddress> endpoints)
    {
        if (consistencyLevel.equals(ConsistencyLevel.LOCAL_QUORUM) || consistencyLevel.equals(ConsistencyLevel.EACH_QUORUM))
        {
            return new DatacenterReadCallback(resolver, consistencyLevel, command, endpoints);
        }
        return new ReadCallback(resolver, consistencyLevel, command, endpoints);
    }

    /*
    * This function executes the read protocol locally.  Consistency checks are performed in the background.
    */

    public static List<Row> getRangeSlice(RangeSliceCommand command, ConsistencyLevel consistency_level)
    throws IOException, UnavailableException, TimeoutException
    {
        if (logger.isDebugEnabled())
            logger.debug(command.toString());
        long startTime = System.nanoTime();
        List<Row> rows;
        // now scan until we have enough results
        try
        {
            rows = new ArrayList<Row>(command.max_keys);
            List<AbstractBounds> ranges = getRestrictedRanges(command.range);
            for (AbstractBounds range : ranges)
            {
                List<InetAddress> liveEndpoints = StorageService.instance.getLiveNaturalEndpoints(command.keyspace, range.right);
                DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getLocalAddress(), liveEndpoints);

                if (consistency_level == ConsistencyLevel.ONE && !liveEndpoints.isEmpty() && liveEndpoints.get(0).equals(FBUtilities.getLocalAddress()))
                {
                    if (logger.isDebugEnabled())
                        logger.debug("local range slice");
                    ColumnFamilyStore cfs = Table.open(command.keyspace).getColumnFamilyStore(command.column_family);
                    try
                    {
                        rows.addAll(cfs.getRangeSlice(command.super_column,
                                                    range,
                                                    command.max_keys,
                                                    QueryFilter.getFilter(command.predicate, cfs.getComparator())));
                    }
                    catch (ExecutionException e)
                    {
                        throw new RuntimeException(e.getCause());
                    }
                    catch (InterruptedException e)
                    {
                        throw new AssertionError(e);
                    }
                }
                else
                {
                    RangeSliceCommand c2 = new RangeSliceCommand(command.keyspace, command.column_family, command.super_column, command.predicate, range, command.max_keys);

                    // collect replies and resolve according to consistency level
                    RangeSliceResponseResolver resolver = new RangeSliceResponseResolver(command.keyspace, liveEndpoints);
                    ReadCallback<Iterable<Row>> handler = getReadCallback(resolver, command, consistency_level, liveEndpoints);
                    handler.assureSufficientLiveNodes();
                    for (InetAddress endpoint : liveEndpoints)
                    {
                        MessagingService.instance().sendRR(c2, endpoint, handler);
                        if (logger.isDebugEnabled())
                            logger.debug("reading " + c2 + " from " + endpoint);
                    }

                    try
                    {
                        for (Row row : handler.get())
                        {
                            rows.add(row);
                            logger.debug("range slices read {}", row.key);
                        }
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
                }

                // if we're done, great, otherwise, move to the next range
                if (rows.size() >= command.max_keys)
                    break;
            }
        }
        finally
        {
            rangeStats.addNano(System.nanoTime() - startTime);
        }
        return rows.size() > command.max_keys ? rows.subList(0, command.max_keys) : rows;
    }

    /**
     * initiate a request/response session with each live node to check whether or not everybody is using the same
     * migration id. This is useful for determining if a schema change has propagated through the cluster. Disagreement
     * is assumed if any node fails to respond.
     */
    public static Map<String, List<String>> describeSchemaVersions()
    {
        final String myVersion = DatabaseDescriptor.getDefsVersion().toString();
        final Map<InetAddress, UUID> versions = new ConcurrentHashMap<InetAddress, UUID>();
        final Set<InetAddress> liveHosts = Gossiper.instance.getLiveMembers();
        final CountDownLatch latch = new CountDownLatch(liveHosts.size());

        IAsyncCallback cb = new IAsyncCallback()
        {
            public void response(Message message)
            {
                // record the response from the remote node.
                logger.debug("Received schema check response from " + message.getFrom().getHostAddress());
                UUID theirVersion = UUID.fromString(new String(message.getMessageBody()));
                versions.put(message.getFrom(), theirVersion);
                latch.countDown();
            }

            public boolean isLatencyForSnitch()
            {
                return false;
            }
        };
        // an empty message acts as a request to the SchemaCheckVerbHandler.
        for (InetAddress endpoint : liveHosts)
        {
            Message message = new Message(FBUtilities.getLocalAddress(), 
                                          StorageService.Verb.SCHEMA_CHECK, 
                                          ArrayUtils.EMPTY_BYTE_ARRAY, 
                                          Gossiper.instance.getVersion(endpoint));
            MessagingService.instance().sendRR(message, endpoint, cb);
        }

        try
        {
            // wait for as long as possible. timeout-1s if possible.
            latch.await(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError("This latch shouldn't have been interrupted.");
        }

        logger.debug("My version is " + myVersion);

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
            logger.debug("Hosts not in agreement. Didn't get a response from everybody: " + StringUtils.join(results.get(UNREACHABLE), ","));
        for (Map.Entry<String, List<String>> entry : results.entrySet())
        {
            // check for version disagreement. log the hosts that don't agree.
            if (entry.getKey().equals(UNREACHABLE) || entry.getKey().equals(myVersion))
                continue;
            for (String host : entry.getValue())
                logger.debug("%s disagrees (%s)", host, entry.getKey());
        }
        if (results.size() == 1)
            logger.debug("Schemas are in agreement.");

        return results;
    }

    /**
     * Compute all ranges we're going to query, in sorted order. Nodes can be replica destinations for many ranges,
     * so we need to restrict each scan to the specific range we want, or else we'd get duplicate results.
     */
    static List<AbstractBounds> getRestrictedRanges(final AbstractBounds queryRange)
    {
        // special case for bounds containing exactly 1 (non-minimum) token
        if (queryRange instanceof Bounds && queryRange.left.equals(queryRange.right) && !queryRange.left.equals(StorageService.getPartitioner().getMinimumToken()))
        {
            if (logger.isDebugEnabled())
                logger.debug("restricted single token match for query " + queryRange);
            return Collections.singletonList(queryRange);
        }

        TokenMetadata tokenMetadata = StorageService.instance.getTokenMetadata();

        List<AbstractBounds> ranges = new ArrayList<AbstractBounds>();
        // divide the queryRange into pieces delimited by the ring and minimum tokens
        Iterator<Token> ringIter = TokenMetadata.ringIterator(tokenMetadata.sortedTokens(), queryRange.left, true);
        AbstractBounds remainder = queryRange;
        while (ringIter.hasNext())
        {
            Token token = ringIter.next();
            if (remainder == null || !(remainder.left.equals(token) || remainder.contains(token)))
                // no more splits
                break;
            Pair<AbstractBounds,AbstractBounds> splits = remainder.split(token);
            if (splits.left != null)
                ranges.add(splits.left);
            remainder = splits.right;
        }
        if (remainder != null)
            ranges.add(remainder);
        if (logger.isDebugEnabled())
            logger.debug("restricted ranges for query " + queryRange + " are " + ranges);

        return ranges;
    }

    public long getReadOperations()
    {
        return readStats.getOpCount();
    }

    public long getTotalReadLatencyMicros()
    {
        return readStats.getTotalLatencyMicros();
    }

    public double getRecentReadLatencyMicros()
    {
        return readStats.getRecentLatencyMicros();
    }

    public long[] getTotalReadLatencyHistogramMicros()
    {
        return readStats.getTotalLatencyHistogramMicros();
    }

    public long[] getRecentReadLatencyHistogramMicros()
    {
        return readStats.getRecentLatencyHistogramMicros();
    }

    public long getRangeOperations()
    {
        return rangeStats.getOpCount();
    }

    public long getTotalRangeLatencyMicros()
    {
        return rangeStats.getTotalLatencyMicros();
    }

    public double getRecentRangeLatencyMicros()
    {
        return rangeStats.getRecentLatencyMicros();
    }

    public long[] getTotalRangeLatencyHistogramMicros()
    {
        return rangeStats.getTotalLatencyHistogramMicros();
    }

    public long[] getRecentRangeLatencyHistogramMicros()
    {
        return rangeStats.getRecentLatencyHistogramMicros();
    }

    public long getWriteOperations()
    {
        return writeStats.getOpCount();
    }

    public long getTotalWriteLatencyMicros()
    {
        return writeStats.getTotalLatencyMicros();
    }

    public double getRecentWriteLatencyMicros()
    {
        return writeStats.getRecentLatencyMicros();
    }

    public long[] getTotalWriteLatencyHistogramMicros()
    {
        return writeStats.getTotalLatencyHistogramMicros();
    }

    public long[] getRecentWriteLatencyHistogramMicros()
    {
        return writeStats.getRecentLatencyHistogramMicros();
    }

    public static List<Row> scan(final String keyspace, String column_family, IndexClause index_clause, SlicePredicate column_predicate, ConsistencyLevel consistency_level)
    throws IOException, TimeoutException, UnavailableException
    {
        IPartitioner p = StorageService.getPartitioner();

        Token leftToken = index_clause.start_key == null ? p.getMinimumToken() : p.getToken(index_clause.start_key);
        List<AbstractBounds> ranges = getRestrictedRanges(new Bounds(leftToken, p.getMinimumToken()));
        logger.debug("scan ranges are " + StringUtils.join(ranges, ","));

        // now scan until we have enough results
        List<Row> rows = new ArrayList<Row>(index_clause.count);
        for (AbstractBounds range : ranges)
        {
            List<InetAddress> liveEndpoints = StorageService.instance.getLiveNaturalEndpoints(keyspace, range.right);
            DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getLocalAddress(), liveEndpoints);

            // collect replies and resolve according to consistency level
            RangeSliceResponseResolver resolver = new RangeSliceResponseResolver(keyspace, liveEndpoints);
            IReadCommand iCommand = new IReadCommand()
            {
                public String getKeyspace()
                {
                    return keyspace;
                }
            };
            ReadCallback<Iterable<Row>> handler = getReadCallback(resolver, iCommand, consistency_level, liveEndpoints);
            handler.assureSufficientLiveNodes();

            IndexScanCommand command = new IndexScanCommand(keyspace, column_family, index_clause, column_predicate, range);
            MessageProducer producer = new CachingMessageProducer(command);
            for (InetAddress endpoint : liveEndpoints)
            {
                MessagingService.instance().sendRR(producer, endpoint, handler);
                if (logger.isDebugEnabled())
                    logger.debug("reading " + command + " from " + endpoint);
            }

            try
            {
                for (Row row : handler.get())
                {
                    rows.add(row);
                    logger.debug("read {}", row);
                }
            }
            catch (TimeoutException ex)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Index scan timeout: {}", ex.toString());
                throw ex;
            }
            catch (DigestMismatchException e)
            {
                throw new RuntimeException(e);
            }
            if (rows.size() >= index_clause.count)
                return rows.subList(0, index_clause.count);
        }

        return rows;
    }

    public boolean getHintedHandoffEnabled()
    {
        return hintedHandoffEnabled;
    }

    public void setHintedHandoffEnabled(boolean b)
    {
        hintedHandoffEnabled = b;
    }

    public static boolean isHintedHandoffEnabled()
    {
        return hintedHandoffEnabled;
    }

    public int getMaxHintWindow()
    {
        return maxHintWindow;
    }

    public void setMaxHintWindow(int ms)
    {
        maxHintWindow = ms;
    }

    public static boolean shouldHint(InetAddress ep)
    {
        return Gossiper.instance.getEndpointDowntime(ep) <= maxHintWindow;
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
        MessageProducer producer = new CachingMessageProducer(truncation);
        for (InetAddress endpoint : allEndpoints)
            MessagingService.instance().sendRR(producer, endpoint, responseHandler);

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

    private interface WritePerformer
    {
        public void apply(IMutation mutation, Multimap<InetAddress, InetAddress> hintedEndpoints, IWriteResponseHandler responseHandler, String localDataCenter, ConsistencyLevel consistency_level) throws IOException;
    }

    private static abstract class DroppableRunnable implements Runnable
    {
        private final long constructionTime = System.currentTimeMillis();
        private final StorageService.Verb verb;

        public DroppableRunnable(StorageService.Verb verb)
        {
            this.verb = verb;
        }

        public final void run()
        {
            if (System.currentTimeMillis() > constructionTime + DatabaseDescriptor.getRpcTimeout())
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
}
