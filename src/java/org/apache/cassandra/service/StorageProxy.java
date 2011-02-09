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
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
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

import static com.google.common.base.Charsets.UTF_8;

public class StorageProxy implements StorageProxyMBean
{
    private static final Logger logger = LoggerFactory.getLogger(StorageProxy.class);

    private static ScheduledExecutorService repairExecutor = new ScheduledThreadPoolExecutor(1); // TODO JMX-enable this

    private static final ThreadLocal<Random> random = new ThreadLocal<Random>()
    {
        @Override
        protected Random initialValue()
        {
            return new Random();
        }
    };

    // mbean stuff
    private static final LatencyTracker readStats = new LatencyTracker();
    private static final LatencyTracker rangeStats = new LatencyTracker();
    private static final LatencyTracker writeStats = new LatencyTracker();
    // we keep counter latency appart from normal write because write with
    // consistency > CL.ONE involves a read in the write path
    private static final LatencyTracker counterWriteStats = new LatencyTracker();
    private static boolean hintedHandoffEnabled = DatabaseDescriptor.hintedHandoffEnabled();
    private static int maxHintWindow = DatabaseDescriptor.getMaxHintWindow();
    private static final String UNREACHABLE = "UNREACHABLE";

    private static final WritePerformer standardWritePerformer;
    private static final WritePerformer counterWritePerformer;

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
                sendToHintedEndpoints((RowMutation) mutation, hintedEndpoints, responseHandler, localDataCenter, true, consistency_level);
            }
        };

        counterWritePerformer = new WritePerformer()
        {
            public void apply(IMutation mutation, Multimap<InetAddress, InetAddress> hintedEndpoints, IWriteResponseHandler responseHandler, String localDataCenter, ConsistencyLevel consistency_level) throws IOException
            {
                applyCounterMutation(mutation, hintedEndpoints, responseHandler, localDataCenter, consistency_level);
            }
        };
    }

    /**
     * Use this method to have these RowMutations applied
     * across all replicas. This method will take care
     * of the possibility of a replica being down and hint
     * the data across to some other replica.
     *
     * @param mutations the mutations to be applied across the replicas
     * @param consistency_level the consistency level for the operation
    */
    public static void mutate(List<RowMutation> mutations, ConsistencyLevel consistency_level) throws UnavailableException, TimeoutException
    {
        write(mutations, consistency_level, standardWritePerformer, true);
    }

    /**
     * Perform the write of a batch of mutations given a WritePerformer.
     * For each mutation, gather the list of write endpoints, apply locally and/or
     * forward the mutation to said write endpoint (deletaged to the actual
     * WritePerformer) and wait for the responses based on consistency level.
     *
     * @param mutations the mutations to be applied
     * @param consistency_level the consistency level for the write operation
     * @param performer the WritePerformer in charge of appliying the mutation
     * given the list of write endpoints (either standardWritePerformer for
     * standard writes or counterWritePerformer for counter writes).
     * @param updateStats whether or not to update the writeStats. This must be
     * true for standard writes but false for counter writes as the latency of
     * the latter is tracked in mutateCounters() by counterWriteStats.
     */
    public static void write(List<? extends IMutation> mutations, ConsistencyLevel consistency_level, WritePerformer performer, boolean updateStats) throws UnavailableException, TimeoutException
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
                String table = mutation.getTable();
                AbstractReplicationStrategy rs = Table.open(table).getReplicationStrategy();

                Collection<InetAddress> writeEndpoints = getWriteEndpoints(table, mutation.key());
                Multimap<InetAddress, InetAddress> hintedEndpoints = rs.getHintedEndpoints(writeEndpoints);

                final IWriteResponseHandler responseHandler = rs.getWriteResponseHandler(writeEndpoints, hintedEndpoints, consistency_level);

                // exit early if we can't fulfill the CL at this time
                responseHandler.assureSufficientLiveNodes();

                responseHandlers.add(responseHandler);
                performer.apply(mutation, hintedEndpoints, responseHandler, localDataCenter, consistency_level);
            }
            // wait for writes.  throws timeoutexception if necessary
            for (IWriteResponseHandler responseHandler : responseHandlers)
            {
                responseHandler.get();
            }
        }
        catch (IOException e)
        {
            assert mostRecentMutation != null;
            throw new RuntimeException("error writing key " + ByteBufferUtil.bytesToHex(mostRecentMutation.key()), e);
        }
        finally
        {
            if (updateStats)
                writeStats.addNano(System.nanoTime() - startTime);
        }
    }

    private static Collection<InetAddress> getWriteEndpoints(String table, ByteBuffer key)
    {
        StorageService ss = StorageService.instance;
        List<InetAddress> naturalEndpoints = ss.getNaturalEndpoints(table, key);
        return ss.getTokenMetadata().getWriteEndpoints(StorageService.getPartitioner().getToken(key), table, naturalEndpoints);
    }

    private static void sendToHintedEndpoints(final RowMutation rm, Multimap<InetAddress, InetAddress> hintedEndpoints, IWriteResponseHandler responseHandler, String localDataCenter, boolean insertLocalMessages, ConsistencyLevel consistency_level)
    throws IOException
    {
        // Multimap that holds onto all the messages and addresses meant for a specific datacenter
        Map<String, Multimap<Message, InetAddress>> dcMessages = new HashMap<String, Multimap<Message, InetAddress>>(hintedEndpoints.size());
        MessageProducer prod = new CachingMessageProducer(rm);

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

                    messages.put(prod.getMessage(Gossiper.instance.getVersion(destination)), destination);
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

            sendMessages(localDataCenter, dcMessages, responseHandler);
        }
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

                if (dataCenter.equals(localDataCenter))
                {
                    // direct writes to local DC
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
        ByteBufferUtil.writeWithShortLength(ByteBuffer.wrap(target.getHostAddress().getBytes(UTF_8)), dos);
        message.setHeader(RowMutation.HINT, bos.toByteArray());
    }

    private static void insertLocal(final RowMutation rm, final IWriteResponseHandler responseHandler)
    {
        if (logger.isDebugEnabled())
            logger.debug("insert writing local " + rm.toString(true));
        Runnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws IOException
            {
                rm.deepCopy().apply();
                responseHandler.response(null);
            }
        };
        StageManager.getStage(Stage.MUTATION).execute(runnable);
    }

    /**
     * The equivalent of mutate() for counters.
     * (Note that each CounterMutation ship the consistency level)
     *
     * A counter mutation needs to first be applied to a replica (that we'll call the leader for the mutation) before being
     * replicated to the other endpoint. To achieve so, there is two case:
     *   1) the coordinator host is a replica: we proceed to applying the update locally and replicate throug
     *   applyCounterMutationOnLeader
     *   2) the coordinator is not a replica: we forward the (counter)mutation to a chosen replica (that will proceed through
     *   applyCounterMutationOnLeader upon receive) and wait for its acknowledgment.
     *
     * Implementation note: We check if we can fulfill the CL on the coordinator host even if he is not a replica to allow
     * quicker response and because the WriteResponseHandlers don't make it easy to send back an error. We also always gather
     * the write latencies at the coordinator node to make gathering point similar to the case of standard writes.
     */
    public static void mutateCounters(List<CounterMutation> mutations) throws UnavailableException, TimeoutException
    {
        long startTime = System.nanoTime();
        ArrayList<IWriteResponseHandler> responseHandlers = new ArrayList<IWriteResponseHandler>();

        CounterMutation mostRecentMutation = null;
        StorageService ss = StorageService.instance;

        try
        {
            for (CounterMutation cm : mutations)
            {
                mostRecentMutation = cm;
                InetAddress endpoint = findSuitableEndpoint(cm.getTable(), cm.key());

                if (endpoint.equals(FBUtilities.getLocalAddress()))
                {
                    applyCounterMutationOnLeader(cm);
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
                    responseHandlers.add(responseHandler);

                    Message message = cm.makeMutationMessage(Gossiper.instance.getVersion(endpoint));
                    if (logger.isDebugEnabled())
                        logger.debug("forwarding counter update of key " + ByteBufferUtil.bytesToHex(cm.key()) + " to " + endpoint);
                    MessagingService.instance().sendRR(message, endpoint, responseHandler);
                }
            }
            // wait for writes.  throws timeoutexception if necessary
            for (IWriteResponseHandler responseHandler : responseHandlers)
            {
                responseHandler.get();
            }
        }
        catch (IOException e)
        {
            if (mostRecentMutation == null)
                throw new RuntimeException("no mutations were seen but found an error during write anyway", e);
            else
                throw new RuntimeException("error writing key " + ByteBufferUtil.bytesToHex(mostRecentMutation.key()), e);
        }
        finally
        {
            counterWriteStats.addNano(System.nanoTime() - startTime);
        }
    }

    private static InetAddress findSuitableEndpoint(String table, ByteBuffer key) throws UnavailableException
    {
        List<InetAddress> endpoints = StorageService.instance.getLiveNaturalEndpoints(table, key);
        DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getLocalAddress(), endpoints);
        if (endpoints.isEmpty())
            throw new UnavailableException();
        return endpoints.get(0);
    }

    // Must be called on a replica of the mutation. This replica becomes the
    // leader of this mutation.
    public static void applyCounterMutationOnLeader(CounterMutation cm) throws UnavailableException, TimeoutException, IOException
    {
        write(Collections.singletonList(cm), cm.consistency(), counterWritePerformer, false);
    }

    private static void applyCounterMutation(final IMutation mutation, final Multimap<InetAddress, InetAddress> hintedEndpoints, final IWriteResponseHandler responseHandler, final String localDataCenter, final ConsistencyLevel consistency_level)
    {
        // we apply locally first, then send it to other replica
        if (logger.isDebugEnabled())
            logger.debug("insert writing local & replicate " + mutation.toString(true));

        Runnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws IOException
            {
                assert mutation instanceof CounterMutation;
                final CounterMutation cm = (CounterMutation) mutation;

                // apply mutation
                cm.apply();

                responseHandler.response(null);

                if (cm.shouldReplicateOnWrite())
                {
                    // We do the replication on another stage because it involves a read (see CM.makeReplicationMutation)
                    // and we want to avoid blocking too much the MUTATION stage
                    StageManager.getStage(Stage.REPLICATE_ON_WRITE).execute(new WrappedRunnable()
                    {
                        public void runMayThrow() throws IOException
                        {
                            // send mutation to other replica
                            sendToHintedEndpoints(cm.makeReplicationMutation(), hintedEndpoints, responseHandler, localDataCenter, false, consistency_level);
                        }
                    });
                }
            }
        };
        StageManager.getStage(Stage.MUTATION).execute(runnable);
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
        List<List<InetAddress>> commandEndpoints = new ArrayList<List<InetAddress>>();
        List<Row> rows = new ArrayList<Row>();
        Set<ReadCommand> repairs = new HashSet<ReadCommand>();

        // send out read requests
        for (ReadCommand command: commands)
        {
            assert !command.isDigestQuery();

            List<InetAddress> endpoints = StorageService.instance.getLiveNaturalEndpoints(command.table, command.key);
            DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getLocalAddress(), endpoints);

            ReadResponseResolver resolver = new ReadResponseResolver(command.table, command.key);
            ReadCallback<Row> handler = getReadCallback(resolver, command.table, consistency_level);
            handler.assureSufficientLiveNodes(endpoints);

            // if we're not going to read repair, cut the endpoints list down to the ones required to satisfy ConsistencyLevel
            if (randomlyReadRepair(command))
            {
                if (endpoints.size() > handler.blockfor)
                    repairs.add(command);
            }
            else
            {
                endpoints = endpoints.subList(0, handler.blockfor);
            }

            // The data-request message is sent to dataPoint, the node that will actually get
            // the data for us. The other replicas are only sent a digest query.
            ReadCommand digestCommand = null;
            if (endpoints.size() > 1)
            {
                digestCommand = command.copy();
                digestCommand.setDigestQuery(true);
            }

            InetAddress dataPoint = endpoints.get(0);
            if (dataPoint.equals(FBUtilities.getLocalAddress()))
            {
                if (logger.isDebugEnabled())
                    logger.debug("reading data for " + command + " locally");
                StageManager.getStage(Stage.READ).execute(new LocalReadRunnable(command, handler));
            }
            else
            {
                Message message = command.getMessage(Gossiper.instance.getVersion(dataPoint));
                if (logger.isDebugEnabled())
                    logger.debug("reading data for " + command + " from " + dataPoint);
                MessagingService.instance().sendRR(message, dataPoint, handler);
            }

            // We lazy-construct the digest Message object since it may not be necessary if we
            // are doing a local digest read, or no digest reads at all.
            MessageProducer prod = new CachingMessageProducer(digestCommand);
            for (InetAddress digestPoint : endpoints.subList(1, endpoints.size()))
            {
                if (digestPoint.equals(FBUtilities.getLocalAddress()))
                {
                    if (logger.isDebugEnabled())
                        logger.debug("reading digest for " + command + " locally");
                    StageManager.getStage(Stage.READ).execute(new LocalReadRunnable(digestCommand, handler));
                }
                else
                {
                    if (logger.isDebugEnabled())
                        logger.debug("reading digest for " + command + " from " + digestPoint);
                    MessagingService.instance().sendRR(prod, digestPoint, handler);
                }
            }

            readCallbacks.add(handler);
            commandEndpoints.add(endpoints);
        }

        // read results and make a second pass for any digest mismatches
        List<RepairCallback<Row>> repairResponseHandlers = null;
        for (int i = 0; i < commands.size(); i++)
        {
            ReadCallback<Row> readCallback = readCallbacks.get(i);
            Row row;
            ReadCommand command = commands.get(i);
            List<InetAddress> endpoints = commandEndpoints.get(i);
            try
            {
                long startTime2 = System.currentTimeMillis();
                row = readCallback.get(); // CL.ONE is special cased here to ignore digests even if some have arrived
                if (row != null)
                    rows.add(row);

                if (logger.isDebugEnabled())
                    logger.debug("Read: " + (System.currentTimeMillis() - startTime2) + " ms.");

                if (repairs.contains(command))
                    repairExecutor.schedule(new RepairRunner(readCallback.resolver, command, endpoints), DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
            }
            catch (DigestMismatchException ex)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Digest mismatch:", ex);
                RepairCallback<Row> handler = repair(command, endpoints);
                if (repairResponseHandlers == null)
                    repairResponseHandlers = new ArrayList<RepairCallback<Row>>();
                repairResponseHandlers.add(handler);
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

    static class LocalReadRunnable extends WrappedRunnable
    {
        private final ReadCommand command;
        private final ReadCallback<Row> handler;
        private final long start = System.currentTimeMillis();

        LocalReadRunnable(ReadCommand command, ReadCallback<Row> handler)
        {
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

    static <T> ReadCallback<T> getReadCallback(IResponseResolver<T> resolver, String table, ConsistencyLevel consistencyLevel)
    {
        if (consistencyLevel.equals(ConsistencyLevel.LOCAL_QUORUM) || consistencyLevel.equals(ConsistencyLevel.EACH_QUORUM))
        {
            return new DatacenterReadCallback(resolver, consistencyLevel, table);
        }
        return new ReadCallback(resolver, consistencyLevel, table);
    }

    private static RepairCallback<Row> repair(ReadCommand command, List<InetAddress> endpoints)
    throws IOException
    {
        ReadResponseResolver resolver = new ReadResponseResolver(command.table, command.key);
        RepairCallback<Row> handler = new RepairCallback<Row>(resolver, endpoints);
        MessageProducer prod = new CachingMessageProducer(command);
        for (InetAddress endpoint : endpoints)
            MessagingService.instance().sendRR(prod, endpoint, handler);
        return handler;
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

                if (consistency_level == ConsistencyLevel.ONE && liveEndpoints.contains(FBUtilities.getLocalAddress()))
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
                    DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getLocalAddress(), liveEndpoints);
                    RangeSliceCommand c2 = new RangeSliceCommand(command.keyspace, command.column_family, command.super_column, command.predicate, range, command.max_keys);

                    // collect replies and resolve according to consistency level
                    RangeSliceResponseResolver resolver = new RangeSliceResponseResolver(command.keyspace, liveEndpoints);
                    ReadCallback<List<Row>> handler = getReadCallback(resolver, command.keyspace, consistency_level);
                    MessageProducer prod = new CachingMessageProducer(c2);
                    // TODO bail early if live endpoints can't satisfy requested consistency level
                    for (InetAddress endpoint : liveEndpoints)
                    {
                        MessagingService.instance().sendRR(prod, endpoint, handler);
                        if (logger.isDebugEnabled())
                            logger.debug("reading " + c2 + " from " + endpoint);
                    }
                    // TODO read repair on remaining replicas?

                    // if we're done, great, otherwise, move to the next range
                    try
                    {
                        if (logger.isDebugEnabled())
                        {
                            for (Row row : handler.get())
                            {
                                logger.debug("range slices read " + row.key);
                            }
                        }
                        rows.addAll(handler.get());
                    }
                    catch (DigestMismatchException e)
                    {
                        throw new AssertionError(e); // no digests in range slices yet
                    }
                }

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
        if (results.get(UNREACHABLE) != null)
            logger.debug("Hosts not in agreement. Didn't get a response from everybody: " + StringUtils.join(results.get(UNREACHABLE), ","));
        // check for version disagreement. log the hosts that don't agree.
        for (Map.Entry<String, List<String>> entry : results.entrySet())
        {
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

    private static boolean randomlyReadRepair(ReadCommand command)
    {
        CFMetaData cfmd = DatabaseDescriptor.getTableMetaData(command.table).get(command.getColumnFamilyName());
        return cfmd.getReadRepairChance() > random.get().nextDouble();
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

    public long getCounterWriteOperations()
    {
        return counterWriteStats.getOpCount();
    }

    public long getTotalCounterWriteLatencyMicros()
    {
        return counterWriteStats.getTotalLatencyMicros();
    }

    public double getRecentCounterWriteLatencyMicros()
    {
        return counterWriteStats.getRecentLatencyMicros();
    }

    public long[] getTotalCounterWriteLatencyHistogramMicros()
    {
        return counterWriteStats.getTotalLatencyHistogramMicros();
    }

    public long[] getRecentCounterWriteLatencyHistogramMicros()
    {
        return counterWriteStats.getRecentLatencyHistogramMicros();
    }

    public static List<Row> scan(String keyspace, String column_family, IndexClause index_clause, SlicePredicate column_predicate, ConsistencyLevel consistency_level)
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
            ReadCallback<List<Row>> handler = getReadCallback(resolver, keyspace, consistency_level);

            // bail early if live endpoints can't satisfy requested consistency level
            if(handler.blockfor > liveEndpoints.size())
                throw new UnavailableException();

            IndexScanCommand command = new IndexScanCommand(keyspace, column_family, index_clause, column_predicate, range);
            MessageProducer prod = new CachingMessageProducer(command);
            for (InetAddress endpoint : liveEndpoints)
            {
                MessagingService.instance().sendRR(prod, endpoint, handler);
                if (logger.isDebugEnabled())
                    logger.debug("reading " + command + " from " + endpoint);
            }

            List<Row> theseRows;
            try
            {
                theseRows = handler.get();
            }
            catch (DigestMismatchException e)
            {
                throw new RuntimeException(e);
            }
            rows.addAll(theseRows);
            if (logger.isDebugEnabled())
            {
                for (Row row : theseRows)
                    logger.debug("read " + row);
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
        MessageProducer prod = new CachingMessageProducer(truncation);
        for (InetAddress endpoint : allEndpoints)
        {
            MessagingService.instance().sendRR(prod, endpoint, responseHandler);
        }

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

    private static class RepairRunner extends WrappedRunnable
    {
        private final IResponseResolver<Row> resolver;
        private final ReadCommand command;
        private final List<InetAddress> endpoints;

        public RepairRunner(IResponseResolver<Row> resolver, ReadCommand command, List<InetAddress> endpoints)
        {
            this.resolver = resolver;
            this.command = command;
            this.endpoints = endpoints;
        }

        protected void runMayThrow() throws IOException
        {
            try
            {
                resolver.resolve();
            }
            catch (DigestMismatchException e)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Digest mismatch:", e);
                final RepairCallback<Row> callback = repair(command, endpoints);
                Runnable runnable = new WrappedRunnable()
                {
                    public void runMayThrow() throws DigestMismatchException, IOException, TimeoutException
                    {
                        callback.get();
                    }
                };
                repairExecutor.schedule(runnable, DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
            }
        }
    }

    private interface WritePerformer
    {
        public void apply(IMutation mutation, Multimap<InetAddress, InetAddress> hintedEndpoints, IWriteResponseHandler responseHandler, String localDataCenter, ConsistencyLevel consistency_level) throws IOException;
    }
}
