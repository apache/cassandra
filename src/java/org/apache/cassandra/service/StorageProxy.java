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

import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import static com.google.common.base.Charsets.UTF_8;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.IAsyncResult;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.LatencyTracker;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.db.filter.QueryFilter;

public class StorageProxy implements StorageProxyMBean
{
    private static final Logger logger = LoggerFactory.getLogger(StorageProxy.class);

    private static final Random random = new Random();
    // mbean stuff
    private static final LatencyTracker readStats = new LatencyTracker();
    private static final LatencyTracker rangeStats = new LatencyTracker();
    private static final LatencyTracker writeStats = new LatencyTracker();
    private static boolean hintedHandoffEnabled = DatabaseDescriptor.hintedHandoffEnabled();
    private static final String UNREACHABLE = "UNREACHABLE";

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
        long startTime = System.nanoTime();
        ArrayList<IWriteResponseHandler> responseHandlers = new ArrayList<IWriteResponseHandler>();

        RowMutation mostRecentRowMutation = null;
        StorageService ss = StorageService.instance;
        
        try
        {
            for (RowMutation rm : mutations)
            {
                mostRecentRowMutation = rm;
                String table = rm.getTable();
                AbstractReplicationStrategy rs = Table.open(table).replicationStrategy;

                List<InetAddress> naturalEndpoints = ss.getNaturalEndpoints(table, rm.key());
                Collection<InetAddress> writeEndpoints = ss.getTokenMetadata().getWriteEndpoints(StorageService.getPartitioner().getToken(rm.key()), table, naturalEndpoints);
                Multimap<InetAddress, InetAddress> hintedEndpoints = rs.getHintedEndpoints(writeEndpoints);
                
                // send out the writes, as in mutate() above, but this time with a callback that tracks responses
                final IWriteResponseHandler responseHandler = rs.getWriteResponseHandler(writeEndpoints, hintedEndpoints, consistency_level);
                responseHandler.assureSufficientLiveNodes();

                responseHandlers.add(responseHandler);
                Message unhintedMessage = null;
                for (Map.Entry<InetAddress, Collection<InetAddress>> entry : hintedEndpoints.asMap().entrySet())
                {
                    InetAddress destination = entry.getKey();
                    Collection<InetAddress> targets = entry.getValue();

                    if (targets.size() == 1 && targets.iterator().next().equals(destination))
                    {
                        // unhinted writes
                        if (destination.equals(FBUtilities.getLocalAddress()))
                        {
                            insertLocalMessage(rm, responseHandler);
                        }
                        else
                        {
                            // belongs on a different server.  send it there.
                            if (unhintedMessage == null)
                            {
                                unhintedMessage = rm.makeRowMutationMessage();
                                MessagingService.instance.addCallback(responseHandler, unhintedMessage.getMessageId());
                            }
                            if (logger.isDebugEnabled())
                                logger.debug("insert writing key " + FBUtilities.bytesToHex(rm.key()) + " to " + unhintedMessage.getMessageId() + "@" + destination);
                            MessagingService.instance.sendOneWay(unhintedMessage, destination);
                        }
                    }
                    else
                    {
                        // hinted
                        Message hintedMessage = rm.makeRowMutationMessage();
                        for (InetAddress target : targets)
                        {
                            if (!target.equals(destination))
                            {
                                addHintHeader(hintedMessage, target);
                                if (logger.isDebugEnabled())
                                    logger.debug("insert writing key " + FBUtilities.bytesToHex(rm.key()) + " to " + hintedMessage.getMessageId() + "@" + destination + " for " + target);
                            }
                        }
                        responseHandler.addHintCallback(hintedMessage, destination);
                        MessagingService.instance.sendOneWay(hintedMessage, destination);
                    }
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
            if (mostRecentRowMutation == null)
                throw new RuntimeException("no mutations were seen but found an error during write anyway", e);
            else
                throw new RuntimeException("error writing key " + FBUtilities.bytesToHex(mostRecentRowMutation.key()), e);
        }
        finally
        {
            writeStats.addNano(System.nanoTime() - startTime);
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
        FBUtilities.writeShortByteArray(ByteBuffer.wrap(target.getHostAddress().getBytes(UTF_8)), dos);
        message.setHeader(RowMutation.HINT, bos.toByteArray());
    }

    private static void insertLocalMessage(final RowMutation rm, final IWriteResponseHandler responseHandler)
    {
        if (logger.isDebugEnabled())
            logger.debug("insert writing local " + rm.toString(true));
        Runnable runnable = new WrappedRunnable()
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
     * Performs the actual reading of a row out of the StorageService, fetching
     * a specific set of column names from a given column family.
     */
    public static List<Row> readProtocol(List<ReadCommand> commands, ConsistencyLevel consistency_level)
            throws IOException, UnavailableException, TimeoutException, InvalidRequestException
    {
        if (StorageService.instance.isBootstrapMode())
            throw new InvalidRequestException("This node cannot accept reads until it has bootstrapped");
        long startTime = System.nanoTime();

        List<Row> rows;
        if (consistency_level == ConsistencyLevel.ONE)
        {
            rows = weakRead(commands);
        }
        else
        {
            assert consistency_level.getValue() >= ConsistencyLevel.QUORUM.getValue();
            rows = strongRead(commands, consistency_level);
        }

        readStats.addNano(System.nanoTime() - startTime);
        return rows;
    }

    private static List<Row> weakRead(List<ReadCommand> commands) throws IOException, UnavailableException, TimeoutException
    {
        List<Row> rows = new ArrayList<Row>();

        // send off all the commands asynchronously
        List<Future<Object>> localFutures = null;
        List<IAsyncResult> remoteResults = null;
        for (ReadCommand command: commands)
        {
            InetAddress endPoint = StorageService.instance.findSuitableEndpoint(command.table, command.key);
            if (endPoint.equals(FBUtilities.getLocalAddress()))
            {
                if (logger.isDebugEnabled())
                    logger.debug("weakread reading " + command + " locally");

                if (localFutures == null)
                    localFutures = new ArrayList<Future<Object>>();
                Callable<Object> callable = new weakReadLocalCallable(command);
                localFutures.add(StageManager.getStage(Stage.READ).submit(callable));
            }
            else
            {
                if (remoteResults == null)
                    remoteResults = new ArrayList<IAsyncResult>();
                Message message = command.makeReadMessage();
                if (logger.isDebugEnabled())
                    logger.debug("weakread reading " + command + " from " + message.getMessageId() + "@" + endPoint);
                if (randomlyReadRepair(command))
                    message.setHeader(ReadCommand.DO_REPAIR, ReadCommand.DO_REPAIR.getBytes());
                remoteResults.add(MessagingService.instance.sendRR(message, endPoint));
            }
        }

        // wait for results
        if (localFutures != null)
        {
            for (Future<Object> future : localFutures)
            {
                Row row;
                try
                {
                    row = (Row) future.get();
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
                rows.add(row);
            }
        }
        if (remoteResults != null)
        {
            for (IAsyncResult iar: remoteResults)
            {
                byte[] body;
                body = iar.get(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
                ByteArrayInputStream bufIn = new ByteArrayInputStream(body);
                ReadResponse response = ReadResponse.serializer().deserialize(new DataInputStream(bufIn));
                if (response.row() != null)
                    rows.add(response.row());
            }
        }

        return rows;
    }

    /*
     * This function executes the read protocol.
        // 1. Get the N nodes from storage service where the data needs to be
        // replicated
        // 2. Construct a message for read\write
         * 3. Set one of the messages to get the data and the rest to get the digest
        // 4. SendRR ( to all the nodes above )
        // 5. Wait for a response from at least X nodes where X <= N and the data node
         * 6. If the digest matches return the data.
         * 7. else carry out read repair by getting data from all the nodes.
        // 5. return success
     */
    private static List<Row> strongRead(List<ReadCommand> commands, ConsistencyLevel consistency_level) throws IOException, UnavailableException, TimeoutException
    {
        List<QuorumResponseHandler<Row>> quorumResponseHandlers = new ArrayList<QuorumResponseHandler<Row>>();
        List<InetAddress[]> commandEndpoints = new ArrayList<InetAddress[]>();
        List<Row> rows = new ArrayList<Row>();

        // send out read requests
        for (ReadCommand command: commands)
        {
            assert !command.isDigestQuery();
            ReadCommand readMessageDigestOnly = command.copy();
            readMessageDigestOnly.setDigestQuery(true);
            Message message = command.makeReadMessage();
            Message messageDigestOnly = readMessageDigestOnly.makeReadMessage();

            InetAddress dataPoint = StorageService.instance.findSuitableEndpoint(command.table, command.key);
            List<InetAddress> endpointList = StorageService.instance.getLiveNaturalEndpoints(command.table, command.key);

            InetAddress[] endpoints = new InetAddress[endpointList.size()];
            Message messages[] = new Message[endpointList.size()];
            // data-request message is sent to dataPoint, the node that will actually get
            // the data for us. The other replicas are only sent a digest query.
            int n = 0;
            for (InetAddress endpoint : endpointList)
            {
                Message m = endpoint.equals(dataPoint) ? message : messageDigestOnly;
                endpoints[n] = endpoint;
                messages[n++] = m;
                if (logger.isDebugEnabled())
                    logger.debug("strongread reading " + (m == message ? "data" : "digest") + " for " + command + " from " + m.getMessageId() + "@" + endpoint);
            }
            AbstractReplicationStrategy rs = Table.open(command.table).replicationStrategy;
            QuorumResponseHandler<Row> quorumResponseHandler = rs.getQuorumResponseHandler(new ReadResponseResolver(command.table), consistency_level);
            MessagingService.instance.sendRR(messages, endpoints, quorumResponseHandler);
            quorumResponseHandlers.add(quorumResponseHandler);
            commandEndpoints.add(endpoints);
        }

        // read results and make a second pass for any digest mismatches
        List<QuorumResponseHandler<Row>> repairResponseHandlers = null;
        for (int i = 0; i < commands.size(); i++)
        {
            QuorumResponseHandler<Row> quorumResponseHandler = quorumResponseHandlers.get(i);
            Row row;
            ReadCommand command = commands.get(i);
            try
            {
                long startTime2 = System.currentTimeMillis();
                row = quorumResponseHandler.get();
                if (row != null)
                    rows.add(row);

                if (logger.isDebugEnabled())
                    logger.debug("quorumResponseHandler: " + (System.currentTimeMillis() - startTime2) + " ms.");
            }
            catch (DigestMismatchException ex)
            {
                AbstractReplicationStrategy rs = Table.open(command.table).replicationStrategy;
                QuorumResponseHandler<Row> qrhRepair = rs.getQuorumResponseHandler(new ReadResponseResolver(command.table), ConsistencyLevel.QUORUM);
                if (logger.isDebugEnabled())
                    logger.debug("Digest mismatch:", ex);
                Message messageRepair = command.makeReadMessage();
                MessagingService.instance.sendRR(messageRepair, commandEndpoints.get(i), qrhRepair);
                if (repairResponseHandlers == null)
                    repairResponseHandlers = new ArrayList<QuorumResponseHandler<Row>>();
                repairResponseHandlers.add(qrhRepair);
            }
        }

        // read the results for the digest mismatch retries
        if (repairResponseHandlers != null)
        {
            for (QuorumResponseHandler<Row> handler : repairResponseHandlers)
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

    /*
    * This function executes the read protocol locally.  Consistency checks are performed in the background.
    */

    public static List<Row> getRangeSlice(RangeSliceCommand command, ConsistencyLevel consistency_level)
    throws IOException, UnavailableException, TimeoutException
    {
        if (logger.isDebugEnabled())
            logger.debug(command.toString());
        long startTime = System.nanoTime();

        List<AbstractBounds> ranges = getRestrictedRanges(command.range);
        // now scan until we have enough results
        List<Row> rows = new ArrayList<Row>(command.max_keys);
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
                Message message = c2.getMessage();

                // collect replies and resolve according to consistency level
                RangeSliceResponseResolver resolver = new RangeSliceResponseResolver(command.keyspace, liveEndpoints);
                AbstractReplicationStrategy rs = Table.open(command.keyspace).replicationStrategy;
                QuorumResponseHandler<List<Row>> handler = rs.getQuorumResponseHandler(resolver, consistency_level);
                // TODO bail early if live endpoints can't satisfy requested consistency level
                for (InetAddress endpoint : liveEndpoints) 
                {
                    MessagingService.instance.sendRR(message, endpoint, handler);
                    if (logger.isDebugEnabled())
                        logger.debug("reading " + c2 + " from " + message.getMessageId() + "@" + endpoint);
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

        rangeStats.addNano(System.nanoTime() - startTime);
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
        final Message msg = new Message(FBUtilities.getLocalAddress(), StorageService.Verb.SCHEMA_CHECK, ArrayUtils.EMPTY_BYTE_ARRAY);
        final CountDownLatch latch = new CountDownLatch(liveHosts.size());
        // an empty message acts as a request to the SchemaCheckVerbHandler.
        MessagingService.instance.sendRR(msg, liveHosts.toArray(new InetAddress[]{}), new IAsyncCallback() 
        {
            public void response(Message msg)
            {
                // record the response from the remote node.
                logger.debug("Received schema check response from " + msg.getFrom().getHostAddress());
                UUID theirVersion = UUID.fromString(new String(msg.getMessageBody()));
                versions.put(msg.getFrom(), theirVersion);
                latch.countDown();
            }
        });
        
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
            if (remainder == null || !remainder.contains(token))
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
        return cfmd.readRepairChance > random.nextDouble();
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

    public static List<Row> scan(String keyspace, String column_family, IndexClause index_clause, SlicePredicate column_predicate, ConsistencyLevel consistency_level)
    throws IOException, TimeoutException
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
            AbstractReplicationStrategy rs = Table.open(keyspace).replicationStrategy;
            QuorumResponseHandler<List<Row>> handler = rs.getQuorumResponseHandler(resolver, consistency_level);
            // TODO bail early if live endpoints can't satisfy requested consistency level
            IndexScanCommand command = new IndexScanCommand(keyspace, column_family, index_clause, column_predicate, range);
            Message message = command.getMessage();
            for (InetAddress endpoint : liveEndpoints)
            {
                MessagingService.instance.sendRR(message, endpoint, handler);
                if (logger.isDebugEnabled())
                    logger.debug("reading " + command + " from " + message.getMessageId() + "@" + endpoint);
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

    static class weakReadLocalCallable implements Callable<Object>
    {
        private ReadCommand command;

        weakReadLocalCallable(ReadCommand command)
        {
            this.command = command;
        }

        public Object call() throws IOException
        {
            if (logger.isDebugEnabled())
                logger.debug("weakreadlocal reading " + command);

            Table table = Table.open(command.table);
            Row row = command.getRow(table);

            // Do the consistency checks in the background
            if (randomlyReadRepair(command))
            {
                List<InetAddress> endpoints = StorageService.instance.getLiveNaturalEndpoints(command.table, command.key);
                if (endpoints.size() > 1)
                    StorageService.instance.doConsistencyCheck(row, endpoints, command);
            }

            return row;
        }
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
        Truncation truncation = new Truncation(keyspace, cfname);
        Message message = truncation.makeTruncationMessage();
        MessagingService.instance.sendRR(message, allEndpoints.toArray(new InetAddress[]{}), responseHandler);

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
}
