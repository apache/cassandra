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

import java.io.IOError;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.lang.management.ManagementFactory;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.AbstractIterator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.io.DataInputBuffer;
import java.net.InetAddress;
import org.apache.cassandra.net.IAsyncResult;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.TimedStatsDeque;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.concurrent.StageManager;

import org.apache.log4j.Logger;

import javax.management.MBeanServer;
import javax.management.ObjectName;


public class StorageProxy implements StorageProxyMBean
{
    private static Logger logger = Logger.getLogger(StorageProxy.class);

    // mbean stuff
    private static TimedStatsDeque readStats = new TimedStatsDeque(60000);
    private static TimedStatsDeque rangeStats = new TimedStatsDeque(60000);
    private static TimedStatsDeque writeStats = new TimedStatsDeque(60000);

    private StorageProxy() {}
    static
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(new StorageProxy(), new ObjectName("org.apache.cassandra.service:type=StorageProxy"));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private static Comparator<String> keyComparator = new Comparator<String>()
    {
        public int compare(String o1, String o2)
        {
            IPartitioner p = StorageService.getPartitioner();
            return p.decorateKey(o1).compareTo(p.decorateKey(o2));
        }
    };

    /**
     * Use this method to have this RowMutation applied
     * across all replicas. This method will take care
     * of the possibility of a replica being down and hint
     * the data across to some other replica.
     *
     * This is the ZERO consistency level. We do not wait for replies.
     *
     * @param rm the mutation to be applied across the replicas
    */
    public static void insert(final RowMutation rm)
    {
        long startTime = System.currentTimeMillis();
        try
        {
            List<InetAddress> naturalEndpoints = StorageService.instance().getNaturalEndpoints(rm.key());
            Map<InetAddress, InetAddress> endpointMap = StorageService.instance().getHintedEndpointMap(rm.key(), naturalEndpoints);
            Message unhintedMessage = null; // lazy initialize for non-local, unhinted writes

            // 3 cases:
            // 1. local, unhinted write: run directly on write stage
            // 2. non-local, unhinted write: send row mutation message
            // 3. hinted write: add hint header, and send message
            for (Map.Entry<InetAddress, InetAddress> entry : endpointMap.entrySet())
            {
                InetAddress target = entry.getKey();
                InetAddress hintedTarget = entry.getValue();
                if (target.equals(hintedTarget))
                {
                    if (target.equals(FBUtilities.getLocalAddress()))
                    {
                        if (logger.isDebugEnabled())
                            logger.debug("insert writing local key " + rm.key());
                        Runnable runnable = new Runnable()
                        {
                            public void run()
                            {
                                try
                                {
                                    rm.apply();
                                }
                                catch (IOException e)
                                {
                                    throw new IOError(e);
                                }
                            }
                        };
                        StageManager.getStage(StageManager.mutationStage_).execute(runnable);
                    }
                    else
                    {
                        if (unhintedMessage == null)
                            unhintedMessage = rm.makeRowMutationMessage();
                        if (logger.isDebugEnabled())
                            logger.debug("insert writing key " + rm.key() + " to " + unhintedMessage.getMessageId() + "@" + target);
                        MessagingService.instance().sendOneWay(unhintedMessage, target);
                    }
                }
                else
                {
                    Message hintedMessage = rm.makeRowMutationMessage();
                    hintedMessage.addHeader(RowMutation.HINT, target.getAddress());
                    if (logger.isDebugEnabled())
                        logger.debug("insert writing key " + rm.key() + " to " + hintedMessage.getMessageId() + "@" + hintedTarget + " for " + target);
                    MessagingService.instance().sendOneWay(hintedMessage, hintedTarget);
                }
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("error inserting key " + rm.key(), e);
        }
        finally
        {
            writeStats.add(System.currentTimeMillis() - startTime);
        }
    }
    
    public static void insertBlocking(final RowMutation rm, int consistency_level) throws UnavailableException, TimedOutException
    {
        long startTime = System.currentTimeMillis();
        try
        {
            List<InetAddress> naturalEndpoints = StorageService.instance().getNaturalEndpoints(rm.key());
            Map<InetAddress, InetAddress> endpointMap = StorageService.instance().getHintedEndpointMap(rm.key(), naturalEndpoints);
            int blockFor = determineBlockFor(naturalEndpoints.size(), endpointMap.size(), consistency_level);

            // avoid starting a write we know can't achieve the required consistency
            int liveNodes = 0;
            for (Map.Entry<InetAddress, InetAddress> entry : endpointMap.entrySet())
            {
                if (entry.getKey().equals(entry.getValue()))
                {
                    liveNodes++;
                }
            }
            if (liveNodes < blockFor)
            {
                throw new UnavailableException();
            }

            // send out the writes, as in insert() above, but this time with a callback that tracks responses
            final WriteResponseHandler responseHandler = StorageService.instance().getWriteResponseHandler(blockFor, consistency_level);
            Message unhintedMessage = null;
            for (Map.Entry<InetAddress, InetAddress> entry : endpointMap.entrySet())
            {
                InetAddress target = entry.getKey();
                InetAddress hintedTarget = entry.getValue();

                if (target.equals(hintedTarget))
                {
                    if (target.equals(FBUtilities.getLocalAddress()))
                    {
                        if (logger.isDebugEnabled())
                            logger.debug("insert writing local key " + rm.key());
                        Runnable runnable = new Runnable()
                        {
                            public void run()
                            {
                                try
                                {
                                    rm.apply();
                                    responseHandler.localResponse();
                                }
                                catch (IOException e)
                                {
                                    throw new IOError(e);
                                }
                            }
                        };
                        StageManager.getStage(StageManager.mutationStage_).execute(runnable);
                    }
                    else
                    {
                        if (unhintedMessage == null)
                        {
                            unhintedMessage = rm.makeRowMutationMessage();
                            MessagingService.instance().addCallback(responseHandler, unhintedMessage.getMessageId());
                        }
                        if (logger.isDebugEnabled())
                            logger.debug("insert writing key " + rm.key() + " to " + unhintedMessage.getMessageId() + "@" + target);
                        MessagingService.instance().sendOneWay(unhintedMessage, target);
                    }
                }
                else
                {
                    // (hints aren't part of the callback since they don't count towards consistency until they are on the final destination node)
                    Message hintedMessage = rm.makeRowMutationMessage();
                    hintedMessage.addHeader(RowMutation.HINT, target.getAddress());
                    if (logger.isDebugEnabled())
                        logger.debug("insert writing key " + rm.key() + " to " + hintedMessage.getMessageId() + "@" + hintedTarget + " for " + target);
                    MessagingService.instance().sendOneWay(hintedMessage, hintedTarget);
                }
            }

            // wait for writes.  throws timeoutexception if necessary
            responseHandler.get();
        }
        catch (TimeoutException e)
        {
            throw new TimedOutException();
        }
        catch (IOException e)
        {
            throw new RuntimeException("error writing key " + rm.key(), e);
        }
        finally
        {
            writeStats.add(System.currentTimeMillis() - startTime);
        }
    }

    private static int determineBlockFor(int naturalTargets, int hintedTargets, int consistency_level)
    {
        assert naturalTargets >= 1;
        assert hintedTargets >= naturalTargets;

        int bootstrapTargets = hintedTargets - naturalTargets;
        int blockFor;
        if (consistency_level == ConsistencyLevel.ONE)
        {
            blockFor = 1 + bootstrapTargets;
        }
        else if (consistency_level == ConsistencyLevel.QUORUM)
        {
            blockFor = (naturalTargets / 2) + 1 + bootstrapTargets;
        }
        else if (consistency_level == ConsistencyLevel.DCQUORUM || consistency_level == ConsistencyLevel.DCQUORUMSYNC)
        {
            // TODO this is broken
            blockFor = naturalTargets;
        }
        else if (consistency_level == ConsistencyLevel.ALL)
        {
            blockFor = naturalTargets + bootstrapTargets;
        }
        else
        {
            throw new UnsupportedOperationException("invalid consistency level " + consistency_level);
        }
        return blockFor;
    }    

    /**
     * Read the data from one replica.  When we get
     * the data we perform consistency checks and figure out if any repairs need to be done to the replicas.
     * @param commands a set of commands to perform reads
     * @return the row associated with command.key
     * @throws Exception
     */
    private static List<Row> weakReadRemote(List<ReadCommand> commands) throws IOException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("weakreadremote reading " + StringUtils.join(commands, ", "));

        List<Row> rows = new ArrayList<Row>();
        List<IAsyncResult> iars = new ArrayList<IAsyncResult>();

        for (ReadCommand command: commands)
        {
            InetAddress endPoint = StorageService.instance().findSuitableEndPoint(command.key);
            Message message = command.makeReadMessage();

            if (logger.isDebugEnabled())
                logger.debug("weakreadremote reading " + command + " from " + message.getMessageId() + "@" + endPoint);
            message.addHeader(ReadCommand.DO_REPAIR, ReadCommand.DO_REPAIR.getBytes());
            iars.add(MessagingService.instance().sendRR(message, endPoint));
        }

        for (IAsyncResult iar: iars)
        {
            byte[] body;
            try
            {
                body = iar.get(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
            }
            catch (TimeoutException e)
            {
                throw new TimedOutException();
            }
            DataInputBuffer bufIn = new DataInputBuffer();
            bufIn.reset(body, body.length);
            ReadResponse response = ReadResponse.serializer().deserialize(bufIn);
            if (response.row() != null)
                rows.add(response.row());
        }
        return rows;
    }

    /**
     * Performs the actual reading of a row out of the StorageService, fetching
     * a specific set of column names from a given column family.
     */
    public static List<Row> readProtocol(List<ReadCommand> commands, int consistency_level)
            throws IOException, UnavailableException, TimedOutException
    {
        long startTime = System.currentTimeMillis();

        List<Row> rows = new ArrayList<Row>();

        if (consistency_level == ConsistencyLevel.ONE)
        {
            List<ReadCommand> localCommands = new ArrayList<ReadCommand>();
            List<ReadCommand> remoteCommands = new ArrayList<ReadCommand>();

            for (ReadCommand command: commands)
            {
                List<InetAddress> endpoints = StorageService.instance().getNaturalEndpoints(command.key);
                boolean foundLocal = endpoints.contains(FBUtilities.getLocalAddress());
                //TODO: Throw InvalidRequest if we're in bootstrap mode?
                if (foundLocal && !StorageService.instance().isBootstrapMode())
                {
                    localCommands.add(command);
                }
                else
                {
                    remoteCommands.add(command);
                }
            }
            if (localCommands.size() > 0)
                rows.addAll(weakReadLocal(localCommands));

            if (remoteCommands.size() > 0)
                rows.addAll(weakReadRemote(remoteCommands));
        }
        else
        {
            assert consistency_level >= ConsistencyLevel.QUORUM;
            rows = strongRead(commands, consistency_level);
        }

        readStats.add(System.currentTimeMillis() - startTime);

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
    private static List<Row> strongRead(List<ReadCommand> commands, int consistency_level) throws IOException, UnavailableException, TimedOutException
    {
        List<QuorumResponseHandler<Row>> quorumResponseHandlers = new ArrayList<QuorumResponseHandler<Row>>();
        List<InetAddress[]> commandEndPoints = new ArrayList<InetAddress[]>();
        List<Row> rows = new ArrayList<Row>();

        int responseCount = determineBlockFor(DatabaseDescriptor.getReplicationFactor(), DatabaseDescriptor.getReplicationFactor(), consistency_level);
        int commandIndex = 0;

        for (ReadCommand command: commands)
        {
            assert !command.isDigestQuery();
            ReadCommand readMessageDigestOnly = command.copy();
            readMessageDigestOnly.setDigestQuery(true);
            Message message = command.makeReadMessage();
            Message messageDigestOnly = readMessageDigestOnly.makeReadMessage();

            InetAddress dataPoint = StorageService.instance().findSuitableEndPoint(command.key);
            List<InetAddress> endpointList = StorageService.instance().getLiveNaturalEndpoints(command.key);
            if (endpointList.size() < responseCount)
                throw new UnavailableException();

            InetAddress[] endPoints = new InetAddress[endpointList.size()];
            Message messages[] = new Message[endpointList.size()];
            // data-request message is sent to dataPoint, the node that will actually get
            // the data for us. The other replicas are only sent a digest query.
            int n = 0;
            for (InetAddress endpoint : endpointList)
            {
                Message m = endpoint.equals(dataPoint) ? message : messageDigestOnly;
                endPoints[n] = endpoint;
                messages[n++] = m;
                if (logger.isDebugEnabled())
                    logger.debug("strongread reading " + (m == message ? "data" : "digest") + " for " + command + " from " + m.getMessageId() + "@" + endpoint);
            }
            QuorumResponseHandler<Row> quorumResponseHandler = new QuorumResponseHandler<Row>(DatabaseDescriptor.getQuorum(), new ReadResponseResolver(command.table, responseCount));
            MessagingService.instance().sendRR(messages, endPoints, quorumResponseHandler);
            quorumResponseHandlers.add(quorumResponseHandler);
            commandEndPoints.add(endPoints);
        }

        for (QuorumResponseHandler<Row> quorumResponseHandler: quorumResponseHandlers)
        {
            Row row;
            ReadCommand command = commands.get(commandIndex);
            try
            {
                long startTime2 = System.currentTimeMillis();
                row = quorumResponseHandler.get();
                if (row != null)
                    rows.add(row);

                if (logger.isDebugEnabled())
                    logger.debug("quorumResponseHandler: " + (System.currentTimeMillis() - startTime2) + " ms.");
            }
            catch (TimeoutException e)
            {
                throw new TimedOutException();
            }
            catch (DigestMismatchException ex)
            {
                if (DatabaseDescriptor.getConsistencyCheck())
                {
                    IResponseResolver<Row> readResponseResolverRepair = new ReadResponseResolver(command.table, DatabaseDescriptor.getQuorum());
                    QuorumResponseHandler<Row> quorumResponseHandlerRepair = new QuorumResponseHandler<Row>(
                            DatabaseDescriptor.getQuorum(),
                            readResponseResolverRepair);
                    logger.info("DigestMismatchException: " + ex.getMessage());
                    Message messageRepair = command.makeReadMessage();
                    MessagingService.instance().sendRR(messageRepair, commandEndPoints.get(commandIndex), quorumResponseHandlerRepair);
                    try
                    {
                        row = quorumResponseHandlerRepair.get();
                        if (row != null)
                            rows.add(row);
                    }
                    catch (TimeoutException e)
                    {
                        throw new TimedOutException();
                    }
                    catch (DigestMismatchException e)
                    {
                        // TODO should this be a thrift exception?
                        throw new RuntimeException("digest mismatch reading key " + command.key, e);
                    }
                }
            }
            commandIndex++;
        }

        return rows;
    }

    /*
    * This function executes the read protocol locally.  Consistency checks are performed in the background.
    */
    private static List<Row> weakReadLocal(List<ReadCommand> commands)
    {
        List<Row> rows = new ArrayList<Row>();
        List<Future<Object>> futures = new ArrayList<Future<Object>>();

        for (ReadCommand command: commands)
        {
            Callable<Object> callable = new weakReadLocalCallable(command);
            futures.add(StageManager.getStage(StageManager.readStage_).execute(callable));
        }
        for (Future<Object> future : futures)
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
        return rows;
    }

    public static List<Row> getRangeSlice(RangeSliceCommand command, int consistency_level)
    throws IOException, UnavailableException, TimeoutException
    {
        if (logger.isDebugEnabled())
            logger.debug(command);
        long startTime = System.currentTimeMillis();

        int responseCount = determineBlockFor(DatabaseDescriptor.getReplicationFactor(), DatabaseDescriptor.getReplicationFactor(), consistency_level);

        List<Pair<AbstractBounds, List<InetAddress>>> ranges = getRestrictedRanges(command.range, command.keyspace, responseCount);

        // now scan until we have enough results
        List<Row> rows = new ArrayList<Row>(command.max_keys);
        for (Pair<AbstractBounds, List<InetAddress>> pair : getRangeIterator(ranges, command.range.left))
        {
            AbstractBounds range = pair.left;
            List<InetAddress> endpoints = pair.right;
            RangeSliceCommand c2 = new RangeSliceCommand(command.keyspace, command.column_family, command.super_column, command.predicate, range, command.max_keys);
            Message message = c2.getMessage();

            // collect replies and resolve according to consistency level
            RangeSliceResponseResolver resolver = new RangeSliceResponseResolver(command.keyspace, endpoints);
            QuorumResponseHandler<List<Row>> handler = new QuorumResponseHandler<List<Row>>(responseCount, resolver);

            for (InetAddress endpoint : endpoints)
            {
                MessagingService.instance().sendRR(message, endpoint, handler);
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
            if (rows.size() >= command.max_keys)
                break;
        }

        rangeStats.add(System.currentTimeMillis() - startTime);
        return rows.size() > command.max_keys ? rows.subList(0, command.max_keys) : rows;
    }

    /**
     * returns an iterator that will return ranges in ring order, starting with the one that contains the start token
     */
    private static Iterable<Pair<AbstractBounds, List<InetAddress>>> getRangeIterator(final List<Pair<AbstractBounds, List<InetAddress>>> ranges, Token start)
    {
        // sort ranges in ring order
        Comparator<Pair<AbstractBounds, List<InetAddress>>> comparator = new Comparator<Pair<AbstractBounds, List<InetAddress>>>()
        {
            public int compare(Pair<AbstractBounds, List<InetAddress>> o1, Pair<AbstractBounds, List<InetAddress>> o2)
            {
                // no restricted ranges will overlap so we don't need to worry about inclusive vs exclusive left,
                // just sort by raw token position.
                return o1.left.left.compareTo(o2.left.left);
            }
        };
        Collections.sort(ranges, comparator);

        // find the one to start with
        int i;
        for (i = 0; i < ranges.size(); i++)
        {
            AbstractBounds range = ranges.get(i).left;
            if (range.contains(start) || range.left.equals(start))
                break;
        }
        AbstractBounds range = ranges.get(i).left;
        assert range.contains(start) || range.left.equals(start); // make sure the loop didn't just end b/c ranges were exhausted

        // return an iterable that starts w/ the correct range and iterates the rest in ring order
        final int begin = i;
        return new Iterable<Pair<AbstractBounds, List<InetAddress>>>()
        {
            public Iterator<Pair<AbstractBounds, List<InetAddress>>> iterator()
            {
                return new AbstractIterator<Pair<AbstractBounds, List<InetAddress>>>()
                {
                    int n = 0;

                    protected Pair<AbstractBounds, List<InetAddress>> computeNext()
                    {
                        if (n == ranges.size())
                            return endOfData();
                        return ranges.get((begin + n++) % ranges.size());
                    }
                };
            }
        };
    }

    /**
     * compute all ranges we're going to query, in sorted order, so that we get the correct results back.
     *  1) computing range intersections is necessary because nodes can be replica destinations for many ranges,
     *     so if we do not restrict each scan to the specific range we want we will get duplicate results.
     *  2) sorting the intersection ranges is necessary because wraparound node ranges can be discontiguous.
     *     Consider a 2-node ring, (D, T] and (T, D]. A query for [A, Z] will intersect the 2nd node twice,
     *     at [A, D] and (T, Z]. We need to scan the (D, T] range in between those, or we will skip those
     *     results entirely if the limit is low enough.
     *  3) we unwrap the intersection ranges because otherwise we get results in the wrong order.
     *     Consider a 2-node ring, (D, T] and (T, D].  A query for [D, Z] will get results in the wrong
     *     order if we use (T, D] directly -- we need to start with that range, because our query starts with
     *     D, but we don't want any other results from it until after the (D, T] range.  Unwrapping so that
     *     the ranges we consider are (D, T], (T, MIN], (MIN, D] fixes this.
     */
    private static List<Pair<AbstractBounds, List<InetAddress>>> getRestrictedRanges(AbstractBounds queryRange, String keyspace, int responseCount)
    throws UnavailableException
    {
        TokenMetadata tokenMetadata = StorageService.instance().getTokenMetadata();
        Iterator<Token> iter = TokenMetadata.ringIterator(tokenMetadata.sortedTokens(), queryRange.left);
        List<Pair<AbstractBounds, List<InetAddress>>> ranges = new ArrayList<Pair<AbstractBounds, List<InetAddress>>>();
        while (iter.hasNext())
        {
            Token nodeToken = iter.next();
            Range nodeRange = new Range(tokenMetadata.getPredecessor(nodeToken), nodeToken);
            List<InetAddress> endpoints = StorageService.instance().getLiveNaturalEndpoints(nodeToken);
            if (endpoints.size() < responseCount)
                throw new UnavailableException();

            DatabaseDescriptor.getEndPointSnitch().sortByProximity(FBUtilities.getLocalAddress(), endpoints);
            List<InetAddress> endpointsForCL = endpoints.subList(0, responseCount);
            Set<AbstractBounds> restrictedRanges = queryRange.restrictTo(nodeRange);
            for (AbstractBounds range : restrictedRanges)
            {
                for (AbstractBounds unwrapped : range.unwrap())
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Adding to restricted ranges " + unwrapped + " for " + nodeRange);
                    ranges.add(new Pair<AbstractBounds, List<InetAddress>>(unwrapped, endpointsForCL));
                }
            }
        }
        return ranges;
    }

    static List<String> getKeyRange(RangeCommand command) throws IOException, UnavailableException, TimedOutException
    {
        long startTime = System.currentTimeMillis();
        TokenMetadata tokenMetadata = StorageService.instance().getTokenMetadata();
        Set<String> uniqueKeys = new HashSet<String>(command.maxResults);

        InetAddress endPoint = StorageService.instance().findSuitableEndPoint(command.startWith);
        InetAddress startEndpoint = endPoint;

        do
        {
            Message message = command.getMessage();
            if (logger.isDebugEnabled())
                logger.debug("reading " + command + " from " + message.getMessageId() + "@" + endPoint);
            IAsyncResult iar = MessagingService.instance().sendRR(message, endPoint);

            // read response
            byte[] responseBody;
            try
            {
                responseBody = iar.get(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
            }
            catch (TimeoutException e)
            {
                throw new TimedOutException();
            }
            RangeReply rangeReply = RangeReply.read(responseBody);
            uniqueKeys.addAll(rangeReply.keys);

            if (uniqueKeys.size() >= command.maxResults || rangeReply.rangeCompletedLocally)
            {
                break;
            }

            // set up the next query --
            // it's tempting to try to optimize this by starting with the last key seen for the next node,
            // but that won't work when you have a replication factor of more than one--any node, not just
            // the one holding the keys where the range wraps, could include both the smallest keys, and the largest,
            // so starting with the largest in our scan of the next node means we'd never see keys from the middle.
            do
            {
                endPoint = tokenMetadata.getSuccessor(endPoint);
            } while (!FailureDetector.instance().isAlive(endPoint));
        } while (!endPoint.equals(startEndpoint));

        rangeStats.add(System.currentTimeMillis() - startTime);
        List<String> allKeys = new ArrayList<String>(uniqueKeys);
        Collections.sort(allKeys, keyComparator);
        return (allKeys.size() > command.maxResults)
               ? allKeys.subList(0, command.maxResults)
               : allKeys;
    }

    public double getReadLatency()
    {
        return readStats.mean();
    }

    public double getRangeLatency()
    {
        return rangeStats.mean();
    }

    public double getWriteLatency()
    {
        return writeStats.mean();
    }

    public int getReadOperations()
    {
        return readStats.size();
    }

    public int getRangeOperations()
    {
        return rangeStats.size();
    }

    public int getWriteOperations()
    {
        return writeStats.size();
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
            List<InetAddress> endpoints = StorageService.instance().getLiveNaturalEndpoints(command.key);
            /* Remove the local storage endpoint from the list. */
            endpoints.remove(FBUtilities.getLocalAddress());

            if (logger.isDebugEnabled())
                logger.debug("weakreadlocal reading " + command);

            Table table = Table.open(command.table);
            Row row = command.getRow(table);

            // Do the consistency checks in the background and return the non NULL row
            if (endpoints.size() > 0 && DatabaseDescriptor.getConsistencyCheck())
                StorageService.instance().doConsistencyCheck(row, endpoints, command);

            return row;
        }
    }
}
