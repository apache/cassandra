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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.lang.management.ManagementFactory;

import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.IAsyncResult;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.TimedStatsDeque;
import org.apache.cassandra.locator.TokenMetadata;

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

    /**
     * This method is responsible for creating Message to be
     * sent over the wire to N replicas where some of the replicas
     * may be hints.
     */
    private static Map<EndPoint, Message> createWriteMessages(RowMutation rm, Map<EndPoint, EndPoint> endpointMap) throws IOException
    {
		Map<EndPoint, Message> messageMap = new HashMap<EndPoint, Message>();
		Message message = rm.makeRowMutationMessage();

		for (Map.Entry<EndPoint, EndPoint> entry : endpointMap.entrySet())
		{
            EndPoint target = entry.getKey();
            EndPoint hint = entry.getValue();
            if ( !target.equals(hint) )
			{
				Message hintedMessage = rm.makeRowMutationMessage();
				hintedMessage.addHeader(RowMutation.HINT, EndPoint.toBytes(hint) );
				if (logger.isDebugEnabled())
				    logger.debug("Sending the hint of " + hint.getHost() + " to " + target.getHost());
				messageMap.put(target, hintedMessage);
			}
			else
			{
				messageMap.put(target, message);
			}
		}
		return messageMap;
    }
    
    /**
     * Use this method to have this RowMutation applied
     * across all replicas. This method will take care
     * of the possibility of a replica being down and hint
     * the data across to some other replica. 
     * @param rm the mutation to be applied across the replicas
    */
    public static void insert(RowMutation rm)
	{
        /*
         * Get the N nodes from storage service where the data needs to be
         * replicated
         * Construct a message for write
         * Send them asynchronously to the replicas.
        */

        long startTime = System.currentTimeMillis();
		try
		{
            // (This is the ZERO consistency level, so user doesn't care if we don't really have N destinations available.)
			Map<EndPoint, EndPoint> endpointMap = StorageService.instance().getHintedStorageEndpointMap(rm.key());
			Map<EndPoint, Message> messageMap = createWriteMessages(rm, endpointMap);
			for (Map.Entry<EndPoint, Message> entry : messageMap.entrySet())
			{
                Message message = entry.getValue();
                EndPoint endpoint = entry.getKey();
                if (logger.isDebugEnabled())
                    logger.debug("insert writing key " + rm.key() + " to " + message.getMessageId() + "@" + endpoint);
                MessagingService.getMessagingInstance().sendOneWay(message, endpoint);
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
    
    public static void insertBlocking(RowMutation rm, int consistency_level) throws UnavailableException
    {
        long startTime = System.currentTimeMillis();
        Message message = null;
        try
        {
            message = rm.makeRowMutationMessage();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        try
        {
            Map<EndPoint, EndPoint> endpointMap = StorageService.instance().getHintedStorageEndpointMap(rm.key());
            int blockFor = determineBlockFor(consistency_level);
            List<EndPoint> primaryNodes = getUnhintedNodes(endpointMap);
            if (primaryNodes.size() < blockFor) // guarantee blockFor = W live nodes.
            {
                throw new UnavailableException();
            }
            QuorumResponseHandler<Boolean> quorumResponseHandler = new QuorumResponseHandler<Boolean>(blockFor, new WriteResponseResolver());
            if (logger.isDebugEnabled())
                logger.debug("insertBlocking writing key " + rm.key() + " to " + message.getMessageId() + "@[" + StringUtils.join(endpointMap.keySet(), ", ") + "]");

            // Get all the targets and stick them in an array
            MessagingService.getMessagingInstance().sendRR(message, primaryNodes.toArray(new EndPoint[primaryNodes.size()]), quorumResponseHandler);
            if (!quorumResponseHandler.get())
                throw new UnavailableException();
            if (primaryNodes.size() < endpointMap.size()) // Do we need to bother with Hinted Handoff?
            {
                for (Map.Entry<EndPoint, EndPoint> e : endpointMap.entrySet())
                {
                    if (e.getKey() != e.getValue()) // Hinted Handoff to target
                    {
                        MessagingService.getMessagingInstance().sendOneWay(message, e.getKey());
                    }
                }
            }
        }
        catch (Exception e)
        {
            logger.error("error writing key " + rm.key(), e);
            throw new UnavailableException();
        }
        finally
        {
            writeStats.add(System.currentTimeMillis() - startTime);
        }
    }

    private static List<EndPoint> getUnhintedNodes(Map<EndPoint, EndPoint> endpointMap)
    {
        List<EndPoint> liveEndPoints = new ArrayList<EndPoint>(endpointMap.size());
        for (Map.Entry<EndPoint, EndPoint> e : endpointMap.entrySet())
        {
            if (e.getKey() == e.getValue())
            {
                liveEndPoints.add(e.getKey());
            }
        }
        return liveEndPoints;
    }

    private static int determineBlockFor(int consistency_level)
    {
        int blockFor;
        if (consistency_level == ConsistencyLevel.ONE)
        {
            blockFor = 1;
        }
        else if (consistency_level == ConsistencyLevel.QUORUM)
        {
            blockFor = (DatabaseDescriptor.getReplicationFactor() / 2) + 1;
        }
        else if (consistency_level == ConsistencyLevel.ALL)
        {
            blockFor = DatabaseDescriptor.getReplicationFactor();
        }
        else
        {
            throw new UnsupportedOperationException("invalid consistency level " + consistency_level);
        }
        return blockFor;
    }

    public static void insertBlocking(RowMutation rm) throws UnavailableException
    {
        insertBlocking(rm, ConsistencyLevel.QUORUM);
    }
    

    /**
     * Read the data from one replica.  If there is no reply, read the data from another.  In the event we get
     * the data we perform consistency checks and figure out if any repairs need to be done to the replicas.
     * @param commands a set of commands to perform reads
     * @return the row associated with command.key
     * @throws Exception
     */
    private static List<Row> weakReadRemote(List<ReadCommand> commands) throws IOException, UnavailableException
    {
        if (logger.isDebugEnabled())
            logger.debug("weakreadlocal reading " + StringUtils.join(commands, ", "));

        List<Row> rows = new ArrayList<Row>();
        List<IAsyncResult> iars = new ArrayList<IAsyncResult>();
        int commandIndex = 0;

        for (ReadCommand command: commands)
        {
            EndPoint endPoint = StorageService.instance().findSuitableEndPoint(command.key);
            Message message = command.makeReadMessage();

            if (logger.isDebugEnabled())
                logger.debug("weakreadremote reading " + command + " from " + message.getMessageId() + "@" + endPoint);
            message.addHeader(ReadCommand.DO_REPAIR, ReadCommand.DO_REPAIR.getBytes());
            iars.add(MessagingService.getMessagingInstance().sendRR(message, endPoint));
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
                throw new RuntimeException("error reading key " + commands.get(commandIndex).key, e);
                // TODO retry to a different endpoint?
            }
            DataInputBuffer bufIn = new DataInputBuffer();
            bufIn.reset(body, body.length);
            ReadResponse response = ReadResponse.serializer().deserialize(bufIn);
            if (response.row() != null)
                rows.add(response.row());
            commandIndex++;
        }
        return rows;
    }

    /**
     * Performs the actual reading of a row out of the StorageService, fetching
     * a specific set of column names from a given column family.
     */
    public static List<Row> readProtocol(List<ReadCommand> commands, int consistency_level)
    throws IOException, TimeoutException, InvalidRequestException, UnavailableException
    {
        long startTime = System.currentTimeMillis();

        List<Row> rows = new ArrayList<Row>();

        if (consistency_level == ConsistencyLevel.ONE)
        {
            List<ReadCommand> localCommands = new ArrayList<ReadCommand>();
            List<ReadCommand> remoteCommands = new ArrayList<ReadCommand>();

            for (ReadCommand command: commands)
            {
                EndPoint[] endpoints = StorageService.instance().getReadStorageEndPoints(command.key);
                boolean foundLocal = Arrays.asList(endpoints).contains(StorageService.getLocalStorageEndPoint());
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
            assert consistency_level == ConsistencyLevel.QUORUM;
            rows = strongRead(commands);
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
    private static List<Row> strongRead(List<ReadCommand> commands) throws IOException, TimeoutException, InvalidRequestException, UnavailableException
    {
        List<QuorumResponseHandler<Row>> quorumResponseHandlers = new ArrayList<QuorumResponseHandler<Row>>();
        List<EndPoint[]> commandEndPoints = new ArrayList<EndPoint[]>();
        List<Row> rows = new ArrayList<Row>();

        int commandIndex = 0;

        for (ReadCommand command: commands)
        {
            // TODO: throw a thrift exception if we do not have N nodes
            assert !command.isDigestQuery();
            ReadCommand readMessageDigestOnly = command.copy();
            readMessageDigestOnly.setDigestQuery(true);

            Message message = command.makeReadMessage();
            Message messageDigestOnly = readMessageDigestOnly.makeReadMessage();

            IResponseResolver<Row> readResponseResolver = new ReadResponseResolver();
            QuorumResponseHandler<Row> quorumResponseHandler = new QuorumResponseHandler<Row>(
                    DatabaseDescriptor.getQuorum(),
                    readResponseResolver);
            EndPoint dataPoint = StorageService.instance().findSuitableEndPoint(command.key);
            List<EndPoint> endpointList = new ArrayList<EndPoint>(Arrays.asList(StorageService.instance().getReadStorageEndPoints(command.key)));
            /* Remove the local storage endpoint from the list. */
            endpointList.remove(dataPoint);
            EndPoint[] endPoints = new EndPoint[endpointList.size() + 1];
            Message messages[] = new Message[endpointList.size() + 1];

            /*
             * First message is sent to the node that will actually get
             * the data for us. The other two replicas are only sent a
             * digest query.
            */
            endPoints[0] = dataPoint;
            messages[0] = message;
            if (logger.isDebugEnabled())
                logger.debug("strongread reading data for " + command + " from " + message.getMessageId() + "@" + dataPoint);
            for (int i = 1; i < endPoints.length; i++)
            {
                EndPoint digestPoint = endpointList.get(i - 1);
                endPoints[i] = digestPoint;
                messages[i] = messageDigestOnly;
                if (logger.isDebugEnabled())
                    logger.debug("strongread reading digest for " + command + " from " + messageDigestOnly.getMessageId() + "@" + digestPoint);
            }
            MessagingService.getMessagingInstance().sendRR(messages, endPoints, quorumResponseHandler);
            quorumResponseHandlers.add(quorumResponseHandler);
            commandEndPoints.add(endPoints);
        }

        for (QuorumResponseHandler<Row> quorumResponseHandler: quorumResponseHandlers)
        {
            Row row = null;
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
            catch (DigestMismatchException ex)
            {
                if ( DatabaseDescriptor.getConsistencyCheck())
                {
                    IResponseResolver<Row> readResponseResolverRepair = new ReadResponseResolver();
                    QuorumResponseHandler<Row> quorumResponseHandlerRepair = new QuorumResponseHandler<Row>(
                            DatabaseDescriptor.getQuorum(),
                            readResponseResolverRepair);
                    logger.info("DigestMismatchException: " + command.key);
                    Message messageRepair = command.makeReadMessage();
                    MessagingService.getMessagingInstance().sendRR(messageRepair, commandEndPoints.get(commandIndex),
                            quorumResponseHandlerRepair);
                    try
                    {
                        row = quorumResponseHandlerRepair.get();
                        if (row != null)
                            rows.add(row);
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

    private static Map<String, Message[]> constructReplicaMessages(Map<String, ReadCommand[]> readMessages) throws IOException
    {
        Map<String, Message[]> messages = new HashMap<String, Message[]>();
        Set<String> keys = readMessages.keySet();
        
        for ( String key : keys )
        {
            Message[] msg = new Message[DatabaseDescriptor.getReplicationFactor()];
            ReadCommand[] readParameters = readMessages.get(key);
            msg[0] = readParameters[0].makeReadMessage();
            for ( int i = 1; i < msg.length; ++i )
            {
                msg[i] = readParameters[1].makeReadMessage();
            }
        }        
        return messages;
    }

    /*
    * This function executes the read protocol locally and should be used only if consistency is not a concern.
    * Read the data from the local disk and return if the row is NOT NULL. If the data is NULL do the read from
    * one of the other replicas (in the same data center if possible) till we get the data. In the event we get
    * the data we perform consistency checks and figure out if any repairs need to be done to the replicas.
    */
    private static List<Row> weakReadLocal(List<ReadCommand> commands) throws IOException
    {
        List<Row> rows = new ArrayList<Row>();
        for (ReadCommand command: commands)
        {
            List<EndPoint> endpoints = StorageService.instance().getLiveReadStorageEndPoints(command.key);
            /* Remove the local storage endpoint from the list. */
            endpoints.remove(StorageService.getLocalStorageEndPoint());
            // TODO: throw a thrift exception if we do not have N nodes

            if (logger.isDebugEnabled())
                logger.debug("weakreadlocal reading " + command);

            Table table = Table.open(command.table);
            Row row = command.getRow(table);
            if (row != null)
                rows.add(row);
            /*
            * Do the consistency checks in the background and return the
            * non NULL row.
            */
            if (endpoints.size() > 0 && DatabaseDescriptor.getConsistencyCheck())
                StorageService.instance().doConsistencyCheck(row, endpoints, command);

        }

        return rows;
    }

    static List<String> getKeyRange(RangeCommand rawCommand) throws IOException, UnavailableException
    {
        long startTime = System.currentTimeMillis();
        Comparator<String> comparator = StorageService.getPartitioner().getDecoratedKeyComparator();
        TokenMetadata tokenMetadata = StorageService.instance().getTokenMetadata();
        List<String> allKeys = new ArrayList<String>();
        RangeCommand command = rawCommand;

        EndPoint endPoint = StorageService.instance().findSuitableEndPoint(command.startWith);
        EndPoint startEndpoint = endPoint;
        EndPoint wrapEndpoint = tokenMetadata.getFirstEndpoint();

        do
        {
            Message message = command.getMessage();
            if (logger.isDebugEnabled())
                logger.debug("reading " + command + " from " + message.getMessageId() + "@" + endPoint);
            IAsyncResult iar = MessagingService.getMessagingInstance().sendRR(message, endPoint);

            // read response
            byte[] responseBody;
            try
            {
                responseBody = iar.get(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
            }
            catch (TimeoutException e)
            {
                throw new RuntimeException(e);
            }
            RangeReply rangeReply = RangeReply.read(responseBody);
            List<String> rangeKeys = rangeReply.keys;

            // combine keys from most recent response with the others seen so far
            if (rangeKeys.size() > 0)
            {
                if (allKeys.size() > 0)
                {
                    if (comparator.compare(rangeKeys.get(rangeKeys.size() - 1), allKeys.get(0)) <= 0)
                    {
                        // unlikely, but possible
                        if (rangeKeys.get(rangeKeys.size() - 1).equals(allKeys.get(0)))
                        {
                            rangeKeys.remove(rangeKeys.size() - 1);
                        }
                        rangeKeys.addAll(allKeys);
                        allKeys = rangeKeys;
                    }
                    else if (comparator.compare(allKeys.get(allKeys.size() - 1), rangeKeys.get(0)) <= 0)
                    {
                        // common case. deal with simple start/end key overlaps
                        if (allKeys.get(allKeys.size() - 1).equals(rangeKeys.get(0)))
                        {
                            allKeys.remove(allKeys.size() - 1);
                        }
                        allKeys.addAll(rangeKeys);
                    }
                    else
                    {
                        // deal with potential large overlap from scanning the first endpoint, which contains
                        // both the smallest and largest keys
                        HashSet<String> keys = new HashSet<String>(allKeys);
                        keys.addAll(rangeKeys);
                        allKeys = new ArrayList<String>(keys);
                        Collections.sort(allKeys);
                    }
                }
                else
                {
                    allKeys = rangeKeys;
                }
            }

            if (allKeys.size() >= rawCommand.maxResults || rangeReply.rangeCompletedLocally)
            {
                break;
            }

            // set up the next query --
            // it's tempting to try to optimize this by starting with the last key seen for the next node,
            // but that won't work when you have a replication factor of more than one--any node, not just
            // the one holding the keys where the range wraps, could include both the smallest keys, and the largest,
            // so starting with the largest in our scan of the next node means we'd never see keys from the middle.
            endPoint = tokenMetadata.getNextEndpoint(endPoint); // TODO move this into the Strategies & modify for RackAwareStrategy
            int maxResults = endPoint == wrapEndpoint ? rawCommand.maxResults : rawCommand.maxResults - allKeys.size();
            command = new RangeCommand(command.table, command.columnFamily, command.startWith, command.stopAt, maxResults);
        } while (!endPoint.equals(startEndpoint));

        rangeStats.add(System.currentTimeMillis() - startTime);
        return (allKeys.size() > rawCommand.maxResults)
               ? allKeys.subList(0, rawCommand.maxResults)
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
}
