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
import org.apache.cassandra.dht.Token;
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
			Map<EndPoint, EndPoint> endpointMap = StorageService.instance().getNStorageEndPointMap(rm.key());
			// TODO: throw a thrift exception if we do not have N nodes
			Map<EndPoint, Message> messageMap = createWriteMessages(rm, endpointMap);
			for (Map.Entry<EndPoint, Message> entry : messageMap.entrySet())
			{
                Message message = entry.getValue();
                EndPoint endpoint = entry.getKey();
                // Check if local and not hinted
                byte[] hintedBytes = message.getHeader(RowMutation.HINT);
                if (endpoint.equals(StorageService.getLocalStorageEndPoint())
                        && !(hintedBytes!= null && hintedBytes.length>0))
                {
                    if (logger.isDebugEnabled())
                        logger.debug("locally writing writing key " + rm.key()
                                + " to " + endpoint);
                    rm.apply();
                } else
                {
                    if (logger.isDebugEnabled())
                        logger.debug("insert writing key " + rm.key() + " to "
                                + message.getMessageId() + "@" + endpoint);
                	MessagingService.getMessagingInstance().sendOneWay(message, endpoint);
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
            EndPoint[] endpoints = StorageService.instance().getNStorageEndPoint(rm.key());
            if (endpoints.length < (DatabaseDescriptor.getReplicationFactor() / 2) + 1)
            {
                throw new UnavailableException();
            }
            int blockFor;
            if (consistency_level == ConsistencyLevel.ONE)
            {
                blockFor = 1;
            }
            else if (consistency_level == ConsistencyLevel.QUORUM)
            {
                blockFor = (DatabaseDescriptor.getReplicationFactor() >> 1) + 1;
            }
            else if (consistency_level == ConsistencyLevel.ALL)
            {
                blockFor = DatabaseDescriptor.getReplicationFactor();
            }
            else
            {
                throw new UnsupportedOperationException("invalid consistency level " + consistency_level);
            }
            QuorumResponseHandler<Boolean> quorumResponseHandler = new QuorumResponseHandler<Boolean>(blockFor, new WriteResponseResolver());
            if (logger.isDebugEnabled())
                logger.debug("insertBlocking writing key " + rm.key() + " to " + message.getMessageId() + "@[" + StringUtils.join(endpoints, ", ") + "]");

            MessagingService.getMessagingInstance().sendRR(message, endpoints, quorumResponseHandler);
            if (!quorumResponseHandler.get())
                throw new UnavailableException();
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

    public static void insertBlocking(RowMutation rm) throws UnavailableException
    {
        insertBlocking(rm, ConsistencyLevel.QUORUM);
    }
    
    private static Map<String, Message> constructMessages(Map<String, ReadCommand> readMessages) throws IOException
    {
        Map<String, Message> messages = new HashMap<String, Message>();
        Set<String> keys = readMessages.keySet();        
        for ( String key : keys )
        {
            Message message = readMessages.get(key).makeReadMessage();
            messages.put(key, message);
        }        
        return messages;
    }
    
    private static IAsyncResult dispatchMessages(Map<String, EndPoint> endPoints, Map<String, Message> messages)
    {
        Set<String> keys = endPoints.keySet();
        EndPoint[] eps = new EndPoint[keys.size()];
        Message[] msgs  = new Message[keys.size()];
        
        int i = 0;
        for ( String key : keys )
        {
            eps[i] = endPoints.get(key);
            msgs[i] = messages.get(key);
            ++i;
        }
        
        IAsyncResult iar = MessagingService.getMessagingInstance().sendRR(msgs, eps);
        return iar;
    }
    
    /**
     * This is an implementation for the multiget version. 
     * @param readMessages map of key --> ReadMessage to be sent
     * @return map of key --> Row
     * @throws IOException
     * @throws TimeoutException
     */
    public static Map<String, Row> doReadProtocol(Map<String, ReadCommand> readMessages) throws IOException,TimeoutException
    {
        Map<String, Row> rows = new HashMap<String, Row>();
        Set<String> keys = readMessages.keySet();
        /* Find all the suitable endpoints for the keys */
        Map<String, EndPoint> endPoints = StorageService.instance().findSuitableEndPoints(keys.toArray( new String[0] ));
        /* Construct the messages to be sent out */
        Map<String, Message> messages = constructMessages(readMessages);
        /* Dispatch the messages to the respective endpoints */
        IAsyncResult iar = dispatchMessages(endPoints, messages);        
        List<byte[]> results = iar.multiget(2*DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
        
        for ( byte[] body : results )
        {
            DataInputBuffer bufIn = new DataInputBuffer();
            bufIn.reset(body, body.length);
            ReadResponse response = ReadResponse.serializer().deserialize(bufIn);
            Row row = response.row();
            rows.put(row.key(), row);
        }        
        return rows;
    }

    /**
     * Read the data from one replica.  If there is no reply, read the data from another.  In the event we get
     * the data we perform consistency checks and figure out if any repairs need to be done to the replicas.
     * @param command the read to perform
     * @return the row associated with command.key
     * @throws Exception
     */
    private static Row weakReadRemote(ReadCommand command) throws IOException
    {
        EndPoint endPoint = StorageService.instance().findSuitableEndPoint(command.key);
        assert endPoint != null;
        Message message = command.makeReadMessage();
        if (logger.isDebugEnabled())
            logger.debug("weakreadremote reading " + command + " from " + message.getMessageId() + "@" + endPoint);
        message.addHeader(ReadCommand.DO_REPAIR, ReadCommand.DO_REPAIR.getBytes());
        IAsyncResult iar = MessagingService.getMessagingInstance().sendRR(message, endPoint);
        byte[] body;
        try
        {
            body = iar.get(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException e)
        {
            throw new RuntimeException("error reading key " + command.key, e);
            // TODO retry to a different endpoint?
        }
        DataInputBuffer bufIn = new DataInputBuffer();
        bufIn.reset(body, body.length);
        ReadResponse response = ReadResponse.serializer().deserialize(bufIn);
        return response.row();
    }

    /**
     * Performs the actual reading of a row out of the StorageService, fetching
     * a specific set of column names from a given column family.
     */
    public static Row readProtocol(ReadCommand command, int consistency_level)
    throws IOException, TimeoutException, InvalidRequestException
    {
        long startTime = System.currentTimeMillis();

        Row row;
        EndPoint[] endpoints = StorageService.instance().getNStorageEndPoint(command.key);

        if (consistency_level == ConsistencyLevel.ONE)
        {
            boolean foundLocal = Arrays.asList(endpoints).contains(StorageService.getLocalStorageEndPoint());
            if (foundLocal)
            {
                row = weakReadLocal(command);
            }
            else
            {
                row = weakReadRemote(command);
            }
        }
        else
        {
            assert consistency_level == ConsistencyLevel.QUORUM;
            row = strongRead(command);
        }

        readStats.add(System.currentTimeMillis() - startTime);

        return row;
    }

    public static Map<String, Row> readProtocol(String[] keys, ReadCommand readCommand, StorageService.ConsistencyLevel consistencyLevel) throws Exception
    {
        Map<String, Row> rows = new HashMap<String, Row>();        
        switch ( consistencyLevel )
        {
            case WEAK:
                rows = weakReadProtocol(keys, readCommand);
                break;
                
            case STRONG:
                rows = strongReadProtocol(keys, readCommand);
                break;
                
            default:
                rows = weakReadProtocol(keys, readCommand);
                break;
        }
        return rows;
    }

    /**
     * This is a multiget version of the above method.
     */
    public static Map<String, Row> strongReadProtocol(String[] keys, ReadCommand readCommand) throws IOException, TimeoutException
    {       
        Map<String, Row> rows;
        // TODO: throw a thrift exception if we do not have N nodes
        Map<String, ReadCommand[]> readMessages = new HashMap<String, ReadCommand[]>();
        for (String key : keys )
        {
            ReadCommand[] readParameters = new ReadCommand[2];
            readParameters[0] = readCommand.copy();
            readParameters[1] = readCommand.copy();
            readParameters[1].setDigestQuery(true);
            readMessages.put(key, readParameters);
        }        
        rows = doStrongReadProtocol(readMessages);         
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
    private static Row strongRead(ReadCommand command) throws IOException, TimeoutException, InvalidRequestException
    {
        // TODO: throw a thrift exception if we do not have N nodes
        assert !command.isDigestQuery();
        ReadCommand readMessageDigestOnly = command.copy();
        readMessageDigestOnly.setDigestQuery(true);

        Row row = null;
        Message message = command.makeReadMessage();
        Message messageDigestOnly = readMessageDigestOnly.makeReadMessage();

        IResponseResolver<Row> readResponseResolver = new ReadResponseResolver();
        QuorumResponseHandler<Row> quorumResponseHandler = new QuorumResponseHandler<Row>(
                DatabaseDescriptor.getQuorum(),
                readResponseResolver);
        EndPoint dataPoint = StorageService.instance().findSuitableEndPoint(command.key);
        List<EndPoint> endpointList = new ArrayList<EndPoint>(Arrays.asList(StorageService.instance().getNStorageEndPoint(command.key)));
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

        try
        {
            MessagingService.getMessagingInstance().sendRR(messages, endPoints, quorumResponseHandler);

            long startTime2 = System.currentTimeMillis();
            row = quorumResponseHandler.get();
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
                MessagingService.getMessagingInstance().sendRR(messageRepair, endPoints,
                                                               quorumResponseHandlerRepair);
                try
                {
                    row = quorumResponseHandlerRepair.get();
                }
                catch (DigestMismatchException e)
                {
                    // TODO should this be a thrift exception?
                    throw new RuntimeException("digest mismatch reading key " + command.key, e);
                }
            }
        }

        return row;
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
    
    private static MultiQuorumResponseHandler dispatchMessages(Map<String, ReadCommand[]> readMessages, Map<String, Message[]> messages) throws IOException
    {
        Set<String> keys = messages.keySet();
        /* This maps the keys to the original data read messages */
        Map<String, ReadCommand> readMessage = new HashMap<String, ReadCommand>();
        /* This maps the keys to their respective endpoints/replicas */
        Map<String, EndPoint[]> endpoints = new HashMap<String, EndPoint[]>();
        /* Groups the messages that need to be sent to the individual keys */
        Message[][] msgList = new Message[messages.size()][DatabaseDescriptor.getReplicationFactor()];
        /* Respects the above grouping and provides the endpoints for the above messages */
        EndPoint[][] epList = new EndPoint[messages.size()][DatabaseDescriptor.getReplicationFactor()];
        
        int i = 0;
        for ( String key : keys )
        {
            /* This is the primary */
            EndPoint dataPoint = StorageService.instance().findSuitableEndPoint(key);
            List<EndPoint> replicas = new ArrayList<EndPoint>( StorageService.instance().getNLiveStorageEndPoint(key) );
            replicas.remove(dataPoint);
            /* Get the messages to be sent index 0 is the data messages and index 1 is the digest message */
            Message[] message = messages.get(key);           
            msgList[i][0] = message[0];
            int N = DatabaseDescriptor.getReplicationFactor();
            for ( int j = 1; j < N; ++j )
            {
                msgList[i][j] = message[1];
            }
            /* Get the endpoints to which the above messages need to be sent */
            epList[i][0] = dataPoint;
            for ( int j = 1; i < N; ++i )
            {                
                epList[i][j] = replicas.get(j - 1);
            } 
            /* Data ReadMessage associated with this key */
            readMessage.put( key, readMessages.get(key)[0] );
            /* EndPoints for this specific key */
            endpoints.put(key, epList[i]);
            ++i;
        }
                
        /* Handles the read semantics for this entire set of keys */
        MultiQuorumResponseHandler quorumResponseHandlers = new MultiQuorumResponseHandler(readMessage, endpoints);
        MessagingService.getMessagingInstance().sendRR(msgList, epList, quorumResponseHandlers);
        return quorumResponseHandlers;
    }
    
    /**
    *  This method performs the read from the replicas for a bunch of keys.
    *  @param readMessages map of key --> readMessage[] of two entries where 
    *         the first entry is the readMessage for the data and the second
    *         is the entry for the digest 
    *  @return map containing key ---> Row
    *  @throws IOException, TimeoutException
   */
    private static Map<String, Row> doStrongReadProtocol(Map<String, ReadCommand[]> readMessages) throws IOException
    {        
        Map<String, Row> rows = new HashMap<String, Row>();
        /* Construct the messages to be sent to the replicas */
        Map<String, Message[]> replicaMessages = constructReplicaMessages(readMessages);
        /* Dispatch the messages to the different replicas */
        MultiQuorumResponseHandler cb = dispatchMessages(readMessages, replicaMessages);
        try
        {
            Row[] rows2 = cb.get();
            for ( Row row : rows2 )
            {
                rows.put(row.key(), row);
            }
        }
        catch (TimeoutException e)
        {
            throw new RuntimeException("timeout reading keys " + StringUtils.join(rows.keySet(), ", "), e);
        }
        return rows;
    }

    /**
     * This version is used when results for multiple keys needs to be
     * retrieved.
     * 
     * @return a mapping of key --> Row
     * @throws Exception
     */
    public static Map<String, Row> weakReadProtocol(String[] keys, ReadCommand readCommand) throws Exception
    {
        Row row = null;
        Map<String, ReadCommand> readMessages = new HashMap<String, ReadCommand>();
        for ( String key : keys )
        {
            ReadCommand readCmd = readCommand.copy();
            readMessages.put(key, readCmd);
        }
        /* Performs the multiget in parallel */
        Map<String, Row> rows = doReadProtocol(readMessages);
        /*
         * Do the consistency checks for the keys that are being queried
         * in the background.
        */
        for ( String key : keys )
        {
            List<EndPoint> endpoints = StorageService.instance().getNLiveStorageEndPoint(key);
            /* Remove the local storage endpoint from the list. */
            endpoints.remove( StorageService.getLocalStorageEndPoint() );
            if ( endpoints.size() > 0 && DatabaseDescriptor.getConsistencyCheck())
                StorageService.instance().doConsistencyCheck(row, endpoints, readMessages.get(key));
        }
        return rows;
    }

    /*
    * This function executes the read protocol locally and should be used only if consistency is not a concern.
    * Read the data from the local disk and return if the row is NOT NULL. If the data is NULL do the read from
    * one of the other replicas (in the same data center if possible) till we get the data. In the event we get
    * the data we perform consistency checks and figure out if any repairs need to be done to the replicas.
    */
    private static Row weakReadLocal(ReadCommand command) throws IOException
    {
        if (logger.isDebugEnabled())
            logger.debug("weakreadlocal reading " + command);
        List<EndPoint> endpoints = StorageService.instance().getNLiveStorageEndPoint(command.key);
        /* Remove the local storage endpoint from the list. */
        endpoints.remove(StorageService.getLocalStorageEndPoint());
        // TODO: throw a thrift exception if we do not have N nodes

        Table table = Table.open(command.table);
        Row row = command.getRow(table);

        /*
           * Do the consistency checks in the background and return the
           * non NULL row.
           */
        if (endpoints.size() > 0 && DatabaseDescriptor.getConsistencyCheck())
            StorageService.instance().doConsistencyCheck(row, endpoints, command);
        return row;
    }

    static List<String> getKeyRange(RangeCommand rawCommand) throws IOException
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
