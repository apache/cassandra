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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.TouchMessage;
import org.apache.cassandra.db.ColumnFamilyNotDefinedException;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.IAsyncResult;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;


public class StorageProxy
{
    private static Logger logger_ = Logger.getLogger(StorageProxy.class);    
    
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
				logger_.debug("Sending the hint of " + target.getHost() + " to " + hint.getHost());
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
        assert rm.key() != null;

		try
		{
			Map<EndPoint, EndPoint> endpointMap = StorageService.instance().getNStorageEndPointMap(rm.key());
			// TODO: throw a thrift exception if we do not have N nodes
			Map<EndPoint, Message> messageMap = createWriteMessages(rm, endpointMap);
            logger_.debug("insert writing to [" + StringUtils.join(messageMap.keySet(), ", ") + "]");
			for (Map.Entry<EndPoint, Message> entry : messageMap.entrySet())
			{
				MessagingService.getMessagingInstance().sendOneWay(entry.getValue(), entry.getKey());
			}
		}
        catch (Exception e)
        {
            logger_.error( LogUtil.throwableToString(e) );
        }
        return;
    }

    public static boolean insertBlocking(RowMutation rm)
    {
        assert rm.key() != null;

        try
        {
            Message message = rm.makeRowMutationMessage();

            IResponseResolver<Boolean> writeResponseResolver = new WriteResponseResolver();
            QuorumResponseHandler<Boolean> quorumResponseHandler = new QuorumResponseHandler<Boolean>(
                    DatabaseDescriptor.getReplicationFactor(),
                    writeResponseResolver);
            EndPoint[] endpoints = StorageService.instance().getNStorageEndPoint(rm.key());
            logger_.debug("insertBlocking writing to [" + StringUtils.join(endpoints, ", ") + "]");
            // TODO: throw a thrift exception if we do not have N nodes

            MessagingService.getMessagingInstance().sendRR(message, endpoints, quorumResponseHandler);
            return quorumResponseHandler.get();

            // TODO: if the result is false that means the writes to all the
            // servers failed hence we need to throw an exception or return an
            // error back to the client so that it can take appropriate action.
        }
        catch (Exception e)
        {
            logger_.error( LogUtil.throwableToString(e) );
            return false;
        }
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
        List<Object[]> results = iar.multiget(2*DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
        
        for ( Object[] result : results )
        {
            byte[] body = (byte[])result[0];
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
        logger_.debug("weakreadremote reading " + command + " from " + endPoint);
        Message message = command.makeReadMessage();
        message.addHeader(ReadCommand.DO_REPAIR, ReadCommand.DO_REPAIR.getBytes());
        IAsyncResult iar = MessagingService.getMessagingInstance().sendRR(message, endPoint);
        byte[] body;
        try
        {
            Object[] result = iar.get(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
            body = (byte[])result[0];
        }
        catch (TimeoutException e)
        {
            throw new RuntimeException(e);
            // TODO retry to a different endpoint
        }
        DataInputBuffer bufIn = new DataInputBuffer();
        bufIn.reset(body, body.length);
        ReadResponse response = ReadResponse.serializer().deserialize(bufIn);
        return response.row();
    }

    static void touch_local(String tablename, String key, boolean fData ) throws IOException
    {
		Table table = Table.open( tablename );
		table.touch(key, fData);
    }

    static void weakTouchProtocol(String tablename, String key, boolean fData) throws Exception
    {
    	EndPoint endPoint = null;
    	try
    	{
    		endPoint = StorageService.instance().findSuitableEndPoint(key);
    	}
    	catch( Throwable ex)
    	{
    		ex.printStackTrace();
    	}
    	if(endPoint != null)
    	{
    		if(endPoint.equals(StorageService.getLocalStorageEndPoint()))
    		{
    	    	touch_local(tablename, key, fData);
    	    	return;
    	    }
            TouchMessage touchMessage = null;
            touchMessage = new TouchMessage(tablename, key, fData);
            Message message = TouchMessage.makeTouchMessage(touchMessage);
            MessagingService.getMessagingInstance().sendOneWay(message, endPoint);
    	}
    	return ;
    }
    
    static void strongTouchProtocol(String tablename, String key, boolean fData) throws Exception
    {
        Map<EndPoint, EndPoint> endpointMap = StorageService.instance().getNStorageEndPointMap(key);
        Set<EndPoint> endpoints = endpointMap.keySet();
        TouchMessage touchMessage = null;
        touchMessage = new TouchMessage(tablename, key, fData);
        Message message = TouchMessage.makeTouchMessage(touchMessage);
        for(EndPoint endpoint : endpoints)
        {
            MessagingService.getMessagingInstance().sendOneWay(message, endpoint);
        }    	
    }
    
    /*
     * Only touch data on the most suitable end point.
     */
    public static void touchProtocol(String tablename, String key, boolean fData, StorageService.ConsistencyLevel consistencyLevel) throws Exception
    {
        switch ( consistencyLevel )
        {
	        case WEAK:
	            weakTouchProtocol(tablename, key, fData);
	            break;
	            
	        case STRONG:
	            strongTouchProtocol(tablename, key, fData);
	            break;
	            
	        default:
	            weakTouchProtocol(tablename, key, fData);
	            break;
        }
    }  

    /**
     * Performs the actual reading of a row out of the StorageService, fetching
     * a specific set of column names from a given column family.
     */
    public static Row readProtocol(ReadCommand command, StorageService.ConsistencyLevel consistencyLevel)
    throws IOException, ColumnFamilyNotDefinedException, TimeoutException
    {
        assert command.key != null;
        long startTime = System.currentTimeMillis();
        Row row = null;
        EndPoint[] endpoints = StorageService.instance().getNStorageEndPoint(command.key);

        if (consistencyLevel == StorageService.ConsistencyLevel.WEAK)
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
            assert consistencyLevel == StorageService.ConsistencyLevel.STRONG;
            row = strongRead(command);
        }

        logger_.debug("Finished reading " + row + " in " + (System.currentTimeMillis() - startTime) + " ms.");
        return row;
    }

    public static Map<String, Row> readProtocol(String tablename, String[] keys, String columnFamily, int start, int count, StorageService.ConsistencyLevel consistencyLevel) throws Exception
    {
        Map<String, Row> rows = new HashMap<String, Row>();        
        switch ( consistencyLevel )
        {
            case WEAK:
                rows = weakReadProtocol(tablename, keys, columnFamily, start, count);
                break;
                
            case STRONG:
                rows = strongReadProtocol(tablename, keys, columnFamily, start, count);
                break;
                
            default:
                rows = weakReadProtocol(tablename, keys, columnFamily, start, count);
                break;
        }
        return rows;
    }

    /**
     * This is a multiget version of the above method.
     * @param tablename
     * @param keys
     * @param columnFamily
     * @param start
     * @param count
     * @return
     * @throws IOException
     * @throws TimeoutException
     */
    public static Map<String, Row> strongReadProtocol(String tablename, String[] keys, String columnFamily, int start, int count) throws IOException, TimeoutException
    {       
        Map<String, Row> rows = new HashMap<String, Row>();
        long startTime = System.currentTimeMillis();        
        // TODO: throw a thrift exception if we do not have N nodes
        Map<String, ReadCommand[]> readMessages = new HashMap<String, ReadCommand[]>();
        for (String key : keys )
        {
            ReadCommand[] readParameters = new ReadCommand[2];
            if( start >= 0 && count < Integer.MAX_VALUE)
            {
                readParameters[0] = new ReadCommand(tablename, key, columnFamily, start, count);
            }
            else
            {
                readParameters[0] = new ReadCommand(tablename, key, columnFamily);
            }            
            if( start >= 0 && count < Integer.MAX_VALUE)
            {
                readParameters[1] = new ReadCommand(tablename, key, columnFamily, start, count);
            }
            else
            {
                readParameters[1] = new ReadCommand(tablename, key, columnFamily);
            }
            readParameters[1].setDigestQuery(true);
        }        
        rows = doStrongReadProtocol(readMessages);         
        logger_.debug("readProtocol: " + (System.currentTimeMillis() - startTime) + " ms.");
        return rows;
    }

    /*
     * This function executes the read protocol.
        // 1. Get the N nodes from storage service where the data needs to be
        // replicated
        // 2. Construct a message for read\write
         * 3. Set one of teh messages to get teh data and teh rest to get teh digest
        // 4. SendRR ( to all the nodes above )
        // 5. Wait for a response from atleast X nodes where X <= N and teh data node
         * 6. If the digest matches return teh data.
         * 7. else carry out read repair by getting data from all the nodes.
        // 5. return success
     */
    private static Row strongRead(ReadCommand command) throws IOException, TimeoutException
    {
        // TODO: throw a thrift exception if we do not have N nodes

        ReadCommand readMessageDigestOnly = command.copy();
        readMessageDigestOnly.setDigestQuery(true);

        Row row = null;
        Message message = command.makeReadMessage();
        Message messageDigestOnly = readMessageDigestOnly.makeReadMessage();

        IResponseResolver<Row> readResponseResolver = new ReadResponseResolver();
        QuorumResponseHandler<Row> quorumResponseHandler = new QuorumResponseHandler<Row>(
                DatabaseDescriptor.getReplicationFactor(),
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
        for (int i = 1; i < endPoints.length; i++)
        {
            endPoints[i] = endpointList.get(i - 1);
            messages[i] = messageDigestOnly;
        }
        logger_.debug("strongread reading " + command + " from " + StringUtils.join(endPoints, ", "));

        try
        {
            MessagingService.getMessagingInstance().sendRR(messages, endPoints, quorumResponseHandler);

            long startTime2 = System.currentTimeMillis();
            row = quorumResponseHandler.get();
            logger_.debug("quorumResponseHandler: " + (System.currentTimeMillis() - startTime2) + " ms.");
        }
        catch (DigestMismatchException ex)
        {
            if ( DatabaseDescriptor.getConsistencyCheck())
            {
                IResponseResolver<Row> readResponseResolverRepair = new ReadResponseResolver();
                QuorumResponseHandler<Row> quorumResponseHandlerRepair = new QuorumResponseHandler<Row>(
                        DatabaseDescriptor.getReplicationFactor(),
                        readResponseResolverRepair);
                command.setDigestQuery(false);
                logger_.info("DigestMismatchException: " + command.key);
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
                    throw new RuntimeException(e);
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
        catch ( TimeoutException ex )
        {
            logger_.info("Operation timed out waiting for responses ...");
            logger_.info(LogUtil.throwableToString(ex));
        }
        return rows;
    }

    /**
     * This version is used when results for multiple keys needs to be
     * retrieved.
     * 
     * @param tablename name of the table that needs to be queried
     * @param keys keys whose values we are interested in 
     * @param columnFamily name of the "column" we are interested in
     * @param columns the columns we are interested in
     * @return a mapping of key --> Row
     * @throws Exception
     */
    public static Map<String, Row> weakReadProtocol(String tablename, String[] keys, String columnFamily, List<String> columns) throws Exception
    {
        Row row = null;
        long startTime = System.currentTimeMillis();
        Map<String, ReadCommand> readMessages = new HashMap<String, ReadCommand>();
        for ( String key : keys )
        {
            ReadCommand readCommand = new ReadCommand(tablename, key, columnFamily, columns);
            readMessages.put(key, readCommand);
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
                StorageService.instance().doConsistencyCheck(row, endpoints, columnFamily, columns);
        }
        return rows;
    }

    /*
    * This function executes the read protocol locally and should be used only if consistency is not a concern.
    * Read the data from the local disk and return if the row is NOT NULL. If the data is NULL do the read from
    * one of the other replicas (in the same data center if possible) till we get the data. In the event we get
    * the data we perform consistency checks and figure out if any repairs need to be done to the replicas.
    */
    private static Row weakReadLocal(ReadCommand command) throws IOException, ColumnFamilyNotDefinedException
    {
        logger_.debug("weakreadlocal for " + command);
        List<EndPoint> endpoints = StorageService.instance().getNLiveStorageEndPoint(command.key);
        /* Remove the local storage endpoint from the list. */
        endpoints.remove(StorageService.getLocalStorageEndPoint());
        // TODO: throw a thrift exception if we do not have N nodes

        Table table = Table.open(DatabaseDescriptor.getTables().get(0));
        Row row = command.getRow(table);

        /*
           * Do the consistency checks in the background and return the
           * non NULL row.
           */
        if (endpoints.size() > 0 && DatabaseDescriptor.getConsistencyCheck())
            StorageService.instance().doConsistencyCheck(row, endpoints, command);
        return row;
    }

    /**
     * This version is used when results for multiple keys needs to be
     * retrieved.
     * 
     * @param tablename name of the table that needs to be queried
     * @param keys keys whose values we are interested in 
     * @param columnFamily name of the "column" we are interested in
     * @param start start index
     * @param count the number of columns we are interested in
     * @return a mapping of key --> Row
     * @throws Exception
     */
    public static Map<String, Row> weakReadProtocol(String tablename, String[] keys, String columnFamily, int start, int count) throws Exception
    {
        Row row = null;
        long startTime = System.currentTimeMillis();
        Map<String, ReadCommand> readMessages = new HashMap<String, ReadCommand>();
        for ( String key : keys )
        {
            ReadCommand readCommand = new ReadCommand(tablename, key, columnFamily, start, count);
            readMessages.put(key, readCommand);
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
                StorageService.instance().doConsistencyCheck(row, endpoints, columnFamily, start, count);
        }
        return rows;         
    }

    /**
     * This version is used when results for multiple keys needs to be
     * retrieved.
     * 
     * @param tablename name of the table that needs to be queried
     * @param keys keys whose values we are interested in 
     * @param columnFamily name of the "column" we are interested in
     * @param sinceTimestamp this is lower bound of the timestamp
     * @return a mapping of key --> Row
     * @throws Exception
     */
    public static Map<String, Row> weakReadProtocol(String tablename, String[] keys, String columnFamily, long sinceTimestamp) throws Exception
    {
        Row row = null;
        long startTime = System.currentTimeMillis();
        Map<String, ReadCommand> readMessages = new HashMap<String, ReadCommand>();
        for ( String key : keys )
        {
            ReadCommand readCommand = new ReadCommand(tablename, key, columnFamily, sinceTimestamp);
            readMessages.put(key, readCommand);
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
                StorageService.instance().doConsistencyCheck(row, endpoints, columnFamily, sinceTimestamp);
        }
        return rows;         
    }
}
