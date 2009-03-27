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
import org.apache.cassandra.db.ReadMessage;
import org.apache.cassandra.db.ReadResponseMessage;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.RowMutationMessage;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.TouchMessage;
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
    
    public static Row doReadProtocol(String key, ReadMessage readMessage) throws IOException,TimeoutException
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
            Message message = ReadMessage.makeReadMessage(readMessage);
            IAsyncResult iar = MessagingService.getMessagingInstance().sendRR(message, endPoint);
            Object[] result = iar.get(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
            byte[] body = (byte[])result[0];
            DataInputBuffer bufIn = new DataInputBuffer();
            bufIn.reset(body, body.length);
            ReadResponseMessage responseMessage = ReadResponseMessage.serializer().deserialize(bufIn);
            return responseMessage.row();
        }
        else
        {
            logger_.warn(" Alert : Unable to find a suitable end point for the key : " + key );
        }
        return null;
    }

    static void touch_local (String tablename, String key, boolean fData ) throws IOException
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
        
    
    
    public static Row readProtocol(String tablename, String key, String columnFamily, List<String> columnNames, StorageService.ConsistencyLevel consistencyLevel) throws Exception
    {
        Row row = null;
        boolean foundLocal = false;
        EndPoint[] endpoints = StorageService.instance().getNStorageEndPoint(key);
        for(EndPoint endPoint: endpoints)
        {
            if(endPoint.equals(StorageService.getLocalStorageEndPoint()))
            {
                foundLocal = true;
                break;
            }
        }   
        if(!foundLocal && consistencyLevel == StorageService.ConsistencyLevel.WEAK)
        {
            ReadMessage readMessage = null;
            readMessage = new ReadMessage(tablename, key, columnFamily, columnNames);
            return doReadProtocol(key, readMessage);
        }
        else
        {
            switch ( consistencyLevel )
            {
            case WEAK:
                row = weakReadProtocol(tablename, key, columnFamily, columnNames);
                break;
                
            case STRONG:
                row = strongReadProtocol(tablename, key, columnFamily, columnNames);
                break;
                
            default:
                row = weakReadProtocol(tablename, key, columnFamily, columnNames);
                break;
            }
        }
        return row;
        
        
    }
        
    public static Row readProtocol(String tablename, String key, String columnFamily, int start, int count, StorageService.ConsistencyLevel consistencyLevel) throws Exception
    {
        Row row = null;
        boolean foundLocal = false;
        EndPoint[] endpoints = StorageService.instance().getNStorageEndPoint(key);
        for(EndPoint endPoint: endpoints)
        {
            if(endPoint.equals(StorageService.getLocalStorageEndPoint()))
            {
                foundLocal = true;
                break;
            }
        }   
        if(!foundLocal && consistencyLevel == StorageService.ConsistencyLevel.WEAK)
        {
            ReadMessage readMessage = null;
            readMessage = new ReadMessage(tablename, key, columnFamily, start, count);
            return doReadProtocol(key, readMessage);
        }
        else
        {
            switch ( consistencyLevel )
            {
            case WEAK:
                row = weakReadProtocol(tablename, key, columnFamily, start, count);
                break;
                
            case STRONG:
                row = strongReadProtocol(tablename, key, columnFamily, start, count);
                break;
                
            default:
                row = weakReadProtocol(tablename, key, columnFamily, start, count);
                break;
            }
        }
        return row;
    }
    
    public static Row readProtocol(String tablename, String key, String columnFamily, long sinceTimestamp, StorageService.ConsistencyLevel consistencyLevel) throws Exception
    {
        Row row = null;
        boolean foundLocal = false;
        EndPoint[] endpoints = StorageService.instance().getNStorageEndPoint(key);
        for(EndPoint endPoint: endpoints)
        {
            if(endPoint.equals(StorageService.getLocalStorageEndPoint()))
            {
                foundLocal = true;
                break;
            }
        }   
        if(!foundLocal && consistencyLevel == StorageService.ConsistencyLevel.WEAK)
        {
            ReadMessage readMessage = null;
            readMessage = new ReadMessage(tablename, key, columnFamily, sinceTimestamp);
            return doReadProtocol(key, readMessage);
        }
        else
        {
            switch ( consistencyLevel )
            {
            case WEAK:
                row = weakReadProtocol(tablename, key, columnFamily, sinceTimestamp);
                break;
                
            case STRONG:
                row = strongReadProtocol(tablename, key, columnFamily, sinceTimestamp);
                break;
                
            default:
                row = weakReadProtocol(tablename, key, columnFamily, sinceTimestamp);
                break;
            }
        }
        return row;
    }

    public static Row strongReadProtocol(String tablename, String key, String columnFamily, List<String> columns) throws Exception
    {       
        long startTime = System.currentTimeMillis();        
        // TODO: throw a thrift exception if we do not have N nodes
        ReadMessage readMessage = new ReadMessage(tablename, key, columnFamily, columns);               
        
        ReadMessage readMessageDigestOnly = new ReadMessage(tablename, key, columnFamily, columns);     
        readMessageDigestOnly.setIsDigestQuery(true);        
        
        Row row = StorageProxy.doStrongReadProtocol(key, readMessage, readMessageDigestOnly);
        logger_.debug("readProtocol: " + (System.currentTimeMillis() - startTime) + " ms.");     
        return row;
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
     * 
     */
    public static Row strongReadProtocol(String tablename, String key, String columnFamily, int start, int count) throws IOException, TimeoutException
    {       
        long startTime = System.currentTimeMillis();        
        // TODO: throw a thrift exception if we do not have N nodes
        ReadMessage readMessage = null;
        ReadMessage readMessageDigestOnly = null;
        if( start >= 0 && count < Integer.MAX_VALUE)
        {
            readMessage = new ReadMessage(tablename, key, columnFamily, start, count);
        }
        else
        {
            readMessage = new ReadMessage(tablename, key, columnFamily);
        }
        Message message = ReadMessage.makeReadMessage(readMessage);
        if( start >= 0 && count < Integer.MAX_VALUE)
        {
            readMessageDigestOnly = new ReadMessage(tablename, key, columnFamily, start, count);
        }
        else
        {
            readMessageDigestOnly = new ReadMessage(tablename, key, columnFamily);
        }
        readMessageDigestOnly.setIsDigestQuery(true);        
        Row row = doStrongReadProtocol(key, readMessage, readMessageDigestOnly);
        logger_.debug("readProtocol: " + (System.currentTimeMillis() - startTime) + " ms.");
        return row;
    }
    
    public static Row strongReadProtocol(String tablename, String key, String columnFamily, long sinceTimestamp) throws IOException, TimeoutException
    {       
        long startTime = System.currentTimeMillis();        
        // TODO: throw a thrift exception if we do not have N nodes
        ReadMessage readMessage = null;
        ReadMessage readMessageDigestOnly = null;
        readMessage = new ReadMessage(tablename, key, columnFamily, sinceTimestamp);
        Message message = ReadMessage.makeReadMessage(readMessage);
        readMessageDigestOnly = new ReadMessage(tablename, key, columnFamily, sinceTimestamp);
        readMessageDigestOnly.setIsDigestQuery(true);        
        Row row = doStrongReadProtocol(key, readMessage, readMessageDigestOnly);
        logger_.debug("readProtocol: " + (System.currentTimeMillis() - startTime) + " ms.");
        return row;
    }

    /*
     * This method performs the actual read from the replicas.
     *  param @ key - key for which the data is required.
     *  param @ readMessage - the read message to get the actual data
     *  param @ readMessageDigest - the read message to get the digest.
    */
    private static Row doStrongReadProtocol(String key, ReadMessage readMessage, ReadMessage readMessageDigest) throws IOException, TimeoutException
    {
        Row row = null;
        Message message = ReadMessage.makeReadMessage(readMessage);
        Message messageDigestOnly = ReadMessage.makeReadMessage(readMessageDigest);
        
        IResponseResolver<Row> readResponseResolver = new ReadResponseResolver();
        QuorumResponseHandler<Row> quorumResponseHandler = new QuorumResponseHandler<Row>(
                DatabaseDescriptor.getReplicationFactor(),
                readResponseResolver);
        EndPoint dataPoint = StorageService.instance().findSuitableEndPoint(key);
        List<EndPoint> endpointList = new ArrayList<EndPoint>( Arrays.asList( StorageService.instance().getNStorageEndPoint(key) ) );
        /* Remove the local storage endpoint from the list. */ 
        endpointList.remove( dataPoint );
        EndPoint[] endPoints = new EndPoint[endpointList.size() + 1];
        Message messages[] = new Message[endpointList.size() + 1];
        
        // first message is the data Point 
        endPoints[0] = dataPoint;
        messages[0] = message;
        
        for(int i=1; i < endPoints.length ; i++)
        {
            endPoints[i] = endpointList.get(i-1);
            messages[i] = messageDigestOnly;
        }
        
        try
        {
            MessagingService.getMessagingInstance().sendRR(messages, endPoints, quorumResponseHandler);
            
            long startTime2 = System.currentTimeMillis();
            row = quorumResponseHandler.get();
            logger_.debug("quorumResponseHandler: " + (System.currentTimeMillis() - startTime2)
                    + " ms.");
            if (row == null)
            {
                logger_.info("ERROR No row for this key .....: " + key);
                // TODO: throw a thrift exception 
                return row;
            }
        }
        catch (DigestMismatchException ex)
        {
            if ( DatabaseDescriptor.getConsistencyCheck())
            {
	            IResponseResolver<Row> readResponseResolverRepair = new ReadResponseResolver();
	            QuorumResponseHandler<Row> quorumResponseHandlerRepair = new QuorumResponseHandler<Row>(
	                    DatabaseDescriptor.getReplicationFactor(),
	                    readResponseResolverRepair);
	            readMessage.setIsDigestQuery(false);
	            logger_.info("DigestMismatchException: " + key);            
	            Message messageRepair = ReadMessage.makeReadMessage(readMessage);
	            MessagingService.getMessagingInstance().sendRR(messageRepair, endPoints,
	                    quorumResponseHandlerRepair);
	            try
	            {
	                row = quorumResponseHandlerRepair.get();
	            }
	            catch(DigestMismatchException dex)
	            {
	                logger_.warn(LogUtil.throwableToString(dex));
	            }
	            if (row == null)
	            {
	                logger_.info("ERROR No row for this key .....: " + key);                
	            }
            }
        }        
        return row;
    }
    
    public static Row weakReadProtocol(String tablename, String key, String columnFamily, List<String> columns) throws Exception
    {       
        long startTime = System.currentTimeMillis();
        List<EndPoint> endpoints = StorageService.instance().getNLiveStorageEndPoint(key);
        /* Remove the local storage endpoint from the list. */ 
        endpoints.remove( StorageService.getLocalStorageEndPoint() );
        // TODO: throw a thrift exception if we do not have N nodes
        
        Table table = Table.open( DatabaseDescriptor.getTables().get(0) );
        Row row = table.getRow(key, columnFamily, columns);
        
        logger_.debug("Local Read Protocol: " + (System.currentTimeMillis() - startTime) + " ms.");
        /*
         * Do the consistency checks in the background and return the
         * non NULL row.
         */
        if ( endpoints.size() > 0 && DatabaseDescriptor.getConsistencyCheck())
            StorageService.instance().doConsistencyCheck(row, endpoints, columnFamily, columns);
        return row;
    }
    
    /*
     * This function executes the read protocol locally and should be used only if consistency is not a concern. 
     * Read the data from the local disk and return if the row is NOT NULL. If the data is NULL do the read from
     * one of the other replicas (in the same data center if possible) till we get the data. In the event we get
     * the data we perform consistency checks and figure out if any repairs need to be done to the replicas. 
     */
    public static Row weakReadProtocol(String tablename, String key, String columnFamily, int start, int count) throws Exception
    {
        Row row = null;
        long startTime = System.currentTimeMillis();
        List<EndPoint> endpoints = StorageService.instance().getNLiveStorageEndPoint(key);
        /* Remove the local storage endpoint from the list. */ 
        endpoints.remove( StorageService.getLocalStorageEndPoint() );
        // TODO: throw a thrift exception if we do not have N nodes
        
        Table table = Table.open( DatabaseDescriptor.getTables().get(0) );
        if( start >= 0 && count < Integer.MAX_VALUE)
        {
            row = table.getRow(key, columnFamily, start, count);
        }
        else
        {
            row = table.getRow(key, columnFamily);
        }
        
        logger_.debug("Local Read Protocol: " + (System.currentTimeMillis() - startTime) + " ms.");
        /*
         * Do the consistency checks in the background and return the
         * non NULL row.
         */
        if ( endpoints.size() > 0 && DatabaseDescriptor.getConsistencyCheck())
        	StorageService.instance().doConsistencyCheck(row, endpoints, columnFamily, start, count);
        return row;         
    }
    
    public static Row weakReadProtocol(String tablename, String key, String columnFamily, long sinceTimestamp) throws Exception
    {
        Row row = null;
        long startTime = System.currentTimeMillis();
        List<EndPoint> endpoints = StorageService.instance().getNLiveStorageEndPoint(key);
        /* Remove the local storage endpoint from the list. */ 
        endpoints.remove( StorageService.getLocalStorageEndPoint() );
        // TODO: throw a thrift exception if we do not have N nodes
        
        Table table = Table.open( DatabaseDescriptor.getTables().get(0) );
        row = table.getRow(key, columnFamily,sinceTimestamp);
        logger_.debug("Local Read Protocol: " + (System.currentTimeMillis() - startTime) + " ms.");
        /*
         * Do the consistency checks in the background and return the
         * non NULL row.
         */
        if ( endpoints.size() > 0 && DatabaseDescriptor.getConsistencyCheck())
        	StorageService.instance().doConsistencyCheck(row, endpoints, columnFamily, sinceTimestamp);
        return row;         
    }

}
