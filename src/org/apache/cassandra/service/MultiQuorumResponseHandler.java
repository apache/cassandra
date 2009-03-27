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
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ReadMessage;
import org.apache.cassandra.db.ReadResponseMessage;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.WriteResponseMessage;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;
import org.apache.cassandra.utils.*;
/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class MultiQuorumResponseHandler implements IAsyncCallback
{ 
    private static Logger logger_ = Logger.getLogger( QuorumResponseHandler.class );
    private Lock lock_ = new ReentrantLock();
    private Condition condition_;
    /* This maps the keys to the original data read messages */
    private Map<String, ReadMessage> readMessages_ = new HashMap<String, ReadMessage>();
    /* This maps the key to its set of replicas */
    private Map<String, EndPoint[]> endpoints_ = new HashMap<String, EndPoint[]>();
    /* This maps the groupId to the individual callback for the set of messages */
    private Map<String, SingleQuorumResponseHandler> handlers_ = new HashMap<String, SingleQuorumResponseHandler>();
    /* This should hold all the responses for the keys */
    private List<Row> responses_ = new ArrayList<Row>();
    private AtomicBoolean done_ = new AtomicBoolean(false);
    
    /**
     * This is used to handle the responses from the individual messages
     * that are sent out to the replicas.
     * @author alakshman
     *
    */
    private class SingleQuorumResponseHandler implements IAsyncCallback
    {
        private Lock lock_ = new ReentrantLock();
        private IResponseResolver<Row> responseResolver_;
        private List<Message> responses_ = new ArrayList<Message>();
        
        SingleQuorumResponseHandler(IResponseResolver<Row> responseResolver)
        {
            responseResolver_ = responseResolver;
        }
        
        public void attachContext(Object o)
        {
            throw new UnsupportedOperationException("This operation is not supported in this implementation");
        }
        
        public void response(Message response)
        {
            lock_.lock();
            try
            {
                responses_.add(response);
                int majority = (DatabaseDescriptor.getReplicationFactor() >> 1) + 1;                            
                if ( responses_.size() >= majority && responseResolver_.isDataPresent(responses_))
                {
                    onCompletion();               
                }
            }
            catch ( IOException ex )
            {
                logger_.info( LogUtil.throwableToString(ex) );
            }
            finally
            {
                lock_.unlock();
            }
        }
        
        private void onCompletion() throws IOException
        {
            try
            {
                Row row = responseResolver_.resolve(responses_);
                MultiQuorumResponseHandler.this.onCompleteResponse(row);
            }
            catch ( DigestMismatchException ex )
            {
                /* 
                 * The DigestMismatchException has the key for which the mismatch
                 * occured bundled in it as context 
                */
                String key = ex.getMessage();
                onDigestMismatch(key);
            }
        }
        
        /**
         * This method is invoked on a digest match. We pass in the key
         * in order to retrieve the appropriate data message that needs
         * to be sent out to the replicas. 
         * 
         * @param key for which the mismatch occured.
        */
        private void onDigestMismatch(String key) throws IOException
        {
            if ( DatabaseDescriptor.getConsistencyCheck())
            {                                
                ReadMessage readMessage = readMessages_.get(key);
                readMessage.setIsDigestQuery(false);            
                Message messageRepair = ReadMessage.makeReadMessage(readMessage);
                EndPoint[] endpoints = MultiQuorumResponseHandler.this.endpoints_.get( readMessage.key() );
                Message[][] messages = new Message[][]{ {messageRepair, messageRepair, messageRepair} };
                EndPoint[][] epList = new EndPoint[][]{ endpoints };
                MessagingService.getMessagingInstance().sendRR(messages, epList, MultiQuorumResponseHandler.this);                
            }
        }
    }
    
    public MultiQuorumResponseHandler(Map<String, ReadMessage> readMessages, Map<String, EndPoint[]> endpoints)
    {        
        condition_ = lock_.newCondition();
        readMessages_ = readMessages;
        endpoints_ = endpoints;
    }
    
    public Row[] get() throws TimeoutException
    {
        long startTime = System.currentTimeMillis();
        lock_.lock();
        try
        {            
            boolean bVal = true;            
            try
            {
                if ( !done_.get() )
                {                   
                    bVal = condition_.await(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
                }
            }
            catch ( InterruptedException ex )
            {
                logger_.debug( LogUtil.throwableToString(ex) );
            }
            
            if ( !bVal && !done_.get() )
            {
                StringBuilder sb = new StringBuilder("");
                for ( Row row : responses_ )
                {
                    sb.append(row.key());
                    sb.append(":");
                }                
                throw new TimeoutException("Operation timed out - received only " +  responses_.size() + " responses from " + sb.toString() + " .");
            }
        }
        finally
        {
            lock_.unlock();
        }
        
        logger_.info("MultiQuorumResponseHandler: " + (System.currentTimeMillis() - startTime) + " ms.");
        return responses_.toArray( new Row[0] );
    }
    
    /**
     * Invoked when a complete response has been obtained
     * for one of the sub-groups a.k.a keys for the query 
     * has been performed.
     * 
     * @param row obtained as a result of the response.
     */
    void onCompleteResponse(Row row)
    {        
        if ( !done_.get() )
        {
            responses_.add(row);
            if ( responses_.size() == readMessages_.size() )
            {
                done_.set(true);
                condition_.signal();                
            }
        }
    }
    
    /**
     * The handler of the response message that has been
     * sent by one of the replicas for one of the keys.
     * 
     * @param message the reponse message for one of the
     *        message that we sent out.
     */
    public void response(Message message)
    {
        lock_.lock();
        try
        {
            SingleQuorumResponseHandler handler = handlers_.get(message.getMessageId());
            handler.response(message);
        }
        finally
        {
            lock_.unlock();
        }
    }
    
    /**
     * The context that is passed in for the query of
     * multiple keys in the system. For each message 
     * id in the context register a callback handler 
     * for the same. This is done so that all responses
     * for a given key use the same callback handler.
     * 
     * @param o the context which is an array of strings
     *        corresponding to the message id's for each
     *        key.
     */
    public void attachContext(Object o)
    {
        String[] gids = (String[])o;
        for ( String gid : gids )
        {
            IResponseResolver<Row> responseResolver = new ReadResponseResolver();
            SingleQuorumResponseHandler handler = new SingleQuorumResponseHandler(responseResolver);
            handlers_.put(gid, handler);
        }
    }
}
