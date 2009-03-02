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

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.WriteResponseMessage;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;
import org.apache.cassandra.utils.*;
/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class QuorumResponseHandler<T> implements IAsyncCallback
{
    private static Logger logger_ = Logger.getLogger( QuorumResponseHandler.class );
    private Lock lock_ = new ReentrantLock();
    private Condition condition_;
    private int responseCount_;
    private List<Message> responses_ = new ArrayList<Message>();
    private IResponseResolver<T> responseResolver_;
    private AtomicBoolean done_ = new AtomicBoolean(false);
    
    public QuorumResponseHandler(int responseCount, IResponseResolver<T> responseResolver)
    {        
        condition_ = lock_.newCondition();
        responseCount_ = responseCount;
        responseResolver_ =  responseResolver;
    }
    
    public void  setResponseCount(int responseCount)
    {
        responseCount_ = responseCount;
    }
    
    public T get() throws TimeoutException, DigestMismatchException
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
                for ( Message message : responses_ )
                {
                    sb.append(message.getFrom());                    
                }                
                throw new TimeoutException("Operation timed out - received only " +  responses_.size() + " responses from " + sb.toString() + " .");
            }
        }
        finally
        {
            lock_.unlock();
            for(Message response : responses_)
            {
            	MessagingService.removeRegisteredCallback( response.getMessageId() );
            }
        }
        logger_.info("QuorumResponseHandler: " + (System.currentTimeMillis() - startTime)
                + " ms.");

    	return responseResolver_.resolve( responses_);
    }
    
    public void response(Message message)
    {
        lock_.lock();
        try
        {
        	int majority = (responseCount_ >> 1) + 1;            
            if ( !done_.get() )
            {
            	responses_.add( message );
            	if ( responses_.size() >= majority && responseResolver_.isDataPresent(responses_))
            	{
            		done_.set(true);
            		condition_.signal();            	
            	}
            }
        }
        finally
        {
            lock_.unlock();
        }
    }
}
