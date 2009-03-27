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

package org.apache.cassandra.db;

import java.util.Collection;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.concurrent.ThreadFactoryImpl;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.IComponentShutdown;
import org.apache.cassandra.service.IResponseResolver;
import org.apache.cassandra.service.QuorumResponseHandler;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.WriteResponseResolver;
import org.apache.log4j.Logger;


/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class HintedHandOffManager implements IComponentShutdown
{
    private static HintedHandOffManager instance_;
    private static Lock lock_ = new ReentrantLock();
    private static Logger logger_ = Logger.getLogger(HintedHandOffManager.class);
    public static final String key_ = "HintedHandOffKey";
    final static long intervalInMins_ = 20;
    private ScheduledExecutorService executor_ = new DebuggableScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("HINTED-HANDOFF-POOL"));


    public static HintedHandOffManager instance()
    {
        if ( instance_ == null )
        {
            lock_.lock();
            try
            {
                if ( instance_ == null )
                    instance_ = new HintedHandOffManager();
            }
            finally
            {
                lock_.unlock();
            }
        }
        return instance_;
    }

    class HintedHandOff implements Runnable
    {
        private ColumnFamilyStore columnFamilyStore_ = null;
        private EndPoint endPoint_ = null;

        HintedHandOff(ColumnFamilyStore columnFamilyStore)
        {
        	columnFamilyStore_ = columnFamilyStore;
        }
        HintedHandOff(EndPoint endPoint)
        {
        	endPoint_ = endPoint;
        }

        private boolean sendMessage(String endpointAddress, String key) throws Exception
        {
        	boolean success = false; // TODO : fix the hack we need to make sure the data is written on the other end.
        	if(FailureDetector.instance().isAlive(new EndPoint(endpointAddress, DatabaseDescriptor.getControlPort())))
        	{
        		success = true;
        	}
        	else
        	{
        		return success;
        	}
        	Table table = Table.open(DatabaseDescriptor.getTables().get(0));
        	Row row = null;
        	row = table.get(key);
        	RowMutation rm = new RowMutation(DatabaseDescriptor.getTables().get(0), row);
			RowMutationMessage rmMsg = new RowMutationMessage(rm);
			Message message = RowMutationMessage.makeRowMutationMessage( rmMsg );
			EndPoint endPoint = new EndPoint(endpointAddress, DatabaseDescriptor.getStoragePort());
			MessagingService.getMessagingInstance().sendOneWay(message, endPoint);
			return success;
        }

        private void deleteEndPoint(String endpointAddress, String key) throws Exception
        {
        	RowMutation rm = new RowMutation(DatabaseDescriptor.getTables().get(0), key_);
        	rm.delete(Table.hints_ + ":" + key + ":" + endpointAddress);
        	rm.apply();
        }

        private void deleteKey(String key) throws Exception
        {
        	RowMutation rm = new RowMutation(DatabaseDescriptor.getTables().get(0), key_);
        	rm.delete(Table.hints_ + ":" + key);
        	rm.apply();
        }

        private void runHints()
        {
            logger_.debug("Started  hinted handoff " + columnFamilyStore_.columnFamily_);

            // 1. Scan through all the keys that we need to handoff
            // 2. For each key read the list of recepients and send
            // 3. Delete that recepient from the key if write was successful
            // 4. If all writes were success for a given key we can even delete the key .
            // 5. Now force a flush
            // 6. Do major compaction to clean up all deletes etc.
            // 7. I guess we r done
            Table table =  Table.open(DatabaseDescriptor.getTables().get(0));
            ColumnFamily hintedColumnFamily = null;
            boolean success = false;
            boolean allsuccess = true;
            try
            {
            	hintedColumnFamily = table.get(key_, Table.hints_);
            	if(hintedColumnFamily == null)
            	{
                    // Force flush now
                    columnFamilyStore_.forceFlush();
            		return;
            	}
            	Collection<IColumn> keys = hintedColumnFamily.getAllColumns();
            	if(keys != null)
            	{
                	for(IColumn key : keys)
                	{
                		// Get all the endpoints for teh key
                		Collection<IColumn> endpoints =  key.getSubColumns();
                		allsuccess = true;
                		if ( endpoints != null )
                		{
                			for(IColumn endpoint : endpoints )
                			{
                				success = sendMessage(endpoint.name(), key.name());
                				if(success)
                				{
                					// Delete the endpoint from the list
                					deleteEndPoint(endpoint.name(), key.name());
                				}
                				else
                				{
                					allsuccess = false;
                				}
                			}
                		}
                		if(endpoints == null || allsuccess)
                		{
                			// Delete the key itself.
                			deleteKey(key.name());
                		}
                	}
            	}
                // Force flush now
                columnFamilyStore_.forceFlush();

                // Now do a major compaction
                columnFamilyStore_.forceCompaction(null, null, 0, null);
            }
            catch ( Exception ex)
            {
            	logger_.warn(ex.getMessage());
            }
            logger_.debug("Finished hinted handoff ..."+columnFamilyStore_.columnFamily_);
        }

        private void runDeliverHints(EndPoint to)
        {
            logger_.debug("Started  hinted handoff for endPoint " + endPoint_.getHost());

            // 1. Scan through all the keys that we need to handoff
            // 2. For each key read the list of recepients if teh endpoint matches send
            // 3. Delete that recepient from the key if write was successful

            Table table =  Table.open(DatabaseDescriptor.getTables().get(0));
            ColumnFamily hintedColumnFamily = null;
            boolean success = false;
            try
            {
            	hintedColumnFamily = table.get(key_, Table.hints_);
            	if(hintedColumnFamily == null)
            		return;
            	Collection<IColumn> keys = hintedColumnFamily.getAllColumns();
            	if(keys != null)
            	{
                	for(IColumn key : keys)
                	{
                		// Get all the endpoints for teh key
                		Collection<IColumn> endpoints =  key.getSubColumns();
                		if ( endpoints != null )
                		{
                			for(IColumn endpoint : endpoints )
                			{
                				if(endpoint.name().equals(endPoint_.getHost()))
                				{
	                				success = sendMessage(endpoint.name(), key.name());
	                				if(success)
	                				{
	                					// Delete the endpoint from the list
	                					deleteEndPoint(endpoint.name(), key.name());
	                				}
                				}
                			}
                		}
                		if(endpoints == null)
                		{
                			// Delete the key itself.
                			deleteKey(key.name());
                		}
                	}
            	}
            }
            catch ( Exception ex)
            {
            	logger_.warn(ex.getMessage());
            }
            logger_.debug("Finished hinted handoff for endpoint ..." + endPoint_.getHost());
        }

        public void run()
        {
        	if(endPoint_ == null)
        	{
        		runHints();
        	}
        	else
        	{
        		runDeliverHints(endPoint_);
        	}

        }
    }

    public HintedHandOffManager()
    {
    	StorageService.instance().registerComponentForShutdown(this);
    }

    public void submit(ColumnFamilyStore columnFamilyStore)
    {
    	executor_.scheduleWithFixedDelay(new HintedHandOff(columnFamilyStore), HintedHandOffManager.intervalInMins_,
    			HintedHandOffManager.intervalInMins_, TimeUnit.MINUTES);
    }

    /*
     * This method is used to deliver hints to a particular endpoint.
     * When we learn that some endpoint is back up we deliver the data
     * to him via an event driven mechanism.
    */
    public void deliverHints(EndPoint to)
    {
    	executor_.submit(new HintedHandOff(to));
    }

    public void shutdown()
    {
    	executor_.shutdownNow();
    }
}
