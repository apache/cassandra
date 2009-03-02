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

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.locks.*;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.RowMutationMessage;
import org.apache.cassandra.db.SuperColumn;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.Header;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Cachetable;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ICacheExpungeHook;
import org.apache.cassandra.utils.ICachetable;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;



/*
 * This class manages the read repairs . This is a singleton class
 * it basically uses the cache table construct to schedule writes that have to be 
 * made for read repairs. 
 * A cachetable is created which wakes up every n  milliseconds specified by 
 * expirationTimeInMillis and calls a global hook fn on pending entries 
 * This fn basically sends the message to the appropriate servers to update them
 * with the latest changes.
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */
class ReadRepairManager
{
    private static Logger logger_ = Logger.getLogger(ReadRepairManager.class);
	private static final long expirationTimeInMillis = 2000;
	private static Lock lock_ = new ReentrantLock();
	private static ReadRepairManager self_ = null;

	/*
	 * This is the internal class which actually
	 * implements the global hook fn called by the readrepair manager
	 */
	static class ReadRepairPerformer implements
			ICacheExpungeHook<String, Message>
	{
		private static Logger logger_ = Logger.getLogger(ReadRepairPerformer.class);
		/*
		 * The hook fn which takes the end point and the row mutation that 
		 * needs to be sent to the end point in order 
		 * to perform read repair.
		 */
		public void callMe(String target,
				Message message)
		{
			String[] pieces = FBUtilities.strip(target, ":");
			EndPoint to = new EndPoint(pieces[0], Integer.parseInt(pieces[1]));
			MessagingService.getMessagingInstance().sendOneWay(message, to);			
		}

	}

	private ICachetable<String, Message> readRepairTable_ = new Cachetable<String, Message>(expirationTimeInMillis, new ReadRepairManager.ReadRepairPerformer());

	protected ReadRepairManager()
	{

	}

	public  static ReadRepairManager instance()
	{
		if (self_ == null)
		{
            lock_.lock();
            try
            {
                if ( self_ == null )
                    self_ = new ReadRepairManager();
            }
            finally
            {
                lock_.unlock();
            }
		}
		return self_;
	}

	/*
	 * This is the fn that should be used to scheule a read repair 
	 * specify a endpoint on whcih the read repair should happen and the row mutaion
	 * message that has the repaired row.
	 */
	public void schedule(EndPoint target, RowMutationMessage rowMutationMessage)
	{
        /*
		Message message = new Message(StorageService.getLocalStorageEndPoint(),
				StorageService.mutationStage_,
				StorageService.readRepairVerbHandler_, new Object[]
				{ rowMutationMessage });
        */
        try
        {
            Message message = RowMutationMessage.makeRowMutationMessage(rowMutationMessage, StorageService.readRepairVerbHandler_);
    		String key = target + ":" + message.getMessageId();
    		readRepairTable_.put(key, message);
        }
        catch ( IOException ex )
        {
            logger_.info(LogUtil.throwableToString(ex));
        }
	}
}
