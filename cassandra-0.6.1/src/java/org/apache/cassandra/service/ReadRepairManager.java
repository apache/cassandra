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
import java.io.IOError;

import org.apache.cassandra.db.RowMutationMessage;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ExpiringMap;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.cache.ICacheExpungeHook;

import org.apache.log4j.Logger;


/*
 * This class manages the read repairs . This is a singleton class
 * it basically uses the cache table construct to schedule writes that have to be 
 * made for read repairs. 
 * A cachetable is created which wakes up every n  milliseconds specified by 
 * expirationTimeInMillis and calls a global hook function on pending entries 
 * This function basically sends the message to the appropriate servers to update them
 * with the latest changes.
 */
class ReadRepairManager
{
    private static final Logger logger_ = Logger.getLogger(ReadRepairManager.class);
    private static final long expirationTimeInMillis = 2000;
    public static final ReadRepairManager instance = new ReadRepairManager();

	/*
	 * This is the internal class which actually
	 * implements the global hook function called by the read repair manager
	 */
	static class ReadRepairPerformer implements ICacheExpungeHook<String, Message>
	{
		/*
		 * The hook function which takes the end point and the row mutation that 
		 * needs to be sent to the end point in order 
		 * to perform read repair.
		 */
		public void callMe(String target, Message message)
		{
			String[] pieces = FBUtilities.strip(target, ":");
            InetAddress to = null;
            try
            {
                to = InetAddress.getByName(pieces[0]);
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
            MessagingService.instance.sendOneWay(message, to);
		}

	}

	private ExpiringMap<String, Message> readRepairTable_ = new ExpiringMap<String, Message>(expirationTimeInMillis, new ReadRepairManager.ReadRepairPerformer());

	protected ReadRepairManager()
	{

	}

	/*
	 * Schedules a read repair.
	 * @param target endpoint on which the read repair should happen
	 * @param rowMutationMessage the row mutation message that has the repaired row.
	 */
	public void schedule(InetAddress target, RowMutationMessage rowMutationMessage)
	{
        try
        {
            Message message = rowMutationMessage.makeRowMutationMessage(StorageService.Verb.READ_REPAIR);
    		String key = target.getHostAddress() + ":" + message.getMessageId();
    		readRepairTable_.put(key, message);
        }
        catch (IOException ex)
        {
            throw new IOError(ex);
        }
    }
}
