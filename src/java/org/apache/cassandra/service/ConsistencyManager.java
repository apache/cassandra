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
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Cachetable;
import org.apache.cassandra.utils.ICacheExpungeHook;
import org.apache.cassandra.utils.ICachetable;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;


class ConsistencyManager implements Runnable
{
	private static Logger logger_ = Logger.getLogger(ConsistencyManager.class);
	
	class DigestResponseHandler implements IAsyncCallback
	{
		List<Message> responses_ = new ArrayList<Message>();
		
		public void response(Message msg)
		{
			logger_.debug("Received reponse : " + msg.toString());
			responses_.add(msg);
			if ( responses_.size() == ConsistencyManager.this.replicas_.size() )
				handleDigestResponses();
		}
        
        public void attachContext(Object o)
        {
            throw new UnsupportedOperationException("This operation is not currently supported.");
        }
		
		private void handleDigestResponses()
		{
			DataInputBuffer bufIn = new DataInputBuffer();
			logger_.debug("Handle Digest reponses");
			for( Message response : responses_ )
			{
				byte[] body = response.getMessageBody();            
	            bufIn.reset(body, body.length);
	            try
	            {	               
	                ReadResponse result = ReadResponse.serializer().deserialize(bufIn);
	                byte[] digest = result.digest();
	                if( !Arrays.equals(row_.digest(), digest) )
					{
	                	doReadRepair();
	                	break;
					}
	            }
	            catch( IOException ex )
	            {
	            	logger_.info(LogUtil.throwableToString(ex));
	            }
			}
		}
		
		private void doReadRepair() throws IOException
		{
			IResponseResolver<Row> readResponseResolver = new ReadResponseResolver();
            /* Add the local storage endpoint to the replicas_ list */
            replicas_.add(StorageService.getLocalStorageEndPoint());
			IAsyncCallback responseHandler = new DataRepairHandler(ConsistencyManager.this.replicas_.size(), readResponseResolver);	
			String table = DatabaseDescriptor.getTables().get(0);
            ReadCommand readCommand = constructReadMessage(false);
			// ReadMessage readMessage = new ReadMessage(table, row_.key(), columnFamily_);
            Message message = readCommand.makeReadMessage();
			MessagingService.getMessagingInstance().sendRR(message, replicas_.toArray( new EndPoint[0] ), responseHandler);			
		}
	}
	
	class DataRepairHandler implements IAsyncCallback, ICacheExpungeHook<String, String>
	{
		private List<Message> responses_ = new ArrayList<Message>();
		private IResponseResolver<Row> readResponseResolver_;
		private int majority_;
		
		DataRepairHandler(int responseCount, IResponseResolver<Row> readResponseResolver)
		{
			readResponseResolver_ = readResponseResolver;
			majority_ = (responseCount >> 1) + 1;  
		}
		
		public void response(Message message)
		{
			logger_.debug("Received responses in DataRepairHandler : " + message.toString());
			responses_.add(message);
			if ( responses_.size() == majority_ )
			{
				String messageId = message.getMessageId();
				readRepairTable_.put(messageId, messageId, this);				
			}
		}
        
        public void attachContext(Object o)
        {
            throw new UnsupportedOperationException("This operation is not currently supported.");
        }
		
		public void callMe(String key, String value)
		{
			handleResponses();
		}
		
		private void handleResponses()
		{
			try
			{
				readResponseResolver_.resolve(new ArrayList<Message>(responses_));
			}
			catch ( DigestMismatchException ex )
			{
				logger_.info("We should not be coming here under any circumstances ...");
				logger_.info(LogUtil.throwableToString(ex));
			}
		}
	}

	private static long scheduledTimeMillis_ = 600;
	private static ICachetable<String, String> readRepairTable_ = new Cachetable<String, String>(scheduledTimeMillis_);
	private final Row row_;
	protected final List<EndPoint> replicas_;
	private final ReadCommand readCommand_;

    public ConsistencyManager(Row row, List<EndPoint> replicas, ReadCommand readCommand)
    {
        row_ = row;
        replicas_ = replicas;
        readCommand_ = readCommand;
    }

	public void run()
	{
		logger_.debug(" Run the consistency checks for " + readCommand_.getColumnFamilyName());		
        ReadCommand readCommandDigestOnly = constructReadMessage(true);
		try
		{
			Message messageDigestOnly = readCommandDigestOnly.makeReadMessage();
			IAsyncCallback digestResponseHandler = new DigestResponseHandler();
			MessagingService.getMessagingInstance().sendRR(messageDigestOnly, replicas_.toArray(new EndPoint[replicas_.size()]), digestResponseHandler);
		}
		catch ( IOException ex )
		{
			logger_.info(LogUtil.throwableToString(ex));
		}
	}
    
    private ReadCommand constructReadMessage(boolean isDigestQuery)
    {
        ReadCommand readCommand = readCommand_.copy();
        readCommand.setDigestQuery(isDigestQuery);
        return readCommand;
    }
}
