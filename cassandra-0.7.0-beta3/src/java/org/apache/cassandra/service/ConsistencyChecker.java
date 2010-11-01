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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

import org.apache.cassandra.utils.WrappedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class ConsistencyChecker implements Runnable
{
    private static Logger logger_ = LoggerFactory.getLogger(ConsistencyChecker.class);

    private static ScheduledExecutorService executor_ = new ScheduledThreadPoolExecutor(1); // TODO add JMX

    private final String table_;
    private final Row row_;
    protected final List<InetAddress> replicas_;
    private final ReadCommand readCommand_;

    public ConsistencyChecker(String table, Row row, List<InetAddress> endpoints, ReadCommand readCommand)
    {
        table_ = table;
        row_ = row;
        replicas_ = endpoints;
        readCommand_ = readCommand;
    }

    public void run()
	{
        ReadCommand readCommandDigestOnly = constructReadMessage(true);
		try
		{
			Message message = readCommandDigestOnly.makeReadMessage();
            if (logger_.isDebugEnabled())
              logger_.debug("Reading consistency digest for " + readCommand_.key + " from " + message.getMessageId() + "@[" + StringUtils.join(replicas_, ", ") + "]");

            MessagingService.instance.addCallback(new DigestResponseHandler(), message.getMessageId());
            for (InetAddress endpoint : replicas_)
            {
                if (!endpoint.equals(FBUtilities.getLocalAddress()))
                    MessagingService.instance.sendOneWay(message, endpoint);
            }
		}
		catch (IOException ex)
		{
			throw new RuntimeException(ex);
		}
	}

    private ReadCommand constructReadMessage(boolean isDigestQuery)
    {
        ReadCommand readCommand = readCommand_.copy();
        readCommand.setDigestQuery(isDigestQuery);
        return readCommand;
    }

    class DigestResponseHandler implements IAsyncCallback
	{
        private boolean repairInvoked;

		public synchronized void response(Message response)
		{
            if (repairInvoked)
                return;

            try
            {
                byte[] body = response.getMessageBody();
                ByteArrayInputStream bufIn = new ByteArrayInputStream(body);
                ReadResponse result = ReadResponse.serializer().deserialize(new DataInputStream(bufIn));
                ByteBuffer digest = result.digest();

                if (!ColumnFamily.digest(row_.cf).equals(digest))
                {
                    IResponseResolver<Row> readResponseResolver = new ReadResponseResolver(table_);
                    IAsyncCallback responseHandler;
                    if (replicas_.contains(FBUtilities.getLocalAddress()))
                        responseHandler = new DataRepairHandler(row_, replicas_.size(), readResponseResolver);
                    else
                        responseHandler = new DataRepairHandler(replicas_.size(), readResponseResolver);

                    ReadCommand readCommand = constructReadMessage(false);
                    Message message = readCommand.makeReadMessage();
                    if (logger_.isDebugEnabled())
                      logger_.debug("Performing read repair for " + readCommand_.key + " to " + message.getMessageId() + "@[" + StringUtils.join(replicas_, ", ") + "]");
                    MessagingService.instance.addCallback(responseHandler, message.getMessageId());
                    for (InetAddress endpoint : replicas_)
                    {
                        if (!endpoint.equals(FBUtilities.getLocalAddress()))
                            MessagingService.instance.sendOneWay(message, endpoint);
                    }

                    repairInvoked = true;
                }
            }
            catch (Exception e)
            {
                throw new RuntimeException("Error handling responses for " + row_, e);
            }
        }
    }

	static class DataRepairHandler implements IAsyncCallback
	{
		private final Collection<Message> responses_ = new LinkedBlockingQueue<Message>();
		private final IResponseResolver<Row> readResponseResolver_;
		private final int majority_;
		
		DataRepairHandler(int responseCount, IResponseResolver<Row> readResponseResolver)
		{
			readResponseResolver_ = readResponseResolver;
			majority_ = (responseCount / 2) + 1;  
		}

        public DataRepairHandler(Row localRow, int responseCount, IResponseResolver<Row> readResponseResolver) throws IOException
        {
            this(responseCount, readResponseResolver);
            // wrap localRow in a response Message so it doesn't need to be special-cased in the resolver
            ReadResponse readResponse = new ReadResponse(localRow);
            DataOutputBuffer out = new DataOutputBuffer();
            ReadResponse.serializer().serialize(readResponse, out);
            byte[] bytes = new byte[out.getLength()];
            System.arraycopy(out.getData(), 0, bytes, 0, bytes.length);
            responses_.add(new Message(FBUtilities.getLocalAddress(), StorageService.Verb.READ_RESPONSE, bytes));
        }

        // synchronized so the " == majority" is safe
		public synchronized void response(Message message)
		{
			if (logger_.isDebugEnabled())
			  logger_.debug("Received responses in DataRepairHandler : " + message.toString());
			responses_.add(message);
            if (responses_.size() == majority_)
            {
                Runnable runnable = new WrappedRunnable()
                {
                    public void runMayThrow() throws IOException, DigestMismatchException
                    {
                        readResponseResolver_.resolve(responses_);
                    }
                };
                // give remaining replicas until timeout to reply and get added to responses_
                executor_.schedule(runnable, DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
            }
        }
    }
}
