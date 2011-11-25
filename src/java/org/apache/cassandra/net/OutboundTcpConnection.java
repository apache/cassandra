package org.apache.cassandra.net;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class OutboundTcpConnection extends Thread
{
    private static final Logger logger = LoggerFactory.getLogger(OutboundTcpConnection.class);

    private static final Message CLOSE_SENTINEL = new Message(FBUtilities.getBroadcastAddress(),
                                                              StorageService.Verb.INTERNAL_RESPONSE,
                                                              ArrayUtils.EMPTY_BYTE_ARRAY,
                                                              MessagingService.version_);

    private static final int OPEN_RETRY_DELAY = 100; // ms between retries
    private final BlockingQueue<Pair<Message, String>> queue = new LinkedBlockingQueue<Pair<Message, String>>();
    private final OutboundTcpConnectionPool poolReference;    

    private DataOutputStream out;
    private Socket socket;
    private long completedCount;

    public OutboundTcpConnection(OutboundTcpConnectionPool pool)
    {
        super("WRITE-" + pool.endPoint());
        this.poolReference = pool;
    }

    public void enqueue(Message message, String id)
    {
        try
        {
            queue.put(Pair.create(message, id));
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }

    void closeSocket()
    {
        queue.clear();
        enqueue(CLOSE_SENTINEL, null);
    }

    public void run()
    {
        while (true)
        {
            Pair<Message, String> pair = take();
            Message m = pair.left;
            String id = pair.right;
            if (m == CLOSE_SENTINEL)
            {
                disconnect();
                continue;
            }
            if (socket != null || connect())
                writeConnected(m, id);
            else
                // clear out the queue, else gossip messages back up.
                queue.clear();            
        }
    }

    public int getPendingMessages()
    {
        return queue.size();
    }

    public long getCompletedMesssages()
    {
        return completedCount;
    }

    private void writeConnected(Message message, String id)
    {
        try
        {
            write(message, id, out);
            if (queue.peek() == null)
            {
                out.flush();
            }
        }
        catch (Exception e)
        {
            // Non IO exceptions is likely a programming error so let's not silence it
            if (!(e instanceof IOException))
                logger.error("error writing to " + poolReference.endPoint(), e);
            else if (logger.isDebugEnabled())
                logger.debug("error writing to " + poolReference.endPoint(), e);
            disconnect();
        }
    }

    static void write(Message message, String id, DataOutputStream out) throws IOException
    {
        /*
         Setting up the protocol header. This is 4 bytes long
         represented as an integer. The first 2 bits indicate
         the serializer type. The 3rd bit indicates if compression
         is turned on or off. It is turned off by default. The 4th
         bit indicates if we are in streaming mode. It is turned off
         by default. The 5th-8th bits are reserved for future use.
         The next 8 bits indicate a version number. Remaining 15 bits
         are not used currently.
        */
        int header = 0;
        // Setting up the serializer bit
        header |= MessagingService.serializerType_.ordinal();
        // set compression bit.
        if (false)
            header |= 4;
        // Setting up the version bit
        header |= (message.getVersion() << 8);

        out.writeInt(MessagingService.PROTOCOL_MAGIC);
        out.writeInt(header);
        // compute total Message length for compatibility w/ 0.8 and earlier
        byte[] bytes = message.getMessageBody();
        int total = messageLength(message.header_, id, bytes);
        out.writeInt(total);
        out.writeUTF(id);
        Header.serializer().serialize(message.header_, out, message.getVersion());
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public static int messageLength(Header header, String id, byte[] bytes)
    {
        return 2 + FBUtilities.encodedUTF8Length(id) + header.serializedSize() + 4 + bytes.length;
    }

    private void disconnect()
    {
        if (socket != null)
        {
            try
            {
                socket.close();
            }
            catch (IOException e)
            {
                if (logger.isDebugEnabled())
                    logger.debug("exception closing connection to " + poolReference.endPoint(), e);
            }
            out = null;
            socket = null;
        }
    }

    private Pair<Message, String> take()
    {
        Pair<Message, String> pair;
        try
        {
            pair = queue.take();
            completedCount++;
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
        return pair;
    }

    private boolean connect()
    {
        if (logger.isDebugEnabled())
            logger.debug("attempting to connect to " + poolReference.endPoint());
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() < start + DatabaseDescriptor.getRpcTimeout())
        {
            try
            {
                socket = poolReference.newSocket();
                socket.setKeepAlive(true);
                socket.setTcpNoDelay(true);
                out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 4096));
                return true;
            }
            catch (IOException e)
            {
                socket = null;
                if (logger.isTraceEnabled())
                    logger.trace("unable to connect to " + poolReference.endPoint(), e);
                try
                {
                    Thread.sleep(OPEN_RETRY_DELAY);
                }
                catch (InterruptedException e1)
                {
                    throw new AssertionError(e1);
                }
            }
        }
        return false;
    }
}
