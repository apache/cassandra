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

package org.apache.cassandra.net;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;

import org.apache.cassandra.streaming.StreamOutManager;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.WrappedRunnable;

public class FileStreamTask extends WrappedRunnable
{
    private static Logger logger = LoggerFactory.getLogger( FileStreamTask.class );
    
    public static final int CHUNK_SIZE = 32*1024*1024;
    // around 10 minutes at the default rpctimeout
    public static final int MAX_CONNECT_ATTEMPTS = 8;

    private final String file;
    private final long startPosition;
    private final long endPosition;
    private final InetAddress to;

    FileStreamTask(String file, long startPosition, long endPosition, InetAddress from, InetAddress to)
    {
        this.file = file;
        this.startPosition = startPosition;
        this.endPosition = endPosition;
        this.to = to;
    }
    
    public void runMayThrow() throws IOException
    {
        SocketChannel channel = connect();

        // successfully connected: stream.
        // (at this point, if we fail, it is the receiver's job to re-request)
        try
        {
            stream(channel);
        }
        finally
        {
            try
            {
                channel.close();
            }
            catch (IOException e)
            {
                if (logger.isDebugEnabled())
                    logger.debug("error closing socket", e);
            }
        }
        if (logger.isDebugEnabled())
          logger.debug("Done streaming " + file);
    }

    private void stream(SocketChannel channel) throws IOException
    {
        long start = startPosition;
        RandomAccessFile raf = new RandomAccessFile(new File(file), "r");
        try
        {
            FileChannel fc = raf.getChannel();

            ByteBuffer buffer = MessagingService.constructStreamHeader(false);
            channel.write(buffer);
            assert buffer.remaining() == 0;

            while (start < endPosition)
            {
                long bytesTransferred = fc.transferTo(start, CHUNK_SIZE, channel);
                if (logger.isDebugEnabled())
                    logger.debug("Bytes transferred " + bytesTransferred);
                start += bytesTransferred;
                StreamOutManager.get(to).update(file, start);
            }
        }
        finally
        {
            try
            {
                raf.close();
            }
            catch (IOException e)
            {
                throw new AssertionError(e);
            }
        }
    }

    /**
     * Connects to the destination, with backoff for failed attempts.
     * TODO: all nodes on a cluster must currently use the same storage port
     * @throws IOException If all attempts fail.
     */
    private SocketChannel connect() throws IOException
    {
        SocketChannel channel = SocketChannel.open();
        // force local binding on correctly specified interface.
        channel.socket().bind(new InetSocketAddress(FBUtilities.getLocalAddress(), 0));
        int attempts = 0;
        while (true)
        {
            try
            {
                channel.connect(new InetSocketAddress(to, DatabaseDescriptor.getStoragePort()));
                // success
                return channel;
            }
            catch (IOException e)
            {
                if (++attempts >= MAX_CONNECT_ATTEMPTS)
                    throw e;

                long waitms = DatabaseDescriptor.getRpcTimeout() * (long)Math.pow(2, attempts);
                logger.warn("Failed attempt " + attempts + " to connect to " + to + " to stream " + file + ". Retrying in " + waitms + " ms. (" + e + ")");
                try
                {
                    Thread.sleep(waitms);
                }
                catch (InterruptedException wtf)
                {
                    throw new RuntimeException(wtf);
                }
            }
        }
    }
}
