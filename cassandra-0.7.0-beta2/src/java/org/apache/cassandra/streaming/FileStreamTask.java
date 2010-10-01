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

package org.apache.cassandra.streaming;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;

import org.apache.cassandra.streaming.StreamHeader;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.WrappedRunnable;

public class FileStreamTask extends WrappedRunnable
{
    private static Logger logger = LoggerFactory.getLogger( FileStreamTask.class );
    
    // 10MB chunks
    public static final int CHUNK_SIZE = 10*1024*1024;
    // around 10 minutes at the default rpctimeout
    public static final int MAX_CONNECT_ATTEMPTS = 8;

    private final StreamHeader header;
    private final InetAddress to;
    
    public FileStreamTask(StreamHeader header, InetAddress to)
    {
        this.header = header;
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
            logger.debug("Done streaming " + header.file);
    }

    private void stream(SocketChannel channel) throws IOException
    {
        ByteBuffer buffer = MessagingService.constructStreamHeader(header, false);
        channel.write(buffer);
        assert buffer.remaining() == 0;
        if (header.file == null)
            return;

        RandomAccessFile raf = new RandomAccessFile(new File(header.file.getFilename()), "r");
        try
        {
            FileChannel fc = raf.getChannel();
            // stream sections of the file as returned by PendingFile.currentSection
            for (Pair<Long, Long> section : header.file.sections)
            {
                long length = section.right - section.left;
                long bytesTransferred = 0;
                while (bytesTransferred < length)
                {
                    long toTransfer = Math.min(CHUNK_SIZE, length - bytesTransferred);
                    long lastWrite = fc.transferTo(section.left + bytesTransferred, toTransfer, channel);
                    bytesTransferred += lastWrite;
                    header.file.progress += lastWrite;
                }
                if (logger.isDebugEnabled())
                    logger.debug("Bytes transferred " + bytesTransferred + "/" + header.file.size);
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
                logger.warn("Failed attempt " + attempts + " to connect to " + to + " to stream " + header.file + ". Retrying in " + waitms + " ms. (" + e + ")");
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
