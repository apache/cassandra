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

import org.apache.cassandra.utils.FBUtilities;
import org.apache.log4j.Logger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.WrappedRunnable;

public class FileStreamTask extends WrappedRunnable
{
    private static Logger logger = Logger.getLogger( FileStreamTask.class );
    
    public static final int CHUNK_SIZE = 64*1024*1024;

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
        SocketChannel channel = SocketChannel.open();
        // force local binding on correctly specified interface.
        channel.socket().bind(new InetSocketAddress(FBUtilities.getLocalAddress(), 0));
        // obey the unwritten law that all nodes on a cluster must use the same storage port.
        channel.connect(new InetSocketAddress(to, DatabaseDescriptor.getStoragePort()));
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

}
