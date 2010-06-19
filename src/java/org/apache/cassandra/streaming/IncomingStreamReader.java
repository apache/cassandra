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

import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.io.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.net.FileStreamTask;
import org.apache.cassandra.utils.Pair;

public class IncomingStreamReader
{
    private static Logger logger = LoggerFactory.getLogger(IncomingStreamReader.class);
    private PendingFile pendingFile;
    private FileStatus streamStatus;
    private SocketChannel socketChannel;

    public IncomingStreamReader(SocketChannel socketChannel)
    {
        this.socketChannel = socketChannel;
        InetSocketAddress remoteAddress = (InetSocketAddress)socketChannel.socket().getRemoteSocketAddress();
        // this is the part where we are assuming files come in order from a particular host. it is brittle because
        // any node could send a stream message to this host and it would just assume it is receiving the next file.
        pendingFile = StreamInManager.getNextIncomingFile(remoteAddress.getAddress());
        StreamInManager.activeStreams.put(remoteAddress.getAddress(), pendingFile);
        assert pendingFile != null;
        streamStatus = StreamInManager.getStreamStatus(remoteAddress.getAddress());
        assert streamStatus != null;
    }

    public void read() throws IOException
    {
        logger.debug("Receiving stream");
        InetSocketAddress remoteAddress = (InetSocketAddress)socketChannel.socket().getRemoteSocketAddress();
        if (logger.isDebugEnabled())
          logger.debug("Creating file for " + pendingFile.getFilename());
        FileOutputStream fos = new FileOutputStream(pendingFile.getFilename(), true);
        FileChannel fc = fos.getChannel();

        long offset = 0;
        try
        {
            for (Pair<Long, Long> section : pendingFile.sections)
            {
                long length = section.right - section.left;
                long bytesRead = 0;
                while (bytesRead < length)
                    bytesRead += fc.transferFrom(socketChannel, offset + bytesRead, length - bytesRead);
                offset += length;
            }
        }
        catch (IOException ex)
        {
            logger.debug("Receiving stream: recovering from IO error");
            /* Ask the source node to re-stream this file. */
            streamStatus.setAction(FileStatus.Action.STREAM);
            handleFileStatus(remoteAddress.getAddress());
            /* Delete the orphaned file. */
            FileUtils.deleteWithConfirm(new File(pendingFile.getFilename()));
            throw ex;
        }
        finally
        {
            fc.close();
            StreamInManager.activeStreams.remove(remoteAddress.getAddress(), pendingFile);
        }

        if (logger.isDebugEnabled())
            logger.debug("Removing stream context " + pendingFile);
        streamStatus.setAction(FileStatus.Action.DELETE);
        handleFileStatus(remoteAddress.getAddress());
    }

    private void handleFileStatus(InetAddress remoteHost) throws IOException
    {
        /*
         * Streaming is complete. If all the data that has to be received inform the sender via
         * the stream completion callback so that the source may perform the requisite cleanup.
        */
        FileStatusHandler handler = StreamInManager.getFileStatusHandler(remoteHost);
        if (handler != null)
            handler.onStatusChange(remoteHost, pendingFile, streamStatus);
    }
}
