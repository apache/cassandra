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
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.io.*;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;

public class IncomingStreamReader
{
    private static final Logger logger = LoggerFactory.getLogger(IncomingStreamReader.class);

    private PendingFile pendingFile;
    private PendingFile lastFile;
    private FileStatus streamStatus;
    private final SocketChannel socketChannel;
    // indicates an transfer initiated by the source, as opposed to one requested by the recipient
    private final boolean initiatedTransfer;
    private final StreamInSession session;

    public IncomingStreamReader(StreamHeader header, SocketChannel socketChannel) throws IOException
    {
        this.socketChannel = socketChannel;
        InetSocketAddress remoteAddress = (InetSocketAddress)socketChannel.socket().getRemoteSocketAddress();
        // pendingFile gets the new context for the local node.
        pendingFile = StreamIn.getContextMapping(header.getStreamFile());
        // lastFile has the old context, which was registered in the manager.
        lastFile = header.getStreamFile();
        initiatedTransfer = header.initiatedTransfer;
        assert pendingFile != null;
        session = StreamInSession.get(remoteAddress.getAddress(), header.getSessionId());
        session.addActiveStream(pendingFile);
        // For transfers setup the status and for replies to requests, prepare the list
        // of available files to request.
        if (initiatedTransfer)
            streamStatus = new FileStatus(lastFile.getFilename(), header.getSessionId());
        else if (header.getPendingFiles() != null)
            session.addFilesToRequest(header.getPendingFiles());
    }

    public void read() throws IOException
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("Receiving stream");
            logger.debug("Creating file for {}", pendingFile.getFilename());
        }
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
            if (initiatedTransfer)
                handleFileStatus(FileStatus.Action.STREAM);
            else
                session.requestFile(lastFile);

            /* Delete the orphaned file. */
            FileUtils.deleteWithConfirm(new File(pendingFile.getFilename()));
            throw ex;
        }
        finally
        {
            fc.close();
            session.removeActiveStream(pendingFile);
        }

        if (logger.isDebugEnabled())
            logger.debug("Removing stream context {}", pendingFile);
        if (initiatedTransfer)
            handleFileStatus(FileStatus.Action.DELETE);
        else
        {
            FileStatusHandler.addSSTable(pendingFile);
            session.finishAndRequestNext(lastFile);
        }
    }

    private void handleFileStatus(FileStatus.Action action) throws IOException
    {
        streamStatus.setAction(action);
        FileStatusHandler.onStatusChange(session, pendingFile, streamStatus);
    }
}
