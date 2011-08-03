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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;

public class IncomingStreamReader
{
    private static final Logger logger = LoggerFactory.getLogger(IncomingStreamReader.class);

    protected final PendingFile localFile;
    protected final PendingFile remoteFile;
    private final SocketChannel socketChannel;
    protected final StreamInSession session;

    public IncomingStreamReader(StreamHeader header, Socket socket) throws IOException
    {
        this.socketChannel = socket.getChannel();
        InetSocketAddress remoteAddress = (InetSocketAddress)socket.getRemoteSocketAddress();
        session = StreamInSession.get(remoteAddress.getAddress(), header.sessionId);
        session.addFiles(header.pendingFiles);
        // set the current file we are streaming so progress shows up in jmx
        session.setCurrentFile(header.file);
        session.setTable(header.table);
        // pendingFile gets the new context for the local node.
        remoteFile = header.file;
        localFile = remoteFile != null ? StreamIn.getContextMapping(remoteFile) : null;
    }

    public void read() throws IOException
    {
        if (remoteFile != null)
            readFile();

        session.closeIfFinished();
    }

    protected void readFile() throws IOException
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("Receiving stream");
            logger.debug("Creating file for {}", localFile.getFilename());
        }
        FileOutputStream fos = new FileOutputStream(localFile.getFilename(), true);
        FileChannel fc = fos.getChannel();

        long offset = 0;
        try
        {
            for (Pair<Long, Long> section : localFile.sections)
            {
                long length = section.right - section.left;
                long bytesRead = 0;
                while (bytesRead < length)
                {
                    bytesRead = readnwrite(length, bytesRead, offset, fc);
                }
                offset += length;
            }
        }
        catch (IOException ex)
        {
            /* Ask the source node to re-stream this file. */
            session.retry(remoteFile);

            /* Delete the orphaned file. */
            FileUtils.deleteWithConfirm(new File(localFile.getFilename()));
            throw ex;
        }
        finally
        {
            fc.close();
        }

        session.finished(remoteFile, localFile);
    }

    protected long readnwrite(long length, long bytesRead, long offset, FileChannel fc) throws IOException
    {
        long toRead = Math.min(FileStreamTask.CHUNK_SIZE, length - bytesRead);
        long lastRead = fc.transferFrom(socketChannel, offset + bytesRead, toRead);
        // if the other side fails, we will not get an exception, but instead transferFrom will constantly return 0 byte read
        // and we would thus enter an infinite loop. So intead, if no bytes are tranferred we assume the other side is dead and
        // raise an exception (that will be catch belove and 'the right thing' will be done).
        if (lastRead == 0)
            throw new IOException("Transfer failed for remote file " + remoteFile);
        bytesRead += lastRead;
        remoteFile.progress += lastRead;
        return bytesRead;
    }
}
