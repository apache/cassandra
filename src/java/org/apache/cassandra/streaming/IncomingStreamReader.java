/*
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
import java.net.Socket;
import java.util.Collections;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.PrecompactedRow;
import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.StreamingMetrics;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.compress.CompressedInputStream;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.BytesReadTracker;
import org.apache.cassandra.utils.Pair;
import com.ning.compress.lzf.LZFInputStream;

public class IncomingStreamReader
{
    private static final Logger logger = LoggerFactory.getLogger(IncomingStreamReader.class);

    protected final PendingFile localFile;
    protected final PendingFile remoteFile;
    protected final StreamInSession session;
    private final InputStream underliningStream;
    private final StreamingMetrics metrics;

    public IncomingStreamReader(StreamHeader header, Socket socket) throws IOException
    {
        socket.setSoTimeout(DatabaseDescriptor.getStreamingSocketTimeout());
        InetAddress host = header.broadcastAddress != null ? header.broadcastAddress
                           : ((InetSocketAddress)socket.getRemoteSocketAddress()).getAddress();
        session = StreamInSession.get(host, header.sessionId);
        session.setSocket(socket);

        session.addFiles(header.pendingFiles);
        // set the current file we are streaming so progress shows up in jmx
        session.setCurrentFile(header.file);
        session.setTable(header.table);
        // pendingFile gets the new context for the local node.
        remoteFile = header.file;
        localFile = remoteFile != null ? StreamIn.getContextMapping(remoteFile) : null;

        if (remoteFile != null)
        {
            if (remoteFile.compressionInfo == null)
                underliningStream = new LZFInputStream(socket.getInputStream());
            else
                underliningStream = new CompressedInputStream(socket.getInputStream(), remoteFile.compressionInfo);
        }
        else
        {
            underliningStream = null;
        }
        metrics = StreamingMetrics.get(socket.getInetAddress());
    }

    /**
     * @throws IOException if reading the remote sstable fails. Will throw an RTE if local write fails.
     */
    public void read() throws IOException
    {
        if (remoteFile != null)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Receiving stream");
                logger.debug("Creating file for {} with {} estimated keys",
                             localFile.getFilename(),
                             remoteFile.estimatedKeys);
            }

            assert remoteFile.estimatedKeys > 0;
            DataInput dis = new DataInputStream(underliningStream);
            try
            {
                SSTableReader reader = streamIn(dis, localFile, remoteFile);
                session.finished(remoteFile, reader);
            }
            catch (IOException ex)
            {
                retry();
                throw ex;
            }
        }

        session.closeIfFinished();
    }

    /**
     * @throws IOException if reading the remote sstable fails. Will throw an RTE if local write fails.
     */
    private SSTableReader streamIn(DataInput input, PendingFile localFile, PendingFile remoteFile) throws IOException
    {
        ColumnFamilyStore cfs = Table.open(localFile.desc.ksname).getColumnFamilyStore(localFile.desc.cfname);
        DecoratedKey key;
        SSTableWriter writer = new SSTableWriter(localFile.getFilename(), remoteFile.estimatedKeys);
        CompactionController controller = new CompactionController(cfs, Collections.<SSTableReader>emptyList(), Integer.MIN_VALUE, true);

        try
        {
            BytesReadTracker in = new BytesReadTracker(input);
            long totalBytesRead = 0;

            for (Pair<Long, Long> section : localFile.sections)
            {
                long length = section.right - section.left;
                // skip to beginning of section inside chunk
                if (remoteFile.compressionInfo != null)
                    ((CompressedInputStream) underliningStream).position(section.left);
                long bytesRead = 0;
                while (bytesRead < length)
                {
                    in.reset(0);
                    key = SSTableReader.decodeKey(StorageService.getPartitioner(), localFile.desc, ByteBufferUtil.readWithShortLength(in));
                    long dataSize = SSTableReader.readRowSize(in, localFile.desc);

                    if (cfs.containsCachedRow(key) && remoteFile.type == OperationType.AES && dataSize <= DatabaseDescriptor.getInMemoryCompactionLimit())
                    {
                        // need to update row cache
                        // Note: Because we won't just echo the columns, there is no need to use the PRESERVE_SIZE flag, contrarily to what appendFromStream does below
                        SSTableIdentityIterator iter = new SSTableIdentityIterator(cfs.metadata, in, localFile.getFilename(), key, 0, dataSize, IColumnSerializer.Flag.FROM_REMOTE);
                        PrecompactedRow row = new PrecompactedRow(controller, Collections.singletonList(iter));
                        // We don't expire anything so the row shouldn't be empty
                        assert !row.isEmpty();
                        writer.append(row);

                        // update cache
                        ColumnFamily cf = row.getFullColumnFamily();
                        cfs.updateRowCache(key, cf);
                    }
                    else
                    {
                        writer.appendFromStream(key, cfs.metadata, dataSize, in);
                        cfs.invalidateCachedRow(key);
                    }

                    bytesRead += in.getBytesRead();
                    // when compressed, report total bytes of decompressed chunks since remoteFile.size is the sum of chunks transferred
                    remoteFile.progress += remoteFile.compressionInfo != null
                                           ? ((CompressedInputStream) underliningStream).uncompressedBytes()
                                           : in.getBytesRead();
                    totalBytesRead += in.getBytesRead();
                }
            }
            StreamingMetrics.totalIncomingBytes.inc(totalBytesRead);
            metrics.incomingBytes.inc(totalBytesRead);
            return writer.closeAndOpenReader();
        }
        catch (Throwable e)
        {
            writer.abort();
            if (e instanceof IOException)
                throw (IOException) e;
            else
                throw Throwables.propagate(e);
        }
    }

    private void retry()
    {
        /* Ask the source node to re-stream this file. */
        session.retry(remoteFile);

        /* Delete the orphaned file. */
        if (new File(localFile.getFilename()).isFile())
            FileUtils.deleteWithConfirm(new File(localFile.getFilename()));
    }
}
