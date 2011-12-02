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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collections;

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
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.BytesReadTracker;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import com.ning.compress.lzf.LZFInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IncomingStreamReader
{
    private static final Logger logger = LoggerFactory.getLogger(IncomingStreamReader.class);

    protected final PendingFile localFile;
    protected final PendingFile remoteFile;
    protected final StreamInSession session;
    private final Socket socket;

    public IncomingStreamReader(StreamHeader header, Socket socket) throws IOException
    {
        this.socket = socket;
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
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Receiving stream");
                logger.debug("Creating file for {} with {} estimated keys",
                             localFile.getFilename(),
                             remoteFile.estimatedKeys);
            }

            assert remoteFile.estimatedKeys > 0;
            SSTableReader reader = null;
            logger.debug("Estimated keys {}", remoteFile.estimatedKeys);
            DataInputStream dis = new DataInputStream(new LZFInputStream(socket.getInputStream()));
            try
            {
                reader = streamIn(dis, localFile, remoteFile);
            }
            catch (IOException ex)
            {
                retry();
                throw ex;
            }
            finally
            {
                dis.close();
            }

            session.finished(remoteFile, reader);
        }

        session.closeIfFinished();
    }

    private SSTableReader streamIn(DataInput input, PendingFile localFile, PendingFile remoteFile) throws IOException
    {
        ColumnFamilyStore cfs = Table.open(localFile.desc.ksname).getColumnFamilyStore(localFile.desc.cfname);
        DecoratedKey key;
        SSTableWriter writer = new SSTableWriter(localFile.getFilename(), remoteFile.estimatedKeys);
        CompactionController controller = null;

        try
        {
            BytesReadTracker in = new BytesReadTracker(input);

            for (Pair<Long, Long> section : localFile.sections)
            {
                long length = section.right - section.left;
                long bytesRead = 0;
                while (bytesRead < length)
                {
                    in.reset(0);
                    key = SSTableReader.decodeKey(StorageService.getPartitioner(), localFile.desc, ByteBufferUtil.readWithShortLength(in));
                    long dataSize = SSTableReader.readRowSize(in, localFile.desc);

                    ColumnFamily cached = cfs.getRawCachedRow(key);
                    if (cached != null && remoteFile.type == OperationType.AES && dataSize <= DatabaseDescriptor.getInMemoryCompactionLimit())
                    {
                        // need to update row cache
                        if (controller == null)
                            controller = new CompactionController(cfs, Collections.<SSTableReader>emptyList(), Integer.MIN_VALUE, true);
                        // Note: Because we won't just echo the columns, there is no need to use the PRESERVE_SIZE flag, contrarily to what appendFromStream does below
                        SSTableIdentityIterator iter = new SSTableIdentityIterator(cfs.metadata, in, key, 0, dataSize, IColumnSerializer.Flag.FROM_REMOTE);
                        PrecompactedRow row = new PrecompactedRow(controller, Collections.singletonList(iter));
                        // We don't expire anything so the row shouldn't be empty
                        assert !row.isEmpty();
                        writer.append(row);
                        // row append does not update the max timestamp on its own
                        writer.updateMaxTimestamp(row.maxTimestamp());

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
                    remoteFile.progress += in.getBytesRead();
                }
            }
            return writer.closeAndOpenReader();
        }
        catch (Exception e)
        {
            writer.abort();
            throw FBUtilities.unchecked(e);
        }
    }

    private void retry() throws IOException
    {
        /* Ask the source node to re-stream this file. */
        session.retry(remoteFile);

        /* Delete the orphaned file. */
        FileUtils.deleteWithConfirm(new File(localFile.getFilename()));
    }
}
