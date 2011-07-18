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
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.PrecompactedRow;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.BytesReadTracker;
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
            logger.debug("Creating file for {} with {} estimated keys",
                         localFile.getFilename(),
                         remoteFile.estimatedKeys);
        }

        SSTableReader reader = null;
        if (remoteFile.estimatedKeys > 0)
        {
            logger.debug("Estimated keys {}", remoteFile.estimatedKeys);
            DataInputStream dis = new DataInputStream(socketChannel.socket().getInputStream());
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
        }
        else
        {
            // backwards compatibility path
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
                retry();
                throw ex;
            }
            finally
            {
                fc.close();
            }
        }

        session.finished(remoteFile, localFile, reader);
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

    private SSTableReader streamIn(DataInput input, PendingFile localFile, PendingFile remoteFile) throws IOException
    {
        ColumnFamilyStore cfs = Table.open(localFile.desc.ksname).getColumnFamilyStore(localFile.desc.cfname);
        DecoratedKey key;
        SSTableWriter writer = new SSTableWriter(localFile.getFilename(), remoteFile.estimatedKeys);
        CompactionController controller = null;

        BytesReadTracker in = new BytesReadTracker(input);

        for (Pair<Long, Long> section : localFile.sections)
        {
            long length = section.right - section.left;
            long bytesRead = 0;
            while (bytesRead < length)
            {
                in.reset();
                key = SSTableReader.decodeKey(StorageService.getPartitioner(), localFile.desc, ByteBufferUtil.readWithShortLength(in));
                long dataSize = SSTableReader.readRowSize(in, localFile.desc);
                ColumnFamily cf = null;
                if (cfs.metadata.getDefaultValidator().isCommutative())
                {
                    // take care of counter column family
                    if (controller == null)
                        controller = new CompactionController(cfs, Collections.<SSTableReader>emptyList(), Integer.MAX_VALUE, true);
                    SSTableIdentityIterator iter = new SSTableIdentityIterator(cfs.metadata, in, key, 0, dataSize, true);
                    AbstractCompactedRow row = controller.getCompactedRow(iter);
                    writer.append(row);

                    if (row instanceof PrecompactedRow)
                    {
                        // we do not purge so we should not get a null here
                        cf = ((PrecompactedRow)row).getFullColumnFamily();
                    }
                }
                else
                {
                    // skip BloomFilter
                    IndexHelper.skipBloomFilter(in);
                    // skip Index
                    IndexHelper.skipIndex(in);

                    // restore ColumnFamily
                    cf = ColumnFamily.create(cfs.metadata);
                    ColumnFamily.serializer().deserializeFromSSTableNoColumns(cf, in);
                    ColumnFamily.serializer().deserializeColumns(in, cf, true, true);

                    // write key and cf
                    writer.append(key, cf);
                }

                // update cache
                ColumnFamily cached = cfs.getRawCachedRow(key);
                if (cached != null)
                {
                    switch (remoteFile.type)
                    {
                        case AES:
                            if (dataSize > DatabaseDescriptor.getInMemoryCompactionLimit())
                            {
                                // We have a key in cache for a very big row, that is fishy. We don't fail here however because that would prevent the sstable
                                // from being build (and there is no real point anyway), so we just invalidate the row for correction and log a warning.
                                logger.warn("Found a cached row over the in memory compaction limit during post-streaming rebuilt; it is highly recommended to avoid huge row on column family with row cache enabled.");
                                cfs.invalidateCachedRow(key);
                            }
                            else
                            {
                                assert cf != null;
                                cfs.updateRowCache(key, cf);
                            }
                            break;
                        default:
                            cfs.invalidateCachedRow(key);
                            break;
                    }
                }

                bytesRead += in.getBytesRead();
                remoteFile.progress += in.getBytesRead();
            }
        }
        return writer.closeAndOpenReader();
    }

    private void retry() throws IOException
    {
        /* Ask the source node to re-stream this file. */
        session.retry(remoteFile);

        /* Delete the orphaned file. */
        FileUtils.deleteWithConfirm(new File(localFile.getFilename()));
    }
}
