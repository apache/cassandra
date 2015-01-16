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
package org.apache.cassandra.streaming.compress;

import java.io.DataInputStream;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import com.google.common.base.Throwables;

import org.apache.cassandra.io.sstable.format.SSTableWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.StreamReader;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.messages.FileMessageHeader;
import org.apache.cassandra.utils.BytesReadTracker;
import org.apache.cassandra.utils.Pair;

/**
 * StreamReader that reads from streamed compressed SSTable
 */
public class CompressedStreamReader extends StreamReader
{
    private static final Logger logger = LoggerFactory.getLogger(CompressedStreamReader.class);

    protected final CompressionInfo compressionInfo;

    public CompressedStreamReader(FileMessageHeader header, StreamSession session)
    {
        super(header, session);
        this.compressionInfo = header.compressionInfo;
    }

    /**
     * @return SSTable transferred
     * @throws java.io.IOException if reading the remote sstable fails. Will throw an RTE if local write fails.
     */
    @Override
    public SSTableWriter read(ReadableByteChannel channel) throws IOException
    {
        logger.debug("reading file from {}, repairedAt = {}", session.peer, repairedAt);
        long totalSize = totalSize();

        Pair<String, String> kscf = Schema.instance.getCF(cfId);
        if (kscf == null)
        {
            // schema was dropped during streaming
            throw new IOException("CF " + cfId + " was dropped during streaming");
        }
        ColumnFamilyStore cfs = Keyspace.open(kscf.left).getColumnFamilyStore(kscf.right);

        SSTableWriter writer = createWriter(cfs, totalSize, repairedAt, format);

        CompressedInputStream cis = new CompressedInputStream(Channels.newInputStream(channel), compressionInfo);
        BytesReadTracker in = new BytesReadTracker(new DataInputStream(cis));
        try
        {
            for (Pair<Long, Long> section : sections)
            {
                assert in.getBytesRead() < totalSize;
                int sectionLength = (int) (section.right - section.left);

                // skip to beginning of section inside chunk
                cis.position(section.left);
                in.reset(0);

                while (in.getBytesRead() < sectionLength)
                {
                    writeRow(writer, in, cfs);

                    // when compressed, report total bytes of compressed chunks read since remoteFile.size is the sum of chunks transferred
                    session.progress(desc, ProgressInfo.Direction.IN, cis.getTotalCompressedBytesRead(), totalSize);
                }
            }
            return writer;
        }
        catch (Throwable e)
        {
            writer.abort();
            drain(cis, in.getBytesRead());
            if (e instanceof IOException)
                throw (IOException) e;
            else
                throw Throwables.propagate(e);
        }
    }

    @Override
    protected long totalSize()
    {
        long size = 0;
        // calculate total length of transferring chunks
        for (CompressionMetadata.Chunk chunk : compressionInfo.chunks)
            size += chunk.length + 4; // 4 bytes for CRC
        return size;
    }
}
