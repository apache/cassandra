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

package org.apache.cassandra.db.streaming;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.net.async.ByteBufDataOutputStreamPlus;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.streaming.StreamManager.StreamRateLimiter;

/**
 * CassandraBlockStreamWriter streams the entire SSTable to given channel.
 */
public class CassandraBlockStreamWriter implements IStreamWriter
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraBlockStreamWriter.class);

    protected final SSTableReader sstable;
    protected final List<ComponentInfo> components;
    protected final StreamSession session;
    private final StreamRateLimiter limiter;

    public CassandraBlockStreamWriter(SSTableReader sstable, StreamSession session, List<ComponentInfo> components)
    {
        this.session = session;
        this.sstable = sstable;
        this.components = components;
        this.limiter =  StreamManager.getRateLimiter(session.peer);
    }

    /**
     * Stream the entire file to given channel.
     * <p>
     *
     * @param output where this writes data to
     * @throws IOException on any I/O error
     */
    @Override
    public void write(DataOutputStreamPlus output) throws IOException
    {
        long totalSize = totalSize();
        logger.debug("[Stream #{}] Start streaming sstable {} to {}, repairedAt = {}, totalSize = {}", session.planId(),
                     sstable.getFilename(), session.peer, sstable.getSSTableMetadata().repairedAt, totalSize);

        long progress = 0L;
        ByteBufDataOutputStreamPlus byteBufDataOutputStreamPlus = (ByteBufDataOutputStreamPlus) output;

        for (ComponentInfo info : components)
        {
            @SuppressWarnings("resource") // this is closed after the file is transferred by ByteBufDataOutputStreamPlus
            FileChannel in = new RandomAccessFile(sstable.descriptor.filenameFor(Component.parse(info.type.repr)), "r").getChannel();

            // Total Length to transmit for this file
            long length = in.size();

            // tracks write progress
            long bytesRead = 0;
            logger.debug("[Stream #{}] Block streaming {}.{} gen {} component {} size {}", session.planId(),
                        sstable.getKeyspaceName(), sstable.getColumnFamilyName(), sstable.descriptor.generation, info.type, length);

            bytesRead += byteBufDataOutputStreamPlus.writeToChannel(in, limiter);
            progress += bytesRead;

            session.progress(sstable.descriptor.filenameFor(Component.parse(info.type.repr)), ProgressInfo.Direction.OUT, bytesRead,
                             length);

            logger.debug("[Stream #{}] Finished block streaming {}.{} gen {} component {} to {}, xfered = {}, length = {}, totalSize = {}",
                         session.planId(), sstable.getKeyspaceName(), sstable.getColumnFamilyName(),
                         sstable.descriptor.generation, info.type, session.peer, FBUtilities.prettyPrintMemory(bytesRead),
                         FBUtilities.prettyPrintMemory(length), FBUtilities.prettyPrintMemory(totalSize));

            byteBufDataOutputStreamPlus.flush();

            logger.debug("[Stream #{}] Finished block streaming sstable {} to {}, xfered = {}, totalSize = {}",
                         session.planId(), sstable.getFilename(), session.peer, FBUtilities.prettyPrintMemory(progress),
                         FBUtilities.prettyPrintMemory(totalSize));

        }
    }

    protected long totalSize()
    {
        long size = 0;
        for (ComponentInfo component : components)
            size += component.length;
        return size;
    }
}
