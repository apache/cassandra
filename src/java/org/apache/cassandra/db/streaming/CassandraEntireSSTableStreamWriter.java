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
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamingDataOutputPlus;

import static org.apache.cassandra.streaming.StreamManager.StreamRateLimiter;
import static org.apache.cassandra.utils.FBUtilities.prettyPrintMemory;

/**
 * CassandraEntireSSTableStreamWriter streams the entire SSTable to given channel.
 */
public class CassandraEntireSSTableStreamWriter
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraEntireSSTableStreamWriter.class);

    private final SSTableReader sstable;
    private final ComponentContext context;
    private final ComponentManifest manifest;
    private final StreamSession session;
    private final StreamRateLimiter limiter;

    public CassandraEntireSSTableStreamWriter(SSTableReader sstable, StreamSession session, ComponentContext context)
    {
        this.session = session;
        this.sstable = sstable;
        this.context = context;
        this.manifest = context.manifest();
        this.limiter = StreamManager.getEntireSSTableRateLimiter(session.peer);
    }

    /**
     * Stream the entire file to given channel.
     * <p>
     * TODO: this currently requires a companion thread, but could be performed entirely asynchronously
     * @param out where this writes data to
     * @throws IOException on any I/O error
     */
    public void write(StreamingDataOutputPlus out) throws IOException
    {
        long totalSize = manifest.totalSize();
        logger.debug("[Stream #{}] Start streaming sstable {} to {}, repairedAt = {}, totalSize = {}",
                     session.planId(),
                     sstable.getFilename(),
                     session.peer,
                     sstable.getSSTableMetadata().repairedAt,
                     prettyPrintMemory(totalSize));

        long progress = 0L;

        for (Component component : manifest.components())
        {
            // Total Length to transmit for this file
            long length = manifest.sizeOf(component);

            // tracks write progress
            logger.debug("[Stream #{}] Streaming {}.{} gen {} component {} size {}", session.planId(),
                         sstable.getKeyspaceName(),
                         sstable.getColumnFamilyName(),
                         sstable.descriptor.id,
                         component,
                         prettyPrintMemory(length));

            FileChannel channel = context.channel(sstable.descriptor, component, length);
            long bytesWritten = out.writeFileToChannel(channel, limiter);
            progress += bytesWritten;

            session.progress(sstable.descriptor.fileFor(component).toString(), ProgressInfo.Direction.OUT, bytesWritten, bytesWritten, length);

            logger.debug("[Stream #{}] Finished streaming {}.{} gen {} component {} to {}, xfered = {}, length = {}, totalSize = {}",
                         session.planId(),
                         sstable.getKeyspaceName(),
                         sstable.getColumnFamilyName(),
                         sstable.descriptor.id,
                         component,
                         session.peer,
                         prettyPrintMemory(bytesWritten),
                         prettyPrintMemory(length),
                         prettyPrintMemory(totalSize));
        }

        out.flush();

        logger.debug("[Stream #{}] Finished streaming sstable {} to {}, xfered = {}, totalSize = {}",
                     session.planId(),
                     sstable.getFilename(),
                     session.peer,
                     prettyPrintMemory(progress),
                     prettyPrintMemory(totalSize));

    }
}
