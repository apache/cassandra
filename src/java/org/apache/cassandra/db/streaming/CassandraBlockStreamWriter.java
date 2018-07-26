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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.net.async.ByteBufDataOutputStreamPlus;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamSession;

import static org.apache.cassandra.streaming.StreamManager.StreamRateLimiter;
import static org.apache.cassandra.utils.FBUtilities.prettyPrintMemory;

/**
 * CassandraBlockStreamWriter streams the entire SSTable to given channel.
 */
public class CassandraBlockStreamWriter implements IStreamWriter
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraBlockStreamWriter.class);

    private final SSTableReader sstable;
    private final ComponentManifest manifest;
    private final StreamSession session;
    private final StreamRateLimiter limiter;

    public CassandraBlockStreamWriter(SSTableReader sstable, StreamSession session, ComponentManifest manifest)
    {
        this.session = session;
        this.sstable = sstable;
        this.manifest = manifest;
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
        long totalSize = manifest.totalSize();
        logger.debug("[Stream #{}] Start streaming sstable {} to {}, repairedAt = {}, totalSize = {}",
                     session.planId(),
                     sstable.getFilename(),
                     session.peer,
                     sstable.getSSTableMetadata().repairedAt,
                     prettyPrintMemory(totalSize));

        long progress = 0L;
        ByteBufDataOutputStreamPlus byteBufDataOutputStreamPlus = (ByteBufDataOutputStreamPlus) output;

        for (Component component : manifest.components())
        {
            @SuppressWarnings("resource") // this is closed after the file is transferred by ByteBufDataOutputStreamPlus
            FileChannel in = new RandomAccessFile(sstable.descriptor.filenameFor(component), "r").getChannel();

            // Total Length to transmit for this file
            long length = in.size();

            // tracks write progress
            logger.debug("[Stream #{}] Block streaming {}.{} gen {} component {} size {}", session.planId(),
                         sstable.getKeyspaceName(),
                         sstable.getColumnFamilyName(),
                         sstable.descriptor.generation,
                         component, length);

            long bytesRead = byteBufDataOutputStreamPlus.writeToChannel(in, limiter);
            progress += bytesRead;

            session.progress(sstable.descriptor.filenameFor(component), ProgressInfo.Direction.OUT, bytesRead, length);

            logger.debug("[Stream #{}] Finished block streaming {}.{} gen {} component {} to {}, xfered = {}, length = {}, totalSize = {}",
                         session.planId(),
                         sstable.getKeyspaceName(),
                         sstable.getColumnFamilyName(),
                         sstable.descriptor.generation,
                         component,
                         session.peer,
                         prettyPrintMemory(bytesRead),
                         prettyPrintMemory(length),
                         prettyPrintMemory(totalSize));
        }

        byteBufDataOutputStreamPlus.flush();

        logger.debug("[Stream #{}] Finished block streaming sstable {} to {}, xfered = {}, totalSize = {}",
                     session.planId(),
                     sstable.getFilename(),
                     session.peer,
                     prettyPrintMemory(progress),
                     prettyPrintMemory(totalSize));

    }
}
