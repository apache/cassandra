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

package org.apache.cassandra.tcm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.tcm.serialization.VerboseMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.NoSpamLogger;

public interface MetadataSnapshots
{
    Logger logger = LoggerFactory.getLogger(MetadataSnapshots.class);

    ClusterMetadata getSnapshot(Epoch epoch);
    ClusterMetadata getSnapshotBefore(Epoch epoch);
    ClusterMetadata getLatestSnapshot();
    List<Epoch> listSnapshotsSince(Epoch epoch);
    void storeSnapshot(ClusterMetadata metadata);

    static ByteBuffer toBytes(ClusterMetadata metadata) throws IOException
    {
        Version serializationVersion = Version.minCommonSerializationVersion();
        long serializedSize = VerboseMetadataSerializer.serializedSize(ClusterMetadata.serializer, metadata, serializationVersion);
        ByteBuffer bytes = ByteBuffer.allocate((int) serializedSize);
        try (DataOutputBuffer dob = new DataOutputBuffer(bytes))
        {
            VerboseMetadataSerializer.serialize(ClusterMetadata.serializer, metadata, dob, serializationVersion);
        }
        bytes.flip().rewind();
        return bytes;
    }

    @SuppressWarnings("resource")
    static ClusterMetadata fromBytes(ByteBuffer serialized) throws IOException
    {
        if (serialized == null)
            return null;

        return VerboseMetadataSerializer.deserialize(ClusterMetadata.serializer,
                                                     new DataInputBuffer(serialized, false));
    }

    MetadataSnapshots NO_OP = new NoOp();

    class NoOp implements MetadataSnapshots
    {
        @Override
        public ClusterMetadata getSnapshot(Epoch epoch)
        {
            return null;
        }

        @Override
        public ClusterMetadata getSnapshotBefore(Epoch epoch) {return null;}

        @Override
        public ClusterMetadata getLatestSnapshot() {return null;}

        @Override
        public List<Epoch> listSnapshotsSince(Epoch epoch)
        {
            return Collections.emptyList();
        }

        @Override
        public void storeSnapshot(ClusterMetadata metadata) {}
    }

    class SystemKeyspaceMetadataSnapshots implements MetadataSnapshots
    {
        // Once the serialised ClusterMetadata reaches this fraction of max_mutation_size, warn the operator
        // that snapshots are approaching the point where storeSnapshot's single-mutation write will start
        // failing with MutationExceededMaxSizeException.
        private static final double SNAPSHOT_SIZE_WARNING_THRESHOLD = 0.75;

        @Override
        public ClusterMetadata getSnapshot(Epoch epoch)
        {
            try
            {
                return fromBytes(SystemKeyspace.getSnapshot(epoch));
            }
            catch (IOException e)
            {
                logger.error("Could not load snapshot", e);
                return null;
            }
        }

        @Override
        public ClusterMetadata getSnapshotBefore(Epoch epoch)
        {
            try
            {
                return fromBytes(SystemKeyspace.findSnapshotBefore(epoch));
            }
            catch (IOException e)
            {
                logger.error("Could not load snapshot before " + epoch, e);
                return null;
            }
        }

        @Override
        public ClusterMetadata getLatestSnapshot()
        {
            try
            {
                ByteBuffer snapshot = SystemKeyspace.findLastSnapshot();
                if (snapshot != null)
                    return fromBytes(snapshot);
            }
            catch (IOException e)
            {
                logger.error("Could not find latest snapshot");
            }
            return null;
        }

        @Override
        public List<Epoch> listSnapshotsSince(Epoch epoch)
        {
            return SystemKeyspace.listSnapshotsSince(epoch);
        }

        @Override
        public void storeSnapshot(ClusterMetadata metadata)
        {
            try
            {
                ByteBuffer bytes = toBytes(metadata);
                maybeWarnOnSize(metadata, bytes);
                SystemKeyspace.storeSnapshot(metadata.epoch, bytes);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        /**
         * Logs a rate-limited WARN once the serialised snapshot approaches max_mutation_size, since
         * SystemKeyspace.storeSnapshot writes the whole blob as a single mutation and will start throwing
         * MutationExceededMaxSizeException (silently dropping snapshots, absent this warning) once it's exceeded.
         */
        private void maybeWarnOnSize(ClusterMetadata metadata, ByteBuffer bytes)
        {
            int maxMutationSize = DatabaseDescriptor.getMaxMutationSize();
            int serializedSize = bytes.remaining();
            TCMMetrics.instance.recordSnapshotSize(serializedSize);
            if (serializedSize > maxMutationSize * SNAPSHOT_SIZE_WARNING_THRESHOLD)
            {
                int tableCount = metadata.schema.getKeyspaces()
                                                 .stream()
                                                 .mapToInt(ksm -> ksm.tables.size())
                                                 .sum();
                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 5, TimeUnit.MINUTES,
                                  "Serialised cluster metadata snapshot at epoch {} is {} bytes, which is at least " +
                                  "{}% of the max_mutation_size limit of {} bytes ({} tables in schema). Snapshots " +
                                  "will start silently failing to store once the serialised size exceeds " +
                                  "max_mutation_size; consider raising max_mutation_size or reducing the number of tables.",
                                  metadata.epoch, serializedSize, (int) (SNAPSHOT_SIZE_WARNING_THRESHOLD * 100),
                                  maxMutationSize, tableCount);
            }
        }
    }
}
