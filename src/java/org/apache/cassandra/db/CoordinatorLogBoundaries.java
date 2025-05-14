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

package org.apache.cassandra.db;

import java.io.IOException;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.replication.CoordinatorLogId;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.utils.vint.VIntCoding;

/**
 * Max mutation ID present in this SSTable for each coordinator log, to determine whether an SSTable is reconciled or
 * not. Once max mutation IDs are reconciled, next compaction can safely mark this SSTabled as repaired. Note that peers
 * may have reconciled all mutations included in an SSTable, but {@link StatsMetadata#repairedAt} is dependent on
 * compaction timing, so "nodetool repair --validate" may report temporary disagreements on the repaired set.
 * <p>
 * A reference to this class should be treated as immutable. Do not cast to {@link CoordinatorLogBoundariesMap}.
 * Iterable over {@link CoordinatorLogId}.
 */
public interface CoordinatorLogBoundaries extends Iterable<Long>
{
    int maxOffset(long logId);
    MutationId max(long logId);
    int size();

    IVersionedSerializer<CoordinatorLogBoundaries> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(CoordinatorLogBoundaries boundaries, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_52)
                return;
            out.writeUnsignedVInt32(boundaries.size());
            for (long logId : boundaries)
                MutationId.serializer.serialize(boundaries.max(logId), out, version);
        }

        @Override
        public CoordinatorLogBoundaries deserialize(DataInputPlus in, int version) throws IOException
        {
            if (version < MessagingService.VERSION_52)
                return CoordinatorLogBoundaries.NONE;
            int size = in.readUnsignedVInt32();
            CoordinatorLogBoundariesMap boundaries = new CoordinatorLogBoundariesMap(size);
            for (int i = 0; i < size; i++)
            {
                MutationId mutationId = MutationId.serializer.deserialize(in, version);
                boundaries.add(mutationId);
            }
            return boundaries;
        }

        @Override
        public long serializedSize(CoordinatorLogBoundaries boundaries, int version)
        {
            if (version < MessagingService.VERSION_52)
                return 0;
            long size = 0;
            size += VIntCoding.computeUnsignedVIntSize(boundaries.size());
            for (long logId : boundaries)
                size += MutationId.serializer.serializedSize(boundaries.max(logId), version);
            return size;
        }
    };

    CoordinatorLogBoundaries NONE = new CoordinatorLogBoundariesMap();
}
