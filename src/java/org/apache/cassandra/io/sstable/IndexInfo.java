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

package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.RowIndexEntry;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * {@code IndexInfo} is embedded in the indexed version of {@link RowIndexEntry}.
 * Each instance roughly covers a range of {@link org.apache.cassandra.config.Config#column_index_size column_index_size} KiB
 * and contains the first and last clustering value (or slice bound), its offset in the data file and width in the data file.
 * <p>
 * Each {@code IndexInfo} object is serialized as follows.
 * </p>
 * <p>
 * Serialization format changed in 3.0; the {@link #endOpenMarker} has been introduced and integer fields are
 * stored using varint encoding.
 * </p>
 * <p>
 * {@code
 *    (*) IndexInfo.firstName (ClusteringPrefix serializer, either Clustering.serializer.serialize or Slice.Bound.serializer.serialize)
 *    (*) IndexInfo.lastName (ClusteringPrefix serializer, either Clustering.serializer.serialize or Slice.Bound.serializer.serialize)
 * (long) IndexInfo.offset
 * (long) IndexInfo.width
 * (bool) IndexInfo.endOpenMarker != null                                 (if 3.0)
 *  (int) (Uint post c14227) IndexInfo.endOpenMarker.localDeletionTime    (if 3.0 && IndexInfo.endOpenMarker != null)
 * (long) IndexInfo.endOpenMarker.markedForDeletionAt                     (if 3.0 && IndexInfo.endOpenMarker != null)
 * }
 * </p>
 */
public class IndexInfo
{
    public static final long EMPTY_SIZE = ObjectSizes.measure(new IndexInfo(null, null, 0, 0, null));

    public final long offset;
    public final long width;
    public final ClusteringPrefix<?> firstName;
    public final ClusteringPrefix<?> lastName;

    // If at the end of the index block there is an open range tombstone marker, this marker
    // deletion infos. null otherwise.
    public final DeletionTime endOpenMarker;

    public IndexInfo(ClusteringPrefix<?> firstName,
                     ClusteringPrefix<?> lastName,
                     long offset,
                     long width,
                     DeletionTime endOpenMarker)
    {
        this.firstName = firstName;
        this.lastName = lastName;
        this.offset = offset;
        this.width = width;
        this.endOpenMarker = endOpenMarker;
    }

    public static IndexInfo.Serializer serializer(Version version, SerializationHeader header)
    {
        return new IndexInfo.Serializer(version, header.clusteringTypes());
    }

    public static class Serializer implements ISerializer<IndexInfo>
    {
        // This is the default index size that we use to delta-encode width when serializing so we get better vint-encoding.
        // This is imperfect as user can change the index size and ideally we would save the index size used with each index file
        // to use as base. However, that's a bit more involved a change that we want for now and very seldom do use change the index
        // size so using the default is almost surely better than using no base at all.
        public static final long WIDTH_BASE = 64 * 1024;

        private final Version version;
        private final List<AbstractType<?>> clusteringTypes;

        public Serializer(Version version, List<AbstractType<?>> clusteringTypes)
        {
            this.version = version;
            this.clusteringTypes = clusteringTypes;
        }

        public void serialize(IndexInfo info, DataOutputPlus out) throws IOException
        {
            ClusteringPrefix.serializer.serialize(info.firstName, out, version.correspondingMessagingVersion(), clusteringTypes);
            ClusteringPrefix.serializer.serialize(info.lastName, out, version.correspondingMessagingVersion(), clusteringTypes);
            out.writeUnsignedVInt(info.offset);
            out.writeVInt(info.width - WIDTH_BASE);

            out.writeBoolean(info.endOpenMarker != null);
            if (info.endOpenMarker != null)
                DeletionTime.getSerializer(version).serialize(info.endOpenMarker, out);
        }

        public void skip(DataInputPlus in) throws IOException
        {
            ClusteringPrefix.serializer.skip(in, version.correspondingMessagingVersion(), clusteringTypes);
            ClusteringPrefix.serializer.skip(in, version.correspondingMessagingVersion(), clusteringTypes);
            in.readUnsignedVInt();
            in.readVInt();
            if (in.readBoolean())
                DeletionTime.getSerializer(version).skip(in);
        }

        public IndexInfo deserialize(DataInputPlus in) throws IOException
        {
            ClusteringPrefix<byte[]> firstName = ClusteringPrefix.serializer.deserialize(in, version.correspondingMessagingVersion(), clusteringTypes);
            ClusteringPrefix<byte[]> lastName = ClusteringPrefix.serializer.deserialize(in, version.correspondingMessagingVersion(), clusteringTypes);
            long offset = in.readUnsignedVInt();
            long width = in.readVInt() + WIDTH_BASE;
            DeletionTime endOpenMarker = null;
            if (in.readBoolean())
                endOpenMarker = DeletionTime.getSerializer(version).deserialize(in);

            return new IndexInfo(firstName, lastName, offset, width, endOpenMarker);
        }

        public long serializedSize(IndexInfo info)
        {
            long size = ClusteringPrefix.serializer.serializedSize(info.firstName, version.correspondingMessagingVersion(), clusteringTypes)
                      + ClusteringPrefix.serializer.serializedSize(info.lastName, version.correspondingMessagingVersion(), clusteringTypes)
                      + TypeSizes.sizeofUnsignedVInt(info.offset)
                      + TypeSizes.sizeofVInt(info.width - WIDTH_BASE)
                      + TypeSizes.sizeof(info.endOpenMarker != null);

            if (info.endOpenMarker != null)
                size += DeletionTime.getSerializer(version).serializedSize(info.endOpenMarker);

            return size;
        }
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE
             + firstName.unsharedHeapSize()
             + lastName.unsharedHeapSize()
             + (endOpenMarker == null ? 0 : endOpenMarker.unsharedHeapSize());
    }
}
