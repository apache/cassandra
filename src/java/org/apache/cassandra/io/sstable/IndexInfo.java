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

import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * {@code IndexInfo} is embedded in the indexed version of {@link RowIndexEntry}.
 * Each instance roughly covers a range of {@link org.apache.cassandra.config.Config#column_index_size_in_kb column_index_size_in_kb} kB
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
 * (bool) IndexInfo.endOpenMarker != null              (if 3.0)
 *  (int) IndexInfo.endOpenMarker.localDeletionTime    (if 3.0 && IndexInfo.endOpenMarker != null)
 * (long) IndexInfo.endOpenMarker.markedForDeletionAt  (if 3.0 && IndexInfo.endOpenMarker != null)
 * }
 * </p>
 */
public class IndexInfo
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new IndexInfo(null, null, 0, 0, null));

    public final long offset;
    public final long width;
    public final ClusteringPrefix firstName;
    public final ClusteringPrefix lastName;

    // If at the end of the index block there is an open range tombstone marker, this marker
    // deletion infos. null otherwise.
    public final DeletionTime endOpenMarker;

    public IndexInfo(ClusteringPrefix firstName,
                     ClusteringPrefix lastName,
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

    public static class Serializer implements ISerializer<IndexInfo>
    {
        // This is the default index size that we use to delta-encode width when serializing so we get better vint-encoding.
        // This is imperfect as user can change the index size and ideally we would save the index size used with each index file
        // to use as base. However, that's a bit more involved a change that we want for now and very seldom do use change the index
        // size so using the default is almost surely better than using no base at all.
        public static final long WIDTH_BASE = 64 * 1024;

        private final ISerializer<ClusteringPrefix> clusteringSerializer;
        private final Version version;

        public Serializer(Version version, ISerializer<ClusteringPrefix> clusteringSerializer)
        {
            this.clusteringSerializer = clusteringSerializer;
            this.version = version;
        }

        public void serialize(IndexInfo info, DataOutputPlus out) throws IOException
        {
            assert version.storeRows() : "We read old index files but we should never write them";

            clusteringSerializer.serialize(info.firstName, out);
            clusteringSerializer.serialize(info.lastName, out);
            out.writeUnsignedVInt(info.offset);
            out.writeVInt(info.width - WIDTH_BASE);

            out.writeBoolean(info.endOpenMarker != null);
            if (info.endOpenMarker != null)
                DeletionTime.serializer.serialize(info.endOpenMarker, out);
        }

        public void skip(DataInputPlus in) throws IOException
        {
            clusteringSerializer.skip(in);
            clusteringSerializer.skip(in);
            if (version.storeRows())
            {
                in.readUnsignedVInt();
                in.readVInt();
                if (in.readBoolean())
                    DeletionTime.serializer.skip(in);
            }
            else
            {
                in.skipBytes(TypeSizes.sizeof(0L));
                in.skipBytes(TypeSizes.sizeof(0L));
            }
        }

        public IndexInfo deserialize(DataInputPlus in) throws IOException
        {
            ClusteringPrefix firstName = clusteringSerializer.deserialize(in);
            ClusteringPrefix lastName = clusteringSerializer.deserialize(in);
            long offset;
            long width;
            DeletionTime endOpenMarker = null;
            if (version.storeRows())
            {
                offset = in.readUnsignedVInt();
                width = in.readVInt() + WIDTH_BASE;
                if (in.readBoolean())
                    endOpenMarker = DeletionTime.serializer.deserialize(in);
            }
            else
            {
                offset = in.readLong();
                width = in.readLong();
            }
            return new IndexInfo(firstName, lastName, offset, width, endOpenMarker);
        }

        public long serializedSize(IndexInfo info)
        {
            assert version.storeRows() : "We read old index files but we should never write them";

            long size = clusteringSerializer.serializedSize(info.firstName)
                        + clusteringSerializer.serializedSize(info.lastName)
                        + TypeSizes.sizeofUnsignedVInt(info.offset)
                        + TypeSizes.sizeofVInt(info.width - WIDTH_BASE)
                        + TypeSizes.sizeof(info.endOpenMarker != null);

            if (info.endOpenMarker != null)
                size += DeletionTime.serializer.serializedSize(info.endOpenMarker);
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
