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

import java.io.*;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Holds references on serializers that depend on the table definition.
 */
public class Serializers
{
    private final CFMetaData metadata;

    public Serializers(CFMetaData metadata)
    {
        this.metadata = metadata;
    }

    // TODO: Once we drop support for old (pre-3.0) sstables, we can drop this method and inline the calls to
    // ClusteringPrefix.serializer in IndexHelper directly. At which point this whole class probably becomes
    // unecessary (since IndexInfo.Serializer won't depend on the metadata either).
    public ISerializer<ClusteringPrefix> indexEntryClusteringPrefixSerializer(final Version version, final SerializationHeader header)
    {
        if (!version.storeRows() || header ==  null) //null header indicates streaming from pre-3.0 sstables
        {
            return oldFormatSerializer(version);
        }

        return newFormatSerializer(version, header);
    }

    private ISerializer<ClusteringPrefix> oldFormatSerializer(final Version version)
    {
        return new ISerializer<ClusteringPrefix>()
        {
            SerializationHeader newHeader = SerializationHeader.makeWithoutStats(metadata);

            public void serialize(ClusteringPrefix clustering, DataOutputPlus out) throws IOException
            {
                //we deserialize in the old format and serialize in the new format
                ClusteringPrefix.serializer.serialize(clustering, out,
                                                      version.correspondingMessagingVersion(),
                                                      newHeader.clusteringTypes());
            }

            public ClusteringPrefix deserialize(DataInputPlus in) throws IOException
            {
                // We're reading the old cellname/composite
                ByteBuffer bb = ByteBufferUtil.readWithShortLength(in);
                assert bb.hasRemaining(); // empty cellnames were invalid

                int clusteringSize = metadata.clusteringColumns().size();
                // If the table has no clustering column, then the cellname will just be the "column" name, which we ignore here.
                if (clusteringSize == 0)
                    return Clustering.EMPTY;

                if (!metadata.isCompound())
                    return Clustering.make(bb);

                List<ByteBuffer> components = CompositeType.splitName(bb);
                byte eoc = CompositeType.lastEOC(bb);

                if (eoc == 0 || components.size() >= clusteringSize)
                {
                    // That's a clustering.
                    if (components.size() > clusteringSize)
                        components = components.subList(0, clusteringSize);

                    return Clustering.make(components.toArray(new ByteBuffer[clusteringSize]));
                }
                else
                {
                    // It's a range tombstone bound. It is a start since that's the only part we've ever included
                    // in the index entries.
                    Slice.Bound.Kind boundKind = eoc > 0
                                                 ? Slice.Bound.Kind.EXCL_START_BOUND
                                                 : Slice.Bound.Kind.INCL_START_BOUND;

                    return Slice.Bound.create(boundKind, components.toArray(new ByteBuffer[components.size()]));
                }
            }

            public long serializedSize(ClusteringPrefix clustering)
            {
                return ClusteringPrefix.serializer.serializedSize(clustering, version.correspondingMessagingVersion(),
                                                                  newHeader.clusteringTypes());
            }
        };
    }


    private ISerializer<ClusteringPrefix> newFormatSerializer(final Version version, final SerializationHeader header)
    {
        return new ISerializer<ClusteringPrefix>() //Reading and writing from/to the new sstable format
        {
            public void serialize(ClusteringPrefix clustering, DataOutputPlus out) throws IOException
            {
                ClusteringPrefix.serializer.serialize(clustering, out, version.correspondingMessagingVersion(), header.clusteringTypes());
            }

            public ClusteringPrefix deserialize(DataInputPlus in) throws IOException
            {
                return ClusteringPrefix.serializer.deserialize(in, version.correspondingMessagingVersion(), header.clusteringTypes());
            }

            public long serializedSize(ClusteringPrefix clustering)
            {
                return ClusteringPrefix.serializer.serializedSize(clustering, version.correspondingMessagingVersion(), header.clusteringTypes());
            }
        };
    }

}