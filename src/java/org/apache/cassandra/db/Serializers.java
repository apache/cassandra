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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.IndexInfo;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
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

    private Map<Version, IndexInfo.Serializer> otherVersionClusteringSerializers;

    private final IndexInfo.Serializer latestVersionIndexSerializer;

    public Serializers(CFMetaData metadata)
    {
        this.metadata = metadata;
        this.latestVersionIndexSerializer = new IndexInfo.Serializer(BigFormat.latestVersion,
                                                                     indexEntryClusteringPrefixSerializer(BigFormat.latestVersion, SerializationHeader.makeWithoutStats(metadata)));
    }

    IndexInfo.Serializer indexInfoSerializer(Version version, SerializationHeader header)
    {
        // null header indicates streaming from pre-3.0 sstables
        if (version.equals(BigFormat.latestVersion) && header != null)
            return latestVersionIndexSerializer;

        if (otherVersionClusteringSerializers == null)
            otherVersionClusteringSerializers = new ConcurrentHashMap<>();
        IndexInfo.Serializer serializer = otherVersionClusteringSerializers.get(version);
        if (serializer == null)
        {
            serializer = new IndexInfo.Serializer(version,
                                                  indexEntryClusteringPrefixSerializer(version, header));
            otherVersionClusteringSerializers.put(version, serializer);
        }
        return serializer;
    }

    // TODO: Once we drop support for old (pre-3.0) sstables, we can drop this method and inline the calls to
    // ClusteringPrefix.serializer directly. At which point this whole class probably becomes
    // unecessary (since IndexInfo.Serializer won't depend on the metadata either).
    private ISerializer<ClusteringPrefix> indexEntryClusteringPrefixSerializer(Version version, SerializationHeader header)
    {
        if (!version.storeRows() || header ==  null) //null header indicates streaming from pre-3.0 sstables
        {
            return oldFormatSerializer(version);
        }

        return new NewFormatSerializer(version, header.clusteringTypes());
    }

    private ISerializer<ClusteringPrefix> oldFormatSerializer(Version version)
    {
        return new ISerializer<ClusteringPrefix>()
        {
            List<AbstractType<?>> clusteringTypes = SerializationHeader.makeWithoutStats(metadata).clusteringTypes();

            public void serialize(ClusteringPrefix clustering, DataOutputPlus out) throws IOException
            {
                //we deserialize in the old format and serialize in the new format
                ClusteringPrefix.serializer.serialize(clustering, out,
                                                      version.correspondingMessagingVersion(),
                                                      clusteringTypes);
            }

            @Override
            public void skip(DataInputPlus in) throws IOException
            {
                ByteBufferUtil.skipShortLength(in);
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

                if (metadata.isCompound() && CompositeType.isStaticName(bb))
                    return Clustering.STATIC_CLUSTERING;

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
                    ClusteringPrefix.Kind boundKind = eoc > 0
                                                 ? ClusteringPrefix.Kind.EXCL_START_BOUND
                                                 : ClusteringPrefix.Kind.INCL_START_BOUND;

                    return ClusteringBound.create(boundKind, components.toArray(new ByteBuffer[components.size()]));
                }
            }

            public long serializedSize(ClusteringPrefix clustering)
            {
                return ClusteringPrefix.serializer.serializedSize(clustering, version.correspondingMessagingVersion(),
                                                                  clusteringTypes);
            }
        };
    }

    private static class NewFormatSerializer implements ISerializer<ClusteringPrefix>
    {
        private final Version version;
        private final List<AbstractType<?>> clusteringTypes;

        NewFormatSerializer(Version version, List<AbstractType<?>> clusteringTypes)
        {
            this.version = version;
            this.clusteringTypes = clusteringTypes;
        }

        public void serialize(ClusteringPrefix clustering, DataOutputPlus out) throws IOException
        {
            ClusteringPrefix.serializer.serialize(clustering, out, version.correspondingMessagingVersion(), clusteringTypes);
        }

        @Override
        public void skip(DataInputPlus in) throws IOException
        {
            ClusteringPrefix.serializer.skip(in, version.correspondingMessagingVersion(), clusteringTypes);
        }

        public ClusteringPrefix deserialize(DataInputPlus in) throws IOException
        {
            return ClusteringPrefix.serializer.deserialize(in, version.correspondingMessagingVersion(), clusteringTypes);
        }

        public long serializedSize(ClusteringPrefix clustering)
        {
            return ClusteringPrefix.serializer.serializedSize(clustering, version.correspondingMessagingVersion(), clusteringTypes);
        }
    }
}
