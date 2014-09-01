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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.sstable.format.Version;

import static org.apache.cassandra.io.sstable.IndexHelper.IndexInfo;

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

    public IndexInfo.Serializer indexSerializer(Version version)
    {
        return new IndexInfo.Serializer(metadata, version);
    }

    // Note that for the old layout, this will actually discard the cellname parts that are not strictly 
    // part of the clustering prefix. Don't use this if that's not what you want.
    public ISerializer<ClusteringPrefix> clusteringPrefixSerializer(final Version version, final SerializationHeader header)
    {
        if (!version.storeRows())
            throw new UnsupportedOperationException();

        return new ISerializer<ClusteringPrefix>()
        {
            public void serialize(ClusteringPrefix clustering, DataOutputPlus out) throws IOException
            {
                ClusteringPrefix.serializer.serialize(clustering, out, version.correspondingMessagingVersion(), header.clusteringTypes());
            }

            public ClusteringPrefix deserialize(DataInput in) throws IOException
            {
                return ClusteringPrefix.serializer.deserialize(in, version.correspondingMessagingVersion(), header.clusteringTypes());
            }

            public long serializedSize(ClusteringPrefix clustering, TypeSizes sizes)
            {
                return ClusteringPrefix.serializer.serializedSize(clustering, version.correspondingMessagingVersion(), header.clusteringTypes(), sizes);
            }
        };
    }
}
