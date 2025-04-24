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

package org.apache.cassandra.service.accord;

import java.io.IOException;
import java.nio.ByteBuffer;

import accord.utils.VIntCoding;
import org.apache.cassandra.cql3.terms.MultiElements;
import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.db.ArrayClustering;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.io.AsymmetricVersionedSerializer;
import org.apache.cassandra.io.EmbeddedAsymmetricVersionedSerializer;
import org.apache.cassandra.io.ParameterisedUnversionedSerializer;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.serializers.IVersionedSerializer;
import org.apache.cassandra.service.accord.serializers.Version;

import static org.apache.cassandra.db.TypeSizes.sizeof;
import static org.apache.cassandra.db.TypeSizes.sizeofUnsignedVInt;
import static org.apache.cassandra.db.marshal.CollectionType.Kind.LIST;
import static org.apache.cassandra.db.marshal.CollectionType.Kind.MAP;
import static org.apache.cassandra.db.marshal.CollectionType.Kind.SET;

public class AccordSerializers
{
    public static <A, B> EmbeddedAsymmetricVersionedSerializer<A, B, Version> embedded(Version version, AsymmetricVersionedSerializer<A, B, Version> serializer)
    {
        return new EmbeddedAsymmetricVersionedSerializer<>(version, Version.Serializer.instance, serializer);
    }

    public static Term.Terminal deserializeCqlCollectionAsTerm(ByteBuffer buffer, AbstractType<?> type)
    {
        CollectionType<?> collectionType = (CollectionType<?>) type;

        if (collectionType.kind == SET)
            return MultiElements.Value.fromSerialized(buffer, (SetType<?>) type);
        else if (collectionType.kind == LIST)
            return MultiElements.Value.fromSerialized(buffer, (ListType<?>) type);
        else if (collectionType.kind == MAP)
            return MultiElements.Value.fromSerialized(buffer, (MapType<?, ?>) type);

        throw new UnsupportedOperationException("Unsupported collection type: " + type);
    }

    public static final ParameterisedUnversionedSerializer<ColumnMetadata, TableMetadata> columnMetadataSerializer = new ParameterisedUnversionedSerializer<>()
    {
        @Override
        public void serialize(ColumnMetadata column, TableMetadata table, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(column.uniqueId);
        }

        @Override
        public ColumnMetadata deserialize(TableMetadata table, DataInputPlus in) throws IOException
        {
            return table.getColumnById(in.readUnsignedVInt32());
        }

        @Override
        public long serializedSize(ColumnMetadata column, TableMetadata table)
        {
            return VIntCoding.sizeOfUnsignedVInt(column.uniqueId);
        }
    };

    public static final IVersionedSerializer<TableMetadata> tableMetadataSerializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(TableMetadata metadata, DataOutputPlus out, Version version) throws IOException
        {
            metadata.id.serializeCompact(out);
        }

        @Override
        public TableMetadata deserialize(DataInputPlus in, Version version) throws IOException
        {
            return Schema.instance.getTableMetadata(TableId.deserializeCompact(in));
        }

        @Override
        public long serializedSize(TableMetadata metadata, Version version)
        {
            return metadata.id.serializedCompactSize();
        }
    };

    public static final UnversionedSerializer<Clustering<?>> clusteringSerializer = new UnversionedSerializer<Clustering<?>>()
    {
        @Override
        public void serialize(Clustering<?> clustering, DataOutputPlus out) throws IOException
        {
            doSerialize(clustering, out);
        }

        private  <V> void doSerialize(Clustering<V> clustering, DataOutputPlus out) throws IOException
        {
            if (clustering.kind() == ClusteringPrefix.Kind.STATIC_CLUSTERING)
            {
                out.writeBoolean(true);
            }
            else
            {
                out.writeBoolean(false);
                out.writeUnsignedVInt32(clustering.size());
                ValueAccessor<V> accessor = clustering.accessor();
                for (int i = 0; i < clustering.size(); i++)
                {
                    accessor.writeWithVIntLength(clustering.get(i), out);
                }
            }
        }

        @Override
        public Clustering<?> deserialize(DataInputPlus in) throws IOException
        {
            Clustering<?> clustering;
            if (in.readBoolean())
            {
                clustering = Clustering.STATIC_CLUSTERING;
            }
            else
            {
                int numComponents = in.readUnsignedVInt32();
                byte[][] components = new byte[numComponents][];
                for (int ci = 0; ci < numComponents; ci++)
                {
                    int componentLength = in.readUnsignedVInt32();
                    components[ci] = new byte[componentLength];
                    in.readFully(components[ci]);
                }
                clustering = new ArrayClustering(components);
            }
            return clustering;
        }

        @Override
        public long serializedSize(Clustering<?> clustering)
        {
            return computeSerializedSize(clustering);
        }

        private <V> long computeSerializedSize(Clustering<V> clustering)
        {
            int size = sizeof(true);
            if (clustering.kind() != ClusteringPrefix.Kind.STATIC_CLUSTERING)
            {
                size += sizeofUnsignedVInt(clustering.size());
                ValueAccessor<V> accessor = clustering.accessor();
                for (int i = 0; i < clustering.size(); i++)
                {
                    int valueSize = accessor.size(clustering.get(i));
                    size += valueSize;
                    size += sizeofUnsignedVInt(valueSize);
                }
            }
            return size;
        }
    };

    public static final UnversionedSerializer<ConsistencyLevel> consistencyLevelSerializer = new UnversionedSerializer<ConsistencyLevel>()
    {
        @Override
        public void serialize(ConsistencyLevel t, DataOutputPlus out) throws IOException
        {
            out.writeByte(t.code);
        }

        @Override
        public ConsistencyLevel deserialize(DataInputPlus in) throws IOException
        {
            return ConsistencyLevel.fromCode(in.readByte());
        }

        @Override
        public long serializedSize(ConsistencyLevel t)
        {
            return 1;
        }
    };
}