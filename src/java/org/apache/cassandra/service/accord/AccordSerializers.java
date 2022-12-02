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
import java.util.List;

import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.Maps;
import org.apache.cassandra.cql3.Sets;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.ArrayClustering;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

import static com.google.common.primitives.Ints.checkedCast;
import static org.apache.cassandra.db.TypeSizes.sizeof;
import static org.apache.cassandra.db.TypeSizes.sizeofUnsignedVInt;
import static org.apache.cassandra.db.marshal.CollectionType.Kind.LIST;
import static org.apache.cassandra.db.marshal.CollectionType.Kind.MAP;
import static org.apache.cassandra.db.marshal.CollectionType.Kind.SET;

public class AccordSerializers
{
    public static <T> ByteBuffer serialize(T item, IVersionedSerializer<T> serializer)
    {
        int version = MessagingService.current_version;
        long size = serializer.serializedSize(item, version) + sizeofUnsignedVInt(version);
        try (DataOutputBuffer out = new DataOutputBuffer((int) size))
        {
            out.writeUnsignedVInt(version);
            serializer.serialize(item, out, version);
            return out.buffer(false);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static <T> ByteBuffer[] serialize(List<T> items, IVersionedSerializer<T> serializer)
    {
        ByteBuffer[] result = new ByteBuffer[items.size()];
        for (int i = 0, mi = items.size(); i < mi; i++)
            result[i] = serialize(items.get(i), serializer);
        return result;
    }

    public static <T> T deserialize(ByteBuffer bytes, IVersionedSerializer<T> serializer)
    {
        try (DataInputBuffer in = new DataInputBuffer(bytes, true))
        {
            int version = checkedCast(in.readUnsignedVInt());
            return serializer.deserialize(in, version);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static Term.Terminal deserializeCqlCollectionAsTerm(ByteBuffer buffer, AbstractType<?> type, ProtocolVersion version)
    {
        CollectionType<?> collectionType = (CollectionType<?>) type;

        if (collectionType.kind == SET)
            return Sets.Value.fromSerialized(buffer, (SetType<?>) type, version);
        else if (collectionType.kind == LIST)
            return Lists.Value.fromSerialized(buffer, (ListType<?>) type, version);
        else if (collectionType.kind == MAP)
            return Maps.Value.fromSerialized(buffer, (MapType<?, ?>) type, version);

        throw new UnsupportedOperationException("Unsupported collection type: " + type);
    }

    public static final IVersionedSerializer<PartitionUpdate> partitionUpdateSerializer = new IVersionedSerializer<PartitionUpdate>()
    {
        @Override
        public void serialize(PartitionUpdate upd, DataOutputPlus out, int version) throws IOException
        {
            PartitionUpdate.serializer.serialize(upd, out, version);
        }

        @Override
        public PartitionUpdate deserialize(DataInputPlus in, int version) throws IOException
        {
            return PartitionUpdate.serializer.deserialize(in, version, DeserializationHelper.Flag.FROM_REMOTE);
        }

        @Override
        public long serializedSize(PartitionUpdate upd, int version)
        {
            return PartitionUpdate.serializer.serializedSize(upd, version);
        }
    };

    public static final IVersionedSerializer<ColumnMetadata> columnMetadataSerializer = new IVersionedSerializer<ColumnMetadata>()
    {
        @Override
        public void serialize(ColumnMetadata column, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(column.ksName);
            out.writeUTF(column.cfName);
            ByteBufferUtil.writeWithShortLength(column.name.bytes, out);
        }

        @Override
        public ColumnMetadata deserialize(DataInputPlus in, int version) throws IOException
        {
            String keyspace = in.readUTF();
            String table = in.readUTF();
            ByteBuffer name = ByteBufferUtil.readWithShortLength(in);
            TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, table);

            // TODO: Can the metadata be null if the table has been dropped?
            return metadata.getColumn(name);
        }

        @Override
        public long serializedSize(ColumnMetadata column, int version)
        {
            long size = 0;
            size += sizeof(column.ksName);
            size += sizeof(column.cfName);
            size += ByteBufferUtil.serializedSizeWithShortLength(column.name.bytes);
            return size;
        }
    };

    public static final IVersionedSerializer<TableMetadata> tableMetadataSerializer = new IVersionedSerializer<TableMetadata>()
    {
        @Override
        public void serialize(TableMetadata metadata, DataOutputPlus out, int version) throws IOException
        {
            metadata.id.serialize(out);
        }

        @Override
        public TableMetadata deserialize(DataInputPlus in, int version) throws IOException
        {
            return Schema.instance.getTableMetadata(TableId.deserialize(in));
        }

        @Override
        public long serializedSize(TableMetadata metadata, int version)
        {
            return metadata.id.serializedSize();
        }
    };

    public static final IVersionedSerializer<Clustering<?>> clusteringSerializer = new IVersionedSerializer<Clustering<?>>()
    {
        @Override
        public void serialize(Clustering<?> clustering, DataOutputPlus out, int version) throws IOException
        {
            doSerialize(clustering, out);
        }

        public <V> void doSerialize(Clustering<V> clustering, DataOutputPlus out) throws IOException
        {
            if (clustering.kind() == ClusteringPrefix.Kind.STATIC_CLUSTERING)
            {
                out.writeBoolean(true);
            }
            else
            {
                out.writeBoolean(false);
                out.writeUnsignedVInt(clustering.size());
                ValueAccessor<V> accessor = clustering.accessor();
                for (int i = 0; i < clustering.size(); i++)
                {
                    accessor.writeWithVIntLength(clustering.get(i), out);
                }
            }
        }

        @Override
        public Clustering<?> deserialize(DataInputPlus in, int version) throws IOException
        {
            Clustering<?> clustering;
            if (in.readBoolean())
            {
                clustering = Clustering.STATIC_CLUSTERING;
            }
            else
            {
                int numComponents = checkedCast(in.readUnsignedVInt());
                byte[][] components = new byte[numComponents][];
                for (int ci = 0; ci < numComponents; ci++)
                {
                    int componentLength = checkedCast(in.readUnsignedVInt());
                    components[ci] = new byte[componentLength];
                    in.readFully(components[ci]);
                }
                clustering = new ArrayClustering(components);
            }
            return clustering;
        }

        @Override
        public long serializedSize(Clustering<?> clustering, int version)
        {
            return computeSerializedSize(clustering);
        }

        private <V> long computeSerializedSize(Clustering<V> clustering)
        {
            int size = sizeof(true) + sizeofUnsignedVInt(clustering.size());
            ValueAccessor<V> accessor = clustering.accessor();
            for (int i = 0; i < clustering.size(); i++)
            {
                int valueSize = accessor.size(clustering.get(i));
                size += valueSize;
                size += sizeofUnsignedVInt(valueSize);
            }
            return size;
        }
    };
}