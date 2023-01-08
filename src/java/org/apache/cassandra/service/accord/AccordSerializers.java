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

import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.Maps;
import org.apache.cassandra.cql3.Sets;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
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
            size += TypeSizes.sizeof(column.ksName);
            size += TypeSizes.sizeof(column.cfName);
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
}
