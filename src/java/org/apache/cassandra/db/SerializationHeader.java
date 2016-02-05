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
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.IMetadataComponentSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SerializationHeader
{
    public static final Serializer serializer = new Serializer();

    private final boolean isForSSTable;

    private final AbstractType<?> keyType;
    private final List<AbstractType<?>> clusteringTypes;

    private final PartitionColumns columns;
    private final EncodingStats stats;

    private final Map<ByteBuffer, AbstractType<?>> typeMap;

    private SerializationHeader(boolean isForSSTable,
                                AbstractType<?> keyType,
                                List<AbstractType<?>> clusteringTypes,
                                PartitionColumns columns,
                                EncodingStats stats,
                                Map<ByteBuffer, AbstractType<?>> typeMap)
    {
        this.isForSSTable = isForSSTable;
        this.keyType = keyType;
        this.clusteringTypes = clusteringTypes;
        this.columns = columns;
        this.stats = stats;
        this.typeMap = typeMap;
    }

    public static SerializationHeader makeWithoutStats(CFMetaData metadata)
    {
        return new SerializationHeader(true, metadata, metadata.partitionColumns(), EncodingStats.NO_STATS);
    }

    public static SerializationHeader forKeyCache(CFMetaData metadata)
    {
        // We don't save type information in the key cache (we could change
        // that but it's easier right now), so instead we simply use BytesType
        // for both serialization and deserialization. Note that we also only
        // serializer clustering prefixes in the key cache, so only the clusteringTypes
        // really matter.
        int size = metadata.clusteringColumns().size();
        List<AbstractType<?>> clusteringTypes = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
            clusteringTypes.add(BytesType.instance);
        return new SerializationHeader(false,
                                       BytesType.instance,
                                       clusteringTypes,
                                       PartitionColumns.NONE,
                                       EncodingStats.NO_STATS,
                                       Collections.<ByteBuffer, AbstractType<?>>emptyMap());
    }

    public static SerializationHeader make(CFMetaData metadata, Collection<SSTableReader> sstables)
    {
        // The serialization header has to be computed before the start of compaction (since it's used to write)
        // the result. This means that when compacting multiple sources, we won't have perfectly accurate stats
        // (for EncodingStats) since compaction may delete, purge and generally merge rows in unknown ways. This is
        // kind of ok because those stats are only used for optimizing the underlying storage format and so we
        // just have to strive for as good as possible. Currently, we stick to a relatively naive merge of existing
        // global stats because it's simple and probably good enough in most situation but we could probably
        // improve our marging of inaccuracy through the use of more fine-grained stats in the future.
        // Note however that to avoid seeing our accuracy degrade through successive compactions, we don't base
        // our stats merging on the compacted files headers, which as we just said can be somewhat inaccurate,
        // but rather on their stats stored in StatsMetadata that are fully accurate.
        EncodingStats.Collector stats = new EncodingStats.Collector();
        PartitionColumns.Builder columns = PartitionColumns.builder();
        for (SSTableReader sstable : sstables)
        {
            stats.updateTimestamp(sstable.getMinTimestamp());
            stats.updateLocalDeletionTime(sstable.getMinLocalDeletionTime());
            stats.updateTTL(sstable.getMinTTL());
            if (sstable.header == null)
                columns.addAll(metadata.partitionColumns());
            else
                columns.addAll(sstable.header.columns());
        }
        return new SerializationHeader(true, metadata, columns.build(), stats.get());
    }

    public SerializationHeader(boolean isForSSTable,
                               CFMetaData metadata,
                               PartitionColumns columns,
                               EncodingStats stats)
    {
        this(isForSSTable,
             metadata.getKeyValidator(),
             typesOf(metadata.clusteringColumns()),
             columns,
             stats,
             null);
    }

    private static List<AbstractType<?>> typesOf(List<ColumnDefinition> columns)
    {
        return ImmutableList.copyOf(Lists.transform(columns, column -> column.type));
    }

    public PartitionColumns columns()
    {
        return columns;
    }

    public boolean hasStatic()
    {
        return !columns.statics.isEmpty();
    }

    public boolean isForSSTable()
    {
        return isForSSTable;
    }

    public EncodingStats stats()
    {
        return stats;
    }

    public AbstractType<?> keyType()
    {
        return keyType;
    }

    public List<AbstractType<?>> clusteringTypes()
    {
        return clusteringTypes;
    }

    public Columns columns(boolean isStatic)
    {
        return isStatic ? columns.statics : columns.regulars;
    }

    public AbstractType<?> getType(ColumnDefinition column)
    {
        return typeMap == null ? column.type : typeMap.get(column.name.bytes);
    }

    public void writeTimestamp(long timestamp, DataOutputPlus out) throws IOException
    {
        out.writeUnsignedVInt(timestamp - stats.minTimestamp);
    }

    public void writeLocalDeletionTime(int localDeletionTime, DataOutputPlus out) throws IOException
    {
        out.writeUnsignedVInt(localDeletionTime - stats.minLocalDeletionTime);
    }

    public void writeTTL(int ttl, DataOutputPlus out) throws IOException
    {
        out.writeUnsignedVInt(ttl - stats.minTTL);
    }

    public void writeDeletionTime(DeletionTime dt, DataOutputPlus out) throws IOException
    {
        writeTimestamp(dt.markedForDeleteAt(), out);
        writeLocalDeletionTime(dt.localDeletionTime(), out);
    }

    public long readTimestamp(DataInputPlus in) throws IOException
    {
        return in.readUnsignedVInt() + stats.minTimestamp;
    }

    public int readLocalDeletionTime(DataInputPlus in) throws IOException
    {
        return (int)in.readUnsignedVInt() + stats.minLocalDeletionTime;
    }

    public int readTTL(DataInputPlus in) throws IOException
    {
        return (int)in.readUnsignedVInt() + stats.minTTL;
    }

    public DeletionTime readDeletionTime(DataInputPlus in) throws IOException
    {
        long markedAt = readTimestamp(in);
        int localDeletionTime = readLocalDeletionTime(in);
        return new DeletionTime(markedAt, localDeletionTime);
    }

    public long timestampSerializedSize(long timestamp)
    {
        return TypeSizes.sizeofUnsignedVInt(timestamp - stats.minTimestamp);
    }

    public long localDeletionTimeSerializedSize(int localDeletionTime)
    {
        return TypeSizes.sizeofUnsignedVInt(localDeletionTime - stats.minLocalDeletionTime);
    }

    public long ttlSerializedSize(int ttl)
    {
        return TypeSizes.sizeofUnsignedVInt(ttl - stats.minTTL);
    }

    public long deletionTimeSerializedSize(DeletionTime dt)
    {
        return timestampSerializedSize(dt.markedForDeleteAt())
             + localDeletionTimeSerializedSize(dt.localDeletionTime());
    }

    public void skipTimestamp(DataInputPlus in) throws IOException
    {
        in.readUnsignedVInt();
    }

    public void skipLocalDeletionTime(DataInputPlus in) throws IOException
    {
        in.readUnsignedVInt();
    }

    public void skipTTL(DataInputPlus in) throws IOException
    {
        in.readUnsignedVInt();
    }

    public void skipDeletionTime(DataInputPlus in) throws IOException
    {
        skipTimestamp(in);
        skipLocalDeletionTime(in);
    }

    public Component toComponent()
    {
        Map<ByteBuffer, AbstractType<?>> staticColumns = new LinkedHashMap<>();
        Map<ByteBuffer, AbstractType<?>> regularColumns = new LinkedHashMap<>();
        for (ColumnDefinition column : columns.statics)
            staticColumns.put(column.name.bytes, column.type);
        for (ColumnDefinition column : columns.regulars)
            regularColumns.put(column.name.bytes, column.type);
        return new Component(keyType, clusteringTypes, staticColumns, regularColumns, stats);
    }

    @Override
    public String toString()
    {
        return String.format("SerializationHeader[key=%s, cks=%s, columns=%s, stats=%s, typeMap=%s]", keyType, clusteringTypes, columns, stats, typeMap);
    }

    /**
     * We need the CFMetadata to properly deserialize a SerializationHeader but it's clunky to pass that to
     * a SSTable component, so we use this temporary object to delay the actual need for the metadata.
     */
    public static class Component extends MetadataComponent
    {
        private final AbstractType<?> keyType;
        private final List<AbstractType<?>> clusteringTypes;
        private final Map<ByteBuffer, AbstractType<?>> staticColumns;
        private final Map<ByteBuffer, AbstractType<?>> regularColumns;
        private final EncodingStats stats;

        private Component(AbstractType<?> keyType,
                          List<AbstractType<?>> clusteringTypes,
                          Map<ByteBuffer, AbstractType<?>> staticColumns,
                          Map<ByteBuffer, AbstractType<?>> regularColumns,
                          EncodingStats stats)
        {
            this.keyType = keyType;
            this.clusteringTypes = clusteringTypes;
            this.staticColumns = staticColumns;
            this.regularColumns = regularColumns;
            this.stats = stats;
        }

        public MetadataType getType()
        {
            return MetadataType.HEADER;
        }

        public SerializationHeader toHeader(CFMetaData metadata)
        {
            Map<ByteBuffer, AbstractType<?>> typeMap = new HashMap<>(staticColumns.size() + regularColumns.size());
            typeMap.putAll(staticColumns);
            typeMap.putAll(regularColumns);

            PartitionColumns.Builder builder = PartitionColumns.builder();
            for (ByteBuffer name : typeMap.keySet())
            {
                ColumnDefinition column = metadata.getColumnDefinition(name);
                if (column == null)
                {
                    // TODO: this imply we don't read data for a column we don't yet know about, which imply this is theoretically
                    // racy with column addition. Currently, it is up to the user to not write data before the schema has propagated
                    // and this is far from being the only place that has such problem in practice. This doesn't mean we shouldn't
                    // improve this.

                    // If we don't find the definition, it could be we have data for a dropped column, and we shouldn't
                    // fail deserialization because of that. So we grab a "fake" ColumnDefinition that ensure proper
                    // deserialization. The column will be ignore later on anyway.
                    column = metadata.getDroppedColumnDefinition(name);
                    if (column == null)
                        throw new RuntimeException("Unknown column " + UTF8Type.instance.getString(name) + " during deserialization");
                }
                builder.add(column);
            }
            return new SerializationHeader(true, keyType, clusteringTypes, builder.build(), stats, typeMap);
        }

        @Override
        public boolean equals(Object o)
        {
            if(!(o instanceof Component))
                return false;

            Component that = (Component)o;
            return Objects.equals(this.keyType, that.keyType)
                && Objects.equals(this.clusteringTypes, that.clusteringTypes)
                && Objects.equals(this.staticColumns, that.staticColumns)
                && Objects.equals(this.regularColumns, that.regularColumns)
                && Objects.equals(this.stats, that.stats);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(keyType, clusteringTypes, staticColumns, regularColumns, stats);
        }

        @Override
        public String toString()
        {
            return String.format("SerializationHeader.Component[key=%s, cks=%s, statics=%s, regulars=%s, stats=%s]",
                                 keyType, clusteringTypes, staticColumns, regularColumns, stats);
        }

        public AbstractType<?> getKeyType()
        {
            return keyType;
        }

        public List<AbstractType<?>> getClusteringTypes()
        {
            return clusteringTypes;
        }

        public Map<ByteBuffer, AbstractType<?>> getStaticColumns()
        {
            return staticColumns;
        }

        public Map<ByteBuffer, AbstractType<?>> getRegularColumns()
        {
            return regularColumns;
        }

        public EncodingStats getEncodingStats()
        {
            return stats;
        }
    }

    public static class Serializer implements IMetadataComponentSerializer<Component>
    {
        public void serializeForMessaging(SerializationHeader header, ColumnFilter selection, DataOutputPlus out, boolean hasStatic) throws IOException
        {
            EncodingStats.serializer.serialize(header.stats, out);

            if (selection == null)
            {
                if (hasStatic)
                    Columns.serializer.serialize(header.columns.statics, out);
                Columns.serializer.serialize(header.columns.regulars, out);
            }
            else
            {
                if (hasStatic)
                    Columns.serializer.serializeSubset(header.columns.statics, selection.fetchedColumns().statics, out);
                Columns.serializer.serializeSubset(header.columns.regulars, selection.fetchedColumns().regulars, out);
            }
        }

        public SerializationHeader deserializeForMessaging(DataInputPlus in, CFMetaData metadata, ColumnFilter selection, boolean hasStatic) throws IOException
        {
            EncodingStats stats = EncodingStats.serializer.deserialize(in);

            AbstractType<?> keyType = metadata.getKeyValidator();
            List<AbstractType<?>> clusteringTypes = typesOf(metadata.clusteringColumns());

            Columns statics, regulars;
            if (selection == null)
            {
                statics = hasStatic ? Columns.serializer.deserialize(in, metadata) : Columns.NONE;
                regulars = Columns.serializer.deserialize(in, metadata);
            }
            else
            {
                statics = hasStatic ? Columns.serializer.deserializeSubset(selection.fetchedColumns().statics, in) : Columns.NONE;
                regulars = Columns.serializer.deserializeSubset(selection.fetchedColumns().regulars, in);
            }

            return new SerializationHeader(false, keyType, clusteringTypes, new PartitionColumns(statics, regulars), stats, null);
        }

        public long serializedSizeForMessaging(SerializationHeader header, ColumnFilter selection, boolean hasStatic)
        {
            long size = EncodingStats.serializer.serializedSize(header.stats);

            if (selection == null)
            {
                if (hasStatic)
                    size += Columns.serializer.serializedSize(header.columns.statics);
                size += Columns.serializer.serializedSize(header.columns.regulars);
            }
            else
            {
                if (hasStatic)
                    size += Columns.serializer.serializedSubsetSize(header.columns.statics, selection.fetchedColumns().statics);
                size += Columns.serializer.serializedSubsetSize(header.columns.regulars, selection.fetchedColumns().regulars);
            }
            return size;
        }

        // For SSTables
        public void serialize(Version version, Component header, DataOutputPlus out) throws IOException
        {
            EncodingStats.serializer.serialize(header.stats, out);

            writeType(header.keyType, out);
            out.writeUnsignedVInt(header.clusteringTypes.size());
            for (AbstractType<?> type : header.clusteringTypes)
                writeType(type, out);

            writeColumnsWithTypes(header.staticColumns, out);
            writeColumnsWithTypes(header.regularColumns, out);
        }

        // For SSTables
        public Component deserialize(Version version, DataInputPlus in) throws IOException
        {
            EncodingStats stats = EncodingStats.serializer.deserialize(in);

            AbstractType<?> keyType = readType(in);
            int size = (int)in.readUnsignedVInt();
            List<AbstractType<?>> clusteringTypes = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
                clusteringTypes.add(readType(in));

            Map<ByteBuffer, AbstractType<?>> staticColumns = new LinkedHashMap<>();
            Map<ByteBuffer, AbstractType<?>> regularColumns = new LinkedHashMap<>();

            readColumnsWithType(in, staticColumns);
            readColumnsWithType(in, regularColumns);

            return new Component(keyType, clusteringTypes, staticColumns, regularColumns, stats);
        }

        // For SSTables
        public int serializedSize(Version version, Component header)
        {
            int size = EncodingStats.serializer.serializedSize(header.stats);

            size += sizeofType(header.keyType);
            size += TypeSizes.sizeofUnsignedVInt(header.clusteringTypes.size());
            for (AbstractType<?> type : header.clusteringTypes)
                size += sizeofType(type);

            size += sizeofColumnsWithTypes(header.staticColumns);
            size += sizeofColumnsWithTypes(header.regularColumns);
            return size;
        }

        private void writeColumnsWithTypes(Map<ByteBuffer, AbstractType<?>> columns, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt(columns.size());
            for (Map.Entry<ByteBuffer, AbstractType<?>> entry : columns.entrySet())
            {
                ByteBufferUtil.writeWithVIntLength(entry.getKey(), out);
                writeType(entry.getValue(), out);
            }
        }

        private long sizeofColumnsWithTypes(Map<ByteBuffer, AbstractType<?>> columns)
        {
            long size = TypeSizes.sizeofUnsignedVInt(columns.size());
            for (Map.Entry<ByteBuffer, AbstractType<?>> entry : columns.entrySet())
            {
                size += ByteBufferUtil.serializedSizeWithVIntLength(entry.getKey());
                size += sizeofType(entry.getValue());
            }
            return size;
        }

        private void readColumnsWithType(DataInputPlus in, Map<ByteBuffer, AbstractType<?>> typeMap) throws IOException
        {
            int length = (int)in.readUnsignedVInt();
            for (int i = 0; i < length; i++)
            {
                ByteBuffer name = ByteBufferUtil.readWithVIntLength(in);
                typeMap.put(name, readType(in));
            }
        }

        private void writeType(AbstractType<?> type, DataOutputPlus out) throws IOException
        {
            // TODO: we should have a terser serializaion format. Not a big deal though
            ByteBufferUtil.writeWithVIntLength(UTF8Type.instance.decompose(type.toString()), out);
        }

        private AbstractType<?> readType(DataInputPlus in) throws IOException
        {
            ByteBuffer raw = ByteBufferUtil.readWithVIntLength(in);
            return TypeParser.parse(UTF8Type.instance.compose(raw));
        }

        private int sizeofType(AbstractType<?> type)
        {
            return ByteBufferUtil.serializedSizeWithVIntLength(UTF8Type.instance.decompose(type.toString()));
        }
    }
}
