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

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.base.Function;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
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
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SerializationHeader
{
    private static final int DEFAULT_BASE_DELETION = computeDefaultBaseDeletion();

    public static final Serializer serializer = new Serializer();

    private final AbstractType<?> keyType;
    private final List<AbstractType<?>> clusteringTypes;

    private final PartitionColumns columns;
    private final RowStats stats;

    private final Map<ByteBuffer, AbstractType<?>> typeMap;

    private final long baseTimestamp;
    public final int baseDeletionTime;
    private final int baseTTL;

    // Whether or not to store cell in a sparse or dense way. See UnfilteredSerializer for details.
    private final boolean useSparseColumnLayout;

    private SerializationHeader(AbstractType<?> keyType,
                                List<AbstractType<?>> clusteringTypes,
                                PartitionColumns columns,
                                RowStats stats,
                                Map<ByteBuffer, AbstractType<?>> typeMap)
    {
        this.keyType = keyType;
        this.clusteringTypes = clusteringTypes;
        this.columns = columns;
        this.stats = stats;
        this.typeMap = typeMap;

        // Not that if a given stats is unset, it means that either it's unused (there is
        // no tombstone whatsoever for instance) or that we have no information on it. In
        // that former case, it doesn't matter which base we use but in the former, we use
        // bases that are more likely to provide small encoded values than the default
        // "unset" value.
        this.baseTimestamp = stats.hasMinTimestamp() ? stats.minTimestamp : 0;
        this.baseDeletionTime = stats.hasMinLocalDeletionTime() ? stats.minLocalDeletionTime : DEFAULT_BASE_DELETION;
        this.baseTTL = stats.minTTL;

        // For the dense layout, we have a 1 byte overhead for absent columns. For the sparse layout, it's a 1
        // overhead for present columns (in fact we use a 2 byte id, but assuming vint encoding, we'll pay 2 bytes
        // only for the columns after the 128th one and for simplicity we assume that once you have that many column,
        // you'll tend towards a clearly dense or clearly sparse case so that the heurstic above shouldn't still be
        // too inapropriate). So if on average more than half of our columns are set per row, we better go for sparse.
        this.useSparseColumnLayout = stats.avgColumnSetPerRow <= (columns.regulars.columnCount()/ 2);
    }

    public boolean useSparseColumnLayout(boolean isStatic)
    {
        // We always use a dense layout for the static row. Having very many static columns with  only a few set at
        // any given time doesn't feel very common at all (and we already optimize the case where no static at all
        // are provided).
        return isStatic ? false : useSparseColumnLayout;
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
        return new SerializationHeader(BytesType.instance,
                                       clusteringTypes,
                                       PartitionColumns.NONE,
                                       RowStats.NO_STATS,
                                       Collections.<ByteBuffer, AbstractType<?>>emptyMap());
    }

    public static SerializationHeader make(CFMetaData metadata, Collection<SSTableReader> sstables)
    {
        // The serialization header has to be computed before the start of compaction (since it's used to write)
        // the result. This means that when compacting multiple sources, we won't have perfectly accurate stats
        // (for RowStats) since compaction may delete, purge and generally merge rows in unknown ways. This is
        // kind of ok because those stats are only used for optimizing the underlying storage format and so we
        // just have to strive for as good as possible. Currently, we stick to a relatively naive merge of existing
        // global stats because it's simple and probably good enough in most situation but we could probably
        // improve our marging of inaccuracy through the use of more fine-grained stats in the future.
        // Note however that to avoid seeing our accuracy degrade through successive compactions, we don't base
        // our stats merging on the compacted files headers, which as we just said can be somewhat inaccurate,
        // but rather on their stats stored in StatsMetadata that are fully accurate.
        RowStats.Collector stats = new RowStats.Collector();
        PartitionColumns.Builder columns = PartitionColumns.builder();
        for (SSTableReader sstable : sstables)
        {
            stats.updateTimestamp(sstable.getMinTimestamp());
            stats.updateLocalDeletionTime(sstable.getMinLocalDeletionTime());
            stats.updateTTL(sstable.getMinTTL());
            stats.updateColumnSetPerRow(sstable.getTotalColumnsSet(), sstable.getTotalRows());
            if (sstable.header == null)
                columns.addAll(metadata.partitionColumns());
            else
                columns.addAll(sstable.header.columns());
        }
        return new SerializationHeader(metadata, columns.build(), stats.get());
    }

    public SerializationHeader(CFMetaData metadata,
                               PartitionColumns columns,
                               RowStats stats)
    {
        this(metadata.getKeyValidator(),
             typesOf(metadata.clusteringColumns()),
             columns,
             stats,
             null);
    }

    private static List<AbstractType<?>> typesOf(List<ColumnDefinition> columns)
    {
        return ImmutableList.copyOf(Lists.transform(columns, new Function<ColumnDefinition, AbstractType<?>>()
        {
            public AbstractType<?> apply(ColumnDefinition column)
            {
                return column.type;
            }
        }));
    }

    public PartitionColumns columns()
    {
        return columns;
    }

    public boolean hasStatic()
    {
        return !columns.statics.isEmpty();
    }

    private static int computeDefaultBaseDeletion()
    {
        // We need a fixed default, but one that is likely to provide small values (close to 0) when
        // substracted to deletion times. Since deletion times are 'the current time in seconds', we
        // use as base Jan 1, 2015 (in seconds).
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT-0"), Locale.US);
        c.set(Calendar.YEAR, 2015);
        c.set(Calendar.MONTH, Calendar.JANUARY);
        c.set(Calendar.DAY_OF_MONTH, 1);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        return (int)(c.getTimeInMillis() / 1000);
    }

    public RowStats stats()
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

    public long encodeTimestamp(long timestamp)
    {
        return timestamp - baseTimestamp;
    }

    public long decodeTimestamp(long timestamp)
    {
        return baseTimestamp + timestamp;
    }

    public int encodeDeletionTime(int deletionTime)
    {
        return deletionTime - baseDeletionTime;
    }

    public int decodeDeletionTime(int deletionTime)
    {
        return baseDeletionTime + deletionTime;
    }

    public int encodeTTL(int ttl)
    {
        return ttl - baseTTL;
    }

    public int decodeTTL(int ttl)
    {
        return baseTTL + ttl;
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
        return String.format("SerializationHeader[key=%s, cks=%s, columns=%s, stats=%s, typeMap=%s, baseTs=%d, baseDt=%s, baseTTL=%s]",
                             keyType, clusteringTypes, columns, stats, typeMap, baseTimestamp, baseDeletionTime, baseTTL);
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
        private final RowStats stats;

        private Component(AbstractType<?> keyType,
                          List<AbstractType<?>> clusteringTypes,
                          Map<ByteBuffer, AbstractType<?>> staticColumns,
                          Map<ByteBuffer, AbstractType<?>> regularColumns,
                          RowStats stats)
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
            return new SerializationHeader(keyType, clusteringTypes, builder.build(), stats, typeMap);
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
    }

    public static class Serializer implements IMetadataComponentSerializer<Component>
    {
        public void serializeForMessaging(SerializationHeader header, DataOutputPlus out, boolean hasStatic) throws IOException
        {
            RowStats.serializer.serialize(header.stats, out);

            if (hasStatic)
                Columns.serializer.serialize(header.columns.statics, out);
            Columns.serializer.serialize(header.columns.regulars, out);
        }

        public SerializationHeader deserializeForMessaging(DataInput in, CFMetaData metadata, boolean hasStatic) throws IOException
        {
            RowStats stats = RowStats.serializer.deserialize(in);

            AbstractType<?> keyType = metadata.getKeyValidator();
            List<AbstractType<?>> clusteringTypes = typesOf(metadata.clusteringColumns());

            Columns statics = hasStatic ? Columns.serializer.deserialize(in, metadata) : Columns.NONE;
            Columns regulars = Columns.serializer.deserialize(in, metadata);

            return new SerializationHeader(keyType, clusteringTypes, new PartitionColumns(statics, regulars), stats, null);
        }

        public long serializedSizeForMessaging(SerializationHeader header, TypeSizes sizes, boolean hasStatic)
        {
            long size = RowStats.serializer.serializedSize(header.stats, sizes);

            if (hasStatic)
                size += Columns.serializer.serializedSize(header.columns.statics, sizes);
            size += Columns.serializer.serializedSize(header.columns.regulars, sizes);
            return size;
        }

        // For SSTables
        public void serialize(Component header, DataOutputPlus out) throws IOException
        {
            RowStats.serializer.serialize(header.stats, out);

            writeType(header.keyType, out);
            out.writeShort(header.clusteringTypes.size());
            for (AbstractType<?> type : header.clusteringTypes)
                writeType(type, out);

            writeColumnsWithTypes(header.staticColumns, out);
            writeColumnsWithTypes(header.regularColumns, out);
        }

        // For SSTables
        public Component deserialize(Version version, DataInput in) throws IOException
        {
            RowStats stats = RowStats.serializer.deserialize(in);

            AbstractType<?> keyType = readType(in);
            int size = in.readUnsignedShort();
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
        public int serializedSize(Component header)
        {
            TypeSizes sizes = TypeSizes.NATIVE;
            int size = RowStats.serializer.serializedSize(header.stats, sizes);

            size += sizeofType(header.keyType, sizes);
            size += sizes.sizeof((short)header.clusteringTypes.size());
            for (AbstractType<?> type : header.clusteringTypes)
                size += sizeofType(type, sizes);

            size += sizeofColumnsWithTypes(header.staticColumns, sizes);
            size += sizeofColumnsWithTypes(header.regularColumns, sizes);
            return size;
        }

        private void writeColumnsWithTypes(Map<ByteBuffer, AbstractType<?>> columns, DataOutputPlus out) throws IOException
        {
            out.writeShort(columns.size());
            for (Map.Entry<ByteBuffer, AbstractType<?>> entry : columns.entrySet())
            {
                ByteBufferUtil.writeWithShortLength(entry.getKey(), out);
                writeType(entry.getValue(), out);
            }
        }

        private long sizeofColumnsWithTypes(Map<ByteBuffer, AbstractType<?>> columns, TypeSizes sizes)
        {
            long size = sizes.sizeof((short)columns.size());
            for (Map.Entry<ByteBuffer, AbstractType<?>> entry : columns.entrySet())
            {
                size += sizes.sizeofWithShortLength(entry.getKey());
                size += sizeofType(entry.getValue(), sizes);
            }
            return size;
        }

        private void readColumnsWithType(DataInput in, Map<ByteBuffer, AbstractType<?>> typeMap) throws IOException
        {
            int length = in.readUnsignedShort();
            for (int i = 0; i < length; i++)
            {
                ByteBuffer name = ByteBufferUtil.readWithShortLength(in);
                typeMap.put(name, readType(in));
            }
        }

        private void writeType(AbstractType<?> type, DataOutputPlus out) throws IOException
        {
            // TODO: we should have a terser serializaion format. Not a big deal though
            ByteBufferUtil.writeWithLength(UTF8Type.instance.decompose(type.toString()), out);
        }

        private AbstractType<?> readType(DataInput in) throws IOException
        {
            ByteBuffer raw = ByteBufferUtil.readWithLength(in);
            return TypeParser.parse(UTF8Type.instance.compose(raw));
        }

        private int sizeofType(AbstractType<?> type, TypeSizes sizes)
        {
            return sizes.sizeofWithLength(UTF8Type.instance.decompose(type.toString()));
        }
    }
}
