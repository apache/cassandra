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
package org.apache.cassandra.db.rows;

import com.google.common.base.Preconditions;
import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.db.*;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.WrappedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.schema.TableMetadata;

import java.io.IOError;
import java.io.IOException;

/**
 * Static methods to work with row iterators.
 */
public abstract class RowIterators
{
    private static final Logger logger = LoggerFactory.getLogger(RowIterators.class);

    private RowIterators() {}

    public static void digest(RowIterator iterator, Digest digest)
    {
        // TODO: we're not computing digest the same way that old nodes. This is
        // currently ok as this is only used for schema digest and the is no exchange
        // of schema digest between different versions. If this changes however,
        // we'll need to agree on a version.
        digest.update(iterator.partitionKey().getKey());
        iterator.columns().regulars.digest(digest);
        iterator.columns().statics.digest(digest);
        digest.updateWithBoolean(iterator.isReverseOrder());
        iterator.staticRow().digest(digest);

        while (iterator.hasNext())
            iterator.next().digest(digest);
    }

    /**
     * Filter the provided iterator to only include cells that are selected by the user.
     *
     * @param iterator the iterator to filter.
     * @param filter the {@code ColumnFilter} to use when deciding which cells are queried by the user. This should be the filter
     * that was used when querying {@code iterator}.
     * @return the filtered iterator..
     */
    public static RowIterator withOnlyQueriedData(RowIterator iterator, ColumnFilter filter)
    {
        if (filter.allFetchedColumnsAreQueried())
            return iterator;

        return Transformation.apply(iterator, new WithOnlyQueriedData(filter));
    }

    /**
     * Wraps the provided iterator so it logs the returned rows for debugging purposes.
     * <p>
     * Note that this is only meant for debugging as this can log a very large amount of
     * logging at INFO.
     */
    public static RowIterator loggingIterator(RowIterator iterator, final String id)
    {
        TableMetadata metadata = iterator.metadata();
        logger.info("[{}] Logging iterator on {}.{}, partition key={}, reversed={}",
                    id,
                    metadata.keyspace,
                    metadata.name,
                    metadata.partitionKeyType.getString(iterator.partitionKey().getKey()),
                    iterator.isReverseOrder());

        class Log extends Transformation
        {
            @Override
            public Row applyToStatic(Row row)
            {
                if (!row.isEmpty())
                    logger.info("[{}] {}", id, row.toString(metadata));
                return row;
            }

            @Override
            public Row applyToRow(Row row)
            {
                logger.info("[{}] {}", id, row.toString(metadata));
                return row;
            }
        }
        return Transformation.apply(iterator, new Log());
    }

    private static class RowSerializer
    {
        /*
         * Row flags constants.
         */
        private final static int END_OF_PARTITION     = 0x01; // Signal the end of the partition. Nothing follows a <flags> field with that flag.
        private final static int HAS_ALL_COLUMNS      = 0x02; // Whether the encoded row has all of the columns from the header present.
        private final static int IS_STATIC            = 0x04; // Whether the encoded row is a static. If there is no extended flag, the row is assumed not static.


        public static void writeEndOfPartition(DataOutputPlus out) throws IOException
        {
            out.writeByte((byte) END_OF_PARTITION);
        }

        public static void serializeRow(Row row, SerializationHelper helper, DataOutputPlus out, int version)
                throws IOException
        {
            int flags = 0;

            boolean isStatic = row.isStatic();
            SerializationHeader header = helper.header;
            Preconditions.checkArgument(!header.isForSSTable());
            boolean hasAllColumns = row.columnCount() == header.columns(isStatic).size();

            if (isStatic)
                flags |= IS_STATIC;

            if (hasAllColumns)
                flags |= HAS_ALL_COLUMNS;

            out.writeByte((byte)flags);

            if (!isStatic)
                Clustering.serializer.serialize(row.clustering(), out, version, header.clusteringTypes());

            serializeRowBody(row, flags, helper, out);
        }

        public static void serializeStaticRow(Row row, SerializationHelper helper, DataOutputPlus out, int version) throws IOException
        {
            Preconditions.checkArgument(row.isStatic());
            serializeRow(row, helper, out, version);
        }

        @Inline
        private static void serializeRowBody(Row row, int flags, SerializationHelper helper, DataOutputPlus out)
                throws IOException
        {
            boolean isStatic = row.isStatic();

            SerializationHeader header = helper.header;
            Preconditions.checkState(!header.isForSSTable());
            Columns headerColumns = header.columns(isStatic);

            if ((flags & HAS_ALL_COLUMNS) == 0)
                Columns.serializer.serializeSubset(row.columns(), headerColumns, out);

            SearchIterator<ColumnMetadata, ColumnMetadata> si = helper.iterator(isStatic);

            try
            {
                row.apply(cd -> {
                    // We can obtain the column for data directly from data.column(). However, if the cell/complex data
                    // originates from a sstable, the column we'll get will have the type used when the sstable was serialized,
                    // and if that type have been recently altered, that may not be the type we want to serialize the column
                    // with. So we use the ColumnMetadata from the "header" which is "current". Also see #11810 for what
                    // happens if we don't do that.
                    ColumnMetadata column = si.next(cd.column());
                    assert column != null : cd.column.toString();

                    try
                    {
                        if (cd.column.isSimple())
                            Cell.serializer.serialize((Cell<?>) cd, column, out, LivenessInfo.EMPTY, header);
                        else
                            UnfilteredSerializer.writeComplexColumn((ComplexColumnData) cd, column, false, LivenessInfo.EMPTY, header, out);
                    }
                    catch (IOException e)
                    {
                        throw new WrappedException(e);
                    }
                });
            }
            catch (WrappedException e)
            {
                if (e.getCause() instanceof IOException)
                    throw (IOException) e.getCause();

                throw e;
            }
        }

        public static Row deserializeStaticRow(DataInputPlus in, SerializationHeader header, DeserializationHelper helper)
                throws IOException
        {
            int flags = in.readUnsignedByte();
            Preconditions.checkState(!isEndOfPartition(flags));
            Preconditions.checkState(isStatic(flags));
            Row.Builder builder = BTreeRow.sortedBuilder();
            builder.newRow(Clustering.STATIC_CLUSTERING);
            return deserializeRowBody(in, header, helper, flags, builder);
        }

        public static Row deserializeRow(DataInputPlus in, SerializationHeader header, DeserializationHelper helper, Row.Builder builder) throws IOException
        {
            int flags = in.readUnsignedByte();
            if (isEndOfPartition(flags))
                return null;
            builder.newRow(Clustering.serializer.deserialize(in, helper.version, header.clusteringTypes()));
            return deserializeRowBody(in, header, helper, flags, builder);
        }

        public static Row deserializeRowBody(DataInputPlus in,
                                      SerializationHeader header,
                                      DeserializationHelper helper,
                                      int flags,
                                      Row.Builder builder)
                throws IOException
        {
            try
            {
                boolean isStatic = isStatic(flags);
                boolean hasAllColumns = (flags & HAS_ALL_COLUMNS) != 0;
                Columns headerColumns = header.columns(isStatic);

                Preconditions.checkState(!header.isForSSTable());

                Columns columns = hasAllColumns ? headerColumns : Columns.serializer.deserializeSubset(headerColumns, in);

                try
                {
                    DataInputPlus finalIn = in;
                    columns.apply(column -> {
                        try
                        {
                            if (column.isSimple())
                                UnfilteredSerializer.readSimpleColumn(column, finalIn, header, helper, builder, LivenessInfo.EMPTY);
                            else
                                UnfilteredSerializer.readComplexColumn(column, finalIn, header, helper, false, builder, LivenessInfo.EMPTY);
                        }
                        catch (IOException e)
                        {
                            throw new WrappedException(e);
                        }
                    });
                }
                catch (WrappedException e)
                {
                    if (e.getCause() instanceof IOException)
                        throw (IOException) e.getCause();

                    throw e;
                }

                return builder.build();
            }
            catch (RuntimeException | AssertionError e)
            {
                // Corrupted data could be such that it triggers an assertion in the row Builder, or break one of its assumption.
                // Of course, a bug in said builder could also trigger this, but it's impossible a priori to always make the distinction
                // between a real bug and data corrupted in just the bad way. Besides, re-throwing as an IOException doesn't hide the
                // exception, it just make we catch it properly and mark the sstable as corrupted.
                throw new IOException("Error building row with data deserialized from " + in, e);
            }
        }

        public static boolean isEndOfPartition(int flags)
        {
            return (flags & END_OF_PARTITION) != 0;
        }

        public static boolean isStatic(int extendedFlags)
        {
            return (extendedFlags & IS_STATIC) != 0;
        }
    }

    public static class Serializer
    {
        public  static final int IS_EMPTY               = 0x01;
        private static final int IS_REVERSED            = 0x02;
        private static final int HAS_STATIC_ROW         = 0x04;

        public static void serialize(RowIterator iterator, ColumnFilter selection, DataOutputPlus out, int version) throws IOException
        {
            ByteBufferUtil.writeWithVIntLength(iterator.partitionKey().getKey(), out);

            SerializationHeader header = new SerializationHeader(false,
                                                                 iterator.metadata(),
                                                                 iterator.columns(),
                                                                 EncodingStats.NO_STATS);

            int flags = 0;
            if (iterator.isReverseOrder())
                flags |= IS_REVERSED;

            if (iterator.isEmpty())
            {
                out.writeByte(flags | IS_EMPTY);
                return;
            }

            Row staticRow = iterator.staticRow();
            boolean hasStatic = staticRow != Rows.EMPTY_STATIC_ROW;
            if (hasStatic)
                flags |= HAS_STATIC_ROW;

            out.writeByte((byte)flags);

            SerializationHeader.serializer.serializeForMessaging(header, selection, out, hasStatic);
            SerializationHelper helper = new SerializationHelper(header);

            if (hasStatic)
                RowSerializer.serializeStaticRow(staticRow, helper, out, version);

            while (iterator.hasNext())
                RowSerializer.serializeRow(iterator.next(), helper, out, version);
            RowSerializer.writeEndOfPartition(out);
        }

        public static RowIterator deserialize(TableMetadata metadata, ColumnFilter selection, DataInputPlus in, int version) throws IOException
        {
            DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.readWithVIntLength(in));
            int flags = in.readUnsignedByte();
            boolean isReversed = (flags & IS_REVERSED) != 0;
            if ((flags & IS_EMPTY) != 0)
            {
                return EmptyIterators.row(metadata, key, isReversed);
            }

            boolean hasStatic = (flags & HAS_STATIC_ROW) != 0;

            SerializationHeader header = SerializationHeader.serializer.deserializeForMessaging(in, metadata, selection, hasStatic);

            DeserializationHelper helper = new DeserializationHelper(metadata, version, DeserializationHelper.Flag.FROM_REMOTE);
            Row staticRow = hasStatic ? RowSerializer.deserializeStaticRow(in, header, helper) : Rows.EMPTY_STATIC_ROW;

            return new AbstractRowIterator(metadata, key, header.columns(), isReversed, staticRow)
            {
                private final Row.Builder builder = BTreeRow.sortedBuilder();

                @Override
                protected Row computeNext()
                {
                    try
                    {
                        Row row = RowSerializer.deserializeRow(in, header, helper, builder);
                        return row != null ? row : endOfData();
                    }
                    catch (IOException e)
                    {
                        throw new IOError(e);
                    }
                }

                @Override
                public void close()
                {
                    while (hasNext())
                        next();

                    super.close();
                }
            };
        }
    };
}
