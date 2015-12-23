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
import java.io.IOError;
import java.util.Iterator;

import org.apache.cassandra.io.util.RewindableDataInput;
import org.apache.cassandra.utils.AbstractIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.net.MessagingService;

/**
 * Utility class to handle deserializing atom from sstables.
 *
 * Note that this is not a full fledged UnfilteredRowIterator. It's also not closeable, it is always
 * the job of the user to close the underlying ressources.
 */
public abstract class SSTableSimpleIterator extends AbstractIterator<Unfiltered> implements Iterator<Unfiltered>
{
    protected final CFMetaData metadata;
    protected final DataInputPlus in;
    protected final SerializationHelper helper;

    private SSTableSimpleIterator(CFMetaData metadata, DataInputPlus in, SerializationHelper helper)
    {
        this.metadata = metadata;
        this.in = in;
        this.helper = helper;
    }

    public static SSTableSimpleIterator create(CFMetaData metadata, DataInputPlus in, SerializationHeader header, SerializationHelper helper, DeletionTime partitionDeletion)
    {
        if (helper.version < MessagingService.VERSION_30)
            return new OldFormatIterator(metadata, in, helper, partitionDeletion);
        else
            return new CurrentFormatIterator(metadata, in, header, helper);
    }

    public static SSTableSimpleIterator createTombstoneOnly(CFMetaData metadata, DataInputPlus in, SerializationHeader header, SerializationHelper helper, DeletionTime partitionDeletion)
    {
        if (helper.version < MessagingService.VERSION_30)
            return new OldFormatTombstoneIterator(metadata, in, helper, partitionDeletion);
        else
            return new CurrentFormatTombstoneIterator(metadata, in, header, helper);
    }

    public abstract Row readStaticRow() throws IOException;

    private static class CurrentFormatIterator extends SSTableSimpleIterator
    {
        private final SerializationHeader header;

        private final Row.Builder builder;

        private CurrentFormatIterator(CFMetaData metadata, DataInputPlus in, SerializationHeader header, SerializationHelper helper)
        {
            super(metadata, in, helper);
            this.header = header;
            this.builder = BTreeRow.sortedBuilder();
        }

        public Row readStaticRow() throws IOException
        {
            return header.hasStatic() ? UnfilteredSerializer.serializer.deserializeStaticRow(in, header, helper) : Rows.EMPTY_STATIC_ROW;
        }

        protected Unfiltered computeNext()
        {
            try
            {
                Unfiltered unfiltered = UnfilteredSerializer.serializer.deserialize(in, header, helper, builder);
                return unfiltered == null ? endOfData() : unfiltered;
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }
    }

    private static class CurrentFormatTombstoneIterator extends SSTableSimpleIterator
    {
        private final SerializationHeader header;

        private CurrentFormatTombstoneIterator(CFMetaData metadata, DataInputPlus in, SerializationHeader header, SerializationHelper helper)
        {
            super(metadata, in, helper);
            this.header = header;
        }

        public Row readStaticRow() throws IOException
        {
            if (header.hasStatic())
            {
                Row staticRow = UnfilteredSerializer.serializer.deserializeStaticRow(in, header, helper);
                if (!staticRow.deletion().isLive())
                    return BTreeRow.emptyDeletedRow(staticRow.clustering(), staticRow.deletion());
            }
            return Rows.EMPTY_STATIC_ROW;
        }

        protected Unfiltered computeNext()
        {
            try
            {
                Unfiltered unfiltered = UnfilteredSerializer.serializer.deserializeTombstonesOnly((FileDataInput) in, header, helper);
                return unfiltered == null ? endOfData() : unfiltered;
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }
    }

    private static class OldFormatIterator extends SSTableSimpleIterator
    {
        private final UnfilteredDeserializer deserializer;

        private OldFormatIterator(CFMetaData metadata, DataInputPlus in, SerializationHelper helper, DeletionTime partitionDeletion)
        {
            super(metadata, in, helper);
            // We use an UnfilteredDeserializer because even though we don't need all it's fanciness, it happens to handle all
            // the details we need for reading the old format.
            this.deserializer = UnfilteredDeserializer.create(metadata, in, null, helper, partitionDeletion, false);
        }

        public Row readStaticRow() throws IOException
        {
            if (metadata.isCompactTable())
            {
                // For static compact tables, in the old format, static columns are intermingled with the other columns, so we
                // need to extract them. Which imply 2 passes (one to extract the static, then one for other value).
                if (metadata.isStaticCompactTable())
                {
                    assert in instanceof RewindableDataInput;
                    RewindableDataInput file = (RewindableDataInput)in;
                    DataPosition mark = file.mark();
                    Row staticRow = LegacyLayout.extractStaticColumns(metadata, file, metadata.partitionColumns().statics);
                    file.reset(mark);

                    // We've extracted the static columns, so we must ignore them on the 2nd pass
                    ((UnfilteredDeserializer.OldFormatDeserializer)deserializer).setSkipStatic();
                    return staticRow;
                }
                else
                {
                    return Rows.EMPTY_STATIC_ROW;
                }
            }

            return deserializer.hasNext() && deserializer.nextIsStatic()
                 ? (Row)deserializer.readNext()
                 : Rows.EMPTY_STATIC_ROW;

        }

        protected Unfiltered computeNext()
        {
            while (true)
            {
                try
                {
                    if (!deserializer.hasNext())
                        return endOfData();

                    Unfiltered unfiltered = deserializer.readNext();
                    if (metadata.isStaticCompactTable() && unfiltered.kind() == Unfiltered.Kind.ROW)
                    {
                        Row row = (Row) unfiltered;
                        ColumnDefinition def = metadata.getColumnDefinition(LegacyLayout.encodeClustering(metadata, row.clustering()));
                        if (def != null && def.isStatic())
                            continue;
                    }
                    return unfiltered;
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }
        }

    }

    private static class OldFormatTombstoneIterator extends OldFormatIterator
    {
        private OldFormatTombstoneIterator(CFMetaData metadata, DataInputPlus in, SerializationHelper helper, DeletionTime partitionDeletion)
        {
            super(metadata, in, helper, partitionDeletion);
        }

        public Row readStaticRow() throws IOException
        {
            Row row = super.readStaticRow();
            if (!row.deletion().isLive())
                return BTreeRow.emptyDeletedRow(row.clustering(), row.deletion());
            return Rows.EMPTY_STATIC_ROW;
        }

        protected Unfiltered computeNext()
        {
            while (true)
            {
                Unfiltered unfiltered = super.computeNext();
                if (unfiltered == null || unfiltered.isRangeTombstoneMarker())
                    return unfiltered;

                Row row = (Row) unfiltered;
                if (!row.deletion().isLive())
                    return BTreeRow.emptyDeletedRow(row.clustering(), row.deletion());
                // Otherwise read next.
            }
        }

    }
}
