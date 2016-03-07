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
import java.io.IOError;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.*;

import org.apache.cassandra.utils.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.*;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

/**
 * Functions to deal with the old format.
 */
public abstract class LegacyLayout
{
    public final static int MAX_CELL_NAME_LENGTH = FBUtilities.MAX_UNSIGNED_SHORT;

    public final static int STATIC_PREFIX = 0xFFFF;

    public final static int DELETION_MASK        = 0x01;
    public final static int EXPIRATION_MASK      = 0x02;
    public final static int COUNTER_MASK         = 0x04;
    public final static int COUNTER_UPDATE_MASK  = 0x08;
    private final static int RANGE_TOMBSTONE_MASK = 0x10;

    private LegacyLayout() {}

    public static AbstractType<?> makeLegacyComparator(CFMetaData metadata)
    {
        ClusteringComparator comparator = metadata.comparator;
        if (!metadata.isCompound())
        {
            assert comparator.size() == 1;
            return comparator.subtype(0);
        }

        boolean hasCollections = metadata.hasCollectionColumns() || metadata.hasDroppedCollectionColumns();
        List<AbstractType<?>> types = new ArrayList<>(comparator.size() + (metadata.isDense() ? 0 : 1) + (hasCollections ? 1 : 0));

        types.addAll(comparator.subtypes());

        if (!metadata.isDense())
        {
            types.add(UTF8Type.instance);

            if (hasCollections)
            {
                Map<ByteBuffer, CollectionType> defined = new HashMap<>();

                for (CFMetaData.DroppedColumn def : metadata.getDroppedColumns().values())
                    if (def.type instanceof CollectionType && def.type.isMultiCell())
                        defined.put(bytes(def.name), (CollectionType) def.type);

                for (ColumnDefinition def : metadata.partitionColumns())
                    if (def.type instanceof CollectionType && def.type.isMultiCell())
                        defined.put(def.name.bytes, (CollectionType) def.type);

                types.add(ColumnToCollectionType.getInstance(defined));
            }
        }
        return CompositeType.getInstance(types);
    }

    public static LegacyCellName decodeCellName(CFMetaData metadata, ByteBuffer superColumnName, ByteBuffer cellname)
    throws UnknownColumnException
    {
        assert cellname != null;
        if (metadata.isSuper())
        {
            assert superColumnName != null;
            return decodeForSuperColumn(metadata, Clustering.make(superColumnName), cellname);
        }

        assert superColumnName == null;
        return decodeCellName(metadata, cellname);
    }

    private static LegacyCellName decodeForSuperColumn(CFMetaData metadata, Clustering clustering, ByteBuffer subcol)
    {
        ColumnDefinition def = metadata.getColumnDefinition(subcol);
        if (def != null)
        {
            // it's a statically defined subcolumn
            return new LegacyCellName(clustering, def, null);
        }

        def = metadata.compactValueColumn();
        assert def != null && def.type instanceof MapType;
        return new LegacyCellName(clustering, def, subcol);
    }

    public static LegacyCellName decodeCellName(CFMetaData metadata, ByteBuffer cellname) throws UnknownColumnException
    {
        return decodeCellName(metadata, cellname, false);
    }

    public static LegacyCellName decodeCellName(CFMetaData metadata, ByteBuffer cellname, boolean readAllAsDynamic) throws UnknownColumnException
    {
        Clustering clustering = decodeClustering(metadata, cellname);

        if (metadata.isSuper())
            return decodeForSuperColumn(metadata, clustering, CompositeType.extractComponent(cellname, 1));

        if (metadata.isDense() || (metadata.isCompactTable() && readAllAsDynamic))
            return new LegacyCellName(clustering, metadata.compactValueColumn(), null);

        ByteBuffer column = metadata.isCompound() ? CompositeType.extractComponent(cellname, metadata.comparator.size()) : cellname;
        if (column == null)
        {
            // Tables for composite 2ndary indexes used to be compound but dense, but we've transformed them into regular tables
            // (non compact ones) but with no regular column (i.e. we only care about the clustering). So we'll get here
            // in that case, and what we want to return is basically a row marker.
            if (metadata.partitionColumns().isEmpty())
                return new LegacyCellName(clustering, null, null);

            // Otherwise, we shouldn't get there
            throw new IllegalArgumentException("No column name component found in cell name");
        }

        // Row marker, this is ok
        if (!column.hasRemaining())
            return new LegacyCellName(clustering, null, null);

        ColumnDefinition def = metadata.getColumnDefinition(column);
        if ((def == null) || def.isPrimaryKeyColumn())
        {
            // If it's a compact table, it means the column is in fact a "dynamic" one
            if (metadata.isCompactTable())
                return new LegacyCellName(Clustering.make(column), metadata.compactValueColumn(), null);

            if (def == null)
                throw new UnknownColumnException(metadata, column);
            else
                throw new IllegalArgumentException("Cannot add primary key column to partition update");
        }

        ByteBuffer collectionElement = metadata.isCompound() ? CompositeType.extractComponent(cellname, metadata.comparator.size() + 1) : null;

        // Note that because static compact columns are translated to static defs in the new world order, we need to force a static
        // clustering if the definition is static (as it might not be in this case).
        return new LegacyCellName(def.isStatic() ? Clustering.STATIC_CLUSTERING : clustering, def, collectionElement);
    }

    public static LegacyBound decodeBound(CFMetaData metadata, ByteBuffer bound, boolean isStart)
    {
        if (!bound.hasRemaining())
            return isStart ? LegacyBound.BOTTOM : LegacyBound.TOP;

        List<CompositeType.CompositeComponent> components = metadata.isCompound()
                                                          ? CompositeType.deconstruct(bound)
                                                          : Collections.singletonList(new CompositeType.CompositeComponent(bound, (byte) 0));

        // Either it's a prefix of the clustering, or it's the bound of a collection range tombstone (and thus has
        // the collection column name)
        assert components.size() <= metadata.comparator.size() || (!metadata.isCompactTable() && components.size() == metadata.comparator.size() + 1);

        List<CompositeType.CompositeComponent> prefix = components.size() <= metadata.comparator.size()
                                                      ? components
                                                      : components.subList(0, metadata.comparator.size());
        Slice.Bound.Kind boundKind;
        if (isStart)
        {
            if (components.get(components.size() - 1).eoc > 0)
                boundKind = Slice.Bound.Kind.EXCL_START_BOUND;
            else
                boundKind = Slice.Bound.Kind.INCL_START_BOUND;
        }
        else
        {
            if (components.get(components.size() - 1).eoc < 0)
                boundKind = Slice.Bound.Kind.EXCL_END_BOUND;
            else
                boundKind = Slice.Bound.Kind.INCL_END_BOUND;
        }

        ByteBuffer[] prefixValues = new ByteBuffer[prefix.size()];
        for (int i = 0; i < prefix.size(); i++)
            prefixValues[i] = prefix.get(i).value;
        Slice.Bound sb = Slice.Bound.create(boundKind, prefixValues);

        ColumnDefinition collectionName = components.size() == metadata.comparator.size() + 1
                                        ? metadata.getColumnDefinition(components.get(metadata.comparator.size()).value)
                                        : null;
        return new LegacyBound(sb, metadata.isCompound() && CompositeType.isStaticName(bound), collectionName);
    }

    public static ByteBuffer encodeBound(CFMetaData metadata, Slice.Bound bound, boolean isStart)
    {
        if (bound == Slice.Bound.BOTTOM || bound == Slice.Bound.TOP || metadata.comparator.size() == 0)
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        ClusteringPrefix clustering = bound.clustering();

        if (!metadata.isCompound())
        {
            assert clustering.size() == 1;
            return clustering.get(0);
        }

        CompositeType ctype = CompositeType.getInstance(metadata.comparator.subtypes());
        CompositeType.Builder builder = ctype.builder();
        for (int i = 0; i < clustering.size(); i++)
            builder.add(clustering.get(i));

        if (isStart)
            return bound.isInclusive() ? builder.build() : builder.buildAsEndOfRange();
        else
            return bound.isInclusive() ? builder.buildAsEndOfRange() : builder.build();
    }

    public static ByteBuffer encodeCellName(CFMetaData metadata, ClusteringPrefix clustering, ByteBuffer columnName, ByteBuffer collectionElement)
    {
        boolean isStatic = clustering == Clustering.STATIC_CLUSTERING;

        if (!metadata.isCompound())
        {
            if (isStatic)
                return columnName;

            assert clustering.size() == 1 : "Expected clustering size to be 1, but was " + clustering.size();
            return clustering.get(0);
        }

        // We use comparator.size() rather than clustering.size() because of static clusterings
        int clusteringSize = metadata.comparator.size();
        int size = clusteringSize + (metadata.isDense() ? 0 : 1) + (collectionElement == null ? 0 : 1);
        ByteBuffer[] values = new ByteBuffer[size];
        for (int i = 0; i < clusteringSize; i++)
        {
            if (isStatic)
            {
                values[i] = ByteBufferUtil.EMPTY_BYTE_BUFFER;
                continue;
            }

            ByteBuffer v = clustering.get(i);
            // we can have null (only for dense compound tables for backward compatibility reasons) but that
            // means we're done and should stop there as far as building the composite is concerned.
            if (v == null)
                return CompositeType.build(Arrays.copyOfRange(values, 0, i));

            values[i] = v;
        }

        if (!metadata.isDense())
            values[clusteringSize] = columnName;
        if (collectionElement != null)
            values[clusteringSize + 1] = collectionElement;

        return CompositeType.build(isStatic, values);
    }

    public static Clustering decodeClustering(CFMetaData metadata, ByteBuffer value)
    {
        int csize = metadata.comparator.size();
        if (csize == 0)
            return Clustering.EMPTY;

        if (metadata.isCompound() && CompositeType.isStaticName(value))
            return Clustering.STATIC_CLUSTERING;

        List<ByteBuffer> components = metadata.isCompound()
                                    ? CompositeType.splitName(value)
                                    : Collections.singletonList(value);

        return Clustering.make(components.subList(0, Math.min(csize, components.size())).toArray(new ByteBuffer[csize]));
    }

    public static ByteBuffer encodeClustering(CFMetaData metadata, ClusteringPrefix clustering)
    {
        if (clustering.size() == 0)
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        if (!metadata.isCompound())
        {
            assert clustering.size() == 1;
            return clustering.get(0);
        }

        ByteBuffer[] values = new ByteBuffer[clustering.size()];
        for (int i = 0; i < clustering.size(); i++)
            values[i] = clustering.get(i);
        return CompositeType.build(values);
    }

    /**
     * The maximum number of cells to include per partition when converting to the old format.
     * <p>
     * We already apply the limit during the actual query, but for queries that counts cells and not rows (thrift queries
     * and distinct queries as far as old nodes are concerned), we may still include a little bit more than requested
     * because {@link DataLimits} always include full rows. So if the limit ends in the middle of a queried row, the
     * full row will be part of our result. This would confuse old nodes however so we make sure to truncate it to
     * what's expected before writting it on the wire.
     *
     * @param command the read commmand for which to determine the maximum cells per partition. This can be {@code null}
     * in which case {@code Integer.MAX_VALUE} is returned.
     * @return the maximum number of cells per partition that should be enforced according to the read command if
     * post-query limitation are in order (see above). This will be {@code Integer.MAX_VALUE} if no such limits are
     * necessary.
     */
    private static int maxCellsPerPartition(ReadCommand command)
    {
        if (command == null)
            return Integer.MAX_VALUE;

        DataLimits limits = command.limits();

        // There is 2 types of DISTINCT queries: those that includes only the partition key, and those that include static columns.
        // On old nodes, the latter expects the first row in term of CQL count, which is what we already have and there is no additional
        // limit to apply. The former however expect only one cell per partition and rely on it (See CASSANDRA-10762).
        if (limits.isDistinct())
            return command.columnFilter().fetchedColumns().statics.isEmpty() ? 1 : Integer.MAX_VALUE;

        switch (limits.kind())
        {
            case THRIFT_LIMIT:
            case SUPER_COLUMN_COUNTING_LIMIT:
                return limits.perPartitionCount();
            default:
                return Integer.MAX_VALUE;
        }
    }

    // For serializing to old wire format
    public static LegacyUnfilteredPartition fromUnfilteredRowIterator(ReadCommand command, UnfilteredRowIterator iterator)
    {
        // we need to extract the range tombstone so materialize the partition. Since this is
        // used for the on-wire format, this is not worst than it used to be.
        final ImmutableBTreePartition partition = ImmutableBTreePartition.create(iterator);
        DeletionInfo info = partition.deletionInfo();
        Pair<LegacyRangeTombstoneList, Iterator<LegacyCell>> pair = fromRowIterator(partition.metadata(), partition.iterator(), partition.staticRow());

        LegacyLayout.LegacyRangeTombstoneList rtl = pair.left;

        // Processing the cell iterator results in the LegacyRangeTombstoneList being populated, so we do this
        // before we use the LegacyRangeTombstoneList at all
        List<LegacyLayout.LegacyCell> cells = Lists.newArrayList(pair.right);

        int maxCellsPerPartition = maxCellsPerPartition(command);
        if (cells.size() > maxCellsPerPartition)
            cells = cells.subList(0, maxCellsPerPartition);

        // The LegacyRangeTombstoneList already has range tombstones for the single-row deletions and complex
        // deletions.  Go through our normal range tombstones and add then to the LegacyRTL so that the range
        // tombstones all get merged and sorted properly.
        if (info.hasRanges())
        {
            Iterator<RangeTombstone> rangeTombstoneIterator = info.rangeIterator(false);
            while (rangeTombstoneIterator.hasNext())
            {
                RangeTombstone rt = rangeTombstoneIterator.next();
                Slice slice = rt.deletedSlice();
                LegacyLayout.LegacyBound start = new LegacyLayout.LegacyBound(slice.start(), false, null);
                LegacyLayout.LegacyBound end = new LegacyLayout.LegacyBound(slice.end(), false, null);
                rtl.add(start, end, rt.deletionTime().markedForDeleteAt(), rt.deletionTime().localDeletionTime());
            }
        }

        return new LegacyUnfilteredPartition(info.getPartitionDeletion(), rtl, cells);
    }

    public static void serializeAsLegacyPartition(ReadCommand command, UnfilteredRowIterator partition, DataOutputPlus out, int version) throws IOException
    {
        assert version < MessagingService.VERSION_30;

        out.writeBoolean(true);

        LegacyLayout.LegacyUnfilteredPartition legacyPartition = LegacyLayout.fromUnfilteredRowIterator(command, partition);

        UUIDSerializer.serializer.serialize(partition.metadata().cfId, out, version);
        DeletionTime.serializer.serialize(legacyPartition.partitionDeletion, out);

        legacyPartition.rangeTombstones.serialize(out, partition.metadata());

        // begin cell serialization
        out.writeInt(legacyPartition.cells.size());
        for (LegacyLayout.LegacyCell cell : legacyPartition.cells)
        {
            ByteBufferUtil.writeWithShortLength(cell.name.encode(partition.metadata()), out);
            out.writeByte(cell.serializationFlags());
            if (cell.isExpiring())
            {
                out.writeInt(cell.ttl);
                out.writeInt(cell.localDeletionTime);
            }
            else if (cell.isTombstone())
            {
                out.writeLong(cell.timestamp);
                out.writeInt(TypeSizes.sizeof(cell.localDeletionTime));
                out.writeInt(cell.localDeletionTime);
                continue;
            }
            else if (cell.isCounterUpdate())
            {
                out.writeLong(cell.timestamp);
                long count = CounterContext.instance().getLocalCount(cell.value);
                ByteBufferUtil.writeWithLength(ByteBufferUtil.bytes(count), out);
                continue;
            }
            else if (cell.isCounter())
            {
                out.writeLong(Long.MIN_VALUE);  // timestampOfLastDelete (not used, and MIN_VALUE is the default)
            }

            out.writeLong(cell.timestamp);
            ByteBufferUtil.writeWithLength(cell.value, out);
        }
    }

    // For the old wire format
    // Note: this can return null if an empty partition is serialized!
    public static UnfilteredRowIterator deserializeLegacyPartition(DataInputPlus in, int version, SerializationHelper.Flag flag, ByteBuffer key) throws IOException
    {
        assert version < MessagingService.VERSION_30;

        // This is only used in mutation, and mutation have never allowed "null" column families
        boolean present = in.readBoolean();
        if (!present)
            return null;

        CFMetaData metadata = CFMetaData.serializer.deserialize(in, version);
        LegacyDeletionInfo info = LegacyDeletionInfo.deserialize(metadata, in);
        int size = in.readInt();
        Iterator<LegacyCell> cells = deserializeCells(metadata, in, flag, size);
        SerializationHelper helper = new SerializationHelper(metadata, version, flag);
        return onWireCellstoUnfilteredRowIterator(metadata, metadata.partitioner.decorateKey(key), info, cells, false, helper);
    }

    // For the old wire format
    public static long serializedSizeAsLegacyPartition(ReadCommand command, UnfilteredRowIterator partition, int version)
    {
        assert version < MessagingService.VERSION_30;

        if (partition.isEmpty())
            return TypeSizes.sizeof(false);

        long size = TypeSizes.sizeof(true);

        LegacyLayout.LegacyUnfilteredPartition legacyPartition = LegacyLayout.fromUnfilteredRowIterator(command, partition);

        size += UUIDSerializer.serializer.serializedSize(partition.metadata().cfId, version);
        size += DeletionTime.serializer.serializedSize(legacyPartition.partitionDeletion);
        size += legacyPartition.rangeTombstones.serializedSize(partition.metadata());

        // begin cell serialization
        size += TypeSizes.sizeof(legacyPartition.cells.size());
        for (LegacyLayout.LegacyCell cell : legacyPartition.cells)
        {
            size += ByteBufferUtil.serializedSizeWithShortLength(cell.name.encode(partition.metadata()));
            size += 1;  // serialization flags
            if (cell.kind == LegacyLayout.LegacyCell.Kind.EXPIRING)
            {
                size += TypeSizes.sizeof(cell.ttl);
                size += TypeSizes.sizeof(cell.localDeletionTime);
            }
            else if (cell.kind == LegacyLayout.LegacyCell.Kind.DELETED)
            {
                size += TypeSizes.sizeof(cell.timestamp);
                // localDeletionTime replaces cell.value as the body
                size += TypeSizes.sizeof(TypeSizes.sizeof(cell.localDeletionTime));
                size += TypeSizes.sizeof(cell.localDeletionTime);
                continue;
            }
            else if (cell.kind == LegacyLayout.LegacyCell.Kind.COUNTER)
            {
                size += TypeSizes.sizeof(Long.MIN_VALUE);  // timestampOfLastDelete
            }

            size += TypeSizes.sizeof(cell.timestamp);
            size += ByteBufferUtil.serializedSizeWithLength(cell.value);
        }

        return size;
    }

    // For thrift sake
    public static UnfilteredRowIterator toUnfilteredRowIterator(CFMetaData metadata,
                                                                DecoratedKey key,
                                                                LegacyDeletionInfo delInfo,
                                                                Iterator<LegacyCell> cells)
    {
        SerializationHelper helper = new SerializationHelper(metadata, 0, SerializationHelper.Flag.LOCAL);
        return toUnfilteredRowIterator(metadata, key, delInfo, cells, false, helper);
    }

    // For deserializing old wire format
    public static UnfilteredRowIterator onWireCellstoUnfilteredRowIterator(CFMetaData metadata,
                                                                           DecoratedKey key,
                                                                           LegacyDeletionInfo delInfo,
                                                                           Iterator<LegacyCell> cells,
                                                                           boolean reversed,
                                                                           SerializationHelper helper)
    {

        // If the table is a static compact, the "column_metadata" are now internally encoded as
        // static. This has already been recognized by decodeCellName, but it means the cells
        // provided are not in the expected order (the "static" cells are not necessarily at the front).
        // So sort them to make sure toUnfilteredRowIterator works as expected.
        // Further, if the query is reversed, then the on-wire format still has cells in non-reversed
        // order, but we need to have them reverse in the final UnfilteredRowIterator. So reverse them.
        if (metadata.isStaticCompactTable() || reversed)
        {
            List<LegacyCell> l = new ArrayList<>();
            Iterators.addAll(l, cells);
            Collections.sort(l, legacyCellComparator(metadata, reversed));
            cells = l.iterator();
        }

        return toUnfilteredRowIterator(metadata, key, delInfo, cells, reversed, helper);
    }

    private static UnfilteredRowIterator toUnfilteredRowIterator(CFMetaData metadata,
                                                                 DecoratedKey key,
                                                                 LegacyDeletionInfo delInfo,
                                                                 Iterator<LegacyCell> cells,
                                                                 boolean reversed,
                                                                 SerializationHelper helper)
    {
        // A reducer that basically does nothing, we know the 2 merged iterators can't have conflicting atoms (since we merge cells with range tombstones).
        MergeIterator.Reducer<LegacyAtom, LegacyAtom> reducer = new MergeIterator.Reducer<LegacyAtom, LegacyAtom>()
        {
            private LegacyAtom atom;

            public void reduce(int idx, LegacyAtom current)
            {
                // We're merging cell with range tombstones, so we should always only have a single atom to reduce.
                assert atom == null;
                atom = current;
            }

            protected LegacyAtom getReduced()
            {
                return atom;
            }

            protected void onKeyChange()
            {
                atom = null;
            }
        };
        List<Iterator<LegacyAtom>> iterators = Arrays.asList(asLegacyAtomIterator(cells), asLegacyAtomIterator(delInfo.inRowRangeTombstones()));
        PeekingIterator<LegacyAtom> atoms = Iterators.peekingIterator(MergeIterator.get(iterators, legacyAtomComparator(metadata), reducer));

        // Check if we have some static
        Row staticRow = atoms.hasNext() && atoms.peek().isStatic()
                      ? getNextRow(CellGrouper.staticGrouper(metadata, helper), atoms)
                      : Rows.EMPTY_STATIC_ROW;

        Iterator<Row> rows = convertToRows(new CellGrouper(metadata, helper), atoms);
        Iterator<RangeTombstone> ranges = delInfo.deletionInfo.rangeIterator(reversed);
        return new RowAndDeletionMergeIterator(metadata,
                                               key,
                                               delInfo.deletionInfo.getPartitionDeletion(),
                                               ColumnFilter.all(metadata),
                                               staticRow,
                                               reversed,
                                               EncodingStats.NO_STATS,
                                               rows,
                                               ranges,
                                               true);
    }

    public static Row extractStaticColumns(CFMetaData metadata, DataInputPlus in, Columns statics) throws IOException
    {
        assert !statics.isEmpty();
        assert metadata.isCompactTable();

        if (metadata.isSuper())
            // TODO: there is in practice nothing to do here, but we need to handle the column_metadata for super columns somewhere else
            throw new UnsupportedOperationException();

        Set<ByteBuffer> columnsToFetch = new HashSet<>(statics.size());
        for (ColumnDefinition column : statics)
            columnsToFetch.add(column.name.bytes);

        Row.Builder builder = BTreeRow.unsortedBuilder(FBUtilities.nowInSeconds());
        builder.newRow(Clustering.STATIC_CLUSTERING);

        boolean foundOne = false;
        LegacyAtom atom;
        while ((atom = readLegacyAtom(metadata, in, false)) != null)
        {
            if (atom.isCell())
            {
                LegacyCell cell = atom.asCell();
                if (!columnsToFetch.contains(cell.name.encode(metadata)))
                    continue;

                foundOne = true;
                builder.addCell(new BufferCell(cell.name.column, cell.timestamp, cell.ttl, cell.localDeletionTime, cell.value, null));
            }
            else
            {
                LegacyRangeTombstone tombstone = atom.asRangeTombstone();
                // TODO: we need to track tombstones and potentially ignore cells that are
                // shadowed (or even better, replace them by tombstones).
                throw new UnsupportedOperationException();
            }
        }

        return foundOne ? builder.build() : Rows.EMPTY_STATIC_ROW;
    }

    private static Row getNextRow(CellGrouper grouper, PeekingIterator<? extends LegacyAtom> cells)
    {
        if (!cells.hasNext())
            return null;

        grouper.reset();
        while (cells.hasNext() && grouper.addAtom(cells.peek()))
        {
            // We've added the cell already in the grouper, so just skip it
            cells.next();
        }
        return grouper.getRow();
    }

    @SuppressWarnings("unchecked")
    private static Iterator<LegacyAtom> asLegacyAtomIterator(Iterator<? extends LegacyAtom> iter)
    {
        return (Iterator<LegacyAtom>)iter;
    }

    private static Iterator<Row> convertToRows(final CellGrouper grouper, final PeekingIterator<LegacyAtom> atoms)
    {
        return new AbstractIterator<Row>()
        {
            protected Row computeNext()
            {
                if (!atoms.hasNext())
                    return endOfData();

                return getNextRow(grouper, atoms);
            }
        };
    }

    public static Pair<LegacyRangeTombstoneList, Iterator<LegacyCell>> fromRowIterator(final RowIterator iterator)
    {
        return fromRowIterator(iterator.metadata(), iterator, iterator.staticRow());
    }

    private static Pair<LegacyRangeTombstoneList, Iterator<LegacyCell>> fromRowIterator(final CFMetaData metadata, final Iterator<Row> iterator, final Row staticRow)
    {
        LegacyRangeTombstoneList deletions = new LegacyRangeTombstoneList(new LegacyBoundComparator(metadata.comparator), 10);
        Iterator<LegacyCell> cells = new AbstractIterator<LegacyCell>()
        {
            private Iterator<LegacyCell> currentRow = initializeRow();

            private Iterator<LegacyCell> initializeRow()
            {
                if (staticRow == null || staticRow.isEmpty())
                    return Collections.<LegacyLayout.LegacyCell>emptyIterator();

                Pair<LegacyRangeTombstoneList, Iterator<LegacyCell>> row = fromRow(metadata, staticRow);
                deletions.addAll(row.left);
                return row.right;
            }

            protected LegacyCell computeNext()
            {
                while (true)
                {
                    if (currentRow.hasNext())
                        return currentRow.next();

                    if (!iterator.hasNext())
                        return endOfData();

                    Pair<LegacyRangeTombstoneList, Iterator<LegacyCell>> row = fromRow(metadata, iterator.next());
                    deletions.addAll(row.left);
                    currentRow = row.right;
                }
            }
        };

        return Pair.create(deletions, cells);
    }

    private static Pair<LegacyRangeTombstoneList, Iterator<LegacyCell>> fromRow(final CFMetaData metadata, final Row row)
    {
        // convert any complex deletions or row deletion into normal range tombstones so that we can build and send a proper RangeTombstoneList
        // to legacy nodes
        LegacyRangeTombstoneList deletions = new LegacyRangeTombstoneList(new LegacyBoundComparator(metadata.comparator), 10);

        if (!row.deletion().isLive())
        {
            Clustering clustering = row.clustering();
            Slice.Bound startBound = Slice.Bound.inclusiveStartOf(clustering);
            Slice.Bound endBound = Slice.Bound.inclusiveEndOf(clustering);

            LegacyBound start = new LegacyLayout.LegacyBound(startBound, false, null);
            LegacyBound end = new LegacyLayout.LegacyBound(endBound, false, null);

            deletions.add(start, end, row.deletion().time().markedForDeleteAt(), row.deletion().time().localDeletionTime());
        }

        for (ColumnData cd : row)
        {
            ColumnDefinition col = cd.column();
            if (col.isSimple())
                continue;

            DeletionTime delTime = ((ComplexColumnData)cd).complexDeletion();
            if (!delTime.isLive())
            {
                Clustering clustering = row.clustering();

                Slice.Bound startBound = Slice.Bound.inclusiveStartOf(clustering);
                Slice.Bound endBound = Slice.Bound.inclusiveEndOf(clustering);

                LegacyLayout.LegacyBound start = new LegacyLayout.LegacyBound(startBound, col.isStatic(), col);
                LegacyLayout.LegacyBound end = new LegacyLayout.LegacyBound(endBound, col.isStatic(), col);

                deletions.add(start, end, delTime.markedForDeleteAt(), delTime.localDeletionTime());
            }
        }

        Iterator<LegacyCell> cells = new AbstractIterator<LegacyCell>()
        {
            private final Iterator<Cell> cells = row.cellsInLegacyOrder(metadata, false).iterator();
            // we don't have (and shouldn't have) row markers for compact tables.
            private boolean hasReturnedRowMarker = metadata.isCompactTable();

            protected LegacyCell computeNext()
            {
                if (!hasReturnedRowMarker)
                {
                    hasReturnedRowMarker = true;

                    // don't include a row marker if there's no timestamp on the primary key; this is the 3.0+ equivalent
                    // of a row marker
                    if (!row.primaryKeyLivenessInfo().isEmpty())
                    {
                        LegacyCellName cellName = new LegacyCellName(row.clustering(), null, null);
                        LivenessInfo info = row.primaryKeyLivenessInfo();
                        return new LegacyCell(info.isExpiring() ? LegacyCell.Kind.EXPIRING : LegacyCell.Kind.REGULAR, cellName, ByteBufferUtil.EMPTY_BYTE_BUFFER, info.timestamp(), info.localExpirationTime(), info.ttl());
                    }
                }

                if (!cells.hasNext())
                    return endOfData();

                return makeLegacyCell(row.clustering(), cells.next());
            }
        };
        return Pair.create(deletions, cells);
    }

    private static LegacyCell makeLegacyCell(Clustering clustering, Cell cell)
    {
        LegacyCell.Kind kind;
        if (cell.isCounterCell())
            kind = LegacyCell.Kind.COUNTER;
        else if (cell.isTombstone())
            kind = LegacyCell.Kind.DELETED;
        else if (cell.isExpiring())
            kind = LegacyCell.Kind.EXPIRING;
        else
            kind = LegacyCell.Kind.REGULAR;

        CellPath path = cell.path();
        assert path == null || path.size() == 1;
        LegacyCellName name = new LegacyCellName(clustering, cell.column(), path == null ? null : path.get(0));
        return new LegacyCell(kind, name, cell.value(), cell.timestamp(), cell.localDeletionTime(), cell.ttl());
    }

    public static RowIterator toRowIterator(final CFMetaData metadata,
                                            final DecoratedKey key,
                                            final Iterator<LegacyCell> cells,
                                            final int nowInSec)
    {
        SerializationHelper helper = new SerializationHelper(metadata, 0, SerializationHelper.Flag.LOCAL);
        return UnfilteredRowIterators.filter(toUnfilteredRowIterator(metadata, key, LegacyDeletionInfo.live(), cells, false, helper), nowInSec);
    }

    public static Comparator<LegacyCell> legacyCellComparator(CFMetaData metadata)
    {
        return legacyCellComparator(metadata, false);
    }

    public static Comparator<LegacyCell> legacyCellComparator(final CFMetaData metadata, final boolean reversed)
    {
        final Comparator<LegacyCellName> cellNameComparator = legacyCellNameComparator(metadata, reversed);
        return new Comparator<LegacyCell>()
        {
            public int compare(LegacyCell cell1, LegacyCell cell2)
            {
                LegacyCellName c1 = cell1.name;
                LegacyCellName c2 = cell2.name;

                int c = cellNameComparator.compare(c1, c2);
                if (c != 0)
                    return c;

                // The actual sorting when the cellname is equal doesn't matter, we just want to make
                // sure the cells are not considered equal.
                if (cell1.timestamp != cell2.timestamp)
                    return cell1.timestamp < cell2.timestamp ? -1 : 1;

                if (cell1.localDeletionTime != cell2.localDeletionTime)
                    return cell1.localDeletionTime < cell2.localDeletionTime ? -1 : 1;

                return cell1.value.compareTo(cell2.value);
            }
        };
    }

    // Note that this doesn't exactly compare cells as they were pre-3.0 because within a row they sort columns like
    // in 3.0, that is, with simple columns before complex columns. In other words, this comparator makes sure cells
    // are in the proper order to convert them to actual 3.0 rows.
    public static Comparator<LegacyCellName> legacyCellNameComparator(final CFMetaData metadata, final boolean reversed)
    {
        return new Comparator<LegacyCellName>()
        {
            public int compare(LegacyCellName c1, LegacyCellName c2)
            {
                // Compare clustering first
                if (c1.clustering == Clustering.STATIC_CLUSTERING)
                {
                    if (c2.clustering != Clustering.STATIC_CLUSTERING)
                        return -1;
                }
                else if (c2.clustering == Clustering.STATIC_CLUSTERING)
                {
                    return 1;
                }
                else
                {
                    int c = metadata.comparator.compare(c1.clustering, c2.clustering);
                    if (c != 0)
                        return reversed ? -c : c;
                }

                // Note that when reversed, we only care about the clustering being reversed, so it's ok
                // not to take reversed into account below.

                // Then check the column name
                if (c1.column != c2.column)
                {
                    // A null for the column means it's a row marker
                    if (c1.column == null)
                        return -1;
                    if (c2.column == null)
                        return 1;

                    assert c1.column.isRegular() || c1.column.isStatic();
                    assert c2.column.isRegular() || c2.column.isStatic();
                    int cmp = c1.column.compareTo(c2.column);
                    if (cmp != 0)
                        return cmp;
                }

                assert (c1.collectionElement == null) == (c2.collectionElement == null);

                if (c1.collectionElement != null)
                {
                    AbstractType<?> colCmp = ((CollectionType)c1.column.type).nameComparator();
                    return colCmp.compare(c1.collectionElement, c2.collectionElement);
                }
                return 0;
            }
        };
    }

    private static boolean equalValues(ClusteringPrefix c1, ClusteringPrefix c2, ClusteringComparator comparator)
    {
        assert c1.size() == c2.size();
        for (int i = 0; i < c1.size(); i++)
        {
            if (comparator.compareComponent(i, c1.get(i), c2.get(i)) != 0)
                return false;
        }
        return true;
    }

    private static Comparator<LegacyAtom> legacyAtomComparator(CFMetaData metadata)
    {
        return (o1, o2) ->
        {
            // First we want to compare by clustering, but we have to be careful with range tombstone, because
            // we can have collection deletion and we want those to sort properly just before the column they
            // delete, not before the whole row.
            // We also want to special case static so they sort before any non-static. Note in particular that
            // this special casing is important in the case of one of the Atom being Slice.Bound.BOTTOM: we want
            // it to sort after the static as we deal with static first in toUnfilteredAtomIterator and having
            // Slice.Bound.BOTTOM first would mess that up (note that static deletion is handled through a specific
            // static tombstone, see LegacyDeletionInfo.add()).
            if (o1.isStatic() != o2.isStatic())
                return o1.isStatic() ? -1 : 1;

            ClusteringPrefix c1 = o1.clustering();
            ClusteringPrefix c2 = o2.clustering();

            int clusteringComparison;
            if (c1.size() != c2.size() || (o1.isCell() == o2.isCell()) || !equalValues(c1, c2, metadata.comparator))
            {
                clusteringComparison = metadata.comparator.compare(c1, c2);
            }
            else
            {
                // one is a cell and one is a range tombstone, and both have the same prefix size (that is, the
                // range tombstone is either a row deletion or a collection deletion).
                LegacyRangeTombstone rt = o1.isCell() ? o2.asRangeTombstone() : o1.asRangeTombstone();
                clusteringComparison = rt.isCollectionTombstone()
                                       ? 0
                                       : metadata.comparator.compare(c1, c2);
            }

            // Note that if both are range tombstones and have the same clustering, then they are equal.
            if (clusteringComparison != 0)
                return clusteringComparison;

            if (o1.isCell())
            {
                LegacyCell cell1 = o1.asCell();
                if (o2.isCell())
                {
                    LegacyCell cell2 = o2.asCell();
                    // Check for row marker cells
                    if (cell1.name.column == null)
                        return cell2.name.column == null ? 0 : -1;
                    return cell2.name.column == null ? 1 : cell1.name.column.compareTo(cell2.name.column);
                }

                LegacyRangeTombstone rt2 = o2.asRangeTombstone();
                assert rt2.isCollectionTombstone(); // otherwise, we shouldn't have got a clustering equality
                if (cell1.name.column == null)
                    return -1;
                int cmp = cell1.name.column.compareTo(rt2.start.collectionName);
                // If both are for the same column, then the RT should come first
                return cmp == 0 ? 1 : cmp;
            }
            else
            {
                assert o2.isCell();
                LegacyCell cell2 = o2.asCell();

                LegacyRangeTombstone rt1 = o1.asRangeTombstone();
                assert rt1.isCollectionTombstone(); // otherwise, we shouldn't have got a clustering equality

                if (cell2.name.column == null)
                    return 1;

                int cmp = rt1.start.collectionName.compareTo(cell2.name.column);
                // If both are for the same column, then the RT should come first
                return cmp == 0 ? -1 : cmp;
            }
        };
    }

    public static LegacyAtom readLegacyAtom(CFMetaData metadata, DataInputPlus in, boolean readAllAsDynamic) throws IOException
    {
        while (true)
        {
            ByteBuffer cellname = ByteBufferUtil.readWithShortLength(in);
            if (!cellname.hasRemaining())
                return null; // END_OF_ROW

            try
            {
                int b = in.readUnsignedByte();
                return (b & RANGE_TOMBSTONE_MASK) != 0
                    ? readLegacyRangeTombstoneBody(metadata, in, cellname)
                    : readLegacyCellBody(metadata, in, cellname, b, SerializationHelper.Flag.LOCAL, readAllAsDynamic);
            }
            catch (UnknownColumnException e)
            {
                // We can get there if we read a cell for a dropped column, and ff that is the case,
                // then simply ignore the cell is fine. But also not that we ignore if it's the
                // system keyspace because for those table we actually remove columns without registering
                // them in the dropped columns
                assert metadata.ksName.equals(SystemKeyspace.NAME) || metadata.getDroppedColumnDefinition(e.columnName) != null : e.getMessage();
            }
        }
    }

    public static LegacyCell readLegacyCell(CFMetaData metadata, DataInput in, SerializationHelper.Flag flag) throws IOException, UnknownColumnException
    {
        ByteBuffer cellname = ByteBufferUtil.readWithShortLength(in);
        int b = in.readUnsignedByte();
        return readLegacyCellBody(metadata, in, cellname, b, flag, false);
    }

    public static LegacyCell readLegacyCellBody(CFMetaData metadata, DataInput in, ByteBuffer cellname, int mask, SerializationHelper.Flag flag, boolean readAllAsDynamic)
    throws IOException, UnknownColumnException
    {
        // Note that we want to call decodeCellName only after we've deserialized other parts, since it can throw
        // and we want to throw only after having deserialized the full cell.
        if ((mask & COUNTER_MASK) != 0)
        {
            in.readLong(); // timestampOfLastDelete: this has been unused for a long time so we ignore it
            long ts = in.readLong();
            ByteBuffer value = ByteBufferUtil.readWithLength(in);
            if (flag == SerializationHelper.Flag.FROM_REMOTE || (flag == SerializationHelper.Flag.LOCAL && CounterContext.instance().shouldClearLocal(value)))
                value = CounterContext.instance().clearAllLocal(value);
            return new LegacyCell(LegacyCell.Kind.COUNTER, decodeCellName(metadata, cellname, readAllAsDynamic), value, ts, Cell.NO_DELETION_TIME, Cell.NO_TTL);
        }
        else if ((mask & EXPIRATION_MASK) != 0)
        {
            int ttl = in.readInt();
            int expiration = in.readInt();
            long ts = in.readLong();
            ByteBuffer value = ByteBufferUtil.readWithLength(in);
            return new LegacyCell(LegacyCell.Kind.EXPIRING, decodeCellName(metadata, cellname, readAllAsDynamic), value, ts, expiration, ttl);
        }
        else
        {
            long ts = in.readLong();
            ByteBuffer value = ByteBufferUtil.readWithLength(in);
            LegacyCellName name = decodeCellName(metadata, cellname, readAllAsDynamic);
            return (mask & COUNTER_UPDATE_MASK) != 0
                ? new LegacyCell(LegacyCell.Kind.COUNTER, name, CounterContext.instance().createLocal(ByteBufferUtil.toLong(value)), ts, Cell.NO_DELETION_TIME, Cell.NO_TTL)
                : ((mask & DELETION_MASK) == 0
                        ? new LegacyCell(LegacyCell.Kind.REGULAR, name, value, ts, Cell.NO_DELETION_TIME, Cell.NO_TTL)
                        : new LegacyCell(LegacyCell.Kind.DELETED, name, ByteBufferUtil.EMPTY_BYTE_BUFFER, ts, ByteBufferUtil.toInt(value), Cell.NO_TTL));
        }
    }

    public static LegacyRangeTombstone readLegacyRangeTombstoneBody(CFMetaData metadata, DataInputPlus in, ByteBuffer boundname) throws IOException
    {
        LegacyBound min = decodeBound(metadata, boundname, true);
        LegacyBound max = decodeBound(metadata, ByteBufferUtil.readWithShortLength(in), false);
        DeletionTime dt = DeletionTime.serializer.deserialize(in);
        return new LegacyRangeTombstone(min, max, dt);
    }

    public static Iterator<LegacyCell> deserializeCells(final CFMetaData metadata,
                                                        final DataInput in,
                                                        final SerializationHelper.Flag flag,
                                                        final int size)
    {
        return new AbstractIterator<LegacyCell>()
        {
            private int i = 0;

            protected LegacyCell computeNext()
            {
                if (i >= size)
                    return endOfData();

                ++i;
                try
                {
                    return readLegacyCell(metadata, in, flag);
                }
                catch (UnknownColumnException e)
                {
                    // We can get there if we read a cell for a dropped column, and if that is the case,
                    // then simply ignore the cell is fine. But also not that we ignore if it's the
                    // system keyspace because for those table we actually remove columns without registering
                    // them in the dropped columns
                    if (metadata.ksName.equals(SystemKeyspace.NAME) || metadata.getDroppedColumnDefinition(e.columnName) != null)
                        return computeNext();
                    else
                        throw new IOError(e);
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }
        };
    }

    public static class CellGrouper
    {
        public final CFMetaData metadata;
        private final boolean isStatic;
        private final SerializationHelper helper;
        private final Row.Builder builder;
        private Clustering clustering;

        private LegacyRangeTombstone rowDeletion;
        private LegacyRangeTombstone collectionDeletion;

        public CellGrouper(CFMetaData metadata, SerializationHelper helper)
        {
            this(metadata, helper, false);
        }

        private CellGrouper(CFMetaData metadata, SerializationHelper helper, boolean isStatic)
        {
            this.metadata = metadata;
            this.isStatic = isStatic;
            this.helper = helper;
            // We cannot use a sorted builder because we don't have exactly the same ordering in 3.0 and pre-3.0. More precisely, within a row, we
            // store all simple columns before the complex ones in 3.0, which we use to sort everything sorted by the column name before. Note however
            // that the unsorted builder won't have to reconcile cells, so the exact value we pass for nowInSec doesn't matter.
            this.builder = BTreeRow.unsortedBuilder(FBUtilities.nowInSeconds());
        }

        public static CellGrouper staticGrouper(CFMetaData metadata, SerializationHelper helper)
        {
            return new CellGrouper(metadata, helper, true);
        }

        public void reset()
        {
            this.clustering = null;
            this.rowDeletion = null;
            this.collectionDeletion = null;
        }

        public boolean addAtom(LegacyAtom atom)
        {
            return atom.isCell()
                 ? addCell(atom.asCell())
                 : addRangeTombstone(atom.asRangeTombstone());
        }

        public boolean addCell(LegacyCell cell)
        {
            if (clustering == null)
            {
                clustering = cell.name.clustering;
                assert !isStatic || clustering == Clustering.STATIC_CLUSTERING;
                builder.newRow(clustering);
            }
            else if (!clustering.equals(cell.name.clustering))
            {
                return false;
            }

            // Ignore shadowed cells
            if (rowDeletion != null && rowDeletion.deletionTime.deletes(cell.timestamp))
                return true;

            ColumnDefinition column = cell.name.column;
            if (column == null)
            {
                // It's the row marker
                assert !cell.value.hasRemaining();
                builder.addPrimaryKeyLivenessInfo(LivenessInfo.withExpirationTime(cell.timestamp, cell.ttl, cell.localDeletionTime));
            }
            else
            {
                if (collectionDeletion != null && collectionDeletion.start.collectionName.name.equals(column.name) && collectionDeletion.deletionTime.deletes(cell.timestamp))
                    return true;

                if (helper.includes(column))
                {
                    CellPath path = null;
                    if (column.isComplex())
                    {
                        // Recalling startOfComplexColumn for every cell is a big inefficient, but it's ok in practice
                        // and it's simpler. And since 1) this only matter for super column selection in thrift in
                        // practice and 2) is only used during upgrade, it's probably worth keeping things simple.
                        helper.startOfComplexColumn(column);
                        path = cell.name.collectionElement == null ? null : CellPath.create(cell.name.collectionElement);
                        if (!helper.includes(path))
                            return true;
                    }
                    Cell c = new BufferCell(column, cell.timestamp, cell.ttl, cell.localDeletionTime, cell.value, path);
                    if (!helper.isDropped(c, column.isComplex()))
                        builder.addCell(c);
                    if (column.isComplex())
                    {
                        helper.endOfComplexColumn();
                    }
                }
            }
            return true;
        }

        public boolean addRangeTombstone(LegacyRangeTombstone tombstone)
        {
            if (tombstone.isRowDeletion(metadata))
            {
                // If we're already within a row, it can't be the same one
                if (clustering != null)
                    return false;

                clustering = tombstone.start.getAsClustering(metadata);
                builder.newRow(clustering);
                builder.addRowDeletion(Row.Deletion.regular(tombstone.deletionTime));
                rowDeletion = tombstone;
                return true;
            }

            if (tombstone.isCollectionTombstone())
            {
                if (clustering == null)
                {
                    clustering = tombstone.start.getAsClustering(metadata);
                    builder.newRow(clustering);
                }
                else if (!clustering.equals(tombstone.start.getAsClustering(metadata)))
                {
                    return false;
                }

                builder.addComplexDeletion(tombstone.start.collectionName, tombstone.deletionTime);
                if (rowDeletion == null || tombstone.deletionTime.supersedes(rowDeletion.deletionTime))
                    collectionDeletion = tombstone;
                return true;
            }
            return false;
        }

        public Row getRow()
        {
            return builder.build();
        }
    }

    public static class LegacyUnfilteredPartition
    {
        public final DeletionTime partitionDeletion;
        public final LegacyRangeTombstoneList rangeTombstones;
        public final List<LegacyCell> cells;

        private LegacyUnfilteredPartition(DeletionTime partitionDeletion, LegacyRangeTombstoneList rangeTombstones, List<LegacyCell> cells)
        {
            this.partitionDeletion = partitionDeletion;
            this.rangeTombstones = rangeTombstones;
            this.cells = cells;
        }

        public void digest(CFMetaData metadata, MessageDigest digest)
        {
            for (LegacyCell cell : cells)
            {
                digest.update(cell.name.encode(metadata).duplicate());

                if (cell.isCounter())
                    CounterContext.instance().updateDigest(digest, cell.value);
                else
                    digest.update(cell.value.duplicate());

                FBUtilities.updateWithLong(digest, cell.timestamp);
                FBUtilities.updateWithByte(digest, cell.serializationFlags());

                if (cell.isExpiring())
                    FBUtilities.updateWithInt(digest, cell.ttl);

                if (cell.isCounter())
                {
                    // Counters used to have the timestampOfLastDelete field, which we stopped using long ago and has been hard-coded
                    // to Long.MIN_VALUE but was still taken into account in 2.2 counter digests (to maintain backward compatibility
                    // in the first place).
                    FBUtilities.updateWithLong(digest, Long.MIN_VALUE);
                }
            }

            if (partitionDeletion.markedForDeleteAt() != Long.MIN_VALUE)
                digest.update(ByteBufferUtil.bytes(partitionDeletion.markedForDeleteAt()));

            if (!rangeTombstones.isEmpty())
                rangeTombstones.updateDigest(digest);
        }
    }

    public static class LegacyCellName
    {
        public final Clustering clustering;
        public final ColumnDefinition column;
        public final ByteBuffer collectionElement;

        private LegacyCellName(Clustering clustering, ColumnDefinition column, ByteBuffer collectionElement)
        {
            this.clustering = clustering;
            this.column = column;
            this.collectionElement = collectionElement;
        }

        public ByteBuffer encode(CFMetaData metadata)
        {
            return encodeCellName(metadata, clustering, column == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : column.name.bytes, collectionElement);
        }

        public ByteBuffer superColumnSubName()
        {
            assert collectionElement != null;
            return collectionElement;
        }

        public ByteBuffer superColumnName()
        {
            return clustering.get(0);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < clustering.size(); i++)
                sb.append(i > 0 ? ":" : "").append(clustering.get(i) == null ? "null" : ByteBufferUtil.bytesToHex(clustering.get(i)));
            return String.format("Cellname(clustering=%s, column=%s, collElt=%s)", sb.toString(), column == null ? "null" : column.name, collectionElement == null ? "null" : ByteBufferUtil.bytesToHex(collectionElement));
        }
    }

    public static class LegacyBound
    {
        public static final LegacyBound BOTTOM = new LegacyBound(Slice.Bound.BOTTOM, false, null);
        public static final LegacyBound TOP = new LegacyBound(Slice.Bound.TOP, false, null);

        public final Slice.Bound bound;
        public final boolean isStatic;
        public final ColumnDefinition collectionName;

        public LegacyBound(Slice.Bound bound, boolean isStatic, ColumnDefinition collectionName)
        {
            this.bound = bound;
            this.isStatic = isStatic;
            this.collectionName = collectionName;
        }

        public Clustering getAsClustering(CFMetaData metadata)
        {
            if (isStatic)
                return Clustering.STATIC_CLUSTERING;

            assert bound.size() == metadata.comparator.size();
            ByteBuffer[] values = new ByteBuffer[bound.size()];
            for (int i = 0; i < bound.size(); i++)
                values[i] = bound.get(i);
            return Clustering.make(values);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(bound.kind()).append('(');
            for (int i = 0; i < bound.size(); i++)
                sb.append(i > 0 ? ":" : "").append(bound.get(i) == null ? "null" : ByteBufferUtil.bytesToHex(bound.get(i)));
            sb.append(')');
            return String.format("Bound(%s, collection=%s)", sb.toString(), collectionName == null ? "null" : collectionName.name);
        }
    }

    public interface LegacyAtom
    {
        public boolean isCell();

        public ClusteringPrefix clustering();
        public boolean isStatic();

        public LegacyCell asCell();
        public LegacyRangeTombstone asRangeTombstone();
    }

    /**
     * A legacy cell.
     * <p>
     * This is used as a temporary object to facilitate dealing with the legacy format, this
     * is not meant to be optimal.
     */
    public static class LegacyCell implements LegacyAtom
    {
        private final static int DELETION_MASK        = 0x01;
        private final static int EXPIRATION_MASK      = 0x02;
        private final static int COUNTER_MASK         = 0x04;
        private final static int COUNTER_UPDATE_MASK  = 0x08;
        private final static int RANGE_TOMBSTONE_MASK = 0x10;

        public enum Kind { REGULAR, EXPIRING, DELETED, COUNTER }

        public final Kind kind;

        public final LegacyCellName name;
        public final ByteBuffer value;

        public final long timestamp;
        public final int localDeletionTime;
        public final int ttl;

        private LegacyCell(Kind kind, LegacyCellName name, ByteBuffer value, long timestamp, int localDeletionTime, int ttl)
        {
            this.kind = kind;
            this.name = name;
            this.value = value;
            this.timestamp = timestamp;
            this.localDeletionTime = localDeletionTime;
            this.ttl = ttl;
        }

        public static LegacyCell regular(CFMetaData metadata, ByteBuffer superColumnName, ByteBuffer name, ByteBuffer value, long timestamp)
        throws UnknownColumnException
        {
            return new LegacyCell(Kind.REGULAR, decodeCellName(metadata, superColumnName, name), value, timestamp, Cell.NO_DELETION_TIME, Cell.NO_TTL);
        }

        public static LegacyCell expiring(CFMetaData metadata, ByteBuffer superColumnName, ByteBuffer name, ByteBuffer value, long timestamp, int ttl, int nowInSec)
        throws UnknownColumnException
        {
            return new LegacyCell(Kind.EXPIRING, decodeCellName(metadata, superColumnName, name), value, timestamp, nowInSec + ttl, ttl);
        }

        public static LegacyCell tombstone(CFMetaData metadata, ByteBuffer superColumnName, ByteBuffer name, long timestamp, int nowInSec)
        throws UnknownColumnException
        {
            return new LegacyCell(Kind.DELETED, decodeCellName(metadata, superColumnName, name), ByteBufferUtil.EMPTY_BYTE_BUFFER, timestamp, nowInSec, LivenessInfo.NO_TTL);
        }

        public static LegacyCell counter(CFMetaData metadata, ByteBuffer superColumnName, ByteBuffer name, long value)
        throws UnknownColumnException
        {
            // See UpdateParameters.addCounter() for more details on this
            ByteBuffer counterValue = CounterContext.instance().createLocal(value);
            return counter(decodeCellName(metadata, superColumnName, name), counterValue);
        }

        public static LegacyCell counter(LegacyCellName name, ByteBuffer value)
        {
            return new LegacyCell(Kind.COUNTER, name, value, FBUtilities.timestampMicros(), Cell.NO_DELETION_TIME, Cell.NO_TTL);
        }

        public byte serializationFlags()
        {
            if (isExpiring())
                return EXPIRATION_MASK;
            if (isTombstone())
                return DELETION_MASK;
            if (isCounterUpdate())
                return COUNTER_UPDATE_MASK;
            if (isCounter())
                return COUNTER_MASK;
            return 0;
        }

        private boolean isCounterUpdate()
        {
            // See UpdateParameters.addCounter() for more details on this
            return isCounter() && CounterContext.instance().isLocal(value);
        }

        public ClusteringPrefix clustering()
        {
            return name.clustering;
        }

        public boolean isStatic()
        {
            return name.clustering == Clustering.STATIC_CLUSTERING;
        }

        public boolean isCell()
        {
            return true;
        }

        public LegacyCell asCell()
        {
            return this;
        }

        public LegacyRangeTombstone asRangeTombstone()
        {
            throw new UnsupportedOperationException();
        }

        public boolean isCounter()
        {
            return kind == Kind.COUNTER;
        }

        public boolean isExpiring()
        {
            return kind == Kind.EXPIRING;
        }

        public boolean isTombstone()
        {
            return kind == Kind.DELETED;
        }

        public boolean isLive(int nowInSec)
        {
            if (isTombstone())
                return false;

            return !isExpiring() || nowInSec < localDeletionTime;
        }

        @Override
        public String toString()
        {
            return String.format("LegacyCell(%s, name=%s, v=%s, ts=%s, ldt=%s, ttl=%s)", kind, name, ByteBufferUtil.bytesToHex(value), timestamp, localDeletionTime, ttl);
        }
    }

    /**
     * A legacy range tombstone.
     * <p>
     * This is used as a temporary object to facilitate dealing with the legacy format, this
     * is not meant to be optimal.
     */
    public static class LegacyRangeTombstone implements LegacyAtom
    {
        public final LegacyBound start;
        public final LegacyBound stop;
        public final DeletionTime deletionTime;

        public LegacyRangeTombstone(LegacyBound start, LegacyBound stop, DeletionTime deletionTime)
        {
            // Because of the way RangeTombstoneList work, we can have a tombstone where only one of
            // the bound has a collectionName. That happens if we have a big tombstone A (spanning one
            // or multiple rows) and a collection tombstone B. In that case, RangeTombstoneList will
            // split this into 3 RTs: the first one from the beginning of A to the beginning of B,
            // then B, then a third one from the end of B to the end of A. To make this simpler, if
            // we detect that case we transform the 1st and 3rd tombstone so they don't end in the middle
            // of a row (which is still correct).
            if ((start.collectionName == null) != (stop.collectionName == null))
            {
                if (start.collectionName == null)
                    stop = new LegacyBound(stop.bound, stop.isStatic, null);
                else
                    start = new LegacyBound(start.bound, start.isStatic, null);
            }
            else if (!Objects.equals(start.collectionName, stop.collectionName))
            {
                // We're in the similar but slightly more complex case where on top of the big tombstone
                // A, we have 2 (or more) collection tombstones B and C within A. So we also end up with
                // a tombstone that goes between the end of B and the start of C.
                start = new LegacyBound(start.bound, start.isStatic, null);
                stop = new LegacyBound(stop.bound, stop.isStatic, null);
            }

            this.start = start;
            this.stop = stop;
            this.deletionTime = deletionTime;
        }

        public ClusteringPrefix clustering()
        {
            return start.bound;
        }

        public LegacyRangeTombstone withNewStart(LegacyBound newStart)
        {
            return new LegacyRangeTombstone(newStart, stop, deletionTime);
        }

        public LegacyRangeTombstone withNewEnd(LegacyBound newStop)
        {
            return new LegacyRangeTombstone(start, newStop, deletionTime);
        }

        public boolean isCell()
        {
            return false;
        }

        public boolean isStatic()
        {
            return start.isStatic || stop.isStatic;
        }

        public LegacyCell asCell()
        {
            throw new UnsupportedOperationException();
        }

        public LegacyRangeTombstone asRangeTombstone()
        {
            return this;
        }

        public boolean isCollectionTombstone()
        {
            return start.collectionName != null;
        }

        public boolean isRowDeletion(CFMetaData metadata)
        {
            if (start.collectionName != null
                || stop.collectionName != null
                || start.bound.size() != metadata.comparator.size()
                || stop.bound.size() != metadata.comparator.size())
                return false;

            for (int i = 0; i < start.bound.size(); i++)
                if (!Objects.equals(start.bound.get(i), stop.bound.get(i)))
                    return false;
            return true;
        }

        @Override
        public String toString()
        {
            return String.format("RT(%s-%s, %s)", start, stop, deletionTime);
        }
    }

    public static class LegacyDeletionInfo
    {
        public final MutableDeletionInfo deletionInfo;
        public final List<LegacyRangeTombstone> inRowTombstones = new ArrayList<>();

        private LegacyDeletionInfo(MutableDeletionInfo deletionInfo)
        {
            this.deletionInfo = deletionInfo;
        }

        public static LegacyDeletionInfo live()
        {
            return new LegacyDeletionInfo(MutableDeletionInfo.live());
        }

        public void add(DeletionTime topLevel)
        {
            deletionInfo.add(topLevel);
        }

        private static Slice.Bound staticBound(CFMetaData metadata, boolean isStart)
        {
            // In pre-3.0 nodes, static row started by a clustering with all empty values so we
            // preserve that here. Note that in practice, it doesn't really matter since the rest
            // of the code will ignore the bound for RT that have their static flag set.
            ByteBuffer[] values = new ByteBuffer[metadata.comparator.size()];
            for (int i = 0; i < values.length; i++)
                values[i] = ByteBufferUtil.EMPTY_BYTE_BUFFER;
            return isStart
                 ? Slice.Bound.inclusiveStartOf(values)
                 : Slice.Bound.inclusiveEndOf(values);
        }

        public void add(CFMetaData metadata, LegacyRangeTombstone tombstone)
        {
            if (metadata.hasStaticColumns())
            {
                /*
                 * For table having static columns we have to deal with the following cases:
                 *  1. the end of the tombstone is static (in which case either the start is static or is BOTTOM, which is the same
                 *     for our consideration). This mean that either the range only delete the static row, or that it's a collection
                 *     tombstone of a static collection. In both case, we just add the tombstone to the inRowTombstones.
                 *  2. only the start is static. There is then 2 subcase: either the start is inclusive, and that mean we include the
                 *     static row and more (so we add an inRowTombstone for the static and deal with the rest normally). Or the start
                 *     is exclusive, and that means we explicitely exclude the static (in which case we can just add the tombstone
                 *     as if it started at BOTTOM).
                 *  3. none of the bound are static but the start is BOTTOM. This means we intended to delete the static row so we
                 *     need to add it to the inRowTombstones (and otherwise handle the range normally).
                 */
                if (tombstone.stop.isStatic)
                {
                    // If the start is BOTTOM, we replace it by the beginning of the starting row so as to not confuse the
                    // RangeTombstone.isRowDeletion() method
                    if (tombstone.start == LegacyBound.BOTTOM)
                        tombstone = tombstone.withNewStart(new LegacyBound(staticBound(metadata, true), true, null));
                    inRowTombstones.add(tombstone);
                    return;
                }

                if (tombstone.start.isStatic)
                {
                    if (tombstone.start.bound.isInclusive())
                        inRowTombstones.add(tombstone.withNewEnd(new LegacyBound(staticBound(metadata, false), true, null)));

                    tombstone = tombstone.withNewStart(LegacyBound.BOTTOM);
                }
                else if (tombstone.start == LegacyBound.BOTTOM)
                {
                    inRowTombstones.add(new LegacyRangeTombstone(new LegacyBound(staticBound(metadata, true), true, null),
                                                                 new LegacyBound(staticBound(metadata, false), true, null),
                                                                 tombstone.deletionTime));
                }
            }

            if (tombstone.isCollectionTombstone() || tombstone.isRowDeletion(metadata))
                inRowTombstones.add(tombstone);
            else
                add(metadata, new RangeTombstone(Slice.make(tombstone.start.bound, tombstone.stop.bound), tombstone.deletionTime));
        }

        public void add(CFMetaData metadata, RangeTombstone tombstone)
        {
            deletionInfo.add(tombstone, metadata.comparator);
        }

        public Iterator<LegacyRangeTombstone> inRowRangeTombstones()
        {
            return inRowTombstones.iterator();
        }

        public static LegacyDeletionInfo deserialize(CFMetaData metadata, DataInputPlus in) throws IOException
        {
            DeletionTime topLevel = DeletionTime.serializer.deserialize(in);

            int rangeCount = in.readInt();
            if (rangeCount == 0)
                return new LegacyDeletionInfo(new MutableDeletionInfo(topLevel));

            LegacyDeletionInfo delInfo = new LegacyDeletionInfo(new MutableDeletionInfo(topLevel));
            for (int i = 0; i < rangeCount; i++)
            {
                LegacyBound start = decodeBound(metadata, ByteBufferUtil.readWithShortLength(in), true);
                LegacyBound end = decodeBound(metadata, ByteBufferUtil.readWithShortLength(in), false);
                int delTime =  in.readInt();
                long markedAt = in.readLong();

                delInfo.add(metadata, new LegacyRangeTombstone(start, end, new DeletionTime(markedAt, delTime)));
            }
            return delInfo;
        }
    }

    /**
     * A helper class for LegacyRangeTombstoneList.  This replaces the Comparator<Composite> that RTL used before 3.0.
     */
    private static class LegacyBoundComparator implements Comparator<LegacyBound>
    {
        ClusteringComparator clusteringComparator;

        public LegacyBoundComparator(ClusteringComparator clusteringComparator)
        {
            this.clusteringComparator = clusteringComparator;
        }

        public int compare(LegacyBound a, LegacyBound b)
        {
            // In the legacy sorting, BOTTOM comes before anything else
            if (a == LegacyBound.BOTTOM)
                return b == LegacyBound.BOTTOM ? 0 : -1;
            if (b == LegacyBound.BOTTOM)
                return 1;

            // Excluding BOTTOM, statics are always before anything else.
            if (a.isStatic != b.isStatic)
                return a.isStatic ? -1 : 1;

            int result = this.clusteringComparator.compare(a.bound, b.bound);
            if (result != 0)
                return result;

            // If both have equal "bound" but one is a collection tombstone and not the other, then the other comes before as it points to the beginning of the row.
            if (a.collectionName == null)
                return b.collectionName == null ? 0 : 1;
            if (b.collectionName == null)
                return -1;

            return UTF8Type.instance.compare(a.collectionName.name.bytes, b.collectionName.name.bytes);
        }
    }

    /**
     * Almost an entire copy of RangeTombstoneList from C* 2.1.  The main difference is that LegacyBoundComparator
     * is used in place of {@code Comparator<Composite>} (because Composite doesn't exist any more).
     *
     * This class is needed to allow us to convert single-row deletions and complex deletions into range tombstones
     * and properly merge them into the normal set of range tombstones.
     */
    public static class LegacyRangeTombstoneList
    {
        private final LegacyBoundComparator comparator;

        // Note: we don't want to use a List for the markedAts and delTimes to avoid boxing. We could
        // use a List for starts and ends, but having arrays everywhere is almost simpler.
        private LegacyBound[] starts;
        private LegacyBound[] ends;
        private long[] markedAts;
        private int[] delTimes;

        private int size;

        private LegacyRangeTombstoneList(LegacyBoundComparator comparator, LegacyBound[] starts, LegacyBound[] ends, long[] markedAts, int[] delTimes, int size)
        {
            assert starts.length == ends.length && starts.length == markedAts.length && starts.length == delTimes.length;
            this.comparator = comparator;
            this.starts = starts;
            this.ends = ends;
            this.markedAts = markedAts;
            this.delTimes = delTimes;
            this.size = size;
        }

        public LegacyRangeTombstoneList(LegacyBoundComparator comparator, int capacity)
        {
            this(comparator, new LegacyBound[capacity], new LegacyBound[capacity], new long[capacity], new int[capacity], 0);
        }

        public boolean isEmpty()
        {
            return size == 0;
        }

        public int size()
        {
            return size;
        }

        /**
         * Adds a new range tombstone.
         *
         * This method will be faster if the new tombstone sort after all the currently existing ones (this is a common use case),
         * but it doesn't assume it.
         */
        public void add(LegacyBound start, LegacyBound end, long markedAt, int delTime)
        {
            if (isEmpty())
            {
                addInternal(0, start, end, markedAt, delTime);
                return;
            }

            int c = comparator.compare(ends[size-1], start);

            // Fast path if we add in sorted order
            if (c <= 0)
            {
                addInternal(size, start, end, markedAt, delTime);
            }
            else
            {
                // Note: insertFrom expect i to be the insertion point in term of interval ends
                int pos = Arrays.binarySearch(ends, 0, size, start, comparator);
                insertFrom((pos >= 0 ? pos : -pos-1), start, end, markedAt, delTime);
            }
        }

        /*
         * Inserts a new element starting at index i. This method assumes that:
         *    ends[i-1] <= start <= ends[i]
         *
         * A RangeTombstoneList is a list of range [s_0, e_0]...[s_n, e_n] such that:
         *   - s_i <= e_i
         *   - e_i <= s_i+1
         *   - if s_i == e_i and e_i == s_i+1 then s_i+1 < e_i+1
         * Basically, range are non overlapping except for their bound and in order. And while
         * we allow ranges with the same value for the start and end, we don't allow repeating
         * such range (so we can't have [0, 0][0, 0] even though it would respect the first 2
         * conditions).
         *
         */

        /**
         * Adds all the range tombstones of {@code tombstones} to this RangeTombstoneList.
         */
        public void addAll(LegacyRangeTombstoneList tombstones)
        {
            if (tombstones.isEmpty())
                return;

            if (isEmpty())
            {
                copyArrays(tombstones, this);
                return;
            }

            /*
             * We basically have 2 techniques we can use here: either we repeatedly call add() on tombstones values,
             * or we do a merge of both (sorted) lists. If this lists is bigger enough than the one we add, then
             * calling add() will be faster, otherwise it's merging that will be faster.
             *
             * Let's note that during memtables updates, it might not be uncommon that a new update has only a few range
             * tombstones, while the CF we're adding it to (the one in the memtable) has many. In that case, using add() is
             * likely going to be faster.
             *
             * In other cases however, like when diffing responses from multiple nodes, the tombstone lists we "merge" will
             * be likely sized, so using add() might be a bit inefficient.
             *
             * Roughly speaking (this ignore the fact that updating an element is not exactly constant but that's not a big
             * deal), if n is the size of this list and m is tombstones size, merging is O(n+m) while using add() is O(m*log(n)).
             *
             * But let's not crank up a logarithm computation for that. Long story short, merging will be a bad choice only
             * if this list size is lot bigger that the other one, so let's keep it simple.
             */
            if (size > 10 * tombstones.size)
            {
                for (int i = 0; i < tombstones.size; i++)
                    add(tombstones.starts[i], tombstones.ends[i], tombstones.markedAts[i], tombstones.delTimes[i]);
            }
            else
            {
                int i = 0;
                int j = 0;
                while (i < size && j < tombstones.size)
                {
                    if (comparator.compare(tombstones.starts[j], ends[i]) <= 0)
                    {
                        insertFrom(i, tombstones.starts[j], tombstones.ends[j], tombstones.markedAts[j], tombstones.delTimes[j]);
                        j++;
                    }
                    else
                    {
                        i++;
                    }
                }
                // Addds the remaining ones from tombstones if any (note that addInternal will increment size if relevant).
                for (; j < tombstones.size; j++)
                    addInternal(size, tombstones.starts[j], tombstones.ends[j], tombstones.markedAts[j], tombstones.delTimes[j]);
            }
        }

        private static void copyArrays(LegacyRangeTombstoneList src, LegacyRangeTombstoneList dst)
        {
            dst.grow(src.size);
            System.arraycopy(src.starts, 0, dst.starts, 0, src.size);
            System.arraycopy(src.ends, 0, dst.ends, 0, src.size);
            System.arraycopy(src.markedAts, 0, dst.markedAts, 0, src.size);
            System.arraycopy(src.delTimes, 0, dst.delTimes, 0, src.size);
            dst.size = src.size;
        }

        private void insertFrom(int i, LegacyBound start, LegacyBound end, long markedAt, int delTime)
        {
            while (i < size)
            {
                assert i == 0 || comparator.compare(ends[i-1], start) <= 0;

                int c = comparator.compare(start, ends[i]);
                assert c <= 0;
                if (c == 0)
                {
                    // If start == ends[i], then we can insert from the next one (basically the new element
                    // really start at the next element), except for the case where starts[i] == ends[i].
                    // In this latter case, if we were to move to next element, we could end up with ...[x, x][x, x]...
                    if (comparator.compare(starts[i], ends[i]) == 0)
                    {
                        // The current element cover a single value which is equal to the start of the inserted
                        // element. If the inserted element overwrites the current one, just remove the current
                        // (it's included in what we insert) and proceed with the insert.
                        if (markedAt > markedAts[i])
                        {
                            removeInternal(i);
                            continue;
                        }

                        // Otherwise (the current singleton interval override the new one), we want to leave the
                        // current element and move to the next, unless start == end since that means the new element
                        // is in fact fully covered by the current one (so we're done)
                        if (comparator.compare(start, end) == 0)
                            return;
                    }
                    i++;
                    continue;
                }

                // Do we overwrite the current element?
                if (markedAt > markedAts[i])
                {
                    // We do overwrite.

                    // First deal with what might come before the newly added one.
                    if (comparator.compare(starts[i], start) < 0)
                    {
                        addInternal(i, starts[i], start, markedAts[i], delTimes[i]);
                        i++;
                        // We don't need to do the following line, but in spirit that's what we want to do
                        // setInternal(i, start, ends[i], markedAts, delTime])
                    }

                    // now, start <= starts[i]

                    // Does the new element stops before/at the current one,
                    int endCmp = comparator.compare(end, starts[i]);
                    if (endCmp <= 0)
                    {
                        // Here start <= starts[i] and end <= starts[i]
                        // This means the current element is before the current one. However, one special
                        // case is if end == starts[i] and starts[i] == ends[i]. In that case,
                        // the new element entirely overwrite the current one and we can just overwrite
                        if (endCmp == 0 && comparator.compare(starts[i], ends[i]) == 0)
                            setInternal(i, start, end, markedAt, delTime);
                        else
                            addInternal(i, start, end, markedAt, delTime);
                        return;
                    }

                    // Do we overwrite the current element fully?
                    int cmp = comparator.compare(ends[i], end);
                    if (cmp <= 0)
                    {
                        // We do overwrite fully:
                        // update the current element until it's end and continue
                        // on with the next element (with the new inserted start == current end).

                        // If we're on the last element, we can optimize
                        if (i == size-1)
                        {
                            setInternal(i, start, end, markedAt, delTime);
                            return;
                        }

                        setInternal(i, start, ends[i], markedAt, delTime);
                        if (cmp == 0)
                            return;

                        start = ends[i];
                        i++;
                    }
                    else
                    {
                        // We don't ovewrite fully. Insert the new interval, and then update the now next
                        // one to reflect the not overwritten parts. We're then done.
                        addInternal(i, start, end, markedAt, delTime);
                        i++;
                        setInternal(i, end, ends[i], markedAts[i], delTimes[i]);
                        return;
                    }
                }
                else
                {
                    // we don't overwrite the current element

                    // If the new interval starts before the current one, insert that new interval
                    if (comparator.compare(start, starts[i]) < 0)
                    {
                        // If we stop before the start of the current element, just insert the new
                        // interval and we're done; otherwise insert until the beginning of the
                        // current element
                        if (comparator.compare(end, starts[i]) <= 0)
                        {
                            addInternal(i, start, end, markedAt, delTime);
                            return;
                        }
                        addInternal(i, start, starts[i], markedAt, delTime);
                        i++;
                    }

                    // After that, we're overwritten on the current element but might have
                    // some residual parts after ...

                    // ... unless we don't extend beyond it.
                    if (comparator.compare(end, ends[i]) <= 0)
                        return;

                    start = ends[i];
                    i++;
                }
            }

            // If we got there, then just insert the remainder at the end
            addInternal(i, start, end, markedAt, delTime);
        }
        private int capacity()
        {
            return starts.length;
        }

        private void addInternal(int i, LegacyBound start, LegacyBound end, long markedAt, int delTime)
        {
            assert i >= 0;

            if (size == capacity())
                growToFree(i);
            else if (i < size)
                moveElements(i);

            setInternal(i, start, end, markedAt, delTime);
            size++;
        }

        private void removeInternal(int i)
        {
            assert i >= 0;

            System.arraycopy(starts, i+1, starts, i, size - i - 1);
            System.arraycopy(ends, i+1, ends, i, size - i - 1);
            System.arraycopy(markedAts, i+1, markedAts, i, size - i - 1);
            System.arraycopy(delTimes, i+1, delTimes, i, size - i - 1);

            --size;
            starts[size] = null;
            ends[size] = null;
        }

        /*
         * Grow the arrays, leaving index i "free" in the process.
         */
        private void growToFree(int i)
        {
            int newLength = (capacity() * 3) / 2 + 1;
            grow(i, newLength);
        }

        /*
         * Grow the arrays to match newLength capacity.
         */
        private void grow(int newLength)
        {
            if (capacity() < newLength)
                grow(-1, newLength);
        }

        private void grow(int i, int newLength)
        {
            starts = grow(starts, size, newLength, i);
            ends = grow(ends, size, newLength, i);
            markedAts = grow(markedAts, size, newLength, i);
            delTimes = grow(delTimes, size, newLength, i);
        }

        private static LegacyBound[] grow(LegacyBound[] a, int size, int newLength, int i)
        {
            if (i < 0 || i >= size)
                return Arrays.copyOf(a, newLength);

            LegacyBound[] newA = new LegacyBound[newLength];
            System.arraycopy(a, 0, newA, 0, i);
            System.arraycopy(a, i, newA, i+1, size - i);
            return newA;
        }

        private static long[] grow(long[] a, int size, int newLength, int i)
        {
            if (i < 0 || i >= size)
                return Arrays.copyOf(a, newLength);

            long[] newA = new long[newLength];
            System.arraycopy(a, 0, newA, 0, i);
            System.arraycopy(a, i, newA, i+1, size - i);
            return newA;
        }

        private static int[] grow(int[] a, int size, int newLength, int i)
        {
            if (i < 0 || i >= size)
                return Arrays.copyOf(a, newLength);

            int[] newA = new int[newLength];
            System.arraycopy(a, 0, newA, 0, i);
            System.arraycopy(a, i, newA, i+1, size - i);
            return newA;
        }

        /*
         * Move elements so that index i is "free", assuming the arrays have at least one free slot at the end.
         */
        private void moveElements(int i)
        {
            if (i >= size)
                return;

            System.arraycopy(starts, i, starts, i+1, size - i);
            System.arraycopy(ends, i, ends, i+1, size - i);
            System.arraycopy(markedAts, i, markedAts, i+1, size - i);
            System.arraycopy(delTimes, i, delTimes, i+1, size - i);
            // we set starts[i] to null to indicate the position is now empty, so that we update boundaryHeapSize
            // when we set it
            starts[i] = null;
        }

        private void setInternal(int i, LegacyBound start, LegacyBound end, long markedAt, int delTime)
        {
            starts[i] = start;
            ends[i] = end;
            markedAts[i] = markedAt;
            delTimes[i] = delTime;
        }

        public void updateDigest(MessageDigest digest)
        {
            ByteBuffer longBuffer = ByteBuffer.allocate(8);
            for (int i = 0; i < size; i++)
            {
                for (int j = 0; j < starts[i].bound.size(); j++)
                    digest.update(starts[i].bound.get(j).duplicate());
                if (starts[i].collectionName != null)
                    digest.update(starts[i].collectionName.name.bytes.duplicate());
                for (int j = 0; j < ends[i].bound.size(); j++)
                    digest.update(ends[i].bound.get(j).duplicate());
                if (ends[i].collectionName != null)
                    digest.update(ends[i].collectionName.name.bytes.duplicate());

                longBuffer.putLong(0, markedAts[i]);
                digest.update(longBuffer.array(), 0, 8);
            }
        }

        public void serialize(DataOutputPlus out, CFMetaData metadata) throws IOException
        {
            out.writeInt(size);
            if (size == 0)
                return;

            List<AbstractType<?>> types = new ArrayList<>(comparator.clusteringComparator.subtypes());
            if (!metadata.isDense())
                types.add(UTF8Type.instance);
            CompositeType type = CompositeType.getInstance(types);

            for (int i = 0; i < size; i++)
            {
                LegacyBound start = starts[i];
                LegacyBound end = ends[i];

                CompositeType.Builder startBuilder = type.builder();
                CompositeType.Builder endBuilder = type.builder();
                for (int j = 0; j < start.bound.clustering().size(); j++)
                {
                    startBuilder.add(start.bound.get(j));
                    endBuilder.add(end.bound.get(j));
                }

                if (start.collectionName != null)
                    startBuilder.add(start.collectionName.name.bytes);
                if (end.collectionName != null)
                    endBuilder.add(end.collectionName.name.bytes);

                ByteBufferUtil.writeWithShortLength(startBuilder.build(), out);
                ByteBufferUtil.writeWithShortLength(endBuilder.buildAsEndOfRange(), out);

                out.writeInt(delTimes[i]);
                out.writeLong(markedAts[i]);
            }
        }

        public long serializedSize(CFMetaData metadata)
        {
            long size = 0;
            size += TypeSizes.sizeof(this.size);

            if (this.size == 0)
                return size;

            List<AbstractType<?>> types = new ArrayList<>(comparator.clusteringComparator.subtypes());
            if (!metadata.isDense())
                types.add(UTF8Type.instance);
            CompositeType type = CompositeType.getInstance(types);

            for (int i = 0; i < this.size; i++)
            {
                LegacyBound start = starts[i];
                LegacyBound end = ends[i];

                CompositeType.Builder startBuilder = type.builder();
                CompositeType.Builder endBuilder = type.builder();
                for (int j = 0; j < start.bound.clustering().size(); j++)
                {
                    startBuilder.add(start.bound.get(j));
                    endBuilder.add(end.bound.get(j));
                }

                if (start.collectionName != null)
                    startBuilder.add(start.collectionName.name.bytes);
                if (end.collectionName != null)
                    endBuilder.add(end.collectionName.name.bytes);

                size += ByteBufferUtil.serializedSizeWithShortLength(startBuilder.build());
                size += ByteBufferUtil.serializedSizeWithShortLength(endBuilder.buildAsEndOfRange());

                size += TypeSizes.sizeof(delTimes[i]);
                size += TypeSizes.sizeof(markedAts[i]);
            }
            return size;
        }
    }
}
