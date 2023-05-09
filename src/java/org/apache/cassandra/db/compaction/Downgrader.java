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

package org.apache.cassandra.db.compaction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Throwables;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.BiLongAccumulator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.LongAccumulator;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.memory.Cloner;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class Downgrader
{
    private final ColumnFamilyStore cfs;
    private final SSTableReader sstable;
    private final LifecycleTransaction transaction;
    private final File directory;

    private final CompactionController controller;
    private final CompactionStrategyManager strategyManager;
    private final long estimatedRows;

    private final SSTableFormat.Type inputType;
    private final SSTableFormat.Type outputType;
    private final Version outputVersion;

    private final OutputHandler outputHandler;

    private TableMapper tableMapper;


    private void register(String keyspace, String name, TableMapper tableMapper) {
        if (cfs.metadata.keyspace.equals(keyspace) && cfs.metadata.name.equals(name))
        {
            this.tableMapper = tableMapper;
        }
    }

    public static SSTableFormat.Type forWriting(String version)
    {
        String[] v3 =
        { "ma" , "mb" , "mc" , "md" , "me" , "mf" , "mg" , "mh" , "mi" , "mj" , "mk" ,
          "ml" , "mm" , "mn" , "mo" , "mp" , "mq" , "mr" , "ms" , "mt" , "mu" , "mv" , "mw" , "mx" , "my" , "mz" ,
          "na" };

        String[] v4 =
        { "nb", "nc", "nd", "ne", "nf", "ng", "nh", "ni", "nj", "nk", "nl", "nn", "nn", "no", "np", "nq", "nr", "ns",
          "nt", "nu", "nv", "nw", "nx", "ny", "nz" };

        if (version.compareTo( v3[v3.length-1]) <= 0) {
            for( String s : v3)
            {
                if (s.equals(version))
                {
                    return SSTableFormat.Type.V3;
                }
            }
        }

        if (version.compareTo( v4[v4.length-1]) <= 0) {
            for( String s : v4)
            {
                if (s.equals(version))
                {
                    return SSTableFormat.Type.BIG;
                }
            }
        }
        throw new IllegalArgumentException("No version constant " + version);
    }

    static class TableMapper {
        Collection<String> getRemoveNames() { return Collections.emptyList();}

        final ColumnMetadata map(ColumnMetadata orig) {
            return getRemoveNames().contains(orig.name.toString()) ? null : processColumn(orig);
        }

        ColumnMetadata processColumn(ColumnMetadata column) {
            return column;
        }

        final Collection<ColumnMetadata> removeDeletedColumns(Collection<ColumnMetadata> columns) {
            return columns.stream().filter(columnMetadataFilter()).collect(Collectors.toList());
        }

        final Predicate<ColumnData> columnDataFilter() {
             return cd -> !getRemoveNames().contains(cd.column().name.toString());
        }

        final Predicate<ColumnMetadata> columnMetadataFilter() {
            return m -> !getRemoveNames().contains(m.name.toString());
        }
    }



    public Downgrader(String version, ColumnFamilyStore cfs, LifecycleTransaction txn, OutputHandler outputHandler, SSTableFormat.Type sourceFormat)
    {
        this.cfs = cfs;
        this.transaction = txn;
        this.sstable = txn.onlyOne();
        this.outputHandler = outputHandler;

        this.inputType = sourceFormat;
        this.outputType = forWriting(version);
        this.outputVersion = outputType.info.getVersion(version);

        this.directory = new File(sstable.getFilename()).parent();

        this.controller = new Downgrader.DowngradeController(cfs);

        this.strategyManager = cfs.getCompactionStrategyManager();
        long estimatedTotalKeys = Math.max(cfs.metadata().params.minIndexInterval, SSTableReader.getApproximateKeyCount(Arrays.asList(this.sstable)));
        long estimatedSSTables = Math.max(1, SSTableReader.getTotalBytes(Arrays.asList(this.sstable)) / strategyManager.getMaxSSTableBytes());
        this.estimatedRows = (long) Math.ceil((double) estimatedTotalKeys / estimatedSSTables);
        // register tables that need changing
        register("system","local", new LocalMapper());

        // if the current table has not been registered create a dummyy.
        if (this.tableMapper == null)
            this.tableMapper = new TableMapper(){};

    }



    private SSTableWriter createCompactionWriter(StatsMetadata metadata)
    {
        MetadataCollector sstableMetadataCollector = new MetadataCollector(cfs.getComparator());
        sstableMetadataCollector.sstableLevel(sstable.getSSTableLevel());
        Descriptor newDescriptor = new Descriptor(outputVersion,
                                                      directory,
                                                      cfs.keyspace.getName(),
                                                      cfs.name,
                                                      cfs.getNextsstableId(),
                                                      outputType);
            assert !newDescriptor.fileFor(Component.DATA).exists();

        TableMetadataRef newMetadata = rewrite(cfs.metadata);

        return SSTableWriter.create(newDescriptor,
                                    estimatedRows,
                                    metadata.repairedAt,
                                    metadata.pendingRepair,
                                    metadata.isTransient,
                                    newMetadata,
                                    sstableMetadataCollector,
                                    //SerializationHeader.make(newMetadata.get(), Sets.newHashSet(sstable)),
                                    SerializationHeader.makeWithoutStats(newMetadata.get()),
                                    cfs.indexManager.listIndexes(),
                                    transaction);
    }

    TableMetadataRef rewrite(TableMetadataRef original) {
        TableMetadata.Builder builder = original.get().unbuild();
        for (ColumnMetadata colMeta  : original.get().columns()) {
            if (!colMeta.isPrimaryKeyColumn())
            {
                ColumnMetadata origColMeta= builder.getColumn(colMeta.name);
                ColumnMetadata newColMeta = tableMapper.map(origColMeta);
                if (newColMeta == null)
                {
                    builder.removeRegularOrStaticColumn(colMeta.name);
                    builder.recordColumnDrop( colMeta, 0L);
                    System.out.println(String.format("Dropping column: %s", colMeta.name));
                }
                else
                {
                    if (newColMeta != origColMeta)
                    {
                        System.out.println(String.format("Modifying column: %s", colMeta.name));
                        builder.replaceRegularOrStaticColumn(colMeta.name, newColMeta);
                    }
                }
            }
        }
        System.out.println( "Builder: "+builder.columnNames());
        return TableMetadataRef.forOfflineTools(builder.build());
    }

    public void downgrade(boolean keepOriginals) {
        outputHandler.output("Downgrading " + sstable);
        int nowInSec = FBUtilities.nowInSeconds();
        try (SSTableRewriter writer = SSTableRewriter.construct(cfs, transaction, keepOriginals, CompactionTask.getMaxDataAge(transaction.originals()));
             AbstractCompactionStrategy.ScannerList scanners = strategyManager.getScanners(transaction.originals());
             CompactionIterator iter = new CompactionIterator(transaction.opType(), scanners.scanners, controller, nowInSec, nextTimeUUID()))
        {
            writer.switchWriter(createCompactionWriter(sstable.getSSTableMetadata()));
            iter.setTargetDirectory(writer.currentWriter().getFilename());
            while (iter.hasNext())
            {
                WrappingUnfilteredRowIterator partition = new WrappingUnfilteredRowIterator(iter.next())
                {
                    @Override
                    public Unfiltered next()
                    {
                        Unfiltered unfiltered = super.next();
                        if (unfiltered.kind() == Unfiltered.Kind.ROW)
                        {
                            unfiltered = new WrappingRow((Row) unfiltered);

                        }
                        return unfiltered;
                    }
                };

                writer.append(partition);
            }
            writer.finish();
            outputHandler.output("Downgrade of " + sstable + " complete.");
        } catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        } finally {
            controller.close();
        }
    }

    private static class DowngradeController extends CompactionController
    {
        public DowngradeController(ColumnFamilyStore cfs)
        {
            super(cfs, Integer.MAX_VALUE);
        }

        @Override
        public LongPredicate getPurgeEvaluator(DecoratedKey key)
        {
            return time -> false;
        }
    }



    private class WrappingRow implements Row {
        Row wrapped;

        WrappingRow (Row row) {
            this.wrapped = row;
        }

        @Override
        public Clustering<?> clustering()
        {
            return wrapped.clustering();
        }

        private Stream<ColumnMetadata> getColumnMetadataStream() {
            return wrapped.columns().stream().filter( tableMapper.columnMetadataFilter() );
        }

        @Override
        public Collection<ColumnMetadata> columns()
        {
            return getColumnMetadataStream().collect(Collectors.toList());
        }

        @Override
        public int columnCount()
        {
            return wrapped.columnCount() - tableMapper.getRemoveNames().size();
        }

        @Override
        public Deletion deletion()
        {
            return wrapped.deletion();
        }

        @Override
        public LivenessInfo primaryKeyLivenessInfo()
        {
            return wrapped.primaryKeyLivenessInfo();
        }

        @Override
        public boolean isStatic()
        {
            return wrapped.isStatic();
        }

        @Override
        public boolean isEmpty()
        {
            return primaryKeyLivenessInfo().isEmpty()
                   && deletion().isLive()
                   && !getColumnDataStream().findFirst().isPresent();
        }

        @Override
        public boolean hasLiveData(int nowInSec, boolean enforceStrictLiveness)
        {
            return wrapped.hasLiveData(nowInSec, enforceStrictLiveness);
        }

        @Override
        public Cell<?> getCell(ColumnMetadata c)
        {
            return wrapped.getCell(c);
        }

        @Override
        public Cell<?> getCell(ColumnMetadata c, CellPath path)
        {
            return wrapped.getCell(c, path);
        }

        @Override
        public ComplexColumnData getComplexColumnData(ColumnMetadata c)
        {
            return wrapped.getComplexColumnData(c);
        }

        @Override
        public ColumnData getColumnData(ColumnMetadata c)
        {
            Optional<ColumnData> result =  getColumnDataStream().filter(cd -> cd.column().equals(c)).findFirst();
            return result.isPresent() ? result.get() : null;
        }

        @Override
        public Iterable<Cell<?>> cells()
        {
            return wrapped.cells();
        }

        private Stream<ColumnData> getColumnDataStream() {
            return wrapped.columnData().stream().filter( tableMapper.columnDataFilter() );
        }

        @Override
        public Collection<ColumnData> columnData()
        {
            return getColumnDataStream().collect(Collectors.toList());
        }

        @Override
        public Iterable<Cell<?>> cellsInLegacyOrder(TableMetadata metadata, boolean reversed)
        {
            return wrapped.cellsInLegacyOrder(metadata, reversed);
        }

        @Override
        public boolean hasComplexDeletion()
        {
            return wrapped.hasComplexDeletion();
        }

        @Override
        public boolean hasComplex()
        {
            return wrapped.hasComplex();
        }

        @Override
        public boolean hasDeletion(int nowInSec)
        {
            return wrapped.hasDeletion(nowInSec);
        }

        @Override
        public SearchIterator<ColumnMetadata, ColumnData> searchIterator()
        {
            return wrapped.searchIterator();
        }

        @Override
        public Row filter(ColumnFilter filter, TableMetadata metadata)
        {
            return wrapped.filter(filter, metadata);
        }

        @Override
        public Row filter(ColumnFilter filter, DeletionTime activeDeletion, boolean setActiveDeletionToRow, TableMetadata metadata)
        {
            return wrapped.filter(filter, activeDeletion, setActiveDeletionToRow, metadata);
        }

        @Override
        public Row transformAndFilter(LivenessInfo info, Deletion deletion, Function<ColumnData, ColumnData> function)
        {
            return wrapped.transformAndFilter(info, deletion, function);
        }

        @Override
        public Row transformAndFilter(Function<ColumnData, ColumnData> function)
        {
            return wrapped.transformAndFilter(function);
        }

        @Override
        public Row clone(Cloner cloner)
        {
            return wrapped.clone(cloner);
        }

        @Override
        public Row purge(DeletionPurger purger, int nowInSec, boolean enforceStrictLiveness)
        {
            return wrapped.purge(purger, nowInSec, enforceStrictLiveness);
        }

        @Override
        public Row withOnlyQueriedData(ColumnFilter filter)
        {
            return wrapped.withOnlyQueriedData(filter);
        }

        @Override
        public Row purgeDataOlderThan(long timestamp, boolean enforceStrictLiveness)
        {
            return wrapped.purgeDataOlderThan(timestamp, enforceStrictLiveness);
        }

        @Override
        public Row markCounterLocalToBeCleared()
        {
            return wrapped.markCounterLocalToBeCleared();
        }

        @Override
        public Row updateAllTimestamp(long newTimestamp)
        {
            return wrapped.updateAllTimestamp(newTimestamp);
        }

        @Override
        public Row withRowDeletion(DeletionTime deletion)
        {
            return wrapped.withRowDeletion(deletion);
        }

        @Override
        public int dataSize()
        {
            return wrapped.dataSize();
        }

        @Override
        public long unsharedHeapSizeExcludingData()
        {
            return wrapped.unsharedHeapSizeExcludingData();
        }

        @Override
        public String toString(TableMetadata metadata, boolean fullDetails)
        {
            return wrapped.toString(metadata, fullDetails);
        }

        @Override
        public long unsharedHeapSize()
        {
            return wrapped.unsharedHeapSize();
        }

        @Override
        public void apply(Consumer<ColumnData> function)
        {
            iterator().forEachRemaining(function);
        }

        @Override
        public <A> void apply(BiConsumer<A, ColumnData> function, A arg)
        {
            iterator().forEachRemaining( cd -> function.accept(arg, cd));
        }

        @Override
        public long accumulate(LongAccumulator<ColumnData> accumulator, long initialValue)
        {
            return wrapped.accumulate(accumulator, initialValue);
        }

        @Override
        public long accumulate(LongAccumulator<ColumnData> accumulator, Comparator<ColumnData> comparator, ColumnData from, long initialValue)
        {
            return wrapped.accumulate(accumulator, comparator, from, initialValue);
        }

        @Override
        public <A> long accumulate(BiLongAccumulator<A, ColumnData> accumulator, A arg, long initialValue)
        {
            return wrapped.accumulate(accumulator, arg, initialValue);
        }

        @Override
        public <A> long accumulate(BiLongAccumulator<A, ColumnData> accumulator, A arg, Comparator<ColumnData> comparator, ColumnData from, long initialValue)
        {
            return wrapped.accumulate(accumulator, arg, comparator, from, initialValue);
        }

        @Override
        public Kind kind()
        {
            return wrapped.kind();
        }

        @Override
        public void digest(Digest digest)
        {
            wrapped.digest(digest);
        }

        @Override
        public void validateData(TableMetadata metadata)
        {
            wrapped.validateData(metadata);
        }

        @Override
        public boolean hasInvalidDeletions()
        {
            return wrapped.hasInvalidDeletions();
        }

        @Override
        public String toString(TableMetadata metadata)
        {
            return wrapped.toString(metadata);
        }

        @Override
        public String toString(TableMetadata metadata, boolean includeClusterKeys, boolean fullDetails)
        {
            return wrapped.toString(metadata, includeClusterKeys, fullDetails);
        }

        @Override
        public boolean isRow()
        {
            return wrapped.isRow();
        }

        @Override
        public boolean isRangeTombstoneMarker()
        {
            return wrapped.isRangeTombstoneMarker();
        }

        @Override
        public Iterator<ColumnData> iterator()
        {
            return columnData().iterator();
        }
    }


    private class LocalMapper extends TableMapper {
        @Override
        public Collection<String> getRemoveNames()
        {
            return Arrays.asList("broadcast_port", "listen_port", "rpc_port");
        }
    }


}
