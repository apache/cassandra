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

package org.apache.cassandra.service.paxos.uncommitted;

import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.SchemaElement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PaxosRepairHistory;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Throwables;

import static com.google.common.collect.Iterables.elementsEqual;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.service.paxos.uncommitted.UncommittedDataFile.isCrcFile;
import static org.apache.cassandra.service.paxos.uncommitted.UncommittedDataFile.isTmpFile;
import static org.apache.cassandra.service.paxos.uncommitted.UncommittedDataFile.writer;

/**
 * On memtable flush
 */
public class UncommittedTableData
{
    private static final Logger logger = LoggerFactory.getLogger(UncommittedTableData.class);
    private static final Collection<Range<Token>> FULL_RANGE;

    static
    {
        Token min = DatabaseDescriptor.getPartitioner().getMinimumToken();
        FULL_RANGE = Collections.singleton(new Range<>(min, min));
    }

    private static final SchemaElement UNKNOWN_TABLE = TableMetadata.minimal("UNKNOWN", "UNKNOWN");
    private static final ExecutorPlus executor = executorFactory().sequential("PaxosUncommittedMerge");

    public interface FlushWriter
    {
        void append(PaxosKeyState commitState) throws IOException;

        void finish();

        Throwable abort(Throwable accumulate);

        default void appendAll(Iterable<PaxosKeyState> states) throws IOException
        {
            for (PaxosKeyState state : states)
                append(state);
        }
    }

    private static class FilteringIterator extends AbstractIterator<PaxosKeyState> implements CloseableIterator<PaxosKeyState>
    {
        private final CloseableIterator<PaxosKeyState> wrapped;
        private final PeekingIterator<PaxosKeyState> peeking;
        private final PeekingIterator<Range<Token>> rangeIterator;
        private final PaxosRepairHistory.Searcher historySearcher;

        FilteringIterator(CloseableIterator<PaxosKeyState> wrapped, List<Range<Token>> ranges, PaxosRepairHistory history)
        {
            this.wrapped = wrapped;
            this.peeking = Iterators.peekingIterator(wrapped);
            this.rangeIterator = Iterators.peekingIterator(Range.normalize(ranges).iterator());
            this.historySearcher = history.searcher();
        }

        protected PaxosKeyState computeNext()
        {
            while (true)
            {
                if (!peeking.hasNext() || !rangeIterator.hasNext())
                    return endOfData();

                Range<Token> range = rangeIterator.peek();

                Token token = peeking.peek().key.getToken();
                if (!range.contains(token))
                {
                    if (range.right.compareTo(token) < 0)
                        rangeIterator.next();
                    else
                        peeking.next();
                    continue;
                }

                PaxosKeyState next = peeking.next();

                Ballot lowBound = historySearcher.ballotForToken(token);
                if (Commit.isAfter(lowBound, next.ballot))
                    continue;

                return next;
            }
        }

        public void close()
        {
            wrapped.close();
        }
    }

    static abstract class FilterFactory
    {
        abstract List<Range<Token>> getReplicatedRanges();
        abstract PaxosRepairHistory getPaxosRepairHistory();

        CloseableIterator<PaxosKeyState> filter(CloseableIterator<PaxosKeyState> iterator)
        {
            return new FilteringIterator(iterator, getReplicatedRanges(), getPaxosRepairHistory());
        }
    }

    private static class CFSFilterFactory extends FilterFactory
    {
        private final TableId tableId;

        /**
         * @param tableId must refer to a known CFS
         */
        CFSFilterFactory(TableId tableId)
        {
            this.tableId = tableId;
        }

        List<Range<Token>> getReplicatedRanges()
        {
            if (tableId == null)
                return Range.normalize(FULL_RANGE);

            ColumnFamilyStore table = Schema.instance.getColumnFamilyStoreInstance(tableId);
            if (table == null)
                return Range.normalize(FULL_RANGE);

            String ksName = table.getKeyspaceName();
            List<Range<Token>> ranges = StorageService.instance.getLocalAndPendingRanges(ksName);

            // don't filter anything if we're not aware of any locally replicated ranges
            if (ranges.isEmpty())
                return Range.normalize(FULL_RANGE);

            return Range.normalize(ranges);
        }

        PaxosRepairHistory getPaxosRepairHistory()
        {
            ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(tableId);
            if (cfs == null)
                return PaxosRepairHistory.EMPTY;

            return cfs.getPaxosRepairHistory();
        }
    }

    static class Data
    {
        final ImmutableSet<UncommittedDataFile> files;

        Data(ImmutableSet<UncommittedDataFile> files)
        {
            this.files = files;
        }

        Data withFile(UncommittedDataFile file)
        {
            return new Data(ImmutableSet.<UncommittedDataFile>builder().addAll(files).add(file).build());
        }

        void truncate()
        {
            for (UncommittedDataFile file : files)
                file.markDeleted();
        }
    }

    private static class Reducer extends MergeIterator.Reducer<PaxosKeyState, PaxosKeyState>
    {
        PaxosKeyState merged = null;

        public void reduce(int idx, PaxosKeyState current)
        {
            merged = PaxosKeyState.merge(merged, current);
        }

        protected PaxosKeyState getReduced()
        {
            return merged;
        }

        protected void onKeyChange()
        {
            merged = null;
        }
    }

    private static CloseableIterator<PaxosKeyState> merge(Collection<UncommittedDataFile> files, Collection<Range<Token>> ranges)
    {
        List<CloseableIterator<PaxosKeyState>> iterators = new ArrayList<>(files.size());
        try
        {
            for (UncommittedDataFile file : files)
            {
                CloseableIterator<PaxosKeyState> iterator = file.iterator(ranges);
                if (iterator == null) continue;
                iterators.add(iterator);
            }
            return MergeIterator.get(iterators, PaxosKeyState.KEY_COMPARATOR, new Reducer());
        }
        catch (Throwable t)
        {
            Throwables.close(t, iterators);
            throw t;
        }
    }

    class Merge implements Runnable
    {
        final int generation;
        boolean isScheduled = false;

        Merge(int generation)
        {
            this.generation = generation;
        }

        public void run()
        {
            try
            {
                Preconditions.checkState(!dependsOnActiveFlushes());
                Data current = data;
                SchemaElement name = tableName(tableId);
                UncommittedDataFile.Writer writer = writer(directory, name.elementKeyspace(), name.elementName(), tableId, generation);
                Set<UncommittedDataFile> files = Sets.newHashSet(Iterables.filter(current.files, u -> u.generation() < generation));
                logger.info("merging {} paxos uncommitted files into a new generation {} file for {}.{}", files.size(), generation, keyspace(), table());
                try (CloseableIterator<PaxosKeyState> iterator = filterFactory.filter(merge(files, FULL_RANGE)))
                {
                    while (iterator.hasNext())
                    {
                        PaxosKeyState next = iterator.next();

                        if (next.committed)
                            continue;

                        writer.append(next);
                    }
                    mergeComplete(this, writer.finish());
                }
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }

        void maybeSchedule()
        {
            if (isScheduled)
                return;

            if (dependsOnActiveFlushes())
                return;

            executor.submit(merge);
            merge.isScheduled = true;
        }

        boolean dependsOnActiveFlushes()
        {
            return !activeFlushes.headSet(generation).isEmpty();
        }
    }

    private final File directory;
    private final TableId tableId;
    private final FilterFactory filterFactory;

    private volatile Data data;
    private volatile Merge merge;
    private volatile boolean rebuilding = false;

    private int nextGeneration;
    private final NavigableSet<Integer> activeFlushes = new ConcurrentSkipListSet<>();

    private UncommittedTableData(File directory, TableId tableId, FilterFactory filterFactory, Data data)
    {
        this.directory = directory;
        this.tableId = tableId;
        this.filterFactory = filterFactory;
        this.data = data;
        this.nextGeneration = 1 + (int) data.files.stream().mapToLong(UncommittedDataFile::generation).max().orElse(-1);
    }

    static UncommittedTableData load(File directory, TableId tableId, FilterFactory flushFilterFactory)
    {
        Preconditions.checkArgument(directory.exists());
        Preconditions.checkArgument(directory.isDirectory());
        Preconditions.checkNotNull(tableId);

        String[] fnames = directory.tryListNames();
        Preconditions.checkArgument(fnames != null);

        Pattern pattern = UncommittedDataFile.fileRegexFor(tableId);
        Set<Long> generations = new HashSet<>();
        List<UncommittedDataFile> files = new ArrayList<>();
        for (String fname : fnames)
        {
            Matcher matcher = pattern.matcher(fname);
            if (!matcher.matches())
                continue;

            File file = new File(directory, fname);
            if (isTmpFile(fname))
            {
                logger.info("deleting left over uncommitted paxos temp file {} for tableId {}", file, tableId);
                file.delete();
                continue;
            }

            if (isCrcFile(fname))
                continue;

            File crcFile = new File(directory, UncommittedDataFile.crcName(fname));
            if (!crcFile.exists())
                throw new FSReadError(new IOException(String.format("%s does not have a corresponding crc file", file)), crcFile);
            long generation = Long.parseLong(matcher.group(1));
            files.add(UncommittedDataFile.create(tableId, file, crcFile, generation));
            generations.add(generation);
        }

        // cleanup orphaned crc files
        for (String fname : fnames)
        {
            if (!isCrcFile(fname))
                continue;

            Matcher matcher = pattern.matcher(fname);
            if (!matcher.matches())
                continue;

            long generation = Long.parseLong(matcher.group(1));
            if (!generations.contains(generation))
            {
                File file = new File(directory, fname);
                logger.info("deleting left over uncommitted paxos crc file {} for tableId {}", file, tableId);
                file.delete();
            }
        }

        return new UncommittedTableData(directory, tableId, flushFilterFactory, new Data(ImmutableSet.copyOf(files)));
    }

    static UncommittedTableData load(File directory, TableId tableId)
    {
        return load(directory, tableId, new CFSFilterFactory(tableId));
    }

    static Set<TableId> listTableIds(File directory)
    {
        Preconditions.checkArgument(directory.isDirectory());
        return UncommittedDataFile.listTableIds(directory);
    }

    private static SchemaElement tableName(TableId tableId)
    {
        TableMetadata name = Schema.instance.getTableMetadata(tableId);
        return name != null ? name : UNKNOWN_TABLE;
    }

    int numFiles()
    {
        return data.files.size();
    }

    TableId tableId()
    {
        return tableId;
    }

    public String keyspace()
    {
        return tableName(tableId).elementKeyspace();
    }

    public String table()
    {
        return tableName(tableId).elementName();
    }

    /**
     * Return an iterator of the file contents for the given token ranges. Token ranges
     * must be normalized
     */
    synchronized CloseableIterator<PaxosKeyState> iterator(Collection<Range<Token>> ranges)
    {
        // we don't wait for pending flushes because flushing memtable data is added in PaxosUncommittedIndex
        Preconditions.checkArgument(elementsEqual(Range.normalize(ranges), ranges));
        return filterFactory.filter(merge(data.files, ranges));
    }

    private void flushTerminated(int generation)
    {
        activeFlushes.remove(generation);
        if (merge != null)
            merge.maybeSchedule();
    }

    private synchronized void flushSuccess(int generation, UncommittedDataFile newFile)
    {
        assert newFile == null || generation == newFile.generation();
        if (newFile != null)
            data = data.withFile(newFile);
        flushTerminated(generation);
    }

    private synchronized void flushAborted(int generation)
    {
        flushTerminated(generation);
    }

    private synchronized void mergeComplete(Merge merge, UncommittedDataFile newFile)
    {
        Preconditions.checkArgument(this.merge == merge);
        ImmutableSet.Builder<UncommittedDataFile> files = ImmutableSet.builder();
        files.add(newFile);
        for (UncommittedDataFile file : data.files)
        {
            if (file.generation() > merge.generation)
                files.add(file);
            else
                file.markDeleted();
        }

        data = new Data(files.build());
        this.merge = null;
        logger.info("paxos uncommitted merge completed for {}.{}, new generation {} file added", keyspace(), table(), newFile.generation());
    }

    synchronized FlushWriter flushWriter() throws IOException
    {
        int generation = nextGeneration++;
        UncommittedDataFile.Writer writer = writer(directory, keyspace(), table(), tableId, generation);
        activeFlushes.add(generation);
        logger.info("flushing generation {} uncommitted paxos file for {}.{}", generation, keyspace(), table());

        return new FlushWriter()
        {
            public void append(PaxosKeyState commitState) throws IOException
            {
                writer.append(commitState);
            }

            public void finish()
            {
                flushSuccess(generation, writer.finish());
            }

            public Throwable abort(Throwable accumulate)
            {
                accumulate = writer.abort(accumulate);
                flushAborted(generation);
                return accumulate;
            }
        };
    }

    private synchronized void rebuildComplete(UncommittedDataFile file)
    {
        Preconditions.checkState(rebuilding);
        Preconditions.checkState(!hasInProgressIO());
        Preconditions.checkState(data.files.isEmpty());

        data = new Data(ImmutableSet.of(file));
        logger.info("paxos rebuild completed for {}.{}", keyspace(), table());
        rebuilding = false;
    }

    synchronized FlushWriter rebuildWriter() throws IOException
    {
        Preconditions.checkState(!rebuilding);
        Preconditions.checkState(nextGeneration == 0);
        Preconditions.checkState(!hasInProgressIO());
        rebuilding = true;
        int generation = nextGeneration++;
        UncommittedDataFile.Writer writer = writer(directory, keyspace(), table(), tableId, generation);

        return new FlushWriter()
        {
            public void append(PaxosKeyState commitState) throws IOException
            {
                if (commitState.committed)
                    return;

                writer.append(commitState);
            }

            public void finish()
            {
                rebuildComplete(writer.finish());
            }

            public Throwable abort(Throwable accumulate)
            {
                accumulate = writer.abort(accumulate);
                logger.info("paxos rebuild aborted for {}.{}", keyspace(), table());
                rebuilding = false;
                return accumulate;
            }
        };
    }

    synchronized void maybeScheduleMerge()
    {
        logger.info("Scheduling uncommitted paxos data merge task for {}.{}", keyspace(), table());
        if (data.files.size() < 2 || merge != null)
            return;

        createMergeTask().maybeSchedule();
    }

    @VisibleForTesting
    synchronized Merge createMergeTask()
    {
        Preconditions.checkState(merge == null);
        merge = new Merge(nextGeneration++);
        return merge;
    }

    synchronized boolean hasInProgressIO()
    {
        return merge != null || !activeFlushes.isEmpty();
    }

    void truncate()
    {
        logger.info("truncating uncommitting paxos date for {}.{}", keyspace(), table());
        data.truncate();
        data = new Data(ImmutableSet.of());
    }

    @VisibleForTesting
    Data data()
    {
        return data;
    }

    @VisibleForTesting
    long nextGeneration()
    {
        return nextGeneration;
    }

    @VisibleForTesting
    Merge currentMerge()
    {
        return merge;
    }

    public static void shutdownAndWait(long timeout, TimeUnit units) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownAndWait(timeout, units, executor);
    }
}
