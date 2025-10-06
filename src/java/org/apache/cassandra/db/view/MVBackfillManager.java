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

package org.apache.cassandra.db.view;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.System.nanoTime;

/**
 * Manager for materialized view backfill operations. This class follows the same pattern
 * as ValidationManager used in repair, but is designed specifically for MV backfill.
 * 
 * The manager coordinates the scanning of base table SSTables and translation of base
 * rows to materialized view rows. It handles the lifecycle of the backfill iterator
 * and provides progress tracking.
 */
public class MVBackfillManager
{
    private static final Logger logger = LoggerFactory.getLogger(MVBackfillManager.class);

    /**
     * State tracking for MV backfill operations, similar to ValidationState in repair.
     */
    public static class BackfillState
    {
        public volatile long partitionsProcessed = 0;
        public volatile long rowsProcessed = 0;
        public volatile long rowsSkipped = 0;
        public volatile long viewRowsGenerated = 0;
        public volatile long viewRowResultNull = 0;
        public volatile long bytesRead = 0;
        public volatile long estimatedTotalBytes = 0;
        public volatile long estimatedPartitions = 0;
        public volatile boolean completed = false;
        public volatile Exception failure = null;

        public void start(long estimatedPartitions, long estimatedBytes)
        {
            this.estimatedPartitions = estimatedPartitions;
            this.estimatedTotalBytes = estimatedBytes;
            logger.info("Starting MV backfill: estimated {} partitions, {} bytes", 
                       estimatedPartitions, FBUtilities.prettyPrintMemory(estimatedBytes));
        }

        public void updated(MVBackfillIterator iterator)
        {
            if ((partitionsProcessed > 0 && partitionsProcessed % 1000 == 0))
            {
                iterator.updateBytesRead();
                bytesRead = iterator.getBytesRead();
                double progress = estimatedTotalBytes > 0 ? (double) bytesRead / estimatedTotalBytes * 100 : 0;
                logger.info("MV backfill progress: {} partitions, {} rows, {} view rows, {:.1f}% ({}/{})",
                           partitionsProcessed, rowsProcessed, viewRowsGenerated, progress,
                           FBUtilities.prettyPrintMemory(bytesRead), 
                           FBUtilities.prettyPrintMemory(estimatedTotalBytes));
            }
        }

        public void complete()
        {
            this.completed = true;
            logger.info("MV backfill completed: {} partitions, {} rows, skipped {} rows, {} view rows generated, {} view rows are not matching the view filter.",
                       partitionsProcessed, rowsProcessed, rowsSkipped, viewRowsGenerated, viewRowResultNull);
        }

        public void fail(Exception e)
        {
            this.failure = e;
            this.completed = true;
            logger.error("MV backfill failed after processing {} partitions, {} rows", 
                        partitionsProcessed, rowsProcessed, e);
        }
    }

    /**
     * Sink interface for handling translated MV rows.
     * Implementations can write to SSTables, send mutations, etc.
     */
    public interface BackfillSink
    {
        /**
         * Process a translated view row. This method is called for each row
         * that successfully translates from the base table to the view.
         *
         * @param viewResult the translated view row result, or null if the row doesn't match the view
         */
        void processViewRow(ViewRowTranslator.ViewRowResult viewResult) throws Exception;

        /**
         * Process the translated ranges. This method is called for the ranges that have finished processing all the
         * base table rows in the given ranges
         * For example, for SSTable stream sink, this will stream the generated sstable to remote nodes.
         */
        void postRowProcess(Collection<Range<Token>> baseTableRanges) throws Exception;

        /**
         * Called when backfill processing for each row is complete.
         */
        default void rowProcessComplete() throws Exception {}

        /**
         * Called when backfill processing is complete.
         */
        default void complete() throws Exception {}

        /**
         * Called when backfill processing fails.
         */
        default void fail(Exception e) {}
    }

    private static MVBackfillIterator getBackfillIterator(ColumnFamilyStore baseCfs, 
                                                         Collection<Range<Token>> ranges, 
                                                         int nowInSec) throws IOException
    {
        return new MVBackfillIterator(baseCfs, ranges, nowInSec);
    }

    /**
     * Performs the MV backfill by scanning base table SSTables and translating rows to view rows.
     * This method is similar to doValidation in ValidationManager.
     */
    @SuppressWarnings("resource")
    private void doBackfill(ColumnFamilyStore baseCfs, 
                           View view, 
                           Collection<Range<Token>> ranges,
                           BackfillSink processor,
                           BackfillState state) throws IOException
    {
        // Check if the base table is still valid
        if (!baseCfs.isValid())
        {
            String message = String.format("Base table %s.%s is not valid", baseCfs.keyspace.getName(), baseCfs.name);
            logger.warn(message);
            state.fail(new IllegalStateException(message));
            return;
        }

        int nowInSec = FBUtilities.nowInSeconds();

        baseCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.VIEW_BUILD_STARTED);

        processBaseTableRangesByRow(baseCfs, view, ranges, processor, state, nowInSec);
        try
        {
            processor.postRowProcess(ranges);
            processor.complete();
        }
        catch (Exception e)
        {
            processor.fail(e);
            state.fail(e);
        }
        state.complete();
    }

    private void processBaseTableRangesByRow(ColumnFamilyStore baseCfs,
                                             View view,
                                             Collection<Range<Token>> ranges,
                                             BackfillSink processor,
                                             BackfillState state,
                                             int nowInSec)
    {
        long start = nanoTime();
        try (MVBackfillIterator iterator = getBackfillIterator(baseCfs, ranges, nowInSec))
        {
            if (iterator.isEmpty())
            {
                logger.info("No data to backfill for ranges {} in {}.{}",
                            ranges, baseCfs.keyspace.getName(), baseCfs.name);
                processor.rowProcessComplete();
                state.complete();
                return;
            }

            state.start(iterator.getEstimatedPartitions(), iterator.getEstimatedBytes());

            // Process each partition from the merged SSTable data
            while (iterator.hasNext())
            {
                try (UnfilteredRowIterator partition = iterator.next())
                {
                    DecoratedKey partitionKey = partition.partitionKey();
                    // Process each row in the partition
                    while (partition.hasNext())
                    {
                        Unfiltered unfiltered = partition.next();
                        // Skip range tombstones and other non-row entries
                        if (unfiltered.isRow())
                        {
                            Row baseRow = (Row) unfiltered;

                            // Skip dead rows (should be rare after compaction merge)
                            if (!baseRow.hasLiveData(nowInSec, baseCfs.metadata().enforceStrictLiveness()))
                            {
                                state.rowsSkipped++;
                                continue;
                            }

                            state.rowsProcessed++;
                            try
                            {
                                // Translate base row to view row
                                ViewRowTranslator.ViewRowResult viewResult = ViewRowTranslator.translateForBackfill(
                                view, baseRow, partitionKey, nowInSec);

                                if (viewResult != null)
                                {
                                    state.viewRowsGenerated++;
                                    // Process the result
                                    processor.processViewRow(viewResult);
                                }
                                else
                                {
                                    state.viewRowResultNull++;
                                }
                            }
                            catch (Exception e)
                            {
                                logger.error("Failed to process base row for MV backfill: partition={}, row={}",
                                             partitionKey, baseRow, e);
                                throw e;
                            }
                        }
                    }
                    state.partitionsProcessed++;
                    state.updated(iterator);
                }
            }
            processor.rowProcessComplete();
        }
        catch (Exception e)
        {
            processor.fail(e);
            state.fail(e);
        }
        finally
        {
            long duration = TimeUnit.NANOSECONDS.toMillis(nanoTime() - start);
            logger.debug("MV backfill of {} partitions (~{}) finished in {} msec for view {}.{}",
                         state.partitionsProcessed,
                         FBUtilities.prettyPrintMemory(state.estimatedTotalBytes),
                         duration,
                         view.getDefinition().keyspace(),
                         view.name);
        }
    }

    /**
     * Submits an MV backfill task for execution. Similar to submitValidation in ValidationManager.
     * 
     * @param baseCfs the base table column family store
     * @param view the materialized view to backfill
     * @param ranges the token ranges to process
     * @param processor the processor to handle translated view rows
     * @param state the state tracker for progress monitoring
     * @return a Future representing the backfill task
     */
    public Future<?> submitBackfill(ColumnFamilyStore baseCfs,
                                   View view,
                                   Collection<Range<Token>> ranges,
                                   BackfillSink processor,
                                   BackfillState state)
    {
        Callable<Object> backfill = new Callable<Object>()
        {
            public Object call() throws IOException
            {
                try
                {
                    doBackfill(baseCfs, view, ranges, processor, state);
                }
                catch (CompactionInterruptedException e)
                {
                    logger.warn("MV backfill interrupted: {}", e.getMessage());
                    state.fail(e);
                }
                catch (Throwable e)
                {
                    logger.error("MV backfill failed.", e);
                    state.fail(e instanceof Exception ? (Exception) e : new Exception(e));
                    throw e;
                }
                return this;
            }
        };

        // Submit to the same thread pool used for view build
        return CompactionManager.instance.submitViewBackfill(backfill);
    }

    /**
     * Convenience method to submit backfill for a single token range.
     */
    public Future<?> submitBackfill(ColumnFamilyStore baseCfs,
                                   View view,
                                   Range<Token> range,
                                   BackfillSink processor,
                                   BackfillState state)
    {
        return submitBackfill(baseCfs, view, java.util.Collections.singletonList(range), processor, state);
    }

}
