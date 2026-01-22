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
package org.apache.cassandra.config;

import java.util.ArrayList;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.sstable.format.SSTableFormat;

import static org.apache.cassandra.utils.Throwables.maybeFail;
import static org.apache.cassandra.utils.Throwables.merge;

/**
 * Test utilities for DatabaseDescriptor that require server-side classes.
 * These methods are separated from DatabaseDescriptor to avoid loading
 * server classes (like CompactionManager) during client initialization.
 */
public class TestDatabaseDescriptor
{
    /**
     * Sets the global SSTable format after safely pausing all compactions.
     * <p>
     * This method is named "Unsafe" to signal that it modifies global state
     * that affects all tables across the entire Cassandra instance.
     * The implementation is actually safe due to the compaction pausing logic.
     *
     * @param name the SSTable format name (e.g., "big" or "bti")
     */
    public static void setUnsafeSelectedSSTableFormat(String name)
    {
        SSTableFormat<?, ?> format = DatabaseDescriptor.getSSTableFormats().get(name);
        if (format == null)
            throw new IllegalArgumentException("Unknown sstable format: " + name);
        setUnsafeSelectedSSTableFormat(format);
    }

    /**
     * Sets the global SSTable format after safely pausing all compactions.
     * <p>
     * This method:
     * 1. Pauses global compactions
     * 2. Pauses all table compaction strategies
     * 3. Waits for in-flight compactions to complete
     * 4. Changes the SSTable format
     * 5. Resumes compactions
     *
     * @param format the SSTable format to set
     */
    public static void setUnsafeSelectedSSTableFormat(SSTableFormat<?, ?> format)
    {
        // Get all CFSs across all keyspaces since SSTable format is global
        Iterable<ColumnFamilyStore> allCfs = ColumnFamilyStore.all();

        // Pause both global compactions and all table compaction strategies
        // This prevents NEW compactions from starting
        try (CompactionManager.CompactionPauser globalPause = CompactionManager.instance.pauseGlobalCompaction();
             CompactionManager.CompactionPauser strategiesPause = pauseAllCompactionStrategies(allCfs))
        {
            // Wait for all existing in-flight compactions to complete naturally (don't interrupt)
            // Uses 1-minute timeout per waitForCessation implementation
            CompactionManager.instance.waitForCessation(allCfs, sstable -> true);

            // Now safe to change the global SSTable format
            DatabaseDescriptor.setSelectedSSTableFormat(format);
        }
        // Compactions auto-resume when pausers are closed
    }

    /**
     * Pauses compaction strategies for all given ColumnFamilyStores.
     * Pattern matches {@link ColumnFamilyStore#pauseCompactionStrategies}.
     */
    private static CompactionManager.CompactionPauser pauseAllCompactionStrategies(Iterable<ColumnFamilyStore> toPause)
    {
        ArrayList<ColumnFamilyStore> successfullyPaused = new ArrayList<>();
        try
        {
            for (ColumnFamilyStore cfs : toPause)
            {
                successfullyPaused.ensureCapacity(successfullyPaused.size() + 1); // to avoid OOM after pausing the strategies
                cfs.getCompactionStrategyManager().pause();
                successfullyPaused.add(cfs);
            }
            return () -> maybeFail(resumeAll(null, toPause));
        }
        catch (Throwable t)
        {
            resumeAll(t, successfullyPaused);
            throw t;
        }
    }

    private static Throwable resumeAll(Throwable accumulate, Iterable<ColumnFamilyStore> cfss)
    {
        for (ColumnFamilyStore cfs : cfss)
        {
            try
            {
                cfs.getCompactionStrategyManager().resume();
            }
            catch (Throwable t)
            {
                accumulate = merge(accumulate, t);
            }
        }
        return accumulate;
    }
}
