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

import java.io.File;
import java.io.IOException;
import java.util.*;

import com.google.common.base.Throwables;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.OutputHandler;

public class Upgrader
{
    private final ColumnFamilyStore cfs;
    private final SSTableReader sstable;
    private final Collection<SSTableReader> toUpgrade;
    private final File directory;

    private final OperationType compactionType = OperationType.UPGRADE_SSTABLES;
    private final CompactionController controller;
    private final AbstractCompactionStrategy strategy;
    private final long estimatedRows;

    private final OutputHandler outputHandler;

    public Upgrader(ColumnFamilyStore cfs, SSTableReader sstable, OutputHandler outputHandler)
    {
        this.cfs = cfs;
        this.sstable = sstable;
        this.toUpgrade = Collections.singletonList(sstable);
        this.outputHandler = outputHandler;

        this.directory = new File(sstable.getFilename()).getParentFile();

        this.controller = new UpgradeController(cfs);

        this.strategy = cfs.getCompactionStrategy();
        long estimatedTotalKeys = Math.max(cfs.metadata.getIndexInterval(), SSTableReader.getApproximateKeyCount(toUpgrade, cfs.metadata));
        long estimatedSSTables = Math.max(1, SSTable.getTotalBytes(this.toUpgrade) / strategy.getMaxSSTableBytes());
        this.estimatedRows = (long) Math.ceil((double) estimatedTotalKeys / estimatedSSTables);
    }

    private SSTableWriter createCompactionWriter()
    {
        SSTableMetadata.Collector sstableMetadataCollector = SSTableMetadata.createCollector(cfs.getComparator());

        // Get the max timestamp of the precompacted sstables
        // and adds generation of live ancestors
        // -- note that we always only have one SSTable in toUpgrade here:
        for (SSTableReader sstable : toUpgrade)
        {
            sstableMetadataCollector.addAncestor(sstable.descriptor.generation);
            for (Integer i : sstable.getAncestors())
            {
                if (new File(sstable.descriptor.withGeneration(i).filenameFor(Component.DATA)).exists())
                    sstableMetadataCollector.addAncestor(i);
            }
            sstableMetadataCollector.sstableLevel(sstable.getSSTableLevel());
        }

        return new SSTableWriter(cfs.getTempSSTablePath(directory), estimatedRows, cfs.metadata, cfs.partitioner, sstableMetadataCollector);
    }

    public void upgrade()
    {
        outputHandler.output("Upgrading " + sstable);


        AbstractCompactionIterable ci = new CompactionIterable(compactionType, strategy.getScanners(this.toUpgrade), controller);

        CloseableIterator<AbstractCompactedRow> iter = ci.iterator();

        Collection<SSTableReader> sstables = new ArrayList<SSTableReader>();
        Collection<SSTableWriter> writers = new ArrayList<SSTableWriter>();

        try
        {
            SSTableWriter writer = createCompactionWriter();
            writers.add(writer);
            while (iter.hasNext())
            {
                AbstractCompactedRow row = iter.next();

                writer.append(row);
            }

            long maxAge = CompactionTask.getMaxDataAge(this.toUpgrade);
            for (SSTableWriter completedWriter : writers)
                sstables.add(completedWriter.closeAndOpenReader(maxAge));

            outputHandler.output("Upgrade of " + sstable + " complete.");

        }
        catch (Throwable t)
        {
            for (SSTableWriter writer : writers)
                writer.abort();
            // also remove already completed SSTables
            for (SSTableReader sstable : sstables)
            {
                sstable.markObsolete();
                sstable.releaseReference();
            }
            throw Throwables.propagate(t);
        }
        finally
        {
            controller.close();

            try
            {
                iter.close();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    private static class UpgradeController extends CompactionController
    {
        public UpgradeController(ColumnFamilyStore cfs)
        {
            super(cfs, Integer.MAX_VALUE);
        }

        @Override
        public boolean shouldPurge(DecoratedKey key, long delTimestamp)
        {
            return false;
        }
    }
}

