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
import java.util.*;

import com.google.common.base.Throwables;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.UUIDGen;

public class Upgrader
{
    private final ColumnFamilyStore cfs;
    private final SSTableReader sstable;
    private final LifecycleTransaction transaction;
    private final File directory;

    private final OperationType compactionType = OperationType.UPGRADE_SSTABLES;
    private final CompactionController controller;
    private final AbstractCompactionStrategy strategy;
    private final long estimatedRows;

    private final OutputHandler outputHandler;

    public Upgrader(ColumnFamilyStore cfs, LifecycleTransaction txn, OutputHandler outputHandler)
    {
        this.cfs = cfs;
        this.transaction = txn;
        this.sstable = txn.onlyOne();
        this.outputHandler = outputHandler;

        this.directory = new File(sstable.getFilename()).getParentFile();

        this.controller = new UpgradeController(cfs);

        this.strategy = cfs.getCompactionStrategy();
        long estimatedTotalKeys = Math.max(cfs.metadata.getMinIndexInterval(), SSTableReader.getApproximateKeyCount(Arrays.asList(this.sstable)));
        long estimatedSSTables = Math.max(1, SSTableReader.getTotalBytes(Arrays.asList(this.sstable)) / strategy.getMaxSSTableBytes());
        this.estimatedRows = (long) Math.ceil((double) estimatedTotalKeys / estimatedSSTables);
    }

    private SSTableWriter createCompactionWriter(long repairedAt)
    {
        MetadataCollector sstableMetadataCollector = new MetadataCollector(cfs.getComparator());

        // Get the max timestamp of the precompacted sstables
        // and adds generation of live ancestors
        sstableMetadataCollector.addAncestor(sstable.descriptor.generation);
        for (Integer i : sstable.getAncestors())
        {
            if (new File(sstable.descriptor.withGeneration(i).filenameFor(Component.DATA)).exists())
                sstableMetadataCollector.addAncestor(i);
        }
        sstableMetadataCollector.sstableLevel(sstable.getSSTableLevel());
        return SSTableWriter.create(Descriptor.fromFilename(cfs.getTempSSTablePath(directory)), estimatedRows, repairedAt, cfs.metadata, cfs.partitioner, sstableMetadataCollector);
    }

    public void upgrade()
    {
        outputHandler.output("Upgrading " + sstable);

        try (SSTableRewriter writer = new SSTableRewriter(cfs, transaction, CompactionTask.getMaxDataAge(transaction.originals()), true);
             AbstractCompactionStrategy.ScannerList scanners = strategy.getScanners(transaction.originals());
             CloseableIterator<AbstractCompactedRow> iter = new CompactionIterable(compactionType, scanners.scanners, controller, DatabaseDescriptor.getSSTableFormat(), UUIDGen.getTimeUUID()).iterator())
        {
            writer.switchWriter(createCompactionWriter(sstable.getSSTableMetadata().repairedAt));
            while (iter.hasNext())
            {
                @SuppressWarnings("resource")
                AbstractCompactedRow row = iter.next();
                writer.append(row);
            }

            writer.finish();
            outputHandler.output("Upgrade of " + sstable + " complete.");
        }
        catch (Exception e)
        {
            Throwables.propagate(e);
        }
        finally
        {
            controller.close();
        }
    }

    private static class UpgradeController extends CompactionController
    {
        public UpgradeController(ColumnFamilyStore cfs)
        {
            super(cfs, Integer.MAX_VALUE);
        }

        @Override
        public long maxPurgeableTimestamp(DecoratedKey key)
        {
            return Long.MIN_VALUE;
        }
    }
}

