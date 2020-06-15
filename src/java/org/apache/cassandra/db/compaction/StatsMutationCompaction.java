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
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.UnaryOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.repair.PendingAntiCompaction;
import org.apache.cassandra.db.streaming.ComponentManifest;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.concurrent.Refs;

/**
 * Currently, entire-sstable-streaming requires sstable components to be immutable, because {@link ComponentManifest}
 * with component sizes are sent before sending actual files. If {@link PendingAntiCompaction} or {@link PendingRepairManager}
 * or {@link SingleSSTableLCSTask} mutates stats directly on existing live sstables, the stream receiver will report
 * checksum validation failure as the size recorded in {@link ComponentManifest} is different from actual file.
 *
 * {@link StatsMutationCompaction} will make sure stats metadata remain immutable as it applied stats mutation on newly
 * copied stats file with all other components harklinked.
 */
public class StatsMutationCompaction
{
    private static final Logger logger = LoggerFactory.getLogger(StatsMutationCompaction.class);

    /**
     * Mutate stats metadata on the given sstables and make sure old sstable components remain immutable. This method
     * creates hardlinks on the existing sstable components with a new descriptor on the same disk except for stats
     * metadata file which is copied entirely and muated by given transformation.
     *
     * Note: transaction will be completed or aborted when method call completes.
     *
     * @param cfs table of sstable to be mutated
     * @param txn lifecycle transaction used to track newly added sstables and obsolete old sstables.
     * @param transform transformation to be applied on the stats metadata
     */
    public static void performStatsMutationCompaction(ColumnFamilyStore cfs, LifecycleTransaction txn, UnaryOperator<StatsMetadata> transform) throws IOException
    {
        Collection<SSTableReader> originals = txn.originals();
        try (Refs<SSTableReader> ignored = Refs.ref(originals))
        {

            // 1. create hardlink on all sstable components with new descriptor on the same disk
            Set<LinkedSSTable> linkedSSTables = new HashSet<>();
            for (SSTableReader sstable : originals)
            {
                Descriptor prev = sstable.descriptor;
                Descriptor desc = cfs.getUniqueDescriptorFor(prev, prev.directory);
                Set<Component> components = sstable.components();
                LinkedSSTable linked = new LinkedSSTable(desc, components, cfs.metadata, txn);

                for (Component component : components)
                {
                    File from = prev.fileFor(component);
                    if (!from.exists())
                        continue;

                    File to = desc.fileFor(component);
                    assert from.getParentFile().getCanonicalPath().equals(to.getParentFile().getCanonicalPath());

                    FileUtils.createHardLink(from, to);
                }
                linkedSSTables.add(linked);
            }


            // 2. mutate newly created stats metadata
            for (LinkedSSTable linked : linkedSSTables)
            {
                Descriptor desc = linked.descriptor;
                desc.getMetadataSerializer().mutate(desc, transform);

                SSTableReader reader = SSTableReader.open(desc, linked.components(), cfs.metadata);
                txn.update(reader, false);
            }
            cfs.metric.bytesMutatedAnticompaction.inc(SSTableReader.getTotalBytes(originals));

            // 3. replace original sstables with newly created sstables
            for (SSTableReader original : originals)
                txn.obsolete(original);

            txn.finish();
        }
        catch (Throwable t)
        {
            logger.error("Failed to mutate STATS metadata: {}", t.getMessage());
            throw t;
        }
    }

    /**
     * Must track new sstable before they are created
     */
    private static class LinkedSSTable extends SSTable
    {
        LinkedSSTable(Descriptor descriptor, Set<Component> components, TableMetadataRef metadata, LifecycleNewTracker transaction)
        {
            super(descriptor, components, metadata, DatabaseDescriptor.getDiskOptimizationStrategy());
            transaction.trackNew(this);
        }
    }
}
