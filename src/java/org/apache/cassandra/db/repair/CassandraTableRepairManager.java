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

package org.apache.cassandra.db.repair;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.function.Predicate;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.TopPartitionTracker;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.repair.TableRepairManager;
import org.apache.cassandra.repair.ValidationPartitionIterator;
import org.apache.cassandra.repair.NoSuchRepairSessionException;
import org.apache.cassandra.service.snapshot.SnapshotOptions;
import org.apache.cassandra.service.snapshot.SnapshotManager;
import org.apache.cassandra.service.snapshot.SnapshotType;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.service.ActiveRepairService;

public class CassandraTableRepairManager implements TableRepairManager
{
    private final ColumnFamilyStore cfs;
    private final SharedContext ctx;

    public CassandraTableRepairManager(ColumnFamilyStore cfs)
    {
        this(cfs, SharedContext.Global.instance);
    }

    public CassandraTableRepairManager(ColumnFamilyStore cfs, SharedContext ctx)
    {
        this.cfs = cfs;
        this.ctx = ctx;
    }

    @Override
    public ValidationPartitionIterator getValidationIterator(Collection<Range<Token>> ranges, TimeUUID parentId, TimeUUID sessionID, boolean isIncremental, long nowInSec, boolean dontPurgeTombstones, TopPartitionTracker.Collector topPartitionCollector) throws IOException, NoSuchRepairSessionException
    {
        return new CassandraValidationIterator(cfs, ctx, ranges, parentId, sessionID, isIncremental, nowInSec, dontPurgeTombstones, topPartitionCollector);
    }

    @Override
    public Future<?> submitValidation(Callable<Object> validation)
    {
        return CompactionManager.instance.submitValidation(validation);
    }

    @Override
    public void incrementalSessionCompleted(TimeUUID sessionID)
    {
        CompactionManager.instance.submitBackground(cfs);
    }

    @Override
    public synchronized void snapshot(String name, Collection<Range<Token>> ranges, boolean force)
    {
        try
        {
            ActiveRepairService.instance().snapshotExecutor.submit(() -> {
                if (force || !SnapshotManager.instance.exists(cfs.getKeyspaceName(), cfs.getTableName(), name))
                {
                    Predicate<SSTableReader> predicate = sstable -> sstable != null &&
                                                                    !sstable.metadata().isIndex() && // exclude SSTables from 2i
                                                                    new Bounds<>(sstable.getFirst().getToken(), sstable.getLast().getToken()).intersects(ranges);

                    SnapshotOptions options = SnapshotOptions.systemSnapshot(name, SnapshotType.REPAIR, predicate, cfs.getKeyspaceTableName())
                                                             .ephemeral()
                                                             .build();
                    SnapshotManager.instance.takeSnapshot(options);
                }
            }).get();
        }
        catch (Exception ex)
        {
            throw new RuntimeException(String.format("Unable to take a snapshot %s on %s.%s", name, cfs.metadata.keyspace, cfs.metadata.name), ex);
        }

    }
}
