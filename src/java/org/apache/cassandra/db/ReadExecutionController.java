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

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.utils.MonotonicClock.Global.preciseTime;

public class ReadExecutionController implements AutoCloseable
{
    private static final long NO_SAMPLING = Long.MIN_VALUE;

    // For every reads
    private final OpOrder.Group baseOp;
    private final TableMetadata baseMetadata; // kept to sanity check that we have take the op order on the right table

    // For index reads
    private final ReadExecutionController indexController;
    private final WriteContext writeContext;
    private final ReadCommand command;
    static MonotonicClock clock = preciseTime;

    private final long createdAtNanos; // Only used while sampling

    private final RepairedDataInfo repairedDataInfo;
    private long oldestUnrepairedTombstone = Long.MAX_VALUE;

    ReadExecutionController(ReadCommand command,
                            OpOrder.Group baseOp,
                            TableMetadata baseMetadata,
                            ReadExecutionController indexController,
                            WriteContext writeContext,
                            long createdAtNanos,
                            boolean trackRepairedStatus)
    {
        // We can have baseOp == null, but only when empty() is called, in which case the controller will never really be used
        // (which validForReadOn should ensure). But if it's not null, we should have the proper metadata too.
        assert (baseOp == null) == (baseMetadata == null);
        this.baseOp = baseOp;
        this.baseMetadata = baseMetadata;
        this.indexController = indexController;
        this.writeContext = writeContext;
        this.command = command;
        this.createdAtNanos = createdAtNanos;

        if (trackRepairedStatus)
        {
            DataLimits.Counter repairedReadCount = command.limits().newCounter(command.nowInSec(),
                                                                               false,
                                                                               command.selectsFullPartition(),
                                                                               metadata().enforceStrictLiveness()).onlyCount();
            repairedDataInfo = new RepairedDataInfo(repairedReadCount);
        }
        else
        {
            repairedDataInfo = RepairedDataInfo.NO_OP_REPAIRED_DATA_INFO;
        }
    }

    public boolean isRangeCommand()
    {
        return command != null && command.isRangeRequest();
    }

    public ReadExecutionController indexReadController()
    {
        return indexController;
    }

    public WriteContext getWriteContext()
    {
        return writeContext;
    }

    long oldestUnrepairedTombstone()
    {
        return oldestUnrepairedTombstone;
    }
    
    void updateMinOldestUnrepairedTombstone(long candidate)
    {
        oldestUnrepairedTombstone = Math.min(oldestUnrepairedTombstone, candidate);
    }

    boolean validForReadOn(ColumnFamilyStore cfs)
    {
        return baseOp != null && cfs.metadata.id.equals(baseMetadata.id);
    }

    public static ReadExecutionController empty()
    {
        return new ReadExecutionController(null, null, null, null, null, NO_SAMPLING, false);
    }

    /**
     * Creates an execution controller for the provided command.
     * <p>
     * Note: no code should use this method outside of {@link ReadCommand#executionController} (for
     * consistency sake) and you should use that latter method if you need an execution controller.
     *
     * @param command the command for which to create a controller.
     * @return the created execution controller, which must always be closed.
     */
    @SuppressWarnings("resource") // ops closed during controller close
    static ReadExecutionController forCommand(ReadCommand command, boolean trackRepairedStatus)
    {
        ColumnFamilyStore baseCfs = Keyspace.openAndGetStore(command.metadata());
        ColumnFamilyStore indexCfs = maybeGetIndexCfs(command);

        long createdAtNanos = baseCfs.metric.topLocalReadQueryTime.isEnabled() ? clock.now() : NO_SAMPLING;

        if (indexCfs == null)
            return new ReadExecutionController(command, baseCfs.readOrdering.start(), baseCfs.metadata(), null, null, createdAtNanos, trackRepairedStatus);

        OpOrder.Group baseOp = null;
        WriteContext writeContext = null;
        ReadExecutionController indexController = null;
        // OpOrder.start() shouldn't fail, but better safe than sorry.
        try
        {
            baseOp = baseCfs.readOrdering.start();
            indexController = new ReadExecutionController(command, indexCfs.readOrdering.start(), indexCfs.metadata(), null, null, NO_SAMPLING, false);
            /*
             * TODO: this should perhaps not open and maintain a writeOp for the full duration, but instead only *try*
             * to delete stale entries, without blocking if there's no room
             * as it stands, we open a writeOp and keep it open for the duration to ensure that should this CF get flushed to make room we don't block the reclamation of any room being made
             */
            writeContext = baseCfs.keyspace.getWriteHandler().createContextForRead();
            return new ReadExecutionController(command, baseOp, baseCfs.metadata(), indexController, writeContext, createdAtNanos, trackRepairedStatus);
        }
        catch (RuntimeException e)
        {
            // Note that must have writeContext == null since ReadOrderGroup ctor can't fail
            assert writeContext == null;
            try
            {
                if (baseOp != null)
                    baseOp.close();
            }
            finally
            {
                if (indexController != null)
                    indexController.close();
            }
            throw e;
        }
    }

    private static ColumnFamilyStore maybeGetIndexCfs(ReadCommand command)
    {
        Index.QueryPlan queryPlan = command.indexQueryPlan();
        if (queryPlan == null)
            return null;

        // only the index groups with a single member are allowed to have a backing table
        return queryPlan.getFirst().getBackingTable().orElse(null);
    }

    public TableMetadata metadata()
    {
        return baseMetadata;
    }

    public void close()
    {
        try
        {
            if (baseOp != null)
                baseOp.close();
        }
        finally
        {
            if (indexController != null)
            {
                try
                {
                    indexController.close();
                }
                finally
                {
                    writeContext.close();
                }
            }
        }

        if (createdAtNanos != NO_SAMPLING)
            addSample();
    }

    public boolean isTrackingRepairedStatus()
    {
        return repairedDataInfo != RepairedDataInfo.NO_OP_REPAIRED_DATA_INFO;
    }

    @VisibleForTesting
    public ByteBuffer getRepairedDataDigest()
    {
        return repairedDataInfo.getDigest();
    }

    @VisibleForTesting
    public boolean isRepairedDataDigestConclusive()
    {
        return repairedDataInfo.isConclusive();
    }
    
    public RepairedDataInfo getRepairedDataInfo()
    {
        return repairedDataInfo;
    }

    private void addSample()
    {
        String cql = command.toCQLString();
        int timeMicros = (int) Math.min(TimeUnit.NANOSECONDS.toMicros(clock.now() - createdAtNanos), Integer.MAX_VALUE);
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(baseMetadata.id);
        if (cfs != null)
            cfs.metric.topLocalReadQueryTime.addSample(cql, timeMicros);
    }
}
