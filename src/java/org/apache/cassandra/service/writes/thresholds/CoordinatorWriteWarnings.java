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

package org.apache.cassandra.service.writes.thresholds;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.utils.Pair;

import io.netty.util.concurrent.FastThreadLocal;

/**
 * ThreadLocal manager for write warnings at the coordinator.
 * Accumulates warnings from multiple write operations in a single client request,
 * then sends them to the client and updates metrics.
 */
public class CoordinatorWriteWarnings
{
    private static final Logger logger = LoggerFactory.getLogger(CoordinatorWriteWarnings.class);

    private static final Warnings INIT = new Warnings();
    private static final Warnings EMPTY = new Warnings();

    private static final FastThreadLocal<Warnings> STATE = new FastThreadLocal<>();

    /**
     * Initialize coordinator write warnings for this thread.
     * Must be called at the start of a client request.
     */
    public static void init()
    {
        STATE.set(INIT);
    }

    /**
     * Update warnings for a partition after receiving responses from replicas.
     *
     * @param mutation the mutation that was written
     * @param snapshot the aggregated warnings from replicas
     */
    public static void update(IMutation mutation, WriteWarningsSnapshot snapshot)
    {
        if (snapshot.isEmpty())
            return;

        Warnings warnings = STATE.get();
        if (warnings == null || warnings == EMPTY)
            return;

        for (PartitionUpdate update : mutation.getPartitionUpdates())
        {
            Pair<TableId, DecoratedKey> key = Pair.create(update.metadata().id, update.partitionKey());
            if (warnings == INIT)
            {
                warnings = new Warnings();
                STATE.set(warnings);
            }
            warnings.merge(key, snapshot);
        }
    }

    /**
     * Process accumulated warnings: send to client and update metrics.
     * Must be called at the end of a client request.
     */
    public static void done()
    {
        try
        {
            Warnings warnings = STATE.get();
            if (warnings == null || warnings == INIT || warnings.partitions.isEmpty())
                return;

            for (Map.Entry<Pair<TableId, DecoratedKey>, WriteWarningsSnapshot> entry : warnings.partitions.entrySet())
            {
                Pair<TableId, DecoratedKey> key = entry.getKey();
                WriteWarningsSnapshot snapshot = entry.getValue();

                ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(key.left);
                if (cfs == null)
                    continue;

                TableMetadata metadata = cfs.metadata();
                String partitionKey = metadata.partitionKeyType.getString(key.right.getKey());

                sendWarnings(metadata, partitionKey, snapshot);

                updateMetrics(cfs, snapshot);
            }
        }
        catch (Exception e)
        {
            logger.error("Error processing write warnings", e);
        }
        finally
        {
            STATE.set(EMPTY);
        }
    }

    /**
     * Reset/clear warnings for this thread.
     */
    public static void reset()
    {
        STATE.set(EMPTY);
    }

    private static void sendWarnings(TableMetadata metadata, String partitionKey, WriteWarningsSnapshot snapshot)
    {
        if (!snapshot.writeSize.instances.isEmpty())
        {
            String msg = String.format("Write to %s.%s partition %s: %s",
                                       metadata.keyspace,
                                       metadata.name,
                                       partitionKey,
                                       WriteWarningsSnapshot.writeSizeWarnMessage(
                                       snapshot.writeSize.instances.size(),
                                       snapshot.writeSize.maxValue));
            ClientWarn.instance.warn(msg);
        }

        if (!snapshot.writeTombstone.instances.isEmpty())
        {
            String msg = String.format("Write to %s.%s partition %s: %s",
                                       metadata.keyspace,
                                       metadata.name,
                                       partitionKey,
                                       WriteWarningsSnapshot.writeTombstoneWarnMessage(
                                       snapshot.writeTombstone.instances.size(),
                                       snapshot.writeTombstone.maxValue));
            ClientWarn.instance.warn(msg);
        }
    }

    private static void updateMetrics(ColumnFamilyStore cfs, WriteWarningsSnapshot snapshot)
    {
        if (!snapshot.writeSize.instances.isEmpty())
        {
            cfs.metric.writeSizeWarnings.mark();
            cfs.metric.writeSize.update(snapshot.writeSize.maxValue);
        }

        if (!snapshot.writeTombstone.instances.isEmpty())
        {
            cfs.metric.writeTombstoneWarnings.mark();
        }
    }

    /**
     * Internal state holder for accumulated warnings.
     */
    private static class Warnings
    {
        @Nullable
        Map<Pair<TableId, DecoratedKey>, WriteWarningsSnapshot> partitions;

        void merge(Pair<TableId, DecoratedKey> key, WriteWarningsSnapshot snapshot)
        {
            if (partitions == null)
                partitions = new HashMap<>();

            partitions.merge(key, snapshot, WriteWarningsSnapshot::merge);
        }
    }
}
