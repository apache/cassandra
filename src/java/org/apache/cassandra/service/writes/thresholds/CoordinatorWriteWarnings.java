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

import java.util.Map;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.thresholds.CoordinatorWarningsState;

public class CoordinatorWriteWarnings
{
    private static final Logger logger = LoggerFactory.getLogger(CoordinatorWriteWarnings.class);

    private static final Warnings INIT = new Warnings();
    private static final Warnings EMPTY = new Warnings();

    private static final CoordinatorWarningsState<Warnings> STATE =
    new CoordinatorWarningsState<>("CoordinatorWriteWarnings",
                                   INIT,
                                   EMPTY,
                                   Warnings::new,
                                   () -> EMPTY,
                                   logger,
                                   false);

    /**
     * Initialize coordinator write warnings for this thread. Must be called at the start of a client request.
     */
    public static void init()
    {
        STATE.init();
    }

    /**
     * Update warnings for a partition after receiving responses from replicas.
     *
     * @param mutation the mutation that was written
     * @param snapshot the aggregated warnings from replicas
     */
    public static void update(IMutation mutation, WriteWarningsSnapshot snapshot)
    {
        if (snapshot.isEmpty()) return;

        Warnings warnings = STATE.mutable();
        if (warnings == EMPTY) return;

        warnings.merge(mutation.key(), snapshot);
    }

    /**
     * Process accumulated warnings: send to client and update metrics.
     * Must be called at the end of a client request.
     */
    public static void done()
    {
        STATE.processAndReset(CoordinatorWriteWarnings::processWarnings);
    }

    /**
     * Reset/clear warnings for this thread.
     */
    public static void reset()
    {
        STATE.reset();
    }

    private static void processWarnings(Warnings warnings)
    {
        if (warnings.snapshot == null || warnings.snapshot.isEmpty() || warnings.partitionKey == null)
            return;

        WriteWarningsSnapshot snapshot = warnings.snapshot;

        for (Map.Entry<TableId, Long> entry : snapshot.writeSize.tableValues.entrySet())
        {
            TableId tableId = entry.getKey();
            ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(tableId);
            if (cfs == null)
            {
                logger.warn("ColumnFamilyStore is null for table {}, skipping", tableId);
                continue;
            }

            TableMetadata metadata = cfs.metadata();
            String partitionKey = metadata.partitionKeyType.toCQLString(warnings.partitionKey.getKey());
            String msg = String.format("Write to %s.%s partition %s: %s",
                                       metadata.keyspace,
                                       metadata.name,
                                       partitionKey,
                                       WriteWarningsSnapshot.writeSizeWarnMessage(entry.getValue()));
            ClientWarn.instance.warn(msg);
            logger.warn(msg);
            cfs.metric.writeSizeWarnings.mark();
        }

        for (Map.Entry<TableId, Long> entry : snapshot.writeTombstone.tableValues.entrySet())
        {
            TableId tableId = entry.getKey();
            ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(tableId);
            if (cfs == null)
            {
                logger.warn("ColumnFamilyStore is null for table {}, skipping", tableId);
                continue;
            }

            TableMetadata metadata = cfs.metadata();
            String partitionKey = metadata.partitionKeyType.toCQLString(warnings.partitionKey.getKey());
            String msg = String.format("Write to %s.%s partition %s: %s",
                                       metadata.keyspace,
                                       metadata.name,
                                       partitionKey,
                                       WriteWarningsSnapshot.writeTombstoneWarnMessage(entry.getValue()));
            ClientWarn.instance.warn(msg);
            logger.warn(msg);
            cfs.metric.writeTombstoneWarnings.mark();
        }
    }

    /**
     * Internal state holder for accumulated warnings.
     * A Mutation is always for a single partition key, so we store it once
     * and track warnings per table within that partition.
     */
    private static class Warnings
    {
        @Nullable
        DecoratedKey partitionKey;

        @Nullable
        WriteWarningsSnapshot snapshot;

        void merge(DecoratedKey partitionKey, WriteWarningsSnapshot snapshot)
        {
            if (this.partitionKey == null)
                this.partitionKey = partitionKey;

            this.snapshot = this.snapshot == null ? snapshot : this.snapshot.merge(snapshot);
        }
    }
}
