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

package org.apache.cassandra.service.replication.migration;

import java.util.Collection;

import com.google.common.util.concurrent.FutureCallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.RepairResult;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.transformations.AdvanceMutationTrackingMigration;

/**
 * Repair callback handler for mutation tracking migration.
 * Registered on repair coordinator to advance migration state on successful repairs.
 */
public class MutationTrackingRepairHandler
{
    private static final Logger logger = LoggerFactory.getLogger(MutationTrackingRepairHandler.class);

    public static final FutureCallback<RepairResult> completedRepairJobHandler =
        new FutureCallback<>()
        {
            @Override
            public void onSuccess(RepairResult repairResult)
            {
                try
                {
                    String keyspace = repairResult.desc.keyspace;
                    String tableName = repairResult.desc.columnFamily;
                    Collection<Range<Token>> repairedRanges = repairResult.desc.ranges;

                    ClusterMetadata clusterMetadata = ClusterMetadata.current();

                    // Check if keyspace is migrating
                    KeyspaceMigrationInfo migrationInfo = clusterMetadata.mutationTrackingMigrationState.getKeyspaceInfo(keyspace);

                    if (migrationInfo == null)
                    {
                        return;
                    }

                    // Get table metadata
                    TableMetadata tableMetadata = clusterMetadata.schema.getKeyspaceMetadata(keyspace).getTableOrViewNullable(tableName);

                    if (tableMetadata == null)
                    {
                        logger.warn("Repair completed for unknown table {}.{}, cannot advance migration",
                                   keyspace, tableName);
                        return;
                    }

                    if (migrationInfo.getPendingRangesForTable(tableMetadata.id).isEmpty())
                    {
                        // Table already fully migrated
                        return;
                    }

                    // Epoch eligibility check: Only count repairs started after the migration started
                    if (repairResult.mutationTrackingMigrationRepairResult.minEpoch.isBefore(migrationInfo.startedAtEpoch))
                    {
                        logger.debug("Repair completed for {}.{} but current epoch {} is before migration start epoch {}, ignoring",
                                    keyspace, tableName, clusterMetadata.epoch, migrationInfo.startedAtEpoch);
                        return;
                    }

                    if (!repairResult.mutationTrackingMigrationRepairResult.eligible)
                    {
                        logger.debug("Repair completed for {}.{} but repair is ineligible for mutation tracking migration, ignoring",
                                    keyspace, tableName);
                        return;
                    }

                    logger.info("Repair completed for {}.{}, proposing migration advancement for {} ranges",
                               keyspace, tableName, repairedRanges.size());

                    ClusterMetadataService.instance().commit(
                        new AdvanceMutationTrackingMigration(keyspace, tableMetadata.id, repairedRanges));
                }
                catch (Exception e)
                {
                    logger.error("Error handling repair completion for mutation tracking migration", e);
                }
            }

            @Override
            public void onFailure(Throwable throwable)
            {
                // noop
            }
        };
}
