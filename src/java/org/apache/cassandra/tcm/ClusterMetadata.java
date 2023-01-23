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

package org.apache.cassandra.tcm;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ConsensusTableMigrationState;

import static org.apache.cassandra.service.ConsensusTableMigrationState.MigrationStateSnapshot;
import static org.apache.cassandra.service.ConsensusTableMigrationState.TableMigrationState;

public class ClusterMetadata
{
    public final ConsensusTableMigrationState.MigrationStateSnapshot migrationStateSnapshot;
    public final Epoch epoch;

    public ClusterMetadata(Epoch epoch, MigrationStateSnapshot consensusMigrationState)
    {
        this.migrationStateSnapshot = new MigrationStateSnapshot(consensusMigrationState.tableStates, epoch);
        this.epoch = epoch;
    }

    public static ClusterMetadata current()
    {
        return ClusterMetadataService.instance.metadata();
    }

    public Transformer transformer()
    {
        return new Transformer(this, epoch.nextEpoch(false));
    }

    public static class Transformer
    {
        private MigrationStateSnapshot migrationStateSnapshot;
        public final Epoch epoch;
        private Transformer(ClusterMetadata metadata, Epoch epoch)
        {
            this.migrationStateSnapshot = metadata.migrationStateSnapshot;
            this.epoch = epoch;
        }

        public Transformer withConsensusTableMigrationStates(Map<TableId, TableMigrationState> newTableMigrationStates)
        {
            ImmutableMap.Builder<TableId, TableMigrationState> tableMigrationStatesBuilder = ImmutableMap.builder();
            migrationStateSnapshot.tableStates.entrySet()
                    .stream()
                    .filter(existingTMS -> !newTableMigrationStates.containsKey(existingTMS.getKey()))
                    .forEach(tableMigrationStatesBuilder::put);
            tableMigrationStatesBuilder.putAll(newTableMigrationStates.entrySet());
            migrationStateSnapshot = new MigrationStateSnapshot(tableMigrationStatesBuilder.build(), epoch);
            return this;
        }

        public ClusterMetadata build()
        {
            return new ClusterMetadata(epoch, migrationStateSnapshot);
        }
    }

}
