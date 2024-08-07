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

import java.util.Map;

public interface CompactionStrategyMigrationManagerMBean
{
    /**
     * @return number of column family stores included in migration
     */
    public long getNumCfsToMigrate();

    /**
     * @return total estimated pending compaction tasks for cfs to be migrated
     */
    public long getPendingTasksForMigration();

    /**
     * @return if compaction strategy migration is enabled
     */
    public boolean getCompactionStrategyMigrationEnabled();

    /**
     * @return user cfs with non-default compaction params
     */
    public Map<String, String> getCfsWithNonDefaultCompactionParams();

    /**
     * Execute ALTER TABLE internally to change the options for compaction params
     */
    public void applySchemaChangesForCompactionParams();

    /**
     * Reload options from DatabaseDescriptor, initialize CompactionStrategyMigrationManager, and switch local compaction
     * strategy if feasible
     */
    public void reloadAndOverrideLocalCompactionStrategy();

    /**
     * Set compaction_strategy_migration_options from input JSON string options, and switch local compaction
     * strategy if feasible
     */
    public void setAndOverrideLocalCompactionStrategy(String jsonOptions);
}
