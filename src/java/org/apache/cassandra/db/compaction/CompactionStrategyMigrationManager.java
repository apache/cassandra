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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CompactionStrategyMigrationOptions;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.MBeanWrapper;

public class CompactionStrategyMigrationManager implements CompactionStrategyMigrationManagerMBean
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionStrategyMigrationManager.class);
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=CompactionStrategyMigrationManager";
    public static final CompactionStrategyMigrationManager instance = new CompactionStrategyMigrationManager();
    public Map<ColumnFamilyStore, CompactionParams> cfsToMigrate = new HashMap<>();

    private CompactionStrategyMigrationManager()
    {
        if (!MBeanWrapper.instance.isRegistered(MBEAN_NAME))
            MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);
    }

    public void setup(CompactionStrategyMigrationOptions options)
    {
        Set<String> userKeyspaces = Schema.instance.getUserKeyspaces();
        // if both keyspace_options and table_options are empty, we migrate all user cfs
        if (options.keyspace_options.isEmpty() && options.table_options.isEmpty())
        {
            logger.info("No keyspace_options or table_options found, migrating all user tables");
            if (!isJsonValid(options.compaction_params_json))
            {
                throw new ConfigurationException("can't parse global config for compaction_params_json");
            }
            CompactionParams params = CompactionStrategyMigrationOptions.parseCompactionParamsJson(options.compaction_params_json);
            userKeyspaces.forEach(keyspace -> {
                for (ColumnFamilyStore cfs : Keyspace.open(keyspace).getColumnFamilyStores())
                {
                    cfsToMigrate.put(cfs, params);
                }
            });
        }
        else
        {
            // remove those invalid (ks, params) or (tbl, params) pairs.
            // aggregate all the target cfs to migrate
            options.keyspace_options.forEach((keyspace, paramsJson) -> {
                if (isJsonValid(paramsJson) && isKeyspaceFoundInUserKeyspaces(userKeyspaces, keyspace))
                {
                    // put all cfs to the final map
                    CompactionParams params = CompactionStrategyMigrationOptions.parseCompactionParamsJson(paramsJson);
                    for (ColumnFamilyStore cfs : Keyspace.open(keyspace).getColumnFamilyStores())
                    {
                        cfsToMigrate.put(cfs, params);
                    }
                }
            });

            // table option will override keyspace option here
            options.table_options.forEach((keyspaceTable, paramsJson) -> {
                // get keyspace and table name
                String[] keyspaceAndTable = extractKeyspaceAndTable(keyspaceTable);
                if (keyspaceAndTable == null)
                {
                    logger.warn("unable to extract keyspace and table from {}, format should be ks.tbl", keyspaceTable);
                    return;
                }

                String keyspace = keyspaceAndTable[0];
                String table = keyspaceAndTable[1];
                if (isJsonValid(paramsJson) && isTableFoundInUserKeyspaces(userKeyspaces, keyspace, table))
                {
                    CompactionParams params = CompactionStrategyMigrationOptions.parseCompactionParamsJson(paramsJson);
                    cfsToMigrate.put(Keyspace.open(keyspace).getColumnFamilyStore(table), params);
                }
            });
        }
        logger.info("cfsTomigrate: {}", cfsToMigrate.keySet());
        logger.info("Overriding compaction params for {} cfs", cfsToMigrate.size());
    }

    public void mayOverrideLocalCompactionStrategy()
    {
        CompactionStrategyMigrationOptions options = DatabaseDescriptor.getCompactionStrategyMigrationOptions();
        if (!options.enabled)
        {
            logger.info("Compaction strategy migration not enabled");
            return;
        }
        setup(options);

        // do override: here we override both compaction strategy and options regardless what was set before
        cfsToMigrate.forEach((cfs, params) -> {
            cfs.getCompactionStrategyManager().setNewLocalCompactionStrategy(params);
        });
    }

    private static String[] extractKeyspaceAndTable(String s)
    {
        if (s.contains(".")) {
            String[] parts = s.split("\\.");
            if (parts.length == 2) {
                return parts;
            }
        }
        return null;
    }

    private boolean isJsonValid(String paramsJson)
    {
        if (!CompactionStrategyMigrationOptions.isJsonValid(paramsJson))
        {
            logger.warn("{} is not a valid json for compaction parameters, ignored", paramsJson);
            return false;
        }
        return true;
    }

    private boolean isKeyspaceFoundInUserKeyspaces(Set<String> userKeyspaces, String keyspace)
    {
        if (!userKeyspaces.contains(keyspace))
        {
            logger.warn("{} not found in user keyspaces, ignored", keyspace);
            return false;
        }
        return true;
    }

    private boolean isTableFoundInUserKeyspaces(Set<String> userKeyspaces, String keyspace, String table)
    {
        if (!isKeyspaceFoundInUserKeyspaces(userKeyspaces, keyspace))
        {
            return false;
        }
        TableMetadata tableMetadata = Schema.instance.getTableMetadata(keyspace, table);
        if (tableMetadata == null)
        {
            logger.warn("{} not found in {}, ignored", table, keyspace);
            return false;
        }
        return true;
    }

    public long getNumCfsToMigrate()
    {
        return cfsToMigrate.size();
    }

    public long getPendingTasksForMigration()
    {
        AtomicLong res = new AtomicLong(0L);
        cfsToMigrate.forEach((cfs, params) -> {
            res.addAndGet(cfs.getCompactionStrategyManager().getEstimatedRemainingTasks());
        });
        return res.get();
    }

    @VisibleForTesting
    public void reset()
    {
        cfsToMigrate = new HashMap<>();
    }
}
