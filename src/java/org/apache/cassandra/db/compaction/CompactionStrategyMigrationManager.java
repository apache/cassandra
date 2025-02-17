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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.config.CompactionStrategyMigrationOptions;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;

public class CompactionStrategyMigrationManager implements CompactionStrategyMigrationManagerMBean
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionStrategyMigrationManager.class);
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=CompactionStrategyMigrationManager";
    public static final CompactionStrategyMigrationManager instance = new CompactionStrategyMigrationManager();
    public Map<ColumnFamilyStore, CompactionParams> cfsToMigrate = new HashMap<>();

    private final Map<Class<? extends AbstractCompactionStrategy>, CompactionParams> DEFAULT_PARAMS = ImmutableMap.of(
    SizeTieredCompactionStrategy.class, CompactionParams.stcs(Collections.emptyMap()),
    LeveledCompactionStrategy.class, CompactionParams.lcs(Collections.emptyMap()),
    TimeWindowCompactionStrategy.class, CompactionParams.twcs(Collections.emptyMap())
    );

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
                    if (!cfs.getCompactionStrategyManager().getCompactionParams().klass().equals(params.klass()))
                    {
                        // do override only when compaction strategy is different
                        cfsToMigrate.put(cfs, params);
                    }
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
                        if (!cfs.getCompactionStrategyManager().getCompactionParams().klass().equals(params.klass()))
                        {
                            // do override only when compaction strategy is different
                            cfsToMigrate.put(cfs, params);
                        }
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
                    ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
                    if (!cfs.getCompactionStrategyManager().getCompactionParams().klass().equals(params.klass()))
                    {
                        // do override only when compaction strategy is different
                        cfsToMigrate.put(cfs, params);
                    }
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
        // clean up before setup, in case options are changed at run time
        cfsToMigrate.clear();
        setup(options);

        // do override: here we override both compaction strategy and options regardless what was set before
        cfsToMigrate.forEach((cfs, params) -> {
            cfs.getCompactionStrategyManager().overrideLocalParams(params);
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

    public boolean getCompactionStrategyMigrationEnabled()
    {
        return DatabaseDescriptor.getCompactionStrategyMigrationOptions().enabled;
    }

    public Map<String, String> getCfsWithNonDefaultCompactionParams()
    {
        Map<String, String> res = new HashMap<>();
        Set<String> userKeyspaces = Schema.instance.getUserKeyspaces();
        userKeyspaces.forEach(keyspace -> {
            for (ColumnFamilyStore cfs : Keyspace.open(keyspace).getColumnFamilyStores())
            {
                CompactionParams params = cfs.getCompactionStrategyManager().getCompactionParams();
                if (!isDefaultCompactionParams(params))
                {
                    res.put(cfs.keyspace.getName() + '.' + cfs.name, params.toString());
                }
            }
        });
        return res;
    }

    private boolean isDefaultCompactionParams(CompactionParams params)
    {
        CompactionParams defaultParams = DEFAULT_PARAMS.get(params.klass());
        if (defaultParams == null)
        {
            // DTCS, which is deprecated, will be reported as well
            return false;
        }
        return defaultParams.equals(params);
    }

    public void applySchemaChangesForCompactionParams() throws RuntimeException
    {
        // USE WITH CAUTIOUS: this won't check other nodes' migration status and this will apply the schema change to
        // all nodes and there is no way back (do snapshot before if necessary)

        // safeguard: only able to run this when current node has migration enabled
        if (!getCompactionStrategyMigrationEnabled())
        {
            logger.error("CompactionStrategyMigration not enabled!");
            return;
        }

        ArrayList<ColumnFamilyStore> failedCfs = new ArrayList<>();
        cfsToMigrate.forEach((cfs, params) -> {
            try
            {
                switch (cfs.metadata().kind)
                {
                    case REGULAR:
                        executeInternal(String.format("ALTER TABLE %s.%s WITH compaction = %s;",
                                                      cfs.keyspace.getName(), cfs.name, FBUtilities.toJsonMapStringSingleQuotes(params.asMap())));
                        break;
                    case VIEW:
                        executeInternal(String.format("ALTER MATERIALIZED VIEW %s.%s WITH compaction = %s;",
                                                      cfs.keyspace.getName(), cfs.name, FBUtilities.toJsonMapStringSingleQuotes(params.asMap())));
                        break;
                    default:
                        InvalidRequestException e = new InvalidRequestException(String.format("trying to alter compaction options for cfs=%s, kind=%s", cfs, cfs.metadata().kind));
                        failedCfs.add(cfs);
                        throw e;
                }
            }
            catch (Exception e)
            {
                failedCfs.add(cfs);
                logger.error("failed to alter schema for {}", cfs.metadata().keyspace + '.' + cfs.metadata().name, e);
            }
        });

        // rethrow the exception
        if (!failedCfs.isEmpty()) {
            throw new RuntimeException("alter schema failed for some cfs, result might be partial. Failed cfs: " +
                                       failedCfs.stream().map(cfs -> cfs.metadata().keyspace + '.' + cfs.metadata().name).collect(Collectors.joining(",")));
        }
    }

    public void reloadAndOverrideLocalCompactionStrategy()
    {
        reloadOptionsFromDisk();
        mayOverrideLocalCompactionStrategy();
    }

    public void setAndOverrideLocalCompactionStrategy(String jsonOptions)
    {
        ObjectMapper jsonMapper = new ObjectMapper(new JsonFactory());
        CompactionStrategyMigrationOptions options;
        try
        {
            options =  jsonMapper.readValue(jsonOptions, CompactionStrategyMigrationOptions.class);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        DatabaseDescriptor.setCompactionStrategyMigrationOptions(options);
        mayOverrideLocalCompactionStrategy();
    }

    public void reloadOptionsFromDisk()
    {
        Config config = DatabaseDescriptor.loadConfig();
        DatabaseDescriptor.setCompactionStrategyMigrationOptions(config.compaction_strategy_migration_options);
    }

    @VisibleForTesting
    public void reset()
    {
        cfsToMigrate = new HashMap<>();
    }
}
