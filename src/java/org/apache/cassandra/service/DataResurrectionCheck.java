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

package org.apache.cassandra.service;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.StartupChecksOptions;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.utils.Pair;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.cassandra.exceptions.StartupException.ERR_WRONG_DISK_STATE;
import static org.apache.cassandra.exceptions.StartupException.ERR_WRONG_MACHINE_STATE;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public class DataResurrectionCheck implements StartupCheck
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DataResurrectionCheck.class);

    public static final String HEARTBEAT_FILE_CONFIG_PROPERTY = "heartbeat_file";
    public static final String EXCLUDED_KEYSPACES_CONFIG_PROPERTY = "excluded_keyspaces";
    public static final String EXCLUDED_TABLES_CONFIG_PROPERTY = "excluded_tables";

    public static final String DEFAULT_HEARTBEAT_FILE = "cassandra-heartbeat";

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Heartbeat
    {
        @JsonProperty("last_heartbeat")
        public final Instant lastHeartbeat;

        /** needed for jackson serialization */
        @SuppressWarnings("unused")
        private Heartbeat() {
            this.lastHeartbeat = null;
        }

        public Heartbeat(Instant lastHeartbeat)
        {
            this.lastHeartbeat = lastHeartbeat;
        }

        public void serializeToJsonFile(File outputFile) throws IOException
        {
            JsonUtils.serializeToJsonFile(this, outputFile);
        }

        public static Heartbeat deserializeFromJsonFile(File file) throws IOException
        {
            return JsonUtils.deserializeFromJsonFile(Heartbeat.class, file);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Heartbeat manifest = (Heartbeat) o;
            return Objects.equals(lastHeartbeat, manifest.lastHeartbeat);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(lastHeartbeat);
        }
    }

    @VisibleForTesting
    static class TableGCPeriod
    {
        String table;
        int gcPeriod;

        TableGCPeriod(String table, int gcPeriod)
        {
            this.table = table;
            this.gcPeriod = gcPeriod;
        }
    }

    static File getHeartbeatFile(Map<String, Object> config)
    {
        String heartbeatFileConfigValue = (String) config.get(HEARTBEAT_FILE_CONFIG_PROPERTY);
        File heartbeatFile;

        if (heartbeatFileConfigValue != null)
        {
            heartbeatFile = new File(heartbeatFileConfigValue);
        }
        else
        {
            String[] dataFileLocations = DatabaseDescriptor.getLocalSystemKeyspacesDataFileLocations();
            assert dataFileLocations.length != 0;
            heartbeatFile = new File(dataFileLocations[0], DEFAULT_HEARTBEAT_FILE);
        }

        LOGGER.trace("Resolved heartbeat file for data resurrection check: " + heartbeatFile);

        return heartbeatFile;
    }

    @Override
    public StartupChecks.StartupCheckType getStartupCheckType()
    {
        return StartupChecks.StartupCheckType.check_data_resurrection;
    }

    @Override
    public void execute(StartupChecksOptions options) throws StartupException
    {
        if (options.isDisabled(getStartupCheckType()))
            return;

        Map<String, Object> config = options.getConfig(StartupChecks.StartupCheckType.check_data_resurrection);
        File heartbeatFile = getHeartbeatFile(config);

        if (!heartbeatFile.exists())
        {
            LOGGER.debug("Heartbeat file {} not found! Skipping heartbeat startup check.", heartbeatFile.absolutePath());
            return;
        }

        Heartbeat heartbeat;

        try
        {
            heartbeat = Heartbeat.deserializeFromJsonFile(heartbeatFile);
        }
        catch (IOException ex)
        {
            throw new StartupException(ERR_WRONG_DISK_STATE, "Failed to deserialize heartbeat file " + heartbeatFile);
        }

        if (heartbeat.lastHeartbeat == null)
            return;

        long heartbeatMillis = heartbeat.lastHeartbeat.toEpochMilli();

        List<Pair<String, String>> violations = new ArrayList<>();

        Set<String> excludedKeyspaces = getExcludedKeyspaces(config);
        Set<Pair<String, String>> excludedTables = getExcludedTables(config);

        long currentTimeMillis = currentTimeMillis();

        for (String keyspace : getKeyspaces())
        {
            if (excludedKeyspaces.contains(keyspace))
                continue;

            for (TableGCPeriod userTable : getTablesGcPeriods(keyspace))
            {
                if (excludedTables.contains(Pair.create(keyspace, userTable.table)))
                    continue;

                long gcGraceMillis = ((long) userTable.gcPeriod) * 1000;
                if (heartbeatMillis + gcGraceMillis < currentTimeMillis)
                    violations.add(Pair.create(keyspace, userTable.table));
            }
        }

        if (!violations.isEmpty())
        {
            String invalidTables = violations.stream()
                                             .map(p -> format("%s.%s", p.left, p.right))
                                             .collect(joining(","));

            String exceptionMessage = format("There are tables for which gc_grace_seconds is older " +
                                             "than the lastly known time Cassandra node was up based " +
                                             "on its heartbeat %s with timestamp %s. Cassandra node will not start " +
                                             "as it would likely introduce data consistency " +
                                             "issues (zombies etc). Please resolve these issues manually, " +
                                             "then remove the heartbeat and start the node again. Invalid tables: %s",
                                             heartbeatFile, heartbeat.lastHeartbeat, invalidTables);

            throw new StartupException(ERR_WRONG_MACHINE_STATE, exceptionMessage);
        }
    }

    @Override
    public void postAction(StartupChecksOptions options)
    {
        // Schedule heartbeating after all checks have passed, not as part of the check,
        // as it might happen that other checks after it might fail, but we would be heartbeating already.
        if (options.isEnabled(StartupChecks.StartupCheckType.check_data_resurrection))
        {
            Map<String, Object> config = options.getConfig(StartupChecks.StartupCheckType.check_data_resurrection);
            File heartbeatFile = DataResurrectionCheck.getHeartbeatFile(config);

            ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(() ->
            {
                Heartbeat heartbeat = new Heartbeat(Instant.ofEpochMilli(Clock.Global.currentTimeMillis()));
                try
                {
                    heartbeatFile.parent().createDirectoriesIfNotExists();
                    DataResurrectionCheck.LOGGER.trace("writing heartbeat to file " + heartbeatFile);
                    heartbeat.serializeToJsonFile(heartbeatFile);
                }
                catch (IOException ex)
                {
                    DataResurrectionCheck.LOGGER.error("Unable to serialize heartbeat to " + heartbeatFile, ex);
                }
            }, 0, CassandraRelevantProperties.CHECK_DATA_RESURRECTION_HEARTBEAT_PERIOD.getInt(), MILLISECONDS);
        }
    }

    @VisibleForTesting
    public Set<String> getExcludedKeyspaces(Map<String, Object> config)
    {
        String excludedKeyspacesConfigValue = (String) config.get(EXCLUDED_KEYSPACES_CONFIG_PROPERTY);

        if (excludedKeyspacesConfigValue == null)
            return Collections.emptySet();
        else
            return Arrays.stream(excludedKeyspacesConfigValue.trim().split(","))
                         .map(String::trim)
                         .collect(toSet());
    }

    @VisibleForTesting
    public Set<Pair<String, String>> getExcludedTables(Map<String, Object> config)
    {
        String excludedKeyspacesConfigValue = (String) config.get(EXCLUDED_TABLES_CONFIG_PROPERTY);

        if (excludedKeyspacesConfigValue == null)
            return Collections.emptySet();

        Set<Pair<String, String>> pairs = new HashSet<>();

        for (String keyspaceTable : excludedKeyspacesConfigValue.trim().split(","))
        {
            String[] pair = keyspaceTable.trim().split("\\.");
            if (pair.length != 2)
                continue;

            pairs.add(Pair.create(pair[0].trim(), pair[1].trim()));
        }

        return pairs;
    }

    @VisibleForTesting
    List<String> getKeyspaces()
    {
        return SchemaKeyspace.fetchNonSystemKeyspaces()
                             .stream()
                             .map(keyspaceMetadata -> keyspaceMetadata.name)
                             .collect(toList());
    }

    @VisibleForTesting
    List<TableGCPeriod> getTablesGcPeriods(String userKeyspace)
    {
        Optional<KeyspaceMetadata> keyspaceMetadata = SchemaKeyspace.fetchNonSystemKeyspaces().get(userKeyspace);
        if (!keyspaceMetadata.isPresent())
            return Collections.emptyList();

        KeyspaceMetadata ksmd = keyspaceMetadata.get();
        return ksmd.tables.stream()
                          .filter(tmd -> tmd.params.gcGraceSeconds > 0)
                          .map(tmd -> new TableGCPeriod(tmd.name, tmd.params.gcGraceSeconds)).collect(toList());
    }
}
