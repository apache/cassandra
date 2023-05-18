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

package org.apache.cassandra.distributed.test;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import org.apache.cassandra.config.StartupChecksOptions;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.DataResurrectionCheck;
import org.apache.cassandra.service.DataResurrectionCheck.Heartbeat;
import org.apache.cassandra.service.StartupChecks.StartupCheckType;
import org.apache.cassandra.utils.Clock.Global;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.CHECK_DATA_RESURRECTION_HEARTBEAT_PERIOD;
import static org.apache.cassandra.config.StartupChecksOptions.ENABLED_PROPERTY;
import static org.apache.cassandra.distributed.Cluster.build;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.service.DataResurrectionCheck.DEFAULT_HEARTBEAT_FILE;
import static org.apache.cassandra.service.DataResurrectionCheck.EXCLUDED_KEYSPACES_CONFIG_PROPERTY;
import static org.apache.cassandra.service.DataResurrectionCheck.EXCLUDED_TABLES_CONFIG_PROPERTY;
import static org.apache.cassandra.service.StartupChecks.StartupCheckType.check_data_resurrection;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class DataResurrectionCheckTest extends TestBaseImpl
{
    @Test
    public void testDataResurrectionCheck() throws Exception
    {
        // set it to 1 hour so check will be not updated after it is written, for test purposes
        try (WithProperties properties = new WithProperties().set(CHECK_DATA_RESURRECTION_HEARTBEAT_PERIOD, 3600000))
        {
            // start the node with the check enabled, it will just pass fine as there are not any user tables yet
            // and system tables are young enough
            try (Cluster cluster = build().withNodes(1)
                                          .withDataDirCount(3) // we will expect heartbeat to be in the first data dir
                                          .withConfig(config -> config.with(NATIVE_PROTOCOL)
                                                                      .set("startup_checks",
                                                                           getStartupChecksConfig(ENABLED_PROPERTY, "true")))
                                          .start())
            {
                IInvokableInstance instance = cluster.get(1);

                checkHeartbeat(instance);

                for (String ks : new String[]{ "ks1", "ks2", "ks3" })
                {
                    cluster.schemaChange("CREATE KEYSPACE " + ks + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
                    cluster.schemaChange(format("CREATE TABLE %s.tb1 (pk text PRIMARY KEY) WITH gc_grace_seconds = 10", ks));
                    cluster.schemaChange(format("CREATE TABLE %s.tb2 (pk text PRIMARY KEY)", ks));
                }

                AtomicReference<Throwable> throwable = new AtomicReference<>();
                // periodically execute check on a running instance and wait until the exception is thrown on all keyspaces
                // wait for all violations by Awaitility as due to nature how tables were created,
                // they will not expire on their gc_grace_period exactly at the same time
                await().timeout(1, MINUTES)
                       .pollInterval(5, SECONDS)
                       .until(() -> {
                           Throwable t = executeChecksOnInstance(instance);
                           if (t == null)
                               return false;
                           String message = t.getMessage();
                           if (!message.contains("ks1") || !message.contains("ks2") || !message.contains("ks3"))
                           {
                               return false;
                           }
                           throwable.set(t);
                           return true;
                       });

                assertThat(throwable.get().getMessage(), containsString("Invalid tables"));
                // returned tables in output are not in any particular order
                // it is how they are returned from system tables
                assertThat(throwable.get().getMessage(), containsString("ks1.tb1"));
                assertThat(throwable.get().getMessage(), containsString("ks2.tb1"));
                assertThat(throwable.get().getMessage(), containsString("ks3.tb1"));

                // exclude failing keyspaces which already expired on their gc_grace_seconds, so we will pass the check
                assertNull(executeChecksOnInstance(instance, EXCLUDED_KEYSPACES_CONFIG_PROPERTY, "ks1,ks2,ks3"));

                // exclude failing tables which already expired on their gc_grace_seconds, so we will pass the check
                assertNull(executeChecksOnInstance(instance, EXCLUDED_TABLES_CONFIG_PROPERTY, "ks1.tb1,ks2.tb1,ks3.tb1"));

                // exclude failing tables, but not all of them,
                // so check detects only one table violates the check
                Throwable t = executeChecksOnInstance(instance, EXCLUDED_TABLES_CONFIG_PROPERTY, "ks1.tb1,ks2.tb1");

                assertNotNull(t);
                assertThat(t.getMessage(), containsString("Invalid tables: ks3.tb1"));

                // shadow table exclusion with keyspace exclusion, we have not excluded ks3.tb1, but we excluded whole ks3
                assertNull(executeChecksOnInstance(instance,
                                                   EXCLUDED_TABLES_CONFIG_PROPERTY, "ks1.tb1,ks2.tb1",
                                                   EXCLUDED_KEYSPACES_CONFIG_PROPERTY, "ks3"));
            }
        }
    }

    private Throwable executeChecksOnInstance(IInvokableInstance instance, final String... config)
    {
        assert config.length % 2 == 0;
        return instance.callsOnInstance((IIsolatedExecutor.SerializableCallable<Throwable>) () ->
        {
            try
            {
                DataResurrectionCheck check = new DataResurrectionCheck();
                StartupChecksOptions startupChecksOptions = new StartupChecksOptions();
                startupChecksOptions.enable(check_data_resurrection);

                for (int i = 0; i < config.length - 1; i = i + 2)
                    startupChecksOptions.set(check_data_resurrection, config[i], config[i + 1]);

                check.execute(startupChecksOptions);
                return null;
            }
            catch (StartupException e)
            {
                return e;
            }
        }).call();
    }

    private Map<StartupCheckType, Map<String, Object>> getStartupChecksConfig(String... configs)
    {
        return new EnumMap<StartupCheckType, Map<String, Object>>(StartupCheckType.class)
        {{
            put(check_data_resurrection,
                new HashMap<String, Object>()
                {{
                    for (int i = 0; i < configs.length - 1; i = i + 2)
                        put(configs[i], configs[i + 1]);
                }});
        }};
    }

    private void checkHeartbeat(IInvokableInstance instance) throws Exception
    {
        File heartbeatFile = new File(((String[]) instance.config().get("data_file_directories"))[0],
                                      DEFAULT_HEARTBEAT_FILE);
        assertTrue(heartbeatFile.exists());
        Heartbeat heartbeat = Heartbeat.deserializeFromJsonFile(heartbeatFile);
        assertNotNull(heartbeat.lastHeartbeat);
        assertTrue(heartbeat.lastHeartbeat.toEpochMilli() < Global.currentTimeMillis());
    }
}
