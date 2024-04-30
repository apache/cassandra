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

package org.apache.cassandra.tools.nodetool;

import java.io.IOException;
import java.io.PrintStream;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.AutoRepairConfig;
import org.apache.cassandra.repair.AutoRepairConfig.RepairType;
import org.apache.cassandra.tools.NodeProbe;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Suite.class)
@Suite.SuiteClasses({ GetAutoRepairConfigTest.RegularTests.class, GetAutoRepairConfigTest.ParameterizedTests.class })
public class GetAutoRepairConfigTest
{
    private static final String RESOURCES_DIR = "test/resources/nodetool/getautorepairconfig/";
    protected static final String LEGACY_CONFIG_FILENAME = "legacy_config";
    protected static final String CONFIG_ALL_TYPES_DISABLED_FILENAME = "config_all_types_disabled";
    protected static final String CONFIG_ALL_TYPES_ENABLED_FILENAME = "config_all_types_enabled";
    protected static final String CONFIG_ONE_TYPE_ENABLED_FILENAME = "config_%s_repair_enabled";
    protected static InetAddressAndPort TEST_ENDPOINT;

    protected static String loadExpectedConfig(String filename) {
        try {
            return new String(Files.readAllBytes(Paths.get(RESOURCES_DIR + filename)));
        } catch (IOException e) {
            assertNull("Failed to load expected config", e);
            return  null;
        }
    }

    public static class RegularTests extends CQLTester
    {
        @Mock
        private static NodeProbe probe;

        @Mock
        private static PrintStream out;

        private static GetAutoRepairConfig cmd;

        @BeforeClass
        public static void setup() throws UnknownHostException
        {
            TEST_ENDPOINT = InetAddressAndPort.getByAddress(new byte[]{ 127, 0, 0, 1}).withPort(7000);
            DatabaseDescriptor.setMaterializedViewsEnabled(false);
            DatabaseDescriptor.setCDCEnabled(false);
        }

        @Before
        public void setUp()
        {
            MockitoAnnotations.initMocks(this);
            GetAutoRepairConfig.out = out;
            cmd = new GetAutoRepairConfig();
        }

        @Test
        public void testLegacyConfigDisabled()
        {
            cmd.v2 = false;
            when(probe.isAutoRepairEnabled()).thenReturn(false);

            cmd.execute(probe);

            verify(probe, times(1)).isAutoRepairEnabled();
            verify(out, times(1)).println("AutoRepair is not enabled");
        }

        @Test
        public void testLegacyConfigEnabled()
        {
            cmd.v2 = false;
            when(probe.isAutoRepairEnabled()).thenReturn(true);
            when(probe.getRepairThreads()).thenReturn(9);
            when(probe.getMVRepairEnabled()).thenReturn(true);

            cmd.execute(probe);

            verify(probe, times(1)).isAutoRepairEnabled();
            verify(out, times(1)).println(loadExpectedConfig(LEGACY_CONFIG_FILENAME));
        }

        @Test
        public void testAutoRepairConfigNull()
        {
            cmd.v2 = true;
            when(probe.getAutoRepairConfig()).thenReturn(null);

            cmd.execute(probe);

            verify(probe, times(1)).getAutoRepairConfig();
            verify(out, times(1)).println("Auto-repair is not enabled");
        }

        @Test
        public void testAutoRepairConfigDisabled()
        {
            cmd.v2 = true;
            AutoRepairConfig config = new AutoRepairConfig();
            config.setAutoRepairSchedulingEnabled(false);
            when(probe.getAutoRepairConfig()).thenReturn(config);

            cmd.execute(probe);

            verify(probe, times(1)).getAutoRepairConfig();
            verify(out, times(1)).println("Auto-repair is not enabled");
        }

        @Test
        public void testAllRepairTypesDisabled()
        {
            cmd.v2 = true;
            AutoRepairConfig config = new AutoRepairConfig();
            config.setAutoRepairSchedulingEnabled(true);
            config.setRepairCheckIntervalInSec(3600);
            config.setAutoRepairHistoryClearDeleteHostsBufferInSec(700);
            for (RepairType repairType : RepairType.values())
            {
                config.setAutoRepairEnabled(repairType, false);
                config.setRepairOnlyKeyspaces(repairType, "test");
                config.setIgnoreDCs(repairType, ImmutableSet.of("dc1", "dc2"));
                when(probe.getRepairPriorityForHosts(repairType)).thenReturn(ImmutableSet.of(TEST_ENDPOINT));
            }
            when(probe.getAutoRepairConfig()).thenReturn(config);

            cmd.execute(probe);

            verify(probe, times(1)).getAutoRepairConfig();
            verify(out, times(1)).println(loadExpectedConfig(CONFIG_ALL_TYPES_DISABLED_FILENAME));
        }

        @Test
        public void testAllRepairTypesEnabled()
        {
            cmd.v2 = true;
            AutoRepairConfig config = new AutoRepairConfig();
            config.setAutoRepairSchedulingEnabled(true);
            config.setRepairCheckIntervalInSec(3600);
            config.setAutoRepairHistoryClearDeleteHostsBufferInSec(700);
            for (RepairType repairType : RepairType.values())
            {
                config.setAutoRepairEnabled(repairType, true);
                config.setRepairOnlyKeyspaces(repairType, "test");
                config.setIgnoreDCs(repairType, ImmutableSet.of("dc1", "dc2"));
                when(probe.getRepairPriorityForHosts(repairType)).thenReturn(ImmutableSet.of(TEST_ENDPOINT));
            }
            when(probe.getAutoRepairConfig()).thenReturn(config);

            cmd.execute(probe);

            verify(probe, times(1)).getAutoRepairConfig();
            verify(out, times(1)).println(loadExpectedConfig(CONFIG_ALL_TYPES_ENABLED_FILENAME));
        }
    }

    @RunWith(Parameterized.class)
    public static class ParameterizedTests extends CQLTester
    {
        @BeforeClass
        public static void setup() throws UnknownHostException
        {
            TEST_ENDPOINT = InetAddressAndPort.getByAddress(new byte[]{ 127, 0, 0, 1}).withPort(7000);
            DatabaseDescriptor.setMaterializedViewsEnabled(false);
            DatabaseDescriptor.setCDCEnabled(false);
        }

        @Mock
        private static NodeProbe probe;

        @Mock
        private static PrintStream out;

        private static GetAutoRepairConfig cmd;

        @Parameterized.Parameter
        public RepairType repairType;

        @Parameterized.Parameters(name = "repairType={0}")
        public static Object[] data()
        {
            return RepairType.values();
        }

        @Before
        public void setUp()
        {
            MockitoAnnotations.initMocks(this);
            GetAutoRepairConfig.out = out;
            cmd = new GetAutoRepairConfig();
        }


        @Test
        public void testOneRepairTypeEnabled()
        {
            cmd.v2 = true;
            AutoRepairConfig config = new AutoRepairConfig();
            config.setAutoRepairSchedulingEnabled(true);
            config.setRepairCheckIntervalInSec(3600);
            config.setAutoRepairHistoryClearDeleteHostsBufferInSec(700);
            config.setMVRepairEnabled(repairType, true);
            for (RepairType type : RepairType.values())
            {
                config.setAutoRepairEnabled(type, false);
                config.setAutoRepairEnabled(type, false);
                config.setRepairOnlyKeyspaces(type, "test");
                config.setIgnoreDCs(type, ImmutableSet.of("dc1", "dc2"));
            }
            config.setAutoRepairEnabled(repairType, true);
            config.setIgnoreDCs(repairType, ImmutableSet.of("dc3", "dc4"));
            config.setDCGroups(repairType, ImmutableSet.of("dc5|dc6", "dc7|dc8"));
            when(probe.getRepairPriorityForHosts(any())).thenReturn(ImmutableSet.of(TEST_ENDPOINT));
            when(probe.getAutoRepairConfig()).thenReturn(config);

            cmd.execute(probe);

            verify(probe, times(1)).getAutoRepairConfig();
            verify(out, times(1)).println(loadExpectedConfig(String.format(CONFIG_ONE_TYPE_ENABLED_FILENAME, repairType)));
        }
    }
}
