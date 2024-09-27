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

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig;
import org.apache.cassandra.repair.autorepair.AutoRepairKeyspace;
import org.apache.cassandra.repair.autorepair.AutoRepairUtils;
import org.apache.cassandra.schema.SchemaConstants;

import static org.apache.cassandra.Util.setAutoRepairEnabled;
import static org.apache.cassandra.config.CassandraRelevantProperties.SYSTEM_DISTRIBUTED_DEFAULT_RF;
import static org.junit.Assert.assertEquals;

@RunWith(Suite.class)
@Suite.SuiteClasses({ AutoRepairServiceTest.BasicTests.class, AutoRepairServiceTest.SetterTests.class })
public class AutoRepairServiceTest
{
    public static class BasicTests extends CQLTester
    {
        private static AutoRepairService autoRepairService;
        private static AutoRepairConfig config;

        @Before
        public void setUp()
        {
            System.setProperty("cassandra.streaming.requires_cdc_replay", "false");
            System.setProperty("cassandra.streaming.requires_view_build_during_repair", "false");
            DatabaseDescriptor.setMaterializedViewsEnabled(false);
            DatabaseDescriptor.setCDCEnabled(false);
            config = new AutoRepairConfig();
            autoRepairService = new AutoRepairService();
            autoRepairService.config = config;
        }

        @After
        public void tearDown()
        {
            System.clearProperty("cassandra.streaming.requires_view_build_during_repair");
            System.clearProperty("cassandra.streaming.requires_cdc_replay");
        }

        @Test
        public void testSetup()
        {
            AutoRepairService.instance.config = null;

            AutoRepairService.setup();

            assertEquals(DatabaseDescriptor.getAutoRepairConfig(), AutoRepairService.instance.config);
        }

        @Test
        public void testGetAutoRepairConfigReturnsConfig()
        {
            assertEquals(config, autoRepairService.getAutoRepairConfig());
        }

        @Test
        public void testsetAutoRepairHistoryClearDeleteHostsBufferInSecV2()
        {
            autoRepairService.setAutoRepairHistoryClearDeleteHostsBufferDuration("100s");

            assertEquals(100, config.getAutoRepairHistoryClearDeleteHostsBufferInterval().toSeconds());
        }


        @Test
        public void testsetAutoRepairMaxRetriesCount()
        {
            autoRepairService.setAutoRepairMaxRetriesCount(101);

            assertEquals(101, config.getRepairMaxRetries());
        }


        @Test
        public void testsetAutoRepairRetryBackoffInSec()
        {
            autoRepairService.setAutoRepairRetryBackoff("102s");

            assertEquals(102, config.getRepairRetryBackoff().toSeconds());
        }

        @Test(expected = ConfigurationException.class)
        public void testSetAutoRepairEnabledThrowsWithSchedulerDisabled()
        {
            autoRepairService.config = new AutoRepairConfig(false);

            autoRepairService.setAutoRepairEnabled(AutoRepairConfig.RepairType.incremental, true);
        }

        @Test(expected = ConfigurationException.class)
        public void testSetAutoRepairEnabledThrowsForIRWithMVReplay()
        {
            autoRepairService.config = new AutoRepairConfig(true);
            System.setProperty("cassandra.streaming.requires_view_build_during_repair", "true");

            autoRepairService.setAutoRepairEnabled(AutoRepairConfig.RepairType.incremental, true);
        }

        @Test
        public void testSetAutoRepairEnabledDoesNotThrowForIRWithMVReplayDisabled()
        {
            autoRepairService.config = new AutoRepairConfig(true);
            DatabaseDescriptor.setMaterializedViewsEnabled(true);
            System.setProperty("cassandra.streaming.requires_view_build_during_repair", "false");
            System.setProperty("cassandra.streaming.requires_cdc_replay", "false");

            autoRepairService.setAutoRepairEnabled(AutoRepairConfig.RepairType.incremental, true);
        }

        @Test(expected = ConfigurationException.class)
        public void testSetAutoRepairEnabledThrowsForIRWithCDCReplay()
        {
            autoRepairService.config = new AutoRepairConfig(true);
            System.setProperty("cassandra.streaming.requires_cdc_replay", "true");

            autoRepairService.setAutoRepairEnabled(AutoRepairConfig.RepairType.incremental, true);
        }

        @Test
        public void testSetAutoRepairEnabledDoesNotThrowForIRWithCDCReplayDisabled()
        {
            autoRepairService.config = new AutoRepairConfig(true);
            DatabaseDescriptor.setCDCEnabled(true);
            System.setProperty("cassandra.streaming.requires_cdc_replay", "false");

            autoRepairService.setAutoRepairEnabled(AutoRepairConfig.RepairType.incremental, true);
        }
    }

    @RunWith(Parameterized.class)
    public static class RepairTypeTests extends CQLTester
    {
        @Parameterized.Parameter()
        public AutoRepairConfig.RepairType repairType;

        private final UUID host1 = UUID.fromString("00000000-0000-0000-0000-000000000001");
        private final UUID host2 = UUID.fromString("00000000-0000-0000-0000-000000000002");

        private AutoRepairService instance;

        @Parameterized.Parameters(name = "repairType={0}")
        public static Collection<AutoRepairConfig.RepairType> repairTypes()
        {
            return Arrays.asList(AutoRepairConfig.RepairType.values());
        }


        @BeforeClass
        public static void setupClass() throws Exception
        {
            SYSTEM_DISTRIBUTED_DEFAULT_RF.setInt(1);
            setAutoRepairEnabled(true);
            requireNetwork();
        }

        @Before
        public void setUpTest()
        {
            AutoRepairUtils.setup();
            instance = new AutoRepairService();
        }

        @Test
        public void testGetOnGoingRepairHostIds()
        {
            long now = System.currentTimeMillis();
            AutoRepairUtils.insertNewRepairHistory(repairType, host1, now, now - 1000000);
            AutoRepairUtils.insertNewRepairHistory(repairType, host2, now, now - 1000000);

            Set<String> hosts = instance.getOnGoingRepairHostIds(repairType);

            assertEquals(ImmutableSet.of(host1.toString(), host2.toString()), hosts);
        }
    }

    @RunWith(Parameterized.class)
    public static class SetterTests<T> extends CQLTester
    {
        private static final AutoRepairConfig config = new AutoRepairConfig(true);

        @Parameterized.Parameter
        public AutoRepairConfig.RepairType repairType;

        @Parameterized.Parameter(1)
        public T arg;

        @Parameterized.Parameter(2)
        public BiConsumer<AutoRepairConfig.RepairType, T> setter;

        @Parameterized.Parameter(3)
        public Function<AutoRepairConfig.RepairType, T> getter;

        @Parameterized.Parameters(name = "{index}: repairType={0}, arg={1}")
        public static Collection<Object[]> testCases()
        {
            return Stream.of(
            forEachRepairType(true, AutoRepairService.instance::setAutoRepairEnabled, config::isAutoRepairEnabled),
            forEachRepairType(100, AutoRepairService.instance::setRepairThreads, config::getRepairThreads),
            forEachRepairType(200, AutoRepairService.instance::setRepairSubRangeNum, config::getRepairSubRangeNum),
            forEachRepairType(400, AutoRepairService.instance::setRepairSSTableCountHigherThreshold, config::getRepairSSTableCountHigherThreshold),
            forEachRepairType(ImmutableSet.of("dc1", "dc2"), AutoRepairService.instance::setIgnoreDCs, config::getIgnoreDCs),
            forEachRepairType(true, AutoRepairService.instance::setPrimaryTokenRangeOnly, config::getRepairPrimaryTokenRangeOnly),
            forEachRepairType(600, AutoRepairService.instance::setParallelRepairPercentageInGroup, config::getParallelRepairPercentage),
            forEachRepairType(700, AutoRepairService.instance::setParallelRepairCountInGroup, config::getParallelRepairCount),
            forEachRepairType(true, AutoRepairService.instance::setMVRepairEnabled, config::getMVRepairEnabled),
            forEachRepairType(ImmutableSet.of(InetAddressAndPort.getLocalHost()), AutoRepairService.instance::setRepairPriorityForHosts, AutoRepairUtils::getPriorityHosts),
            forEachRepairType(ImmutableSet.of(InetAddressAndPort.getLocalHost()), AutoRepairService.instance::setForceRepairForHosts, SetterTests::isLocalHostForceRepair)
            ).flatMap(Function.identity()).collect(Collectors.toList());
        }

        private static Set<InetAddressAndPort> isLocalHostForceRepair(AutoRepairConfig.RepairType type)
        {
            UUID hostId = Gossiper.instance.getHostId(InetAddressAndPort.getLocalHost());
            UntypedResultSet resultSet = QueryProcessor.executeInternal(String.format(
            "SELECT force_repair FROM %s.%s WHERE host_id = %s and repair_type = '%s'",
            SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY, hostId, type));

            if (!resultSet.isEmpty() && resultSet.one().getBoolean("force_repair"))
            {
                return ImmutableSet.of(InetAddressAndPort.getLocalHost());
            }
            return ImmutableSet.of();
        }

        private static <T> Stream<Object[]> forEachRepairType(T arg, BiConsumer<AutoRepairConfig.RepairType, T> setter, Function<AutoRepairConfig.RepairType, T> getter)
        {
            Object[][] testCases = new Object[AutoRepairConfig.RepairType.values().length][4];
            for (AutoRepairConfig.RepairType repairType : AutoRepairConfig.RepairType.values())
            {
                testCases[repairType.ordinal()] = new Object[]{ repairType, arg, setter, getter };
            }

            return Arrays.stream(testCases);
        }

        @BeforeClass
        public static void setup() throws Exception
        {
            setAutoRepairEnabled(true);
            requireNetwork();
            DatabaseDescriptor.setMaterializedViewsEnabled(false);
            DatabaseDescriptor.setCDCEnabled(false);
            AutoRepairUtils.setup();
            AutoRepairService.instance.config = config;
        }

        @Before
        public void prepare()
        {
            QueryProcessor.executeInternal(String.format(
            "TRUNCATE %s.%s",
            SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_HISTORY));
            QueryProcessor.executeInternal(String.format(
            "TRUNCATE %s.%s",
            SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, AutoRepairKeyspace.AUTO_REPAIR_PRIORITY));
        }

        @Test
        public void testSetters()
        {
            System.setProperty("cassandra.streaming.requires_cdc_replay", "false");
            System.setProperty("cassandra.streaming.requires_view_build_during_repair", "false");
            setter.accept(repairType, arg);
            assertEquals(arg, getter.apply(repairType));
        }
    }
}
