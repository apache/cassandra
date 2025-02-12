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

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig;
import org.apache.cassandra.repair.autorepair.AutoRepairUtils;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SystemDistributedKeyspace;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.cassandra.Util.setAutoRepairEnabled;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for (updating parameters through JMX) {@link org.apache.cassandra.service.AutoRepairService}
 */
@RunWith(Parameterized.class)
public class AutoRepairServiceSetterTest<T> extends CQLTester
{
    private static final AutoRepairConfig config = new AutoRepairConfig(true);

    @Parameterized.Parameter
    public AutoRepairConfig.RepairType repairTypeStr;

    @Parameterized.Parameter(1)
    public T arg;

    @Parameterized.Parameter(2)
    public BiConsumer<String, T> setter;

    @Parameterized.Parameter(3)
    public Function<AutoRepairConfig.RepairType, T> getter;

    @Parameterized.Parameters(name = "{index}: repairType={0}, arg={1}")
    public static Collection<Object[]> testCases()
    {
        DatabaseDescriptor.setConfig(DatabaseDescriptor.loadConfig());
        return Stream.of(
        forEachRepairType(true, AutoRepairService.instance::setAutoRepairEnabled, config::isAutoRepairEnabled),
        forEachRepairType(100, AutoRepairService.instance::setRepairThreads, config::getRepairThreads),
        forEachRepairType(400, AutoRepairService.instance::setRepairSSTableCountHigherThreshold, config::getRepairSSTableCountHigherThreshold),
        forEachRepairType(ImmutableSet.of("dc1", "dc2"), AutoRepairService.instance::setIgnoreDCs, config::getIgnoreDCs),
        forEachRepairType(true, AutoRepairService.instance::setPrimaryTokenRangeOnly, config::getRepairPrimaryTokenRangeOnly),
        forEachRepairType(600, AutoRepairService.instance::setParallelRepairPercentage, config::getParallelRepairPercentage),
        forEachRepairType(700, AutoRepairService.instance::setParallelRepairCount, config::getParallelRepairCount),
        forEachRepairType(true, AutoRepairService.instance::setMVRepairEnabled, config::getMaterializedViewRepairEnabled),
        forEachRepairType(InetAddressAndPort.getLocalHost().getHostAddressAndPort(), (repairType, commaSeparatedHostSet) -> AutoRepairService.instance.setRepairPriorityForHosts(repairType, (String) commaSeparatedHostSet), AutoRepairUtils::getPriorityHosts),
        forEachRepairType(InetAddressAndPort.getLocalHost().getHostAddressAndPort(), (repairType, commaSeparatedHostSet) -> AutoRepairService.instance.setForceRepairForHosts(repairType, (String) commaSeparatedHostSet), AutoRepairServiceSetterTest::isLocalHostForceRepair)
        ).flatMap(Function.identity()).collect(Collectors.toList());
    }

    private static Set<InetAddressAndPort> isLocalHostForceRepair(AutoRepairConfig.RepairType type)
    {
        UUID hostId = StorageService.instance.getHostIdForEndpoint(InetAddressAndPort.getLocalHost());
        UntypedResultSet resultSet = QueryProcessor.executeInternal(String.format(
        "SELECT force_repair FROM %s.%s WHERE host_id = %s and repair_type = '%s'",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUTO_REPAIR_HISTORY, hostId, type));

        if (!resultSet.isEmpty() && resultSet.one().getBoolean("force_repair"))
        {
            return ImmutableSet.of(InetAddressAndPort.getLocalHost());
        }
        return ImmutableSet.of();
    }

    private static <T> Stream<Object[]> forEachRepairType(T arg, BiConsumer<String, T> setter, Function<AutoRepairConfig.RepairType, T> getter)
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
        DatabaseDescriptor.daemonInitialization();
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
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUTO_REPAIR_HISTORY));
        QueryProcessor.executeInternal(String.format(
        "TRUNCATE %s.%s",
        SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUTO_REPAIR_PRIORITY));
    }

    @Test
    public void testSettersTest()
    {
        DatabaseDescriptor.setMaterializedViewsOnRepairEnabled(false);
        DatabaseDescriptor.setCDCOnRepairEnabled(false);
        setter.accept(repairTypeStr.name(), arg);
        T actualConfig = getter.apply(repairTypeStr);
        if (actualConfig instanceof Set)
            // When performing a setRepairPriorityForHosts or setForceRepairForHosts, a comma-separated list of
            // ip addresses is provided as input. The configuration is expected to return a Set of Strings that
            // represent the configured IP addresses. This especial handling allows verification of this special
            // case where one of the entries in the Set must match the configured input.
            assertThat(actualConfig).satisfiesAnyOf(entry -> assertThat(entry.toString()).contains(arg.toString()));
        else
            assertThat(actualConfig).isEqualTo(arg);
    }
}
