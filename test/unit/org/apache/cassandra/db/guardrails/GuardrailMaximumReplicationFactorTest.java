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

package org.apache.cassandra.db.guardrails;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.locator.AbstractEndpointSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.StorageService;
import org.assertj.core.api.Assertions;

import static java.lang.String.format;

public class GuardrailMaximumReplicationFactorTest extends ThresholdTester
{
    private static final int MAXIMUM_REPLICATION_FACTOR_WARN_THRESHOLD = 2;
    private static final int MAXIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD = 4;
    private static final int DISABLED_GUARDRAIL = -1;

    public GuardrailMaximumReplicationFactorTest()
    {
        super(MAXIMUM_REPLICATION_FACTOR_WARN_THRESHOLD,
              MAXIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD,
              Guardrails.maximumReplicationFactor,
              Guardrails::setMaximumReplicationFactorThreshold,
              Guardrails::getMaximumReplicationFactorWarnThreshold,
              Guardrails::getMaximumReplicationFactorFailThreshold);
    }

    @After
    public void cleanupTest() throws Throwable
    {
        execute("DROP KEYSPACE IF EXISTS ks");
        DatabaseDescriptor.setDefaultKeyspaceRF(1);
    }

    @Override
    protected long currentValue()
    {
        return Long.parseLong((Keyspace.open("ks").getReplicationStrategy()).configOptions.get("datacenter1"));
    }

    @Override
    protected List<String> getWarnings()
    {
        List<String> warnings = ClientWarn.instance.getWarnings();

        // filtering out non-guardrails produced warnings
        return warnings == null
               ? Collections.emptyList()
               : warnings.stream()
                         .filter(w -> !w.contains("keyspace ks is higher than the number of nodes 1 for datacenter1") &&
                                      !w.contains("When increasing replication factor you need to run a full (-full) repair to distribute the data") &&
                                      !w.contains("keyspace ks is higher than the number of nodes") &&
                                      !w.contains("Your replication factor 3 for keyspace ks is higher than the number of nodes 2 for datacenter datacenter2"))
                         .collect(Collectors.toList());
    }

    @Test
    public void testMaxKeyspaceRFDisabled() throws Throwable
    {
        guardrails().setMaximumReplicationFactorThreshold(DISABLED_GUARDRAIL, DISABLED_GUARDRAIL);
        assertMaxThresholdValid("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 6}");
        assertMaxThresholdValid("ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'datacenter1': 10}");
    }

    @Test
    public void testSimpleStrategyCreate() throws Throwable
    {
        guardrails().setMaximumReplicationFactorThreshold(MAXIMUM_REPLICATION_FACTOR_WARN_THRESHOLD, MAXIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD);
        assertWarns("CREATE KEYSPACE ks WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 3}", 3);
        execute("DROP KEYSPACE IF EXISTS ks");
        assertFails("CREATE KEYSPACE ks WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 5}", 5);
    }

    @Test
    public void testSimpleStrategyAlter() throws Throwable
    {
        guardrails().setMaximumReplicationFactorThreshold(MAXIMUM_REPLICATION_FACTOR_WARN_THRESHOLD, MAXIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD);
        execute("CREATE KEYSPACE ks WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 2}");
        assertWarns("ALTER KEYSPACE ks WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 3}", 3);
        assertFails("ALTER KEYSPACE ks WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 5}", 5);
    }

    @Test
    public void testMultipleDatacenter() throws Throwable
    {
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        DatabaseDescriptor.setEndpointSnitch(new AbstractEndpointSnitch()
        {
            public static final String RACK1 = ServerTestUtils.RACK1;

            @Override
            public String getRack(InetAddressAndPort endpoint) { return RACK1; }

            @Override
            public String getDatacenter(InetAddressAndPort endpoint) { return "datacenter2"; }

            @Override
            public int compareEndpoints(InetAddressAndPort target, Replica a1, Replica a2) { return 0; }
        });

        List<String> twoWarnings = Arrays.asList(format("The keyspace ks has a replication factor of 3, above the warning threshold of %s.", MAXIMUM_REPLICATION_FACTOR_WARN_THRESHOLD),
                                                 format("The keyspace ks has a replication factor of 3, above the warning threshold of %s.", MAXIMUM_REPLICATION_FACTOR_WARN_THRESHOLD));

        StorageService.instance.getTokenMetadata().updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.0.0.255"));
        guardrails().setMaximumReplicationFactorThreshold(MAXIMUM_REPLICATION_FACTOR_WARN_THRESHOLD, MAXIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD);
        assertValid("CREATE KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'datacenter1': 2, 'datacenter2' : 2}");
        execute("DROP KEYSPACE IF EXISTS ks");
        assertWarns("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 2, 'datacenter2' : 3}", 3);
        execute("DROP KEYSPACE IF EXISTS ks");
        assertWarns("CREATE KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'datacenter1': 3, 'datacenter2' : 3}", twoWarnings);
        execute("DROP KEYSPACE IF EXISTS ks");
        assertFails("CREATE KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'datacenter1': 2, 'datacenter2' : 5}", 5);
        execute("DROP KEYSPACE IF EXISTS ks");
        assertFails("CREATE KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'datacenter1': 5, 'datacenter2' : 5}", 5);
        execute("DROP KEYSPACE IF EXISTS ks");

        execute("CREATE KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'datacenter1': 1, 'datacenter2' : 1}");
        assertValid("ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'datacenter1': 2, 'datacenter2' : 2}");
        assertWarns("ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'datacenter1': 2, 'datacenter2' : 3}", 3);
        assertWarns("ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'datacenter1': 3, 'datacenter2' : 3}", twoWarnings);
        assertFails("ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'datacenter1': 2, 'datacenter2' : 5}", 5);
        assertFails("ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'datacenter1': 5, 'datacenter2' : 5}", 5);

        DatabaseDescriptor.setEndpointSnitch(snitch);
    }

    @Test
    public void testMaxKeyspaceRFOnlyWarnBelow() throws Throwable
    {
        guardrails().setMaximumReplicationFactorThreshold(MAXIMUM_REPLICATION_FACTOR_WARN_THRESHOLD, DISABLED_GUARDRAIL);
        assertMaxThresholdValid("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 2}");
        assertMaxThresholdValid("ALTER KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 1}");
    }

    @Test
    public void testMaxKeyspaceRFOnlyWarnAbove() throws Throwable
    {
        guardrails().setMaximumReplicationFactorThreshold(MAXIMUM_REPLICATION_FACTOR_WARN_THRESHOLD, DISABLED_GUARDRAIL);
        assertWarns("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 3}", 3);
        assertWarns("ALTER KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 4}", 4);
    }

    @Test
    public void testMaxKeyspaceRFOnlyFailBelow() throws Throwable
    {
        guardrails().setMaximumReplicationFactorThreshold(DISABLED_GUARDRAIL, MAXIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD);
        assertMaxThresholdValid("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 2}");
        assertMaxThresholdValid("ALTER KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 3}");
    }

    @Test
    public void testMaxKeyspaceRFOnlyFailAbove() throws Throwable
    {
        guardrails().setMaximumReplicationFactorThreshold(DISABLED_GUARDRAIL, MAXIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD);
        assertFails("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 5}", 5);
    }

    @Test
    public void testMaxKeyspaceRFOnlyFailAboveAlter() throws Throwable
    {
        guardrails().setMaximumReplicationFactorThreshold(DISABLED_GUARDRAIL, MAXIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD);
        execute("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 3}");
        assertFails("ALTER KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 6}", 6);
    }

    @Test
    public void testMaxKeyspaceRFWarnBelow() throws Throwable
    {
        guardrails().setMaximumReplicationFactorThreshold(MAXIMUM_REPLICATION_FACTOR_WARN_THRESHOLD, MAXIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD);
        assertMaxThresholdValid("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 1}");
        assertMaxThresholdValid("ALTER KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 2}");
    }

    @Test
    public void testMaxKeyspaceRFWarnFailBetween() throws Throwable
    {
        guardrails().setMaximumReplicationFactorThreshold(MAXIMUM_REPLICATION_FACTOR_WARN_THRESHOLD, MAXIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD);
        assertWarns("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 3}", 3);
        assertWarns("ALTER KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 4}", 4);
    }

    @Test
    public void testMaxKeyspaceRFFailAbove() throws Throwable
    {
        guardrails().setMaximumReplicationFactorThreshold(MAXIMUM_REPLICATION_FACTOR_WARN_THRESHOLD, MAXIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD);
        assertFails("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 5}", 5);
    }

    @Test
    public void testMaxKeyspaceRFFailAboveAlter() throws Throwable
    {
        guardrails().setMaximumReplicationFactorThreshold(MAXIMUM_REPLICATION_FACTOR_WARN_THRESHOLD, MAXIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD);
        execute("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 4}");
        assertFails("ALTER KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 5}", 5);
    }

    @Test
    public void testMaxRFLesserThanDefaultRF()
    {
        DatabaseDescriptor.setDefaultKeyspaceRF(3);
        Assertions.assertThatThrownBy(() -> guardrails().setMaximumReplicationFactorThreshold(1, 2))
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessageContaining("maximum_replication_factor_fail_threshold to be set (2) cannot be lesser than default_keyspace_rf (3)");

        DatabaseDescriptor.setDefaultKeyspaceRF(1);
        guardrails().setMaximumReplicationFactorThreshold(1, 2);
        Assertions.assertThatThrownBy(() -> DatabaseDescriptor.setDefaultKeyspaceRF(3))
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessageContaining("default_keyspace_rf to be set (3) cannot be greater than maximum_replication_factor_fail_threshold (2)");
    }

    private void assertWarns(String query, int rf) throws Throwable
    {
        assertWarns(query, format("The keyspace ks has a replication factor of %d, above the warning threshold of %s.",
                                  rf, MAXIMUM_REPLICATION_FACTOR_WARN_THRESHOLD));
    }

    private void assertFails(String query, int rf) throws Throwable
    {
        assertFails(query, format("The keyspace ks has a replication factor of %d, above the failure threshold of %s.",
                                  rf, MAXIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD));
    }
}
