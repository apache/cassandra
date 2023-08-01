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
import org.junit.Before;
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
import static org.junit.Assert.assertNotNull;

public class GuardrailMinimumReplicationFactorTest extends ThresholdTester
{
    private static final int MINIMUM_REPLICATION_FACTOR_WARN_THRESHOLD = 4;
    private static int MINIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD = 1;
    private static final int DEFAULT_REPLICATION_FACTOR = 2;
    private static final int DISABLED_GUARDRAIL = -1;
    private static final String WHAT = "minimum_replication_factor";
    private static final String DATACENTER1 = "datacenter1";
    private static final String KS = "ks";
    private final TriConsumer<Guardrails, Integer, Integer> setter;

    public GuardrailMinimumReplicationFactorTest()
    {
        super(MINIMUM_REPLICATION_FACTOR_WARN_THRESHOLD,
              MINIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD,
              Guardrails.minimumReplicationFactor,
              Guardrails::setMinimumReplicationFactorThreshold,
              Guardrails::getMinimumReplicationFactorWarnThreshold,
              Guardrails::getMinimumReplicationFactorFailThreshold);

        this.setter = Guardrails::setMinimumReplicationFactorThreshold;
    }

    @Before
    public void setupTest() throws Throwable
    {
        DatabaseDescriptor.setDefaultKeyspaceRF(DEFAULT_REPLICATION_FACTOR);
        MINIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD = 2;
    }

    @After
    public void cleanupTest() throws Throwable
    {
        execute("DROP KEYSPACE IF EXISTS ks");
    }

    @Override
    protected long currentValue()
    {
        return Long.parseLong((Keyspace.open(KS).getReplicationStrategy()).configOptions.get(DATACENTER1));
    }

    @Override
    protected List<String> getWarnings()
    {
        List<String> warnings = ClientWarn.instance.getWarnings();

        // filtering out non-guardrails produced warnings
        return warnings == null
               ? Collections.emptyList()
               : warnings.stream()
                         .filter(w -> !w.contains("keyspace ks is higher than the number of nodes 1 for datacenter") &&
                                      !w.contains("When increasing replication factor you need to run a full (-full) repair to distribute the data") &&
                                      !w.contains("keyspace ks is higher than the number of nodes") &&
                                      !w.contains("Your replication factor 4 for keyspace ks is higher than the number of nodes 2 for datacenter datacenter2"))
                         .collect(Collectors.toList());
    }

    @Test
    public void testConfigValidation()
    {
        assertNotNull(guardrail);
        setter.accept(guardrails(), DISABLED_GUARDRAIL, DISABLED_GUARDRAIL);

        assertInvalidPositiveIntProperty((g, a) -> setter.accept(g, DISABLED_GUARDRAIL, a), Integer.MIN_VALUE, Integer.MAX_VALUE, WHAT + "_fail_threshold");
        assertInvalidPositiveIntProperty((g, a) -> setter.accept(g, DISABLED_GUARDRAIL, a), -2, Integer.MAX_VALUE, WHAT + "_fail_threshold");
        assertValidProperty((g, a) -> setter.accept(g, DISABLED_GUARDRAIL, a), DISABLED_GUARDRAIL);
        assertInvalidPositiveIntProperty((g, a) -> setter.accept(g, DISABLED_GUARDRAIL, a), 0, Integer.MAX_VALUE, WHAT + "_fail_threshold");
        assertValidProperty((g, a) -> setter.accept(g, DISABLED_GUARDRAIL, a), 1);
        assertValidProperty((g, a) -> setter.accept(g, DISABLED_GUARDRAIL, a), 2);

        assertInvalidPositiveIntProperty((g, w) -> setter.accept(g, w, DISABLED_GUARDRAIL), Integer.MIN_VALUE, Integer.MAX_VALUE, WHAT + "_warn_threshold");
        assertInvalidPositiveIntProperty((g, w) -> setter.accept(g, w, DISABLED_GUARDRAIL), -2, Integer.MAX_VALUE, WHAT + "_warn_threshold");
        assertValidProperty((g, w) -> setter.accept(g, w, DISABLED_GUARDRAIL), DISABLED_GUARDRAIL);
        assertInvalidPositiveIntProperty((g, w) -> setter.accept(g, w, DISABLED_GUARDRAIL), 0, Integer.MAX_VALUE, WHAT + "_warn_threshold");
        assertValidProperty((g, w) -> setter.accept(g, w, DISABLED_GUARDRAIL), 1);
        assertValidProperty((g, w) -> setter.accept(g, w, DISABLED_GUARDRAIL), 2);

        Assertions.assertThatThrownBy(() -> setter.accept(guardrails(), 1, 2))
                  .hasMessageContaining(guardrail.name + "_warn_threshold should be greater than the fail threshold");
    }

    @Test
    public void testMinKeyspaceRFDisabled() throws Throwable
    {
        guardrails().setMinimumReplicationFactorThreshold(DISABLED_GUARDRAIL, DISABLED_GUARDRAIL);
        assertMinThresholdValid("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 1}");
        assertMinThresholdValid("ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'datacenter1': 3}");
    }

    @Test
    public void testSimpleStrategyCreate() throws Throwable
    {
        guardrails().setMinimumReplicationFactorThreshold(MINIMUM_REPLICATION_FACTOR_WARN_THRESHOLD, MINIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD);
        assertWarns("CREATE KEYSPACE ks WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 3}", 3);
        execute("DROP KEYSPACE IF EXISTS ks");
        assertFails("CREATE KEYSPACE ks WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1}", 1);
    }

    @Test
    public void testSimpleStrategyAlter() throws Throwable
    {
        guardrails().setMinimumReplicationFactorThreshold(MINIMUM_REPLICATION_FACTOR_WARN_THRESHOLD, MINIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD);
        execute("CREATE KEYSPACE ks WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 4}");
        assertWarns("ALTER KEYSPACE ks WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 3}", 3);
        assertFails("ALTER KEYSPACE ks WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1}", 1);
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

        List<String> twoWarnings = Arrays.asList(format("The keyspace %s has a replication factor of 2, below the warning threshold of %d.", KS, MINIMUM_REPLICATION_FACTOR_WARN_THRESHOLD),
                                                 format("The keyspace %s has a replication factor of 2, below the warning threshold of %d.", KS, MINIMUM_REPLICATION_FACTOR_WARN_THRESHOLD));

        StorageService.instance.getTokenMetadata().updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.0.0.255"));
        guardrails().setMinimumReplicationFactorThreshold(MINIMUM_REPLICATION_FACTOR_WARN_THRESHOLD, MINIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD);
        assertValid("CREATE KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'datacenter1': 4, 'datacenter2' : 4 }");
        execute("DROP KEYSPACE IF EXISTS ks");
        assertWarns("CREATE KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'datacenter1': 4, 'datacenter2' : 2 }", 2);
        execute("DROP KEYSPACE IF EXISTS ks");
        assertWarns("CREATE KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'datacenter1': 2, 'datacenter2' : 2 }", twoWarnings);
        execute("DROP KEYSPACE IF EXISTS ks");
        assertFails("CREATE KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'datacenter1': 4, 'datacenter2' : 1 }", 1);
        execute("DROP KEYSPACE IF EXISTS ks");
        assertFails("CREATE KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'datacenter1': 1, 'datacenter2' : 1 }", 1);
        execute("DROP KEYSPACE IF EXISTS ks");

        execute("CREATE KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'datacenter1': 5, 'datacenter2' : 5}");
        assertValid("ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'datacenter1': 4, 'datacenter2' : 4 }");
        assertWarns("ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'datacenter1': 4, 'datacenter2' : 2 }", 2);
        assertWarns("ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'datacenter1': 2, 'datacenter2' : 2 }", twoWarnings);
        assertFails("ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'datacenter1': 4, 'datacenter2' : 1 }", 1);
        assertFails("ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'datacenter1': 1, 'datacenter2' : 1 }", 1);

        DatabaseDescriptor.setEndpointSnitch(snitch);
        execute("DROP KEYSPACE IF EXISTS ks1");
    }

    @Test
    public void testMinKeyspaceRFOnlyWarnAbove() throws Throwable
    {
        guardrails().setMinimumReplicationFactorThreshold(MINIMUM_REPLICATION_FACTOR_WARN_THRESHOLD, DISABLED_GUARDRAIL);
        assertMinThresholdValid("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 6}");
        assertMinThresholdValid("ALTER KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 5}");
    }

    @Test
    public void testMinKeyspaceRFOnlyWarnBelow() throws Throwable
    {
        guardrails().setMinimumReplicationFactorThreshold(MINIMUM_REPLICATION_FACTOR_WARN_THRESHOLD, DISABLED_GUARDRAIL);
        assertWarns("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 3}", 3);
        assertWarns("ALTER KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 2}", 2);
    }

    @Test
    public void testMinKeyspaceRFOnlyFailAbove() throws Throwable
    {
        guardrails().setMinimumReplicationFactorThreshold(DISABLED_GUARDRAIL, MINIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD);
        assertMinThresholdValid("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 4}");
        assertMinThresholdValid("ALTER KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 2}");
    }

    @Test
    public void testMinKeyspaceRFOnlyFailBelow() throws Throwable
    {
        guardrails().setMinimumReplicationFactorThreshold(DISABLED_GUARDRAIL, MINIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD);
        assertFails("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 1}", 1);
    }

    @Test
    public void testMinKeyspaceRFOnlyFailBelowAlter() throws Throwable
    {
        guardrails().setMinimumReplicationFactorThreshold(DISABLED_GUARDRAIL, MINIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD);
        execute("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 3}");
        assertFails("ALTER KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 1}", 1);
    }

    @Test
    public void testMinKeyspaceRFWarnAbove() throws Throwable
    {
        guardrails().setMinimumReplicationFactorThreshold(MINIMUM_REPLICATION_FACTOR_WARN_THRESHOLD, MINIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD);
        assertMinThresholdValid("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 6}");
        assertMinThresholdValid("ALTER KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 5}");
    }

    @Test
    public void testMinKeyspaceRFWarnFailBetween() throws Throwable
    {
        guardrails().setMinimumReplicationFactorThreshold(MINIMUM_REPLICATION_FACTOR_WARN_THRESHOLD, MINIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD);
        assertWarns("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 3}", 3);
        assertWarns("ALTER KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 2}", 2);
    }

    @Test
    public void testMinKeyspaceRFFailBelow() throws Throwable
    {
        guardrails().setMinimumReplicationFactorThreshold(MINIMUM_REPLICATION_FACTOR_WARN_THRESHOLD, MINIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD);
        assertFails("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 1}", 1);
    }

    @Test
    public void testMinKeyspaceRFFailBelowAlter() throws Throwable
    {
        guardrails().setMinimumReplicationFactorThreshold(MINIMUM_REPLICATION_FACTOR_WARN_THRESHOLD, MINIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD);
        execute("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 4}");
        assertFails("ALTER KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 1}", 1);
    }

    @Test
    public void testMinRFGreaterThanDefaultRF()
    {
        DatabaseDescriptor.setDefaultKeyspaceRF(3);
        Assertions.assertThatThrownBy(() -> guardrails().setMinimumReplicationFactorThreshold(5, 4))
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessageContaining("minimum_replication_factor_fail_threshold to be set (4) cannot be greater than default_keyspace_rf (3)");

        DatabaseDescriptor.setDefaultKeyspaceRF(6);
        guardrails().setMinimumReplicationFactorThreshold(5, 4);
        Assertions.assertThatThrownBy(() -> DatabaseDescriptor.setDefaultKeyspaceRF(3))
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessageContaining("default_keyspace_rf to be set (3) cannot be less than minimum_replication_factor_fail_threshold (4)");
    }

    private void assertWarns(String query, int rf) throws Throwable
    {
        assertWarns(query, format("The keyspace ks has a replication factor of %d, below the warning threshold of %s.",
                                  rf, MINIMUM_REPLICATION_FACTOR_WARN_THRESHOLD));
    }

    private void assertFails(String query, int rf) throws Throwable
    {
        assertFails(query, format("The keyspace ks has a replication factor of %d, below the failure threshold of %s.",
                                  rf, MINIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD));
    }
}
