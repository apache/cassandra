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
import org.junit.Assert;
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GuardrailMaximumReplicationFactorTest extends ThresholdTester
{
    private static final int MAXIMUM_REPLICATION_FACTOR_WARN_THRESHOLD = 2;
    private static final int MAXIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD = 4;
    private static final int DEFAULT_REPLICATION_FACTOR = 3;
    private static final int DISABLED_GUARDRAIL = -1;
    private static final String WHAT = "maximum_replication_factor";
    private static final String DATACENTER1 = "datacenter1";
    private static final String DATACENTER2 = "datacenter2";
    private static final String KS = "ks";
    private final TriConsumer<Guardrails, Integer, Integer> setter;

    public GuardrailMaximumReplicationFactorTest()
    {
        super(MAXIMUM_REPLICATION_FACTOR_WARN_THRESHOLD,
              MAXIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD,
              Guardrails.maximumReplicationFactor,
              Guardrails::setMaximumReplicationFactorThreshold,
              Guardrails::getMaximumReplicationFactorWarnThreshold,
              Guardrails::getMaximumReplicationFactorFailThreshold);

        this.setter = Guardrails::setMaximumReplicationFactorThreshold;
    }

    @Before
    public void setupTest() throws Throwable
    {
        DatabaseDescriptor.setDefaultKeyspaceRF(DEFAULT_REPLICATION_FACTOR);
    }

    @After
    public void cleanupTest() throws Throwable
    {
        execute(format("DROP KEYSPACE IF EXISTS %s", KS));
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
        assertValidProperty((g, a) -> setter.accept(g, DISABLED_GUARDRAIL, a), 5);

        assertInvalidPositiveIntProperty((g, w) -> setter.accept(g, w, DISABLED_GUARDRAIL), Integer.MIN_VALUE, Integer.MAX_VALUE, WHAT + "_warn_threshold");
        assertInvalidPositiveIntProperty((g, w) -> setter.accept(g, w, DISABLED_GUARDRAIL), -2, Integer.MAX_VALUE, WHAT + "_warn_threshold");
        assertValidProperty((g, w) -> setter.accept(g, w, DISABLED_GUARDRAIL), DISABLED_GUARDRAIL);
        assertInvalidPositiveIntProperty((g, w) -> setter.accept(g, w, DISABLED_GUARDRAIL), 0, Integer.MAX_VALUE, WHAT + "_warn_threshold");
        assertValidProperty((g, w) -> setter.accept(g, w, DISABLED_GUARDRAIL), 1);

        Assertions.assertThatThrownBy(() -> setter.accept(guardrails(), 5, 4))
                  .hasMessageContaining(guardrail.name + "_warn_threshold should be lower than the fail threshold");
    }

    @Test
    public void testMaxKeyspaceRFDisabled() throws Throwable
    {
        guardrails().setMaximumReplicationFactorThreshold(DISABLED_GUARDRAIL, DISABLED_GUARDRAIL);
        assertMaxThresholdValid(format("CREATE KEYSPACE %s WITH replication = { 'class': 'NetworkTopologyStrategy', '%s': 3}", KS, DATACENTER1));
        assertMaxThresholdValid(format("ALTER KEYSPACE %s WITH replication = { 'class' : 'NetworkTopologyStrategy', '%s': 5}", KS, DATACENTER1));
    }

    @Test
    public void testSimpleStrategy() throws Throwable
    {
        guardrails().setMaximumReplicationFactorThreshold(MAXIMUM_REPLICATION_FACTOR_WARN_THRESHOLD, MAXIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD);
        assertWarns(format("CREATE KEYSPACE %s WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 3}", KS),
                    format("The keyspace %s has a replication factor of 3, above the warning threshold of %s.", KS, MAXIMUM_REPLICATION_FACTOR_WARN_THRESHOLD));
        assertFails(format("ALTER KEYSPACE %s WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 5}", KS),
                    format("The keyspace %s has a replication factor of 5, above the failure threshold of %s.", KS, MAXIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD));
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
            public String getDatacenter(InetAddressAndPort endpoint) { return DATACENTER2; }

            @Override
            public int compareEndpoints(InetAddressAndPort target, Replica a1, Replica a2) { return 0; }
        });

        List<String> twoWarnings = Arrays.asList(format("The keyspace %s has a replication factor of 4, above the warning threshold of %d.", KS, MAXIMUM_REPLICATION_FACTOR_WARN_THRESHOLD),
                                                 format("The keyspace %s has a replication factor of 4, above the warning threshold of %d.", KS, MAXIMUM_REPLICATION_FACTOR_WARN_THRESHOLD));

        StorageService.instance.getTokenMetadata().updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.0.0.255"));
        guardrails().setMaximumReplicationFactorThreshold(MAXIMUM_REPLICATION_FACTOR_WARN_THRESHOLD, MAXIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD);
        assertValid(format("CREATE KEYSPACE %s WITH replication = { 'class' : 'NetworkTopologyStrategy', '%s': 2, '%s' : 2 };", KS, DATACENTER1, DATACENTER2));
        assertWarns(format("ALTER KEYSPACE %s WITH replication = { 'class' : 'NetworkTopologyStrategy', '%s': 4, '%s' : 2 };", KS, DATACENTER1, DATACENTER2),
                    format("The keyspace %s has a replication factor of 4, above the warning threshold of %d.", KS, MAXIMUM_REPLICATION_FACTOR_WARN_THRESHOLD));
        assertWarns(format("ALTER KEYSPACE %s WITH replication = { 'class' : 'NetworkTopologyStrategy', '%s': 4, '%s' : 4 };", KS, DATACENTER1, DATACENTER2),
                    twoWarnings);
        assertFails(format("ALTER KEYSPACE %s WITH replication = { 'class' : 'NetworkTopologyStrategy', '%s': 2, '%s' : 5 };", KS, DATACENTER1, DATACENTER2),
                    format("The keyspace %s has a replication factor of 5, above the failure threshold of %d.", KS, MAXIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD));

        execute(format("DROP KEYSPACE IF EXISTS %s", KS));
        assertFails(format("CREATE KEYSPACE %s WITH replication = { 'class' : 'NetworkTopologyStrategy', '%s': 5, '%s' : 5 };", KS, DATACENTER1, DATACENTER2),
                    format("The keyspace %s has a replication factor of 5, above the failure threshold of %d.", KS, MAXIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD));

        // revert
        DatabaseDescriptor.setEndpointSnitch(snitch);
    }

    @Test
    public void testMaxKeyspaceRFWarn() throws Throwable
    {
        guardrails().setMaximumReplicationFactorThreshold(MAXIMUM_REPLICATION_FACTOR_WARN_THRESHOLD, DISABLED_GUARDRAIL);
        // Valid
        assertMaxThresholdValid(format("CREATE KEYSPACE %s WITH replication = { 'class': 'NetworkTopologyStrategy', '%s': 1}", KS, DATACENTER1));
        // ALTER should warn on above value
        assertWarns(format("ALTER KEYSPACE %s WITH replication = { 'class': 'NetworkTopologyStrategy', '%s': 3}", KS, DATACENTER1),
                    format("The keyspace %s has a replication factor of 3, above the warning threshold of %s.", KS, MAXIMUM_REPLICATION_FACTOR_WARN_THRESHOLD));

        execute(format("DROP KEYSPACE IF EXISTS %s", KS));

        // CREATE should warn on above value
        assertWarns(format("CREATE KEYSPACE %s WITH replication = { 'class': 'NetworkTopologyStrategy', '%s': 3}", KS, DATACENTER1),
                    format("The keyspace %s has a replication factor of 3, above the warning threshold of %s.", KS, MAXIMUM_REPLICATION_FACTOR_WARN_THRESHOLD));
    }

    @Test
    public void testMaxKeyspaceRFFail() throws Throwable
    {
        guardrails().setMaximumReplicationFactorThreshold(DISABLED_GUARDRAIL, MAXIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD);
        // Valid
        assertMaxThresholdValid(format("CREATE KEYSPACE %s WITH replication = { 'class': 'NetworkTopologyStrategy', '%s': 3}", KS, DATACENTER1));
        // ALTER should fail on above value
        assertFails(format("ALTER KEYSPACE %s WITH replication = { 'class': 'NetworkTopologyStrategy', '%s': 5}", KS, DATACENTER1),
                    format("The keyspace %s has a replication factor of 5, above the failure threshold of %s.", KS, MAXIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD));

        execute(format("DROP KEYSPACE IF EXISTS %s", KS));

        // CREATE should fail on above value
        guardrails().setMaximumReplicationFactorThreshold(DISABLED_GUARDRAIL, MAXIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD);
        assertFails(format("CREATE KEYSPACE %s WITH replication = { 'class': 'NetworkTopologyStrategy', '%s': 5}", KS, DATACENTER1),
                    format("The keyspace %s has a replication factor of 5, above the failure threshold of %s.", KS, MAXIMUM_REPLICATION_FACTOR_FAIL_THRESHOLD));
    }

    @Test
    public void testMaxRFGreaterThanDefaultRF()
    {
        int defaultRF = 3;
        int failureThreshold = 2;
        try
        {
            DatabaseDescriptor.setDefaultKeyspaceRF(defaultRF);
            guardrails().setMaximumReplicationFactorThreshold(DISABLED_GUARDRAIL, failureThreshold);
            // fail the test
            Assert.fail("expect failure when default keyspace rf > max_rf_failure_threshold");
        }
        catch (IllegalArgumentException e)
        {
            Assertions.assertThat(e.getMessage()).contains(format("%s_fail_threshold to be set (%d) cannot be lower than default_keyspace_rf (%d)",
                                                                  WHAT, failureThreshold, defaultRF));
        }
    }
}
