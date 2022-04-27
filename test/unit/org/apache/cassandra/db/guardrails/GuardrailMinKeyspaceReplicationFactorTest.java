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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.ClientWarn;
import org.assertj.core.api.Assertions;

import static java.lang.String.format;
import static org.junit.Assert.assertNotNull;

public class GuardrailMinKeyspaceReplicationFactorTest extends ThresholdTester
{
    private static final int MINIMUM_KEYSPACE_RF_WARN_THRESHOLD = 4;
    private static int MINIMUM_KEYSPACE_RF_FAIL_THRESHOLD = 1;
    private static final int DEFAULT_RF = 2;
    private static final int DISABLED_GUARDRAIL = -1;
    private static final String WHAT = "minimum_keyspace_rf";
    private final TriConsumer<Guardrails, Integer, Integer> setter;

    public GuardrailMinKeyspaceReplicationFactorTest()
    {
        super(MINIMUM_KEYSPACE_RF_WARN_THRESHOLD,
              MINIMUM_KEYSPACE_RF_FAIL_THRESHOLD,
              Guardrails.minimumKeyspaceRF,
              Guardrails::setMinimumKeyspaceRFThreshold,
              Guardrails::getMinimumKeyspaceRFWarnThreshold,
              Guardrails::getMinimumKeyspaceRFFailThreshold);

        this.setter = Guardrails::setMinimumKeyspaceRFThreshold;
    }

    @Before
    public void setupTest() throws Throwable
    {
        DatabaseDescriptor.setDefaultKeyspaceRF(DEFAULT_RF);
        MINIMUM_KEYSPACE_RF_FAIL_THRESHOLD = 2;
    }

    @After
    public void cleanupTest() throws Throwable
    {
        execute("DROP KEYSPACE IF EXISTS ks");
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

        return warnings == null
               ? Collections.emptyList()
               : warnings.stream()
                         .filter(w -> !w.contains("keyspace ks is higher than the number of nodes 1 for datacenter") &&
                                      !w.contains("When increasing replication factor you need to run a full (-full) repair to distribute the data") &&
                                      !w.contains("keyspace ks is higher than the number of nodes 1"))
                         .collect(Collectors.toList());
    }

    @Test
    public void testConfigValidation()
    {
        assertNotNull(guardrail);
        setter.accept(guardrails(), DISABLED_GUARDRAIL, DISABLED_GUARDRAIL);

        assertInvalidPositiveProperty((g, a) -> setter.accept(g, DISABLED_GUARDRAIL, a.intValue()), Integer.MIN_VALUE, Integer.MAX_VALUE, WHAT + "_fail_threshold");
        assertInvalidPositiveProperty((g, a) -> setter.accept(g, DISABLED_GUARDRAIL, a.intValue()), -2, Integer.MAX_VALUE, WHAT + "_fail_threshold");
        assertValidProperty((g, a) -> setter.accept(g, DISABLED_GUARDRAIL, a.intValue()), DISABLED_GUARDRAIL);
        assertInvalidPositiveProperty((g, a) -> setter.accept(g, DISABLED_GUARDRAIL, a.intValue()), DISABLED_GUARDRAIL == 0 ? -1 : 0, Integer.MAX_VALUE, WHAT + "_fail_threshold");
        assertValidProperty((g, a) -> setter.accept(g, DISABLED_GUARDRAIL, a.intValue()), 1L);
        assertValidProperty((g, a) -> setter.accept(g, DISABLED_GUARDRAIL, a.intValue()), 2L);

        assertInvalidPositiveProperty((g, w) -> setter.accept(g, w.intValue(), DISABLED_GUARDRAIL), Integer.MIN_VALUE, Integer.MAX_VALUE, WHAT + "_warn_threshold");
        assertInvalidPositiveProperty((g, w) -> setter.accept(g, w.intValue(), DISABLED_GUARDRAIL), -2, Integer.MAX_VALUE, WHAT + "_warn_threshold");
        assertValidProperty((g,w) ->setter.accept(g,w.intValue(),DISABLED_GUARDRAIL), DISABLED_GUARDRAIL);
        assertInvalidPositiveProperty((g, w) -> setter.accept(g, w.intValue(), DISABLED_GUARDRAIL), DISABLED_GUARDRAIL == 0 ? -1 : 0, Integer.MAX_VALUE, WHAT + "_warn_threshold");
        assertValidProperty((g, w) -> setter.accept(g, w.intValue(), DISABLED_GUARDRAIL), 1L);
        assertValidProperty((g, w) -> setter.accept(g, w.intValue(), DISABLED_GUARDRAIL), 2L);

        Assertions.assertThatThrownBy(() -> setter.accept(guardrails(), 1, 2))
                  .hasMessageContaining(guardrail.name + "_warn_threshold should be greater than the fail threshold");
    }

    @Test
    public void testMinKeyspaceRFDisabled() throws Throwable
    {
        guardrails().setMinimumKeyspaceRFThreshold(DISABLED_GUARDRAIL, DISABLED_GUARDRAIL);
        assertMinThresholdValid("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 1}");
        assertMinThresholdValid("ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'datacenter1': 3}");
    }

    @Test
    public void testSimpleStrategy() throws Throwable
    {
        guardrails().setMinimumKeyspaceRFThreshold(MINIMUM_KEYSPACE_RF_WARN_THRESHOLD, MINIMUM_KEYSPACE_RF_FAIL_THRESHOLD);
        assertWarns("CREATE KEYSPACE ks WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 3}",
                    format("Keyspaces with %s equal to 3 exceeds warn threshold of %d.", WHAT, MINIMUM_KEYSPACE_RF_WARN_THRESHOLD));
        assertFails("ALTER KEYSPACE ks WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1}",
                    format("Keyspaces with %s equal to 1 exceeds fail threshold of %d.", WHAT, MINIMUM_KEYSPACE_RF_FAIL_THRESHOLD));
    }

    @Test
    public void testMinKeyspaceRFOnlyWarnAbove() throws Throwable
    {
        guardrails().setMinimumKeyspaceRFThreshold(MINIMUM_KEYSPACE_RF_WARN_THRESHOLD, DISABLED_GUARDRAIL);
        assertMinThresholdValid("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 6}");
        assertMinThresholdValid("ALTER KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 5}");
    }

    @Test
    public void testMinKeyspaceRFOnlyWarnBelow() throws Throwable
    {
        guardrails().setMinimumKeyspaceRFThreshold(MINIMUM_KEYSPACE_RF_WARN_THRESHOLD, DISABLED_GUARDRAIL);
        assertWarns("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 3}",
                    format("Keyspaces with %s equal to 3 exceeds warn threshold of %d", WHAT, MINIMUM_KEYSPACE_RF_WARN_THRESHOLD));
        assertWarns("ALTER KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 2}",
                    format("Keyspaces with %s equal to 2 exceeds warn threshold of %d", WHAT, MINIMUM_KEYSPACE_RF_WARN_THRESHOLD));
    }

    @Test
    public void testMinKeyspaceRFOnlyFailAbove() throws Throwable
    {
        guardrails().setMinimumKeyspaceRFThreshold(DISABLED_GUARDRAIL, MINIMUM_KEYSPACE_RF_FAIL_THRESHOLD);
        assertMinThresholdValid("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 4}");
        assertMinThresholdValid("ALTER KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 2}");
    }

    @Test
    public void testMinKeyspaceRFOnlyFailBelow() throws Throwable
    {
        guardrails().setMinimumKeyspaceRFThreshold(DISABLED_GUARDRAIL, MINIMUM_KEYSPACE_RF_FAIL_THRESHOLD);
        assertFails("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 1}",
                    format("Keyspaces with %s equal to 1 exceeds fail threshold of %d", WHAT, MINIMUM_KEYSPACE_RF_FAIL_THRESHOLD));
    }

    @Test
    public void testMinKeyspaceRFOnlyFailBelowAlter() throws Throwable
    {
        guardrails().setMinimumKeyspaceRFThreshold(DISABLED_GUARDRAIL, MINIMUM_KEYSPACE_RF_FAIL_THRESHOLD);
        execute("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 3}");
        assertFails("ALTER KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 1}",
                    format("Keyspaces with %s equal to 1 exceeds fail threshold of %d", WHAT, MINIMUM_KEYSPACE_RF_FAIL_THRESHOLD));
    }

    @Test
    public void testMinKeyspaceRFWarnAbove() throws Throwable
    {
        guardrails().setMinimumKeyspaceRFThreshold(MINIMUM_KEYSPACE_RF_WARN_THRESHOLD, MINIMUM_KEYSPACE_RF_FAIL_THRESHOLD);
        assertMinThresholdValid("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 6}");
        assertMinThresholdValid("ALTER KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 5}");
    }

    @Test
    public void testMinKeyspaceRFWarnFailBetween() throws Throwable
    {
        guardrails().setMinimumKeyspaceRFThreshold(MINIMUM_KEYSPACE_RF_WARN_THRESHOLD, MINIMUM_KEYSPACE_RF_FAIL_THRESHOLD);
        assertWarns("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 3}",
                    format("Keyspaces with %s equal to 3 exceeds warn threshold of %d", WHAT, MINIMUM_KEYSPACE_RF_WARN_THRESHOLD));
        assertWarns("ALTER KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 2}",
                    format("Keyspaces with %s equal to 2 exceeds warn threshold of %d", WHAT, MINIMUM_KEYSPACE_RF_WARN_THRESHOLD));
    }

    @Test
    public void testMinKeyspaceRFFailBelow() throws Throwable
    {
        guardrails().setMinimumKeyspaceRFThreshold(MINIMUM_KEYSPACE_RF_WARN_THRESHOLD, MINIMUM_KEYSPACE_RF_FAIL_THRESHOLD);
        assertFails("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 1}",
                    format("Keyspaces with %s equal to 1 exceeds fail threshold of %d", WHAT, MINIMUM_KEYSPACE_RF_FAIL_THRESHOLD));
    }

    @Test
    public void testMinKeyspaceRFFailBelowAlter() throws Throwable
    {
        guardrails().setMinimumKeyspaceRFThreshold(MINIMUM_KEYSPACE_RF_WARN_THRESHOLD, MINIMUM_KEYSPACE_RF_FAIL_THRESHOLD);
        execute("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 4}");
        assertFails("ALTER KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 1}",
                    format("Keyspaces with %s equal to 1 exceeds fail threshold of %d", WHAT, MINIMUM_KEYSPACE_RF_FAIL_THRESHOLD));
    }

    @Test
    public void testMinRFGreaterThanDefaultRF()
    {
        try
        {
            DatabaseDescriptor.setDefaultKeyspaceRF(1);
            guardrails().setMinimumKeyspaceRFThreshold(MINIMUM_KEYSPACE_RF_WARN_THRESHOLD, MINIMUM_KEYSPACE_RF_FAIL_THRESHOLD);
        }
        catch (ConfigurationException e)
        {
            String expectedMessage = "";

            if(guardrails().getMinimumKeyspaceRFFailThreshold() > DatabaseDescriptor.getDefaultKeyspaceRF())
                expectedMessage = format("%s_fail_threshold to be set (%d) cannot be greater than default_keyspace_rf (%d)",
                                         WHAT, guardrails().getMinimumKeyspaceRFFailThreshold(), DatabaseDescriptor.getDefaultKeyspaceRF());
            Assertions.assertThat(e.getMessage()).contains(expectedMessage);
        }

    }
}