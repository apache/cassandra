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
import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.index.sasi.SASIIndex;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.service.ClientWarn;

public class GuardrailMaxDCReplicationFactorTest extends ThresholdTester
{
    private static final int MAX_DC_REPLICATION_FACTOR_WARN_THRESHOLD = 2;
    private static final int MAX_DC_REPLICATION_FACTOR_FAIL_THRESHOLD = 3;
    private static final int DISABLED_GUARDRAIL = -1;

    public GuardrailMaxDCReplicationFactorTest()
    {
        super(MAX_DC_REPLICATION_FACTOR_WARN_THRESHOLD,
              MAX_DC_REPLICATION_FACTOR_FAIL_THRESHOLD,
              "maximum_dc_replication_factor",
              Guardrails::setMaxDCReplicationFactorThreshold,
              Guardrails::getMaxDCReplicationFactorWarnThreshold,
              Guardrails::getMaxDCReplicationFactorFailThreshold);
    }

    @After
    public void cleanupTest() throws Throwable
    {
        execute("DROP KEYSPACE ks");
    }

    @Override
    protected long currentValue()
    {
        return Long.parseLong(((NetworkTopologyStrategy) Keyspace.open("ks").getReplicationStrategy()).configOptions.get("datacenter1"));
    }

    @Override
    protected List<String> getWarnings()
    {
        List<String> warnings = ClientWarn.instance.getWarnings();

        return warnings == null
               ? Collections.emptyList()
               : warnings.stream()
                         .filter(w -> !w.contains("keyspace ks is higher than the number of nodes 1 for datacenter"))
                         .collect(Collectors.toList());
    }

    @Test
    public void testMaxDCRFDisabled() throws Throwable
    {
        guardrails().setMaxDCReplicationFactorThreshold(DISABLED_GUARDRAIL, DISABLED_GUARDRAIL);
        assertThresholdValid("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 6}");
    }

    @Test
    public void testMaxDCRFOnlyWarnBelow() throws Throwable
    {
        guardrails().setMaxDCReplicationFactorThreshold(2, DISABLED_GUARDRAIL);
        assertThresholdValid("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 2}");
    }

    @Test
    public void testMaxDCRFOnlyWarnAbove() throws Throwable
    {
        guardrails().setMaxDCReplicationFactorThreshold(2, DISABLED_GUARDRAIL);
        assertThresholdWarns("Keyspaces with exceeds warn threshold of", "CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 3}");
    }

    @Test
    public void testMaxDCRFOnlyFailBelow() throws Throwable
    {
        guardrails().setMaxDCReplicationFactorThreshold(DISABLED_GUARDRAIL, 3);
        assertThresholdValid("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 3}");
    }

    @Test
    public void testMaxDCRFOnlyFailAbove() throws Throwable
    {
        guardrails().setMaxDCReplicationFactorThreshold(DISABLED_GUARDRAIL, 3);
        assertThresholdFails("Keyspaces with datacenter1 exceeds fail threshold of 4", "CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 4}");
    }

    @Test
    public void testMaxDCRFWarnBelow() throws Throwable
    {
        assertThresholdValid("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 2}");
    }

    @Test
    public void testMaxDCRFWarnFailBetween() throws Throwable
    {
        assertThresholdWarns("Keyspaces with exceeds warn threshold of %s", "CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 3}");
    }

    @Test
    public void testMaxDCRFFailAbove() throws Throwable
    {
        assertThresholdFails("Keyspaces with datacenter1 exceeds fail threshold of 4", "CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 4}");
    }
}
