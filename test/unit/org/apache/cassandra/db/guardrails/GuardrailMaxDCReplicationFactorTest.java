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

import org.junit.Test;

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

    @Override
    protected long currentValue(){return 0;}

    @Test
    public void testMaxDCRFDisabled() throws Throwable
    {
        guardrails().setMaxDCReplicationFactorThreshold(DISABLED_GUARDRAIL, DISABLED_GUARDRAIL);
        assertThresholdValid("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 6");
    }

    @Test
    public void testMaxDCRFOnlyWarnBelow()
    {

    }

    @Test
    public void testMaxDCRFOnlyWarnAbove()
    {

    }

    @Test
    public void testMaxDCRFOnlyFailBelow()
    {

    }

    @Test
    public void testMaxDCRFOnlyFailAbove()
    {

    }

    @Test
    public void testMaxDCRFBelowWarn() throws Throwable
    {
        assertThresholdValid("CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 2");
    }

    @Test
    public void testMaxDCRFBetweenWarnFail() throws Throwable
    {
        assertThresholdWarns("", "CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 3");
    }

    @Test
    public void testMaxDCRFAboveFail() throws Throwable
    {
        assertThresholdFails("", "CREATE KEYSPACE ks WITH replication = { 'class': 'NetworkTopologyStrategy', 'datacenter1': 4");
    }
}
