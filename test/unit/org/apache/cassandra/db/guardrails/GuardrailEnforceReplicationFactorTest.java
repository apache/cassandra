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

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static java.lang.String.format;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GuardrailEnforceReplicationFactorTest extends GuardrailTester
{
    private static final int DISABLED_GUARDRAIL = -1;
    private static final int ENFORCE_RF = 3;
    private static final String KS = "ks";
    private static final String DATACENTER1 = "datacenter1";


    @After
    public void cleanupTest() throws Throwable
    {
        execute(format("DROP KEYSPACE IF EXISTS %s", KS));
    }

    @Test
    public void testEnforceRFNetworkTopology() throws Throwable
    {
        DatabaseDescriptor.setDefaultKeyspaceRF(ENFORCE_RF);
        Guardrails.instance.setMaximumReplicationFactorThreshold(DISABLED_GUARDRAIL, ENFORCE_RF);
        Guardrails.instance.setMinimumReplicationFactorThreshold(DISABLED_GUARDRAIL, ENFORCE_RF);
        // Valid
        execute(userClientState, format("CREATE KEYSPACE %s WITH replication = { 'class': 'NetworkTopologyStrategy', '%s': 3 }", KS, DATACENTER1));
        cleanupTest();

        // Fail on < 3 (thrown by minRF guardrail)
        try
        {
            execute(userClientState, format("CREATE KEYSPACE %s WITH replication = { 'class': 'NetworkTopologyStrategy', '%s': 1}", KS, DATACENTER1));
            fail("expecting exception thrown");
        }
        catch (GuardrailViolatedException e)
        {
            assertTrue(e.getMessage().contains(format("The keyspace %s has a replication factor of 1, below the failure threshold of %s.", KS, ENFORCE_RF)));
        }
        finally
        {
            cleanupTest();
        }

        // Fail on > 3 (thrown by maxRF guardrail)
        try
        {
            execute(userClientState, format("CREATE KEYSPACE %s WITH replication = { 'class': 'NetworkTopologyStrategy', '%s': 5}", KS, DATACENTER1));
            fail("expecting exception thrown");
        }
        catch (GuardrailViolatedException e)
        {
            assertTrue(e.getMessage().contains(format("The keyspace %s has a replication factor of 5, above the failure threshold of %s.", KS, ENFORCE_RF)));
        }
        finally
        {
            cleanupTest();
        }
    }

    @Test
    public void testEnforceRFSimpleTopology() throws Throwable
    {
        DatabaseDescriptor.setDefaultKeyspaceRF(ENFORCE_RF);
        Guardrails.instance.setMaximumReplicationFactorThreshold(DISABLED_GUARDRAIL, ENFORCE_RF);
        Guardrails.instance.setMinimumReplicationFactorThreshold(DISABLED_GUARDRAIL, ENFORCE_RF);
        // Valid
        execute(userClientState, format("CREATE KEYSPACE %s WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 3 }", KS));
        cleanupTest();

        // Fail on < 3 (thrown by minRF guardrail)
        try
        {
            execute(userClientState, format("CREATE KEYSPACE %s WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 }", KS));
            fail("expecting exception thrown");
        }
        catch (GuardrailViolatedException e)
        {
            assertTrue(e.getMessage().contains(format("The keyspace %s has a replication factor of 1, below the failure threshold of %s.", KS, ENFORCE_RF)));
        }
        finally
        {
            cleanupTest();
        }

        // Fail on > 3 (thrown by maxRF guardrail)
        try
        {
            execute(userClientState, format("CREATE KEYSPACE %s WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 5}", KS));
            fail("expecting exception thrown");
        }
        catch (GuardrailViolatedException e)
        {
            assertTrue(e.getMessage().contains(format("The keyspace %s has a replication factor of 5, above the failure threshold of %s.", KS, ENFORCE_RF)));
        }
        finally
        {
            cleanupTest();
        }
    }
}
