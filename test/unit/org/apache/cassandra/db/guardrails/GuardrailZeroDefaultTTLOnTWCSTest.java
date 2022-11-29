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

import org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy;

public class GuardrailZeroDefaultTTLOnTWCSTest extends GuardrailTester
{
    private static final String QUERY = "CREATE TABLE IF NOT EXISTS tb1 (k int PRIMARY KEY, a int, b int) " +
                                        "WITH default_time_to_live = 0 " +
                                        "AND compaction = {'class': 'TimeWindowCompactionStrategy', 'enabled': true }";

    private static final String VALID_QUERY_1 = "CREATE TABLE IF NOT EXISTS tb2 (k int PRIMARY KEY, a int, b int) " +
                                                "WITH default_time_to_live = 1 " +
                                                "AND compaction = {'class': 'TimeWindowCompactionStrategy', 'enabled': true }";

    private static final String VALID_QUERY_2 = "CREATE TABLE IF NOT EXISTS tb3 (k int PRIMARY KEY, a int, b int) " +
                                                "WITH default_time_to_live = 0";

    public GuardrailZeroDefaultTTLOnTWCSTest()
    {
        super(Guardrails.zeroTTLOnTWCSEnabled);
    }

    @Test
    public void testGuardrailDisabled() throws Throwable
    {
        prepareTest(false, true);
        assertFails(QUERY, "0 default_time_to_live on a table with " +
                           TimeWindowCompactionStrategy.class.getSimpleName() +
                           " compaction strategy is not allowed");
    }

    @Test
    public void testGuardrailEnabledWarnEnabled() throws Throwable
    {
        prepareTest(true, true);
        assertWarns(QUERY, "0 default_time_to_live on a table with " +
                           TimeWindowCompactionStrategy.class.getSimpleName() +
                           " compaction strategy is not recommended");
    }

    @Test
    public void testGuardrailEnabledWarnDisabled() throws Throwable
    {
        prepareTest(true, false);
        assertValid(QUERY);
    }

    @Test
    public void testGuardrailNotTriggered() throws Throwable
    {
        prepareTest(true, true);
        assertValid(VALID_QUERY_1);
        assertValid(VALID_QUERY_2);

        prepareTest(false, true);
        assertValid(VALID_QUERY_1);
        assertValid(VALID_QUERY_2);
    }

    @Test
    public void testExcludedUsers() throws Throwable
    {
        for (boolean enabled : new boolean[] { false, true })
        {
            for (boolean warned : new boolean[]{ false, true })
            {
                prepareTest(enabled, warned);
                testExcludedUsers(() -> QUERY,
                                  () -> VALID_QUERY_1,
                                  () -> VALID_QUERY_2);
            }
        }
    }

    private void prepareTest(boolean enabled, boolean warned)
    {
        guardrails().setZeroTTLOnTWCSEnabled(enabled);
        guardrails().setZeroTTLOnTWCSWarned(warned);
    }
}
