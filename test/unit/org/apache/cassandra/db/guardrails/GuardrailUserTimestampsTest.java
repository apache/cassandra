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

import org.junit.Before;
import org.junit.Test;

/**
 * Tests the guardrail for disabling user-provided timestamps, {@link Guardrails#userTimestampsEnabled}.
 */
public class GuardrailUserTimestampsTest extends GuardrailTester
{
    public GuardrailUserTimestampsTest()
    {
        super(Guardrails.userTimestampsEnabled);
    }

    @Before
    public void setupTest()
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (k INT, c INT, v TEXT, PRIMARY KEY(k, c))");
    }

    private void setGuardrail(boolean userTimestampsEnabled)
    {
        guardrails().setUserTimestampsEnabled(userTimestampsEnabled);
    }

    @Test
    public void testInsertWithDisabledUserTimestamps() throws Throwable
    {
        setGuardrail(false);
        assertFails("INSERT INTO %s (k, c, v) VALUES (1, 2, 'val') USING TIMESTAMP 1");
    }

    @Test
    public void testInsertWithEnabledUserTimestamps() throws Throwable
    {
        setGuardrail(true);
        assertValid("INSERT INTO %s (k, c, v) VALUES (1, 2, 'val') USING TIMESTAMP 1");
    }

    @Test
    public void testUpdateWithDisabledUserTimestamps() throws Throwable
    {
        setGuardrail(false);
        assertFails("UPDATE %s USING TIMESTAMP 1 SET v = 'val2' WHERE k = 1 and c = 2");
    }

    @Test
    public void testUpdateWithEnabledUserTimestamps() throws Throwable
    {
        setGuardrail(true);
        assertValid("UPDATE %s USING TIMESTAMP 1 SET v = 'val2' WHERE k = 1 and c = 2");
    }

    @Test
    public void testDeleteWithDisabledUserTimestamps() throws Throwable
    {
        setGuardrail(false);
        assertFails("DELETE FROM %s USING TIMESTAMP 1 WHERE k=1");
    }

    @Test
    public void testDeleteWithEnabledUserTimestamps() throws Throwable
    {
        setGuardrail(true);
        assertValid("DELETE FROM %s USING TIMESTAMP 1 WHERE k=1");
    }

    @Test
    public void testBatchWithDisabledUserTimestamps() throws Throwable
    {
        setGuardrail(false);
        assertValid("BEGIN BATCH USING TIMESTAMP 1 " +
                    "INSERT INTO %s (k, c, v) VALUES (1, 2, 'val') " +
                    "APPLY BATCH");
        assertFails("BEGIN BATCH " +
                    "INSERT INTO %s (k, c, v) VALUES (1, 2, 'val') USING TIMESTAMP 1 " +
                    "APPLY BATCH");
    }

    @Test
    public void testBatchWithEnabledUserTimestamps() throws Throwable
    {
        setGuardrail(true);
        assertValid("BEGIN BATCH USING TIMESTAMP 1 " +
                    "INSERT INTO %s (k, c, v) VALUES (1, 2, 'val') " +
                    "APPLY BATCH");
        assertValid("BEGIN BATCH " +
                    "INSERT INTO %s (k, c, v) VALUES (1, 2, 'val') USING TIMESTAMP 1 " +
                    "APPLY BATCH");
    }

    @Test
    public void testExcludedUsers() throws Throwable
    {
        for (boolean userTimestampsEnabled : new boolean[]{ false, true })
        {
            setGuardrail(userTimestampsEnabled);
            testExcludedUsers(() -> "INSERT INTO %s (k, c, v) VALUES (1, 2, 'val') USING TIMESTAMP 1",
                              () -> "UPDATE %s USING TIMESTAMP 1 SET v = 'val2' WHERE k = 1 and c = 2",
                              () -> "DELETE FROM %s USING TIMESTAMP 1 WHERE k=1",
                              () -> "BEGIN BATCH USING TIMESTAMP 1 INSERT INTO %s (k, c, v) VALUES (1, 2, 'v'); APPLY BATCH",
                              () -> "BEGIN BATCH INSERT INTO %s (k, c, v) VALUES (1, 2, 'v') USING TIMESTAMP 1; APPLY BATCH");
        }
    }

    private void assertFails(String query) throws Throwable
    {
        assertFails(query, "User provided timestamps (USING TIMESTAMP) is not allowed");
    }
}
