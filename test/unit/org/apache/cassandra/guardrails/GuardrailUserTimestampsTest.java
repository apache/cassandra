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

package org.apache.cassandra.guardrails;


import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GuardrailUserTimestampsTest extends GuardrailTester
{
    public static final String NOT_ALLOWED_MSG = "User provided timestamps (USING TIMESTAMP) is not allowed";
    private static boolean userTimestampsEnabled;

    @BeforeClass
    public static void setup()
    {
        userTimestampsEnabled = DatabaseDescriptor.getGuardrailsConfig().user_timestamps_enabled;
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.getGuardrailsConfig().user_timestamps_enabled = userTimestampsEnabled;
    }

    @Before
    public void setupTest()
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (k INT, c INT, v TEXT, PRIMARY KEY(k, c))");
    }

    private void setGuardrails(boolean userTimestampsEnabled)
    {
        DatabaseDescriptor.getGuardrailsConfig().user_timestamps_enabled = userTimestampsEnabled;
    }

    private void insert(boolean userTimestampsEnabled) throws Throwable
    {
        setGuardrails(userTimestampsEnabled);
        assertValid("INSERT INTO %s (k, c, v) VALUES (1, 2, 'val') USING TIMESTAMP 1");
    }

    @Test
    public void testInsertWithDisabledUserTimestamps()
    {
        assertThatThrownBy(() -> insert(false))
        .hasMessage(NOT_ALLOWED_MSG);
    }

    @Test
    public void testInsertWithEnabledUserTimestamps() throws Throwable
    {
        // test that it does not throw
        insert(true);
    }

    private void update(boolean userTimestampsEnabled) throws Throwable
    {
        setGuardrails(userTimestampsEnabled);
        assertValid("UPDATE %s USING TIMESTAMP 1 SET v = 'val2' WHERE k = 1 and c = 2");
    }

    @Test
    public void testUpdateWithDisabledUserTimestamps() throws Throwable
    {
        assertThatThrownBy(() -> update(false))
        .hasMessage(NOT_ALLOWED_MSG);
    }

    @Test
    public void testUpdateWithEnabledUserTimestamps() throws Throwable
    {
        // test that it does not throw
        update(true);
    }

    private void delete(boolean userTimestampsEnabled) throws Throwable
    {
        setGuardrails(userTimestampsEnabled);
        assertValid("DELETE FROM %s USING TIMESTAMP 1 WHERE k=1");
    }

    @Test
    public void testDeleteWithDisabledUserTimestamps()
    {
        assertThatThrownBy(() -> delete(false))
        .hasMessage(NOT_ALLOWED_MSG);
    }

    @Test
    public void testDeleteWithEnabledUserTimestamps() throws Throwable
    {
        // test that it does not throw
        delete(true);
    }

    private void batch(boolean userTimestampsEnabled) throws Throwable
    {
        setGuardrails(userTimestampsEnabled);
        assertValid("BEGIN BATCH USING TIMESTAMP 1 " +
                    "INSERT INTO %s (k, c, v) VALUES (1, 2, 'val') " +
                    "APPLY BATCH");
        assertValid("BEGIN BATCH " +
                    "INSERT INTO %s (k, c, v) VALUES (1, 2, 'val') USING TIMESTAMP 1 " +
                    "APPLY BATCH");
    }

    @Test
    public void testBatchWithDisabledUserTimestamps()
    {
        assertThatThrownBy(() -> batch(false))
        .hasMessage(NOT_ALLOWED_MSG);
    }

    @Test
    public void testBatchWithEnabledUserTimestamps() throws Throwable
    {
        // test that it does not throw
        batch(true);
    }

    @Test
    public void testExcludedUsers() throws Throwable
    {
        for (boolean userTimestampsEnabled : new boolean[]{ false, true })
        {
            setGuardrails(userTimestampsEnabled);
            testExcludedUsers("INSERT INTO %s (k, c, v) VALUES (1, 2, 'val') USING TIMESTAMP 1",
                              "UPDATE %s USING TIMESTAMP 1 SET v = 'val2' WHERE k = 1 and c = 2",
                              "DELETE FROM %s USING TIMESTAMP 1 WHERE k=1",
                              "BEGIN BATCH USING TIMESTAMP 1 INSERT INTO %s (k, c, v) VALUES (1, 2, 'v'); APPLY BATCH",
                              "BEGIN BATCH INSERT INTO %s (k, c, v) VALUES (1, 2, 'v') USING TIMESTAMP 1; APPLY BATCH");
        }
    }
}
