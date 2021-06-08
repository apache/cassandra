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

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.config.DatabaseDescriptor;

public class GuardrailLoggedBatchTest extends GuardrailTester
{
    private static boolean loggedBatchEnabled;

    @BeforeClass
    public static void setup()
    {
        loggedBatchEnabled = DatabaseDescriptor.getGuardrailsConfig().logged_batch_enabled;
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.getGuardrailsConfig().logged_batch_enabled = loggedBatchEnabled;
    }

    @Before
    public void setupTest()
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (k INT, c INT, v TEXT, PRIMARY KEY(k, c))");
    }

    private void setGuardrails(boolean logged_batch_enabled)
    {
        DatabaseDescriptor.getGuardrailsConfig().logged_batch_enabled = logged_batch_enabled;
    }

    private void insertBatchAndAssertValid(boolean loggedBatchEnabled, boolean logged) throws Throwable
    {
        setGuardrails(loggedBatchEnabled);

        BatchStatement batch = new BatchStatement(logged ? BatchStatement.Type.LOGGED : BatchStatement.Type.UNLOGGED);
        batch.add(new SimpleStatement(String.format("INSERT INTO %s.%s (k, c, v) VALUES (1, 2, 'val')", keyspace(), currentTable())));
        batch.add(new SimpleStatement(String.format("INSERT INTO %s.%s (k, c, v) VALUES (3, 4, 'val')", keyspace(), currentTable())));

        assertValid(batch);
    }

    @Test
    public void testInsertUnloggedBatch() throws Throwable
    {
        insertBatchAndAssertValid(false, false);
        insertBatchAndAssertValid(true, false);
    }

    @Test(expected = InvalidQueryException.class)
    public void testDisabledLoggedBatch() throws Throwable
    {
        insertBatchAndAssertValid(false, true);
    }

    @Test
    public void testEnabledLoggedBatch() throws Throwable
    {
        insertBatchAndAssertValid(true, true);
    }
}
