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
import java.util.Collection;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests the guardrail for read-before-write list operations, {@link Guardrails#readBeforeWriteListOperationsEnabled}.
 */
@RunWith(Parameterized.class)
public class GuardrailReadBeforeWriteListOperationsTest extends GuardrailTester
{
    @Parameterized.Parameter
    public boolean enabled;

    @Parameterized.Parameters(name = "read_before_write_list_operations_enabled={0}")
    public static Collection<Object> data()
    {
        return Arrays.asList(false, true);
    }

    public GuardrailReadBeforeWriteListOperationsTest()
    {
        super(Guardrails.readBeforeWriteListOperationsEnabled);
    }

    @Before
    public void before()
    {
        guardrails().setReadBeforeWriteListOperationsEnabled(enabled);
        Assert.assertEquals(enabled, guardrails().getReadBeforeWriteListOperationsEnabled());

        createTable("CREATE TABLE %s (k int PRIMARY KEY, l list<int>)");
    }

    @Test
    public void tesInsertFullValue() throws Throwable
    {
        // insert from scratch
        assertValid("INSERT INTO %s (k, l) VALUES (0, [1, 2])");
        assertRows(row(0, list(1, 2)));

        // insert overriding previous value
        assertValid("INSERT INTO %s (k, l) VALUES (0, [2, 3])");
        assertRows(row(0, list(2, 3)));
    }

    @Test
    public void testUpdateFullValue() throws Throwable
    {
        // update from scratch
        assertValid("UPDATE %s SET l = [1, 2] WHERE k = 0");
        assertRows(row(0, list(1, 2)));

        // update overriding previous value
        assertValid("UPDATE %s SET l = [2, 3] WHERE k = 0");
        assertRows(row(0, list(2, 3)));
    }

    @Test
    public void testDeleteFullValue() throws Throwable
    {
        assertValid("INSERT INTO %s (k, l) VALUES (0, [1, 2])");
        assertValid("DELETE l FROM %s WHERE k = 0");
        assertRows(row(0, null));
    }

    @Test
    public void testAppend() throws Throwable
    {
        assertValid("INSERT INTO %s (k, l) VALUES (0, [1, 2])");
        assertValid("UPDATE %s SET l = l + [3, 4] WHERE k = 0");
        assertRows(row(0, list(1, 2, 3, 4)));
    }

    @Test
    public void testPrepend() throws Throwable
    {
        assertValid("INSERT INTO %s (k, l) VALUES (0, [1, 2])");
        assertValid("UPDATE %s SET l = [3, 4] + l WHERE k = 0");
        assertRows(row(0, list(3, 4, 1, 2)));
    }

    @Test
    public void testUpdateByIndex() throws Throwable
    {
        assertValid("INSERT INTO %s (k, l) VALUES (0, [1, 2, 3])");
        testGuardrail("UPDATE %s SET l[1] = 4 WHERE k = 0",
                      "Setting of list items by index requiring read before write is not allowed",
                      row(0, list(1, 4, 3)));
    }

    @Test
    public void testDeleteByIndex() throws Throwable
    {
        assertValid("INSERT INTO %s (k, l) VALUES (0, [1, 2, 3])");
        testGuardrail("DELETE l[1] FROM %s WHERE k = 0",
                      "Removal of list items by index requiring read before write is not allowed",
                      row(0, list(1, 3)));
    }

    @Test
    public void testDeleteByItem() throws Throwable
    {
        assertValid("INSERT INTO %s (k, l) VALUES (0, [1, 2, 3])");
        testGuardrail("UPDATE %s SET l = l - [2] WHERE k = 0",
                      "Removal of list items requiring read before write is not allowed",
                      row(0, list(1, 3)));
    }

    @Test
    public void testBatch() throws Throwable
    {
        assertValid("INSERT INTO %s (k, l) VALUES (0, [1, 2, 3])");

        testGuardrail("BEGIN BATCH UPDATE %s SET l[1] = 0 WHERE k = 0; APPLY BATCH",
                      "Setting of list items by index requiring read before write is not allowed",
                      row(0, list(1, 0, 3)));

        testGuardrail("BEGIN BATCH DELETE l[1] FROM %s WHERE k = 0; APPLY BATCH",
                      "Removal of list items by index requiring read before write is not allowed",
                      row(0, list(1, 3)));

        testGuardrail("BEGIN BATCH UPDATE %s SET l = l - [3] WHERE k = 0; APPLY BATCH",
                      "Removal of list items requiring read before write is not allowed",
                      row(0, list(1)));
    }

    @Test
    public void testExcludedUsers() throws Throwable
    {
        testExcludedUsers(() -> "INSERT INTO %s (k, l) VALUES (0, [1, 2, 3, 4, 5])",
                          () -> "UPDATE %s SET l[1] = 4 WHERE k = 0",
                          () -> "DELETE l[1] FROM %s WHERE k = 0",
                          () -> "INSERT INTO %s (k, l) VALUES (0, [1, 2, 3])",
                          () -> "UPDATE %s SET l = l - [2] WHERE k = 0",
                          () -> "BEGIN BATCH UPDATE %s SET l[1] = 0 WHERE k = 0; APPLY BATCH",
                          () -> "BEGIN BATCH DELETE l[1] FROM %s WHERE k = 0; APPLY BATCH",
                          () -> "BEGIN BATCH UPDATE %s SET l = l - [3] WHERE k = 0; APPLY BATCH");
    }

    private void testGuardrail(String query, String expectedMessage, Object[]... rows) throws Throwable
    {
        if (enabled)
        {
            assertValid(query);
            assertRows(rows);
        }
        else
        {
            assertFails(query, expectedMessage);
        }
    }

    private void assertRows(Object[]... rows) throws Throwable
    {
        assertRowsNet(executeNet("SELECT * FROM %s"), rows);
    }
}
