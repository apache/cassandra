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

import java.util.List;
import java.util.function.Supplier;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.lang.String.format;

/**
 * Tests the guardrail for the usage of the vector type, {@link Guardrails#vectorTypeEnabled}.
 */
@RunWith(Parameterized.class)
public class GuardrailVectorTypeEnabledTest extends GuardrailTester
{
    private final boolean enabled;

    @Parameterized.Parameters(name = "enabled={0}")
    public static List<Object[]> params()
    {
        return List.of(
            new Object[]{true},
            new Object[]{false}
        );
    }

    public GuardrailVectorTypeEnabledTest(boolean enabled)
    {
        super(Guardrails.vectorTypeEnabled);
        this.enabled = enabled;
        Guardrails.instance.setVectorTypeEnabled(enabled);
    }

    @Test
    public void testCreateTable() throws Throwable
    {
        for (String cql : GuardrailVectorDimensionsTest.CREATE_TABLE_CQL)
            testCreateTable(cql);
    }

    private void testCreateTable(String query) throws Throwable
    {
        testColumn(() -> format(query, createTableName()));
    }

    @Test
    public void testCreateType() throws Throwable
    {
        for (String cql : GuardrailVectorDimensionsTest.CREATE_TYPE_CQL)
            testCreateType(cql);;
    }

    private void testCreateType(String query) throws Throwable
    {
        testField(() -> format(query, createTypeName()));
    }

    @Test
    public void testAlterTable() throws Throwable
    {
        for (String cql : GuardrailVectorDimensionsTest.ALTER_TABLE_CQL)
            testAlterTable(cql);
    }

    private void testAlterTable(String query) throws Throwable
    {
        testColumn(() -> {
            createTable("CREATE TABLE %s (k int PRIMARY KEY)");
            return format(query, currentTable());
        });
    }

    @Test
    public void testAlterType() throws Throwable
    {
        for (String cql : GuardrailVectorDimensionsTest.ALTER_TYPE_CQL)
            testAlterType(cql);
    }

    private void testAlterType(String query) throws Throwable
    {
        testField(() -> {
            String name = createType("CREATE TYPE %s (c int)");
            return format(query, name);
        });
    }

    @Test
    public void testExcludedUsers() throws Throwable
    {
        testExcludedUsers(() -> format("CREATE TABLE %s (k int PRIMARY KEY, v vector<int, 1000>)", createTableName()),
                          () -> format("CREATE TYPE %s (c int, v vector<int, 1000>)", createTypeName()),
                          () -> format("ALTER TABLE %s ADD v vector<int, 1000>",
                                       createTable("CREATE TABLE %s (k int PRIMARY KEY)")),
                          () -> format("ALTER TYPE %s ADD v vector<int, 1000>",
                                       createType("CREATE TYPE %s (c int)")));
    }

    private void testColumn(Supplier<String> query) throws Throwable
    {
        testGuardrail(query, "Column v");
    }

    private void testField(Supplier<String> query) throws Throwable
    {
        testGuardrail(query, "Field v");
    }

    private void testGuardrail(Supplier<String> query, String what) throws Throwable
    {
        if (enabled)
            assertValid(query.get(), 1);
        else
            assertFails(query.get(), what, 1);
    }

    private void assertValid(String query, int dimensions) throws Throwable
    {
        super.assertValid(format(query, dimensions));
    }

    private void assertFails(String query, String what, int dimensions) throws Throwable
    {
        assertFails(format(query, dimensions),
                    what + " is not allowed");
    }
}
