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

import static java.lang.String.format;

/**
 * Tests the guardrail for the number of fields in a user-defined type, {@link Guardrails#fieldsPerUDT}.
 */
public class GuardrailFieldsPerUDTTest extends ThresholdTester
{
    private static final int WARN_THRESHOLD = 2;
    private static final int FAIL_THRESHOLD = 4;

    public GuardrailFieldsPerUDTTest()
    {
        super(WARN_THRESHOLD,
              FAIL_THRESHOLD,
              Guardrails.fieldsPerUDT,
              Guardrails::setFieldsPerUDTThreshold,
              Guardrails::getFieldsPerUDTWarnThreshold,
              Guardrails::getFieldsPerUDTFailThreshold);
    }

    @Test
    public void testCreateType() throws Throwable
    {
        assertValid("CREATE TYPE %s (a int)");
        assertValid("CREATE TYPE %s (a int, b int)");
        assertWarns("CREATE TYPE %s (a int, b int, c int)", 3);
        assertWarns("CREATE TYPE %s (a int, b int, c int, d int)", 4);
        assertFails("CREATE TYPE %s (a int, b int, c int, d int, e int)", 5);
        assertFails("CREATE TYPE %s (a int, b int, c int, d int, e int, f int)", 6);
    }

    @Test
    public void testAlterType() throws Throwable
    {
        String name = createType("CREATE TYPE %s (a int)");

        assertValid("ALTER TYPE %s ADD b int", name);
        assertWarns("ALTER TYPE %s ADD c int", name, 3);
        assertWarns("ALTER TYPE %s ADD d int", name, 4);
        assertFails("ALTER TYPE %s ADD e int", name, 5);
    }

    @Test
    public void testExcludedUsers() throws Throwable
    {
        String name = createTypeName();
        testExcludedUsers(() -> format("CREATE TYPE %s (a int, b int, c int, d int, e int)", name),
                          () -> format("ALTER TYPE %s ADD f int", name),
                          () -> format("DROP TYPE %s", name));
    }

    protected void assertValid(String query) throws Throwable
    {
        assertValid(query, createTypeName());
    }

    private void assertValid(String query, String typeName) throws Throwable
    {
        super.assertValid(format(query, typeName));
    }

    private void assertWarns(String query, int numFields) throws Throwable
    {
        String typeName = createTypeName();
        assertWarns(query, typeName, numFields);
    }

    private void assertWarns(String query, String typeName, int numFields) throws Throwable
    {
        assertWarns(format(query, typeName),
                    format("The user type %s has %s columns, this exceeds the warning threshold of %s.",
                           typeName, numFields, WARN_THRESHOLD));
    }

    private void assertFails(String query, int numFields) throws Throwable
    {
        String typeName = createTypeName();
        assertFails(query, typeName, numFields);
    }

    private void assertFails(String query, String typeName, int numFields) throws Throwable
    {
        assertFails(format(query, typeName),
                    format("User types cannot have more than %s columns, but %s provided for user type %s",
                           FAIL_THRESHOLD, numFields, typeName));
    }
}
