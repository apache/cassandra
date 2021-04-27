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


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static java.lang.String.format;

/**
 * Tests the guardrail for max number of fields in a UDT.
 */
public class GuardrailFieldsPerUDTTest extends GuardrailTester
{
    private static final long FIELDS_PER_UDT_THRESHOLD = 2;

    private long defaultFieldsPerUDTThreshold;

    @Before
    public void before()
    {
        defaultFieldsPerUDTThreshold = config().fields_per_udt_failure_threshold;
        config().fields_per_udt_failure_threshold = FIELDS_PER_UDT_THRESHOLD;
    }

    @After
    public void after()
    {
        config().fields_per_udt_failure_threshold = defaultFieldsPerUDTThreshold;
    }

    @Test
    public void testConfigValidation()
    {
        testValidationOfStrictlyPositiveProperty((c, v) -> c.fields_per_udt_failure_threshold = v,
                                                 "fields_per_udt_failure_threshold");
    }

    @Test
    public void testCreateType() throws Throwable
    {
        assertValid("CREATE TYPE %s (a int)");
        assertValid("CREATE TYPE %s (a int, b int)");
        assertFails("CREATE TYPE %s (a int, b int, c int)", 3);
        assertFails("CREATE TYPE %s (a int, b int, c int, d int)", 4);
    }

    @Test
    public void testAlterTypeAddField() throws Throwable
    {
        String name = createType("CREATE TYPE %s (a int)");

        assertValid("ALTER TYPE %s ADD b int", name);
        assertFails("ALTER TYPE %s ADD c int", name, 3);
    }

    @Test
    public void testExcludedUsers() throws Throwable
    {
        String name = createTypeName();
        testExcludedUsers(format("CREATE TYPE %s (a int, b int, c int)", name),
                          format("ALTER TYPE %s ADD d int", name),
                          format("DROP TYPE %s", name));
    }

    private void assertValid(String query) throws Throwable
    {
        assertValid(query, createTypeName());
    }

    private void assertValid(String query, String typeName) throws Throwable
    {
        super.assertValid(format(query, typeName));
    }

    private void assertFails(String query, int numFields) throws Throwable
    {
        String typeName = createTypeName();
        assertFails(query, typeName, numFields);
    }

    private void assertFails(String query, String typeName, int numFields) throws Throwable
    {
        String errorMessage = format("User types cannot have more than %s columns, but %s provided for type %s",
                                     DatabaseDescriptor.getGuardrailsConfig().fields_per_udt_failure_threshold,
                                     numFields,
                                     typeName);
        assertFails(errorMessage, format(query, typeName));
    }
}