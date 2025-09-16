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

import java.util.regex.Pattern;

import org.junit.Test;

import org.apache.cassandra.auth.CassandraRoleManager;
import org.apache.cassandra.auth.IRoleManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;

import static java.util.Collections.singletonList;
import static org.apache.cassandra.db.guardrails.ValueGenerator.GENERATOR_CLASS_NAME_KEY;
import static org.apache.cassandra.db.guardrails.ValueValidator.VALIDATOR_CLASS_NAME_KEY;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GuardrailRoleNamePolicyTest extends AbstractGenerationalTest
{
    @Test
    public void testRoleNameValidation() throws Throwable
    {
        CustomGuardrailConfig config = new CustomGuardrailConfig();
        config.put(VALIDATOR_CLASS_NAME_KEY, TestRoleNameValidator.class.getName());

        setRoleNamePolicy(config);

        assertFails(() -> execute(userClientState, "CREATE ROLE withnumber1"),
                    false,
                    singletonList("Role name is not alphanumeric."),
                    singletonList("Role name is not alphanumeric."));

        assertFails(() -> execute(userClientState, "CREATE ROLE notalphanumeric_"),
                    true,
                    singletonList("Role name is not alphanumeric."),
                    singletonList("Role name is not alphanumeric."));
    }

    /**
     * Test what happens if generator generates invalid value for validator
     * This is normally treated in GuardrailsOptions constructor as a test it done there,
     * but it is the best-effort, as it might still generate something invalid eventually ...
     */
    @Test
    public void testInvalidRoleNameGeneration() throws Throwable
    {
        CustomGuardrailConfig config = new CustomGuardrailConfig();
        config.put(VALIDATOR_CLASS_NAME_KEY, TestRoleNameValidator.class.getName());
        config.put(GENERATOR_CLASS_NAME_KEY, InvalidRoleNameGenerator.class.getName());

        setRoleNamePolicy(config);

        assertFails(() -> execute(userClientState, "CREATE GENERATED ROLE"),
                    true,
                    singletonList("Role name is not alphanumeric."),
                    singletonList("Role name is not alphanumeric."));
    }

    @Test
    public void testInvalidCombinations()
    {
        assertThatThrownBy(() -> execute(userClientState, "CREATE GENERATED ROLE somerole"))
        .isInstanceOf(SyntaxException.class)
        .hasMessage("Name can not be specified together with GENERATED keyword.");
    }

    @Test
    public void testRoleNameGenerationWhenNotEnabled()
    {
        assertThatThrownBy(() -> execute(userClientState, "CREATE GENERATED ROLE"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("You have to enable role_name_policy and its generator_class_name property in cassandra.yaml to be able to generate role names.");
    }

    @Test
    public void testRoleNameGenerationWithOptions() throws Throwable
    {
        IRoleManager oldManager = DatabaseDescriptor.getRoleManager();

        try
        {
            setup();

            String withPrefix = extractGeneratedRoleName(execute(userClientState,
                                                                 "CREATE GENERATED ROLE WITH OPTIONS = {'name_prefix': 'company_'}"));

            assertTrue(withPrefix.startsWith("company_"));

            ///

            String query = "CREATE GENERATED ROLE WITH OPTIONS = {'name_suffix': '_read_only'}";
            String withSuffix = extractGeneratedRoleName(execute(userClientState, query));
            assertTrue(withSuffix.endsWith("_read_only"));

            ///

            query = "CREATE GENERATED ROLE WITH OPTIONS = {'name_prefix': 'company_', 'name_suffix': '_read_only'}";
            String withPrefixAndSuffix = extractGeneratedRoleName(execute(userClientState, query));
            assertTrue(withPrefixAndSuffix.startsWith("company_"));
            assertTrue(withPrefixAndSuffix.endsWith("_read_only"));

            ///

            query = "CREATE GENERATED ROLE WITH OPTIONS = {'name_prefix': 'company_', 'name_suffix': '_read_only', 'name_size': 10}";
            String withPrefixAndSuffixAndSize = extractGeneratedRoleName(execute(userClientState, query));
            assertTrue(withPrefixAndSuffixAndSize.startsWith("company_"));
            assertTrue(withPrefixAndSuffixAndSize.endsWith("_read_only"));

            Pattern pattern1 = Pattern.compile("company_");
            Pattern pattern2 = Pattern.compile("_read_only");

            String withoutPrefix = pattern1.matcher(withPrefixAndSuffixAndSize).replaceAll("");
            String onlyName = pattern2.matcher(withoutPrefix).replaceAll("");
            assertEquals(10, onlyName.length());

            ///

            query = "CREATE GENERATED ROLE WITH OPTIONS = {'name_size': 10}";
            assertEquals(10, extractGeneratedRoleName(execute(userClientState, query)).length());

            assertFails(() -> execute(userClientState, "CREATE GENERATED ROLE WITH OPTIONS = {'name_size': 6}"),
                        true,
                        singletonList("Value of name_size parameter has to be at least 10."),
                        singletonList("Value of name_size parameter has to be at least 10."));
        }
        finally
        {
            DatabaseDescriptor.setRoleManager(oldManager);
        }
    }

    @Test
    public void testRoleGenerationWithIfNotExists()
    {
        IRoleManager oldManager = DatabaseDescriptor.getRoleManager();
        try
        {
            setup();
            assertThatThrownBy(() -> execute(userClientState, "CREATE GENERATED ROLE IF NOT EXISTS"))
            .isInstanceOf(SyntaxException.class)
            .hasMessage("GENERATED keyword for role creation can not be used together with IF NOT EXISTS.");
        }
        finally
        {
            DatabaseDescriptor.setRoleManager(oldManager);
        }
    }

    private void setup()
    {
        // we need to have this set first so CassandraRoleManager sees we do not use NoOP,
        // so it will add OPTIONS among supported ones for us to be able to specify OPTIONS in queries
        CustomGuardrailConfig config = new CustomGuardrailConfig();
        config.put(GENERATOR_CLASS_NAME_KEY, UUIDRoleNameGenerator.class.getName());

        setRoleNamePolicy(config);

        CassandraRoleManager cassandraRoleManager = new CassandraRoleManager();
        cassandraRoleManager.setup(false);
        DatabaseDescriptor.setRoleManager(cassandraRoleManager);
    }
}
