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

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.Pair;

import static java.lang.String.format;
import static org.apache.cassandra.db.guardrails.ValueGenerator.GENERATOR_CLASS_NAME_KEY;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests mixture of role name and password generators.
 */
public class GenerationalGuardrailsTest extends AbstractGenerationalTest
{
    @Test
    public void testMixedGeneration()
    {
        configureGuardrails();
        ValueGenerator<String> passwordGenerator = Guardrails.passwordPolicy.getGenerator();
        ValueGenerator<String> roleNameGenerator = Guardrails.roleNamePolicy.getGenerator();

        Guardrails.passwordPolicy.validate(extractGeneratedRoleName(execute(userClientState, format("CREATE GENERATED ROLE WITH PASSWORD = '%s'", passwordGenerator.generate()))), userClientState);
        Guardrails.roleNamePolicy.validate(extractGeneratedPassword(execute(userClientState, format("CREATE ROLE %s WITH GENERATED PASSWORD", roleNameGenerator.generate()))), userClientState);
    }

    @Test
    public void testUsageWhenBothEnabled()
    {
        assertThatThrownBy(() -> execute(userClientState, "CREATE GENERATED ROLE WITH GENERATED PASSWORD"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("You have to enable role_name_policy and its generator_class_name property in cassandra.yaml to be able to generate role names.");

        configureRoleNamePolicy();

        assertThatThrownBy(() -> execute(userClientState, "CREATE GENERATED ROLE WITH GENERATED PASSWORD"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("You have to enable password_policy and its generator_class_name property in cassandra.yaml to be able to generate passwords.");

        configurePasswordPolicy();

        Pair<String, String> generatedPasswordAndRole = extractPasswordAndRoleName(execute(userClientState, "CREATE GENERATED ROLE WITH GENERATED PASSWORD"));
        Guardrails.passwordPolicy.validate(generatedPasswordAndRole.left, userClientState);
        Guardrails.roleNamePolicy.validate(generatedPasswordAndRole.right, userClientState);
    }

    private void configureGuardrails()
    {
        configureRoleNamePolicy();
        configurePasswordPolicy();
    }

    private void configureRoleNamePolicy()
    {
        CustomGuardrailConfig roleNamePolicyConfig = new CustomGuardrailConfig();
        roleNamePolicyConfig.put(GENERATOR_CLASS_NAME_KEY, UUIDRoleNameGenerator.class.getName());

        setRoleNamePolicy(roleNamePolicyConfig);
    }

    private void configurePasswordPolicy()
    {
        CustomGuardrailConfig passwordPolicyConfig = new CustomGuardrailConfig();
        passwordPolicyConfig.put(GENERATOR_CLASS_NAME_KEY, CassandraPasswordGenerator.class.getName());

        setPasswordPolicy(passwordPolicyConfig);
    }
}
