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

package org.apache.cassandra.auth;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link PasswordDefaultRoleInitializer} and {@link MutualTlsDefaultRoleInitializer} that don't
 * require a running node: unsupported-parameter rejection at construction time, and the parameter-presence checks
 * in {@link IDefaultRoleInitializer#validateConfiguration()}. The authenticator-compatibility half of
 * {@link MutualTlsDefaultRoleInitializer#validateConfiguration()} needs a configured {@code DatabaseDescriptor}
 * authenticator and is covered by {@link AuthConfigTest} instead.
 */
public class DefaultRoleInitializerTest
{
    @Test
    public void passwordInitializerRejectsUnsupportedParameter()
    {
        assertThatThrownBy(() -> new PasswordDefaultRoleInitializer(Map.of("bogus", "x")))
            .isInstanceOf(ConfigurationException.class)
            .hasMessageContaining("Unsupported parameter");
    }

    @Test
    public void passwordInitializerDefaultsPassValidation()
    {
        new PasswordDefaultRoleInitializer().validateConfiguration();
        new PasswordDefaultRoleInitializer(Collections.emptyMap()).validateConfiguration();
    }

    @Test
    public void passwordInitializerRejectsEmptyRole()
    {
        assertThatThrownBy(() -> new PasswordDefaultRoleInitializer(Map.of("role", "", "password", "x")).validateConfiguration())
            .isInstanceOf(ConfigurationException.class)
            .hasMessageContaining("role");
    }

    @Test
    public void passwordInitializerRejectsEmptyPassword()
    {
        assertThatThrownBy(() -> new PasswordDefaultRoleInitializer(Map.of("role", "cassandra", "password", "")).validateConfiguration())
            .isInstanceOf(ConfigurationException.class)
            .hasMessageContaining("password");
    }

    @Test
    public void passwordInitializerRejectsBothPasswordAndHash()
    {
        assertThatThrownBy(() -> new PasswordDefaultRoleInitializer(Map.of("password", "x", "password_hash", "$2a$04$abc")).validateConfiguration())
            .isInstanceOf(ConfigurationException.class)
            .hasMessageContaining("Only one of password, password_hash can be specified.");
    }

    @Test
    public void passwordInitializerDefaultRoleNameMatchesConfiguredRole()
    {
        PasswordDefaultRoleInitializer initializer = new PasswordDefaultRoleInitializer(Map.of("role", "myrole", "password", "x"));
        assertThat(initializer.defaultRoleName()).isEqualTo("myrole");
    }

    @Test
    public void mutualTlsInitializerRejectsUnsupportedParameter()
    {
        assertThatThrownBy(() -> new MutualTlsDefaultRoleInitializer(Map.of("bogus", "x")))
            .isInstanceOf(ConfigurationException.class)
            .hasMessageContaining("Unsupported parameter");
    }

    @Test
    public void mutualTlsInitializerRejectsMissingRole()
    {
        assertThatThrownBy(() -> new MutualTlsDefaultRoleInitializer(Map.of("identity", "spiffe1")).validateConfiguration())
            .isInstanceOf(ConfigurationException.class)
            .hasMessageContaining("role");
    }

    @Test
    public void mutualTlsInitializerRejectsMissingIdentity()
    {
        assertThatThrownBy(() -> new MutualTlsDefaultRoleInitializer(Map.of("role", "cassandra")).validateConfiguration())
            .isInstanceOf(ConfigurationException.class)
            .hasMessageContaining("identity");
    }

    @Test
    public void mutualTlsInitializerRejectsEmptyRoleAndIdentity()
    {
        assertThatThrownBy(() -> new MutualTlsDefaultRoleInitializer(Map.of("role", "", "identity", "")).validateConfiguration())
            .isInstanceOf(ConfigurationException.class)
            .hasMessageContaining("role");
    }

    @Test
    public void mutualTlsInitializerDefaultRoleNameMatchesConfiguredRole()
    {
        MutualTlsDefaultRoleInitializer initializer = new MutualTlsDefaultRoleInitializer(Map.of("role", "cassandra", "identity", "spiffe1"));
        assertThat(initializer.defaultRoleName()).isEqualTo("cassandra");
    }

    @Test
    public void mutualTlsWritesIdentityMappingBeforeRole()
    {
        MutualTlsDefaultRoleInitializer initializer = new MutualTlsDefaultRoleInitializer(Map.of("role", "cassandra", "identity", "spiffe1"));
        List<String> statements = initializer.defaultRoleStatements();

        // The identity mapping must be written before the role row. hasExistingRoles() gates on the role, so writing
        // it last means a retry after a partial write re-drives both idempotent statements and heals the mapping.
        assertThat(statements).hasSize(2);
        assertThat(statements.get(0)).contains(AuthKeyspace.IDENTITY_TO_ROLES).contains("(identity, role)");
        assertThat(statements.get(1)).contains("(role, is_superuser, can_login)");
    }
}
