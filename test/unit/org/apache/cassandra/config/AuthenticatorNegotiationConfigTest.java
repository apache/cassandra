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

package org.apache.cassandra.config;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AuthenticatorNegotiationConfigTest
{
    @Test
    public void testLoadFullConfiguration()
    {
        Map<String, Object> yaml = Map.of(
            "authenticator_negotiation", Map.of(
                "enabled", true,
                "require_authentication", true,
                "default_authenticator", Map.of("class_name", "PasswordAuthenticator"),
                "authenticators", List.of(
                    Map.of(
                        "class_name", "MutualTlsAuthenticator",
                        "parameters", Map.of(
                            "validator_class_name", "org.apache.cassandra.auth.SpiffeCertificateValidator"
                        )
                    ),
                    Map.of("class_name", "PasswordAuthenticator"),
                    Map.of(
                        "class_name", "com.example.authn.CustomAuthenticator",
                        "parameters", Map.of("custom_param", "custom_value")
                    )
                )
            )
        );

        Config config = YamlConfigurationLoader.fromMap(yaml, Config.class);

        assertNotNull(config.authenticator_negotiation);
        assertTrue(config.authenticator_negotiation.enabled);
        assertTrue(config.authenticator_negotiation.require_authentication);
        
        assertNotNull(config.authenticator_negotiation.default_authenticator);
        assertEquals("PasswordAuthenticator", config.authenticator_negotiation.default_authenticator.class_name);
        
        assertEquals(3, config.authenticator_negotiation.authenticators.size());

        // Order of authenticators must be preserved.
        assertThat(config.authenticator_negotiation.authenticators)
            .extracting(pc -> pc.class_name)
            .containsExactly("MutualTlsAuthenticator",
                             "PasswordAuthenticator",
                             "com.example.authn.CustomAuthenticator");

        // Validate expected parameters for each authenticator
        ParameterizedClass mtls = config.authenticator_negotiation.authenticators.get(0);
        assertEquals("org.apache.cassandra.auth.SpiffeCertificateValidator",
                     mtls.parameters.get("validator_class_name"));

        ParameterizedClass password = config.authenticator_negotiation.authenticators.get(1);
        assertNull(password.parameters);

        ParameterizedClass custom = config.authenticator_negotiation.authenticators.get(2);
        assertEquals("custom_value", custom.parameters.get("custom_param"));
    }

    @Test
    public void testDefaultsWhenNotConfigured()
    {
        Config config = YamlConfigurationLoader.fromMap(Collections.emptyMap(), Config.class);

        assertNotNull(config.authenticator_negotiation);
        assertFalse(config.authenticator_negotiation.enabled);
        assertTrue(config.authenticator_negotiation.require_authentication);
        assertNull(config.authenticator_negotiation.default_authenticator);
        assertNotNull(config.authenticator_negotiation.authenticators);
        assertTrue(config.authenticator_negotiation.authenticators.isEmpty());
    }


    @Test
    public void testPartialConfiguration()
    {
        Map<String, Object> yaml = ImmutableMap.of(
            "authenticator_negotiation", ImmutableMap.of(
                "enabled", true,
                "default_authenticator", ImmutableMap.of("class_name", "PasswordAuthenticator")
            )
        );

        Config config = YamlConfigurationLoader.fromMap(yaml, Config.class);

        assertNotNull(config.authenticator_negotiation);
        assertTrue(config.authenticator_negotiation.enabled);
        assertTrue(config.authenticator_negotiation.require_authentication);
        assertNotNull(config.authenticator_negotiation.default_authenticator);
        assertEquals("PasswordAuthenticator", config.authenticator_negotiation.default_authenticator.class_name);
        assertNotNull(config.authenticator_negotiation.authenticators);
        assertTrue(config.authenticator_negotiation.authenticators.isEmpty());
    }

    @Test
    public void testLoadsAuthenticatorsWhenNegotiationDisabled()
    {
        Map<String, Object> yaml = ImmutableMap.of(
            "authenticator_negotiation", ImmutableMap.of(
                "enabled", false,
                "default_authenticator", ImmutableMap.of("class_name", "PasswordAuthenticator"),
                "authenticators", ImmutableList.of(
                    ImmutableMap.of("class_name", "PasswordAuthenticator")
                )
            )
        );

        Config config = YamlConfigurationLoader.fromMap(yaml, Config.class);

        assertFalse(config.authenticator_negotiation.enabled);
        assertNotNull(config.authenticator_negotiation.default_authenticator);
        assertEquals("PasswordAuthenticator", config.authenticator_negotiation.default_authenticator.class_name);
        assertEquals(1, config.authenticator_negotiation.authenticators.size());
    }

    @Test
    public void testEmptyAuthenticatorsList()
    {
        Map<String, Object> yaml = ImmutableMap.of(
            "authenticator_negotiation", ImmutableMap.of(
                "enabled", true,
                "authenticators", Collections.emptyList()
            )
        );

        Config config = YamlConfigurationLoader.fromMap(yaml, Config.class);

        assertTrue(config.authenticator_negotiation.enabled);
        assertTrue(config.authenticator_negotiation.authenticators.isEmpty());
    }

    @Test
    public void testDefaultAuthenticatorWithParameters()
    {
        Map<String, Object> yaml = ImmutableMap.of(
            "authenticator_negotiation", ImmutableMap.of(
                "enabled", true,
                "default_authenticator", ImmutableMap.of(
                    "class_name", "MutualTlsAuthenticator",
                    "parameters", ImmutableMap.of(
                        "validator_class_name", "org.apache.cassandra.auth.SpiffeCertificateValidator"
                    )
                ),
                "authenticators", ImmutableList.of(
                    ImmutableMap.of("class_name", "PasswordAuthenticator")
                )
            )
        );

        Config config = YamlConfigurationLoader.fromMap(yaml, Config.class);

        assertNotNull(config.authenticator_negotiation.default_authenticator);
        assertEquals("MutualTlsAuthenticator", config.authenticator_negotiation.default_authenticator.class_name);
        assertNotNull(config.authenticator_negotiation.default_authenticator.parameters);
        assertEquals("org.apache.cassandra.auth.SpiffeCertificateValidator",
                     config.authenticator_negotiation.default_authenticator.parameters.get("validator_class_name"));
    }

    @Test
    public void testUpdateInPlace()
    {
        Config config = new Config();

        assertFalse(config.authenticator_negotiation.enabled);
        assertTrue(config.authenticator_negotiation.require_authentication);

        // Update to negate defaults.
        Map<String, Object> yaml = ImmutableMap.of(
            "authenticator_negotiation.enabled", true,
            "authenticator_negotiation.require_authentication", false
        );

        Config updated = YamlConfigurationLoader.updateFromMap(yaml, true, config);

        assertThat(updated).isSameAs(config);
        assertTrue(config.authenticator_negotiation.enabled);
        assertFalse(config.authenticator_negotiation.require_authentication);
    }
}
